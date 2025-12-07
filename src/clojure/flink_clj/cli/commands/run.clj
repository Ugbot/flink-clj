(ns flink-clj.cli.commands.run
  "Run a flink-clj job locally.

  Usage:
    flink-clj run [options]
    flink-clj run <main-class> [options]

  Executes a Flink job in a local embedded cluster."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]))

;; =============================================================================
;; CLI Options
;; =============================================================================

(def cli-options
  [["-m" "--main CLASS" "Main class or namespace to run"]
   ["-j" "--jar PATH" "JAR file to run (if not in project)"]
   ["-p" "--parallelism N" "Default parallelism"
    :default 2
    :parse-fn #(Integer/parseInt %)]
   ["-w" "--web-ui" "Enable Flink Web UI"
    :default false]
   ["-P" "--port PORT" "Web UI port"
    :default 8081
    :parse-fn #(Integer/parseInt %)]
   ["-f" "--flink-version VERSION" "Flink version (1.20 or 2.x)"
    :default "1.20"]
   ["-a" "--args ARGS" "Arguments to pass to the job"
    :default ""]
   ["-h" "--help" "Show help"]])

;; =============================================================================
;; Help
;; =============================================================================

(defn- print-help [summary]
  (println "flink-clj run - Run a flink-clj job locally")
  (println)
  (println "Usage:")
  (println "  flink-clj run [options]")
  (println "  flink-clj run --main my.namespace.job")
  (println "  flink-clj run --jar target/my-job.jar")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  flink-clj run                           # Run default main")
  (println "  flink-clj run --main my.job             # Run specific namespace")
  (println "  flink-clj run --web-ui                  # With Web UI")
  (println "  flink-clj run --parallelism 4           # Custom parallelism")
  (println "  flink-clj run --jar target/job.jar      # Run from JAR"))

;; =============================================================================
;; Project Detection
;; =============================================================================

(defn- find-main-class
  "Find main class from project configuration."
  []
  (cond
    (.exists (io/file "project.clj"))
    (let [content (slurp "project.clj")]
      (when-let [match (re-find #":main\s+([^\s\)]+)" content)]
        (second match)))

    (.exists (io/file "deps.edn"))
    (let [content (slurp "deps.edn")]
      (when-let [match (re-find #":main-opts\s+\[\"-m\"\s+\"([^\"]+)\"" content)]
        (second match)))

    :else nil))

(defn- project-type
  "Detect project type."
  []
  (cond
    (.exists (io/file "project.clj")) :leiningen
    (.exists (io/file "deps.edn")) :deps-edn
    :else nil))

;; =============================================================================
;; Job Execution
;; =============================================================================

(defn- run-with-lein!
  "Run job using Leiningen."
  [{:keys [main flink-version parallelism web-ui port args]}]
  (let [profile (if (= flink-version "2.x") "+flink-2.x,+dev" "+flink-1.20,+dev")
        env-vars (cond-> {"PARALLELISM" (str parallelism)}
                   web-ui (assoc "FLINK_WEB_UI" "true"
                                 "FLINK_WEB_UI_PORT" (str port)))
        cmd (if main
              ["lein" "with-profile" profile "run" "-m" main]
              ["lein" "with-profile" profile "run"])]
    (println (format "Running job with Leiningen (Flink %s)..." flink-version))
    (when web-ui
      (println (format "Web UI: http://localhost:%d" port)))
    (println)
    (let [result (apply sh (concat cmd [:env (merge (System/getenv) env-vars)]))]
      (println (:out result))
      (when-not (str/blank? (:err result))
        (binding [*out* *err*]
          (println (:err result))))
      (System/exit (:exit result)))))

(defn- run-with-clj!
  "Run job using Clojure CLI."
  [{:keys [main flink-version parallelism web-ui port args]}]
  (let [aliases (if (= flink-version "2.x") "-M:dev:flink-2.x:run" "-M:dev:flink:run")
        env-vars (cond-> {"PARALLELISM" (str parallelism)}
                   web-ui (assoc "FLINK_WEB_UI" "true"
                                 "FLINK_WEB_UI_PORT" (str port)))
        cmd (if main
              ["clj" aliases "-m" main]
              ["clj" aliases])]
    (println (format "Running job with Clojure CLI (Flink %s)..." flink-version))
    (when web-ui
      (println (format "Web UI: http://localhost:%d" port)))
    (println)
    (let [result (apply sh (concat cmd [:env (merge (System/getenv) env-vars)]))]
      (println (:out result))
      (when-not (str/blank? (:err result))
        (binding [*out* *err*]
          (println (:err result))))
      (System/exit (:exit result)))))

(defn- run-jar!
  "Run job from JAR file."
  [{:keys [jar main parallelism web-ui port args]}]
  (when-not (.exists (io/file jar))
    (println (format "Error: JAR file not found: %s" jar))
    (System/exit 1))

  (let [java-args (cond-> ["-jar" jar]
                    main (concat ["-m" main])
                    (not (str/blank? args)) (concat (str/split args #"\s+")))
        env-vars (cond-> {"PARALLELISM" (str parallelism)}
                   web-ui (assoc "FLINK_WEB_UI" "true"
                                 "FLINK_WEB_UI_PORT" (str port)))]
    (println (format "Running job from JAR: %s" jar))
    (when web-ui
      (println (format "Web UI: http://localhost:%d" port)))
    (println)
    (let [result (apply sh "java" (concat java-args [:env (merge (System/getenv) env-vars)]))]
      (println (:out result))
      (when-not (str/blank? (:err result))
        (binding [*out* *err*]
          (println (:err result))))
      (System/exit (:exit result)))))

;; =============================================================================
;; Command Entry Point
;; =============================================================================

(defn run
  "Execute the 'run' command."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        main-from-args (first arguments)
        main-class (or (:main options) main-from-args (find-main-class))]
    (cond
      (:help options)
      (print-help summary)

      errors
      (do
        (doseq [e errors]
          (println (format "Error: %s" e)))
        (System/exit 1))

      (:jar options)
      (run-jar! (assoc options :main main-class))

      :else
      (let [opts (assoc options :main main-class)]
        (case (project-type)
          :leiningen (run-with-lein! opts)
          :deps-edn (run-with-clj! opts)
          (do
            (println "Error: Not in a flink-clj project directory")
            (println "       Run from a project or use --jar to specify a JAR file")
            (System/exit 1)))))))
