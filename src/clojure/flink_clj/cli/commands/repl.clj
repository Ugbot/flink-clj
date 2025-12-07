(ns flink-clj.cli.commands.repl
  "Start an interactive flink-clj REPL.

  Usage:
    flink-clj repl [options]

  Launches a Clojure REPL with flink-clj pre-configured."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]))

;; =============================================================================
;; CLI Options
;; =============================================================================

(def cli-options
  [["-w" "--web-ui" "Enable Flink Web UI"
    :default false]
   ["-p" "--port PORT" "Web UI port"
    :default 8081
    :parse-fn #(Integer/parseInt %)]
   ["-m" "--mode MODE" "Execution mode (local or remote)"
    :default "local"
    :validate [#(contains? #{"local" "remote"} %)
               "Must be: local or remote"]]
   ["-H" "--host HOST" "Remote cluster host (for remote mode)"
    :default "localhost"]
   ["-P" "--parallelism N" "Default parallelism"
    :default 2
    :parse-fn #(Integer/parseInt %)]
   ["-f" "--flink-version VERSION" "Flink version (1.20 or 2.x)"
    :default "1.20"]
   ["-h" "--help" "Show help"]])

;; =============================================================================
;; Help
;; =============================================================================

(defn- print-help [summary]
  (println "flink-clj repl - Start an interactive flink-clj REPL")
  (println)
  (println "Usage:")
  (println "  flink-clj repl [options]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  flink-clj repl                    # Local mode")
  (println "  flink-clj repl --web-ui           # With Web UI at :8081")
  (println "  flink-clj repl --web-ui --port 8082")
  (println "  flink-clj repl --mode remote --host flink-cluster")
  (println)
  (println "In the REPL:")
  (println "  (start!)                          # Initialize Flink")
  (println "  (start! {:web-ui true})           # With Web UI")
  (println "  env                               # StreamExecutionEnvironment")
  (println "  table-env                         # TableEnvironment")
  (println "  (collect stream)                  # Execute and collect results")
  (println "  (stop!)                           # Cleanup"))

;; =============================================================================
;; Project Detection
;; =============================================================================

(defn- project-type
  "Detect project type based on build file present."
  []
  (cond
    (.exists (io/file "project.clj")) :leiningen
    (.exists (io/file "deps.edn")) :deps-edn
    :else nil))

(defn- in-flink-clj-project?
  "Check if we're in the flink-clj library project itself."
  []
  (and (.exists (io/file "project.clj"))
       (let [content (slurp "project.clj")]
         (str/includes? content "flink-clj"))))

;; =============================================================================
;; REPL Startup
;; =============================================================================

(defn- build-init-forms
  "Build Clojure forms to evaluate on REPL startup."
  [{:keys [web-ui port mode host parallelism]}]
  (let [shell-opts (cond-> {}
                     (= mode "remote") (assoc :mode :remote :host host :port port)
                     (= mode "local") (assoc :mode :local)
                     web-ui (assoc :web-ui true :web-ui-port port)
                     parallelism (assoc :parallelism parallelism))]
    (str "(do "
         "(require '[flink-clj.shell :as shell]) "
         "(require '[flink-clj.dsl :as f]) "
         "(println) "
         "(println \"=== flink-clj REPL ===\") "
         "(println \"(start!) to initialize Flink\") "
         (when (or web-ui (= mode "remote"))
           (format "(println \"Starting with: %s\") " (pr-str shell-opts)))
         "(println) "
         ")")))

(defn- start-lein-repl!
  "Start REPL using Leiningen."
  [{:keys [flink-version] :as opts}]
  (let [profile (if (= flink-version "2.x") "+flink-2.x,+dev" "+flink-1.20,+dev")
        init-forms (build-init-forms opts)]
    (println (format "Starting Leiningen REPL (Flink %s)..." flink-version))
    (let [result (sh "lein" "with-profile" profile "repl"
                     :in (str init-forms "\n"))]
      (when-not (zero? (:exit result))
        (println "REPL exited with errors:")
        (println (:err result))))))

(defn- start-deps-repl!
  "Start REPL using deps.edn/Clojure CLI."
  [{:keys [flink-version] :as opts}]
  (let [aliases (if (= flink-version "2.x") "-M:dev:flink-2.x" "-M:dev:flink")
        init-forms (build-init-forms opts)]
    (println (format "Starting Clojure CLI REPL (Flink %s)..." flink-version))
    (let [result (sh "clj" aliases "-e" init-forms "-r")]
      (when-not (zero? (:exit result))
        (println "REPL exited with errors:")
        (println (:err result))))))

(defn- start-embedded-repl!
  "Start embedded REPL when not in a project directory."
  [opts]
  (println "Starting embedded flink-clj REPL...")
  (println)
  (println "Note: For full functionality, run from within a flink-clj project")
  (println "      or create one with: flink-clj new my-project")
  (println)
  ;; Attempt to start nREPL or fall back to basic REPL
  (require '[flink-clj.shell :as shell])
  (require '[flink-clj.dsl :as f])
  (println "=== flink-clj REPL ===")
  (println "(start!) to initialize Flink")
  (println)
  ;; Start basic Clojure REPL
  (clojure.main/repl))

;; =============================================================================
;; Command Entry Point
;; =============================================================================

(defn run
  "Execute the 'repl' command."
  [args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
      (print-help summary)

      errors
      (do
        (doseq [e errors]
          (println (format "Error: %s" e)))
        (System/exit 1))

      :else
      (case (project-type)
        :leiningen (start-lein-repl! options)
        :deps-edn (start-deps-repl! options)
        (start-embedded-repl! options)))))
