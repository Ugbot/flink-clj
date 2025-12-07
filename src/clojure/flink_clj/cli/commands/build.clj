(ns flink-clj.cli.commands.build
  "Build uberjar for Flink deployment.

  Usage:
    flink-clj build [options]

  Creates a standalone JAR file containing all dependencies."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]))

;; =============================================================================
;; CLI Options
;; =============================================================================

(def cli-options
  [["-f" "--flink-version VERSION" "Flink version (1.20 or 2.x)"
    :default "1.20"
    :validate [#(contains? #{"1.20" "2.x"} %)
               "Must be: 1.20 or 2.x"]]
   ["-o" "--output PATH" "Output JAR path"]
   ["-c" "--clean" "Clean before building"
    :default true]
   ["-h" "--help" "Show help"]])

;; =============================================================================
;; Help
;; =============================================================================

(defn- print-help [summary]
  (println "flink-clj build - Build uberjar for Flink deployment")
  (println)
  (println "Usage:")
  (println "  flink-clj build [options]")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  flink-clj build                       # Build with defaults")
  (println "  flink-clj build --flink-version 2.x   # Build for Flink 2.x")
  (println "  flink-clj build --output my-job.jar   # Custom output path")
  (println "  flink-clj build --no-clean            # Skip clean step"))

;; =============================================================================
;; Project Detection
;; =============================================================================

(defn- project-type
  "Detect project type."
  []
  (cond
    (.exists (io/file "project.clj")) :leiningen
    (.exists (io/file "deps.edn")) :deps-edn
    :else nil))

(defn- find-project-name
  "Find project name from configuration."
  []
  (cond
    (.exists (io/file "project.clj"))
    (let [content (slurp "project.clj")]
      (when-let [match (re-find #"\(defproject\s+([^\s]+)" content)]
        (-> (second match)
            (str/replace #".*/" "")
            (str/replace #"\"" ""))))

    (.exists (io/file "deps.edn"))
    (.getName (io/file "."))

    :else "flink-job"))

;; =============================================================================
;; Build
;; =============================================================================

(defn- build-with-lein!
  "Build uberjar using Leiningen."
  [{:keys [flink-version clean output]}]
  (let [profile (if (= flink-version "2.x") "+flink-2.x,+uberjar" "+flink-1.20,+uberjar")]
    (println (format "Building uberjar for Flink %s..." flink-version))
    (println)

    ;; Clean if requested
    (when clean
      (println "Cleaning...")
      (let [result (sh "lein" "clean")]
        (when-not (zero? (:exit result))
          (println "Warning: clean failed")
          (println (:err result)))))

    ;; Build
    (println "Compiling...")
    (let [result (sh "lein" "with-profile" profile "uberjar")]
      (if (zero? (:exit result))
        (do
          (println)
          (println "Build successful!")
          (println)
          ;; Find the output JAR
          (let [target-dir (io/file "target")
                jars (->> (.listFiles target-dir)
                          (filter #(and (.isFile %)
                                        (str/ends-with? (.getName %) "-standalone.jar")))
                          (sort-by #(.lastModified %) >))]
            (when-let [jar (first jars)]
              (let [output-path (or output (.getAbsolutePath jar))]
                (when (and output (not= output (.getAbsolutePath jar)))
                  (io/copy jar (io/file output)))
                (println (format "Output: %s" output-path))
                (println (format "Size: %.2f MB" (/ (.length jar) 1024.0 1024.0)))))))
        (do
          (println "Build failed!")
          (println)
          (println (:out result))
          (println (:err result))
          (System/exit 1))))))

(defn- build-with-clj!
  "Build uberjar using Clojure CLI with depstar."
  [{:keys [flink-version clean output]}]
  (let [project-name (find-project-name)
        jar-name (or output (format "target/%s.jar" project-name))]
    (println (format "Building uberjar for Flink %s..." flink-version))
    (println)

    ;; Clean if requested
    (when clean
      (println "Cleaning...")
      (let [target-dir (io/file "target")]
        (when (.exists target-dir)
          (doseq [f (file-seq target-dir)]
            (when (.isFile f)
              (.delete f))))))

    ;; Build using depstar (if available) or tools.build
    (println "Compiling...")
    (let [aliases (if (= flink-version "2.x") "-X:flink-2.x:uberjar" "-X:flink:uberjar")
          result (sh "clj" aliases
                     ":jar" (pr-str jar-name)
                     ":aot" "true")]
      (if (zero? (:exit result))
        (do
          (println)
          (println "Build successful!")
          (println)
          (let [jar (io/file jar-name)]
            (when (.exists jar)
              (println (format "Output: %s" (.getAbsolutePath jar)))
              (println (format "Size: %.2f MB" (/ (.length jar) 1024.0 1024.0))))))
        (do
          (println "Build failed!")
          (println)
          (println "Note: Make sure you have :uberjar alias configured in deps.edn")
          (println "      or use Leiningen for building uberjars.")
          (println)
          (println (:out result))
          (println (:err result))
          (System/exit 1))))))

;; =============================================================================
;; Command Entry Point
;; =============================================================================

(defn run
  "Execute the 'build' command."
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
        :leiningen (build-with-lein! options)
        :deps-edn (build-with-clj! options)
        (do
          (println "Error: Not in a flink-clj project directory")
          (println "       Run from a project directory with project.clj or deps.edn")
          (System/exit 1))))))
