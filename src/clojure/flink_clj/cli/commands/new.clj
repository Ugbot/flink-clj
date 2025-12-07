(ns flink-clj.cli.commands.new
  "Create new flink-clj projects from templates.

  Usage:
    flink-clj new <project-name> [options]

  Templates:
    etl          - ETL Pipeline (extract, transform, load)
    analytics    - Stream Analytics (real-time aggregations)
    cdc          - CDC Processor (change data capture)
    event-driven - Event-Driven (pattern matching, CEP)"
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [flink-clj.cli.templates.common :as tmpl])
  (:import [java.io File]))

;; =============================================================================
;; CLI Options
;; =============================================================================

(def cli-options
  [["-t" "--template TEMPLATE" "Project template (etl, analytics, cdc, event-driven)"
    :default "etl"
    :validate [#(contains? #{"etl" "analytics" "cdc" "event-driven"} %)
               "Must be: etl, analytics, cdc, or event-driven"]]
   ["-f" "--flink-version VERSION" "Flink version (1.20 or 2.x)"
    :default "1.20"
    :validate [#(contains? #{"1.20" "2.x"} %)
               "Must be: 1.20 or 2.x"]]
   ["-o" "--output DIR" "Output directory"
    :default "."]
   ["-d" "--deps-edn" "Use deps.edn instead of project.clj"
    :default false]
   ["-h" "--help" "Show help"]])

;; =============================================================================
;; Help
;; =============================================================================

(defn- print-help [summary]
  (println "flink-clj new - Create a new flink-clj project")
  (println)
  (println "Usage:")
  (println "  flink-clj new <project-name> [options]")
  (println)
  (println "Templates:")
  (println "  etl          ETL Pipeline - extract, transform, load workflows")
  (println "  analytics    Stream Analytics - real-time aggregations and metrics")
  (println "  cdc          CDC Processor - change data capture from databases")
  (println "  event-driven Event-Driven - pattern matching and CEP")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  flink-clj new my-pipeline")
  (println "  flink-clj new my-pipeline --template analytics")
  (println "  flink-clj new my-pipeline --template cdc --flink-version 2.x")
  (println "  flink-clj new my-pipeline --deps-edn"))

;; =============================================================================
;; Project Creation
;; =============================================================================

(defn- sanitize-name
  "Convert project name to valid Clojure namespace."
  [name]
  (-> name
      (str/replace #"[^a-zA-Z0-9_-]" "-")
      (str/replace #"-+" "-")
      (str/replace #"^-|-$" "")))

(defn- name->namespace
  "Convert project name to namespace format."
  [name]
  (-> name
      sanitize-name
      (str/replace "-" "_")))

(defn- create-project!
  "Create a new flink-clj project from template."
  [project-name {:keys [template flink-version output deps-edn]}]
  (let [sanitized-name (sanitize-name project-name)
        namespace (name->namespace project-name)
        project-dir (io/file output sanitized-name)]

    (when (.exists project-dir)
      (println (format "Error: Directory '%s' already exists" sanitized-name))
      (System/exit 1))

    (println (format "Creating %s project: %s" template sanitized-name))
    (println)

    ;; Create directory structure
    (.mkdirs project-dir)
    (.mkdirs (io/file project-dir "src" namespace))
    (.mkdirs (io/file project-dir "test" namespace))
    (.mkdirs (io/file project-dir "resources"))
    (.mkdirs (io/file project-dir "dev"))

    ;; Generate project files
    (tmpl/generate-project! project-dir
                            {:name sanitized-name
                             :namespace namespace
                             :template (keyword template)
                             :flink-version flink-version
                             :deps-edn? deps-edn})

    (println "Project created successfully!")
    (println)
    (println "Next steps:")
    (println (format "  cd %s" sanitized-name))
    (if deps-edn
      (do
        (println "  clj -M:dev          # Start REPL")
        (println "  clj -M:run          # Run job"))
      (do
        (println "  lein deps           # Download dependencies")
        (println "  lein repl           # Start REPL")
        (println "  lein run            # Run job")))
    (println)
    (println "Or use flink-clj commands:")
    (println (format "  flink-clj repl      # Interactive REPL"))
    (println (format "  flink-clj build     # Build uberjar"))
    (println (format "  flink-clj deploy    # Deploy to cluster"))))

;; =============================================================================
;; Command Entry Point
;; =============================================================================

(defn run
  "Execute the 'new' command."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        project-name (first arguments)]
    (cond
      (:help options)
      (print-help summary)

      errors
      (do
        (doseq [e errors]
          (println (format "Error: %s" e)))
        (System/exit 1))

      (nil? project-name)
      (do
        (println "Error: Project name required")
        (println)
        (print-help summary)
        (System/exit 1))

      :else
      (create-project! project-name options))))
