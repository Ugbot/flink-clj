(ns flink-clj.cli.core
  "flink-clj CLI entry point.

  A complete build and project scaffolding CLI for Apache Flink with Clojure.

  Usage:
    flink-clj <command> [options]

  Commands:
    new       Create a new flink-clj project
    repl      Start interactive REPL
    run       Run a job locally
    build     Build an uberjar
    deploy    Deploy to a Flink cluster
    jobs      List/manage cluster jobs
    ververica Manage Ververica Cloud deployments
    help      Show help"
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str])
  (:gen-class))

;; =============================================================================
;; Version and Configuration
;; =============================================================================

(def version "0.1.0")

(def cli-options
  [["-h" "--help" "Show help"]
   ["-v" "--version" "Show version"]
   ["-V" "--verbose" "Verbose output"]])

;; =============================================================================
;; Command Registry
;; =============================================================================

(defn- lazy-require [sym]
  "Lazily require a namespace and return the var."
  (require (symbol (namespace sym)))
  (resolve sym))

(def commands
  {"new"       {:desc "Create a new flink-clj project"
                :fn 'flink-clj.cli.commands.new/run}
   "repl"      {:desc "Start interactive REPL"
                :fn 'flink-clj.cli.commands.repl/run}
   "run"       {:desc "Run a job locally"
                :fn 'flink-clj.cli.commands.run/run}
   "build"     {:desc "Build an uberjar"
                :fn 'flink-clj.cli.commands.build/run}
   "deploy"    {:desc "Deploy to a Flink cluster"
                :fn 'flink-clj.cli.commands.deploy/run}
   "jobs"      {:desc "List/manage cluster jobs"
                :fn 'flink-clj.cli.commands.jobs/run}
   "ververica" {:desc "Manage Ververica Cloud deployments"
                :fn 'flink-clj.cli.commands.ververica/run}
   "vvc"       {:desc "Manage Ververica Cloud (alias)"
                :fn 'flink-clj.cli.commands.ververica/run}})

;; =============================================================================
;; Help and Usage
;; =============================================================================

(defn- print-version []
  (println (format "flink-clj %s" version))
  (println)
  (println "Clojure CLI for Apache Flink"))

(defn- print-usage []
  (println "flink-clj - Clojure CLI for Apache Flink")
  (println)
  (println "Usage:")
  (println "  flink-clj <command> [options]")
  (println)
  (println "Commands:")
  (doseq [[cmd {:keys [desc]}] (sort-by key commands)]
    (println (format "  %-10s %s" cmd desc)))
  (println "  help       Show help for a command")
  (println)
  (println "Options:")
  (println "  -h, --help     Show this help")
  (println "  -v, --version  Show version")
  (println "  -V, --verbose  Verbose output")
  (println)
  (println "Examples:")
  (println "  flink-clj new my-pipeline --template etl")
  (println "  flink-clj repl --web-ui")
  (println "  flink-clj build --flink-version 1.20")
  (println "  flink-clj deploy target/my-job.jar --target k8s")
  (println)
  (println "Ververica Cloud:")
  (println "  flink-clj ververica login")
  (println "  flink-clj ververica workspace list")
  (println "  flink-clj ververica deployment create --name my-job --jar target/job.jar")
  (println "  flink-clj ververica job start --deployment <id>")
  (println)
  (println "Run 'flink-clj <command> --help' for more information on a command."))

(defn- print-command-help [cmd]
  (if-let [command (get commands cmd)]
    (do
      (println (format "flink-clj %s - %s" cmd (:desc command)))
      (println)
      (println (format "Run 'flink-clj %s --help' for detailed options." cmd)))
    (do
      (println (format "Unknown command: %s" cmd))
      (println)
      (print-usage))))

(defn- help-cmd [args]
  (if-let [cmd (first args)]
    (print-command-help cmd)
    (print-usage)))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(defn execute-command
  "Execute a CLI command."
  [cmd args]
  (if-let [command (get commands cmd)]
    (let [cmd-fn (lazy-require (:fn command))]
      (cmd-fn args))
    (do
      (println (format "Unknown command: %s" cmd))
      (println)
      (print-usage)
      (System/exit 1))))

(defn -main
  "Main entry point for flink-clj CLI."
  [& args]
  (let [{:keys [options arguments errors]} (parse-opts args cli-options :in-order true)
        [cmd & cmd-args] arguments]
    (try
      (cond
        errors
        (do
          (doseq [e errors]
            (println (format "Error: %s" e)))
          (System/exit 1))

        (:help options)
        (print-usage)

        (:version options)
        (print-version)

        (nil? cmd)
        (print-usage)

        (= cmd "help")
        (help-cmd cmd-args)

        :else
        (execute-command cmd cmd-args))
      (catch Exception e
        (binding [*out* *err*]
          (println (format "Error: %s" (.getMessage e)))
          (when (:verbose options)
            (.printStackTrace e)))
        (System/exit 1)))))
