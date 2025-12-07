(ns flink-clj.cli.commands.deploy
  "Deploy jobs to Flink clusters.

  Usage:
    flink-clj deploy <jar-path> [options]

  Deployment targets:
    local      - Run in local embedded cluster
    standalone - Deploy to standalone Flink cluster
    k8s        - Deploy to Kubernetes using Flink Operator"
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [flink-clj.cli.deploy.local :as local]
            [flink-clj.cli.deploy.standalone :as standalone]
            [flink-clj.cli.deploy.kubernetes :as k8s])
  (:import [java.io File]))

;; =============================================================================
;; CLI Options
;; =============================================================================

(def cli-options
  [["-t" "--target TARGET" "Deployment target (local, standalone, k8s)"
    :default "local"
    :validate [#(contains? #{"local" "standalone" "k8s" "kubernetes"} %)
               "Must be: local, standalone, or k8s"]]
   ["-H" "--host HOST" "Flink cluster host (standalone mode)"
    :default "localhost"]
   ["-p" "--port PORT" "Flink cluster REST port"
    :default 8081
    :parse-fn #(Integer/parseInt %)]
   ["-P" "--parallelism N" "Job parallelism"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-m" "--main CLASS" "Main class to run"]
   ["-n" "--name NAME" "Deployment/job name"]
   ["-N" "--namespace NS" "Kubernetes namespace"
    :default "default"]
   ["-i" "--image IMAGE" "Flink Docker image (k8s)"
    :default "flink:1.20"]
   ["-s" "--savepoint PATH" "Resume from savepoint"]
   ["-a" "--args ARGS" "Arguments to pass to job"
    :default ""]
   ["-h" "--help" "Show help"]])

;; =============================================================================
;; Help
;; =============================================================================

(defn- print-help [summary]
  (println "flink-clj deploy - Deploy jobs to Flink clusters")
  (println)
  (println "Usage:")
  (println "  flink-clj deploy <jar-path> [options]")
  (println)
  (println "Deployment Targets:")
  (println "  local      Run in local embedded Flink cluster")
  (println "  standalone Deploy to standalone Flink cluster via REST API")
  (println "  k8s        Deploy to Kubernetes using Flink Operator")
  (println)
  (println "Options:")
  (println summary)
  (println)
  (println "Examples:")
  (println "  # Local execution")
  (println "  flink-clj deploy target/my-job.jar")
  (println)
  (println "  # Standalone cluster")
  (println "  flink-clj deploy target/my-job.jar --target standalone --host flink-cluster")
  (println)
  (println "  # Kubernetes")
  (println "  flink-clj deploy target/my-job.jar --target k8s --name my-pipeline")
  (println "  flink-clj deploy target/my-job.jar --target k8s --namespace production"))

;; =============================================================================
;; Deployment
;; =============================================================================

(defn- find-jar
  "Find JAR file to deploy."
  [jar-path]
  (cond
    ;; Explicit path provided
    jar-path
    (let [f (io/file jar-path)]
      (if (.exists f)
        f
        (do
          (println (format "Error: JAR file not found: %s" jar-path))
          (System/exit 1))))

    ;; Look in target directory
    (.exists (io/file "target"))
    (let [jars (->> (.listFiles (io/file "target"))
                    (filter #(and (.isFile %)
                                  (or (clojure.string/ends-with? (.getName %) "-standalone.jar")
                                      (clojure.string/ends-with? (.getName %) ".jar"))))
                    (sort-by #(.lastModified %) >))]
      (if-let [jar (first jars)]
        jar
        (do
          (println "Error: No JAR files found in target/")
          (println "       Run 'flink-clj build' first")
          (System/exit 1))))

    :else
    (do
      (println "Error: JAR file path required")
      (System/exit 1))))

(defn- deploy!
  "Deploy JAR to specified target."
  [jar-path {:keys [target] :as opts}]
  (let [jar (find-jar jar-path)]
    (println (format "Deploying: %s" (.getAbsolutePath jar)))
    (println (format "Target: %s" target))
    (println)

    (case target
      "local"
      (local/deploy! jar opts)

      "standalone"
      (standalone/deploy! jar opts)

      ("k8s" "kubernetes")
      (k8s/deploy! jar opts))))

;; =============================================================================
;; Command Entry Point
;; =============================================================================

(defn run
  "Execute the 'deploy' command."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        jar-path (first arguments)]
    (cond
      (:help options)
      (print-help summary)

      errors
      (do
        (doseq [e errors]
          (println (format "Error: %s" e)))
        (System/exit 1))

      :else
      (deploy! jar-path options))))
