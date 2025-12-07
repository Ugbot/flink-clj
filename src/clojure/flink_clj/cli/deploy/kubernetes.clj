(ns flink-clj.cli.deploy.kubernetes
  "Deploy Flink jobs to Kubernetes using Flink Operator.

  This handler generates FlinkDeployment CRDs and applies them
  to the Kubernetes cluster."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.data.json :as json])
  (:import [java.io File]))

;; =============================================================================
;; Kubernetes Utilities
;; =============================================================================

(defn- kubectl-available?
  "Check if kubectl is available."
  []
  (let [{:keys [exit]} (sh "which" "kubectl")]
    (zero? exit)))

(defn- check-k8s-context
  "Get current Kubernetes context."
  []
  (let [{:keys [exit out err]} (sh "kubectl" "config" "current-context")]
    (if (zero? exit)
      (str/trim out)
      (do
        (println "Error: Cannot get Kubernetes context")
        (println "       Make sure kubectl is configured correctly")
        (System/exit 1)))))

(defn- namespace-exists?
  "Check if namespace exists."
  [namespace]
  (let [{:keys [exit]} (sh "kubectl" "get" "namespace" namespace)]
    (zero? exit)))

(defn- flink-operator-installed?
  "Check if Flink Kubernetes Operator is installed."
  []
  (let [{:keys [exit out]} (sh "kubectl" "get" "crd" "flinkdeployments.flink.apache.org"
                                "-o" "name" "--ignore-not-found")]
    (and (zero? exit)
         (not (str/blank? out)))))

;; =============================================================================
;; FlinkDeployment Template
;; =============================================================================

(defn- generate-flink-deployment
  "Generate FlinkDeployment CRD manifest."
  [{:keys [name namespace image jar-uri parallelism main savepoint args
           job-manager-memory task-manager-memory task-slots flink-version]}]
  (let [flink-ver (or flink-version "1.20")
        version-tag (str "v" (str/replace flink-ver "." "_"))]
    {:apiVersion "flink.apache.org/v1beta1"
     :kind "FlinkDeployment"
     :metadata {:name name
                :namespace namespace}
     :spec {:image (or image (format "flink:%s" flink-ver))
            :flinkVersion version-tag
            :flinkConfiguration {"taskmanager.numberOfTaskSlots" (str (or task-slots 2))
                                 "state.savepoints.dir" "file:///flink-data/savepoints"
                                 "state.checkpoints.dir" "file:///flink-data/checkpoints"}
            :serviceAccount "flink"
            :jobManager {:resource {:memory (or job-manager-memory "1024m")
                                    :cpu 0.5}}
            :taskManager {:resource {:memory (or task-manager-memory "1024m")
                                     :cpu 0.5}}
            :job (cond-> {:jarURI jar-uri
                          :parallelism (or parallelism 2)
                          :upgradeMode "stateless"}
                   main (assoc :entryClass main)
                   savepoint (assoc :initialSavepointPath savepoint)
                   (not (str/blank? args)) (assoc :args (str/split args #"\s+")))}}))

(defn- manifest->yaml
  "Convert manifest map to YAML string (simplified)."
  [manifest]
  (letfn [(yaml-value [v indent]
            (cond
              (map? v)
              (str "\n" (yaml-map v (+ indent 2)))

              (vector? v)
              (str "\n" (str/join "\n" (map #(str (apply str (repeat indent " "))
                                                   "- " (yaml-value % indent))
                                             v)))

              (string? v)
              (if (re-find #"[:\n]" v)
                (str "\"" v "\"")
                v)

              :else
              (str v)))

          (yaml-map [m indent]
            (str/join "\n"
              (for [[k v] m]
                (str (apply str (repeat indent " "))
                     (name k) ": "
                     (yaml-value v indent)))))]
    (yaml-map manifest 0)))

;; =============================================================================
;; Deployment
;; =============================================================================

(defn deploy!
  "Deploy JAR to Kubernetes using Flink Operator."
  [^File jar {:keys [name namespace image parallelism main savepoint args]}]
  ;; Validate prerequisites
  (when-not (kubectl-available?)
    (println "Error: kubectl not found in PATH")
    (System/exit 1))

  (let [context (check-k8s-context)]
    (println (format "Kubernetes context: %s" context))
    (println (format "Namespace: %s" namespace))
    (println))

  (when-not (namespace-exists? namespace)
    (println (format "Warning: Namespace '%s' does not exist" namespace))
    (println "         Creating namespace...")
    (let [{:keys [exit err]} (sh "kubectl" "create" "namespace" namespace)]
      (when-not (zero? exit)
        (println (format "Error creating namespace: %s" err))
        (System/exit 1))))

  (when-not (flink-operator-installed?)
    (println "Error: Flink Kubernetes Operator not installed")
    (println)
    (println "Install the operator with:")
    (println "  helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/")
    (println "  helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator")
    (System/exit 1))

  ;; Generate deployment name if not provided
  (let [deployment-name (or name
                            (-> (.getName jar)
                                (str/replace #"\.jar$" "")
                                (str/replace #"[^a-zA-Z0-9-]" "-")
                                (str/lower-case)))
        ;; For K8s, JAR needs to be accessible. Using local:// assumes
        ;; the JAR is in the container's classpath. For production,
        ;; this would typically be a URL to an artifact server.
        jar-uri (format "local:///opt/flink/usrlib/%s" (.getName jar))

        manifest (generate-flink-deployment
                   {:name deployment-name
                    :namespace namespace
                    :image image
                    :jar-uri jar-uri
                    :parallelism parallelism
                    :main main
                    :savepoint savepoint
                    :args args})

        yaml-content (manifest->yaml manifest)
        yaml-file (io/file (str deployment-name "-deployment.yaml"))]

    (println "Generated FlinkDeployment manifest:")
    (println (format "  File: %s" (.getName yaml-file)))
    (println)

    ;; Write YAML file
    (spit yaml-file yaml-content)
    (println "Manifest content:")
    (println "---")
    (println yaml-content)
    (println "---")
    (println)

    ;; Apply to cluster
    (println "Applying to Kubernetes cluster...")
    (let [{:keys [exit out err]} (sh "kubectl" "apply" "-f" (.getAbsolutePath yaml-file))]
      (if (zero? exit)
        (do
          (println out)
          (println)
          (println "Deployment submitted successfully!")
          (println)
          (println "Monitor with:")
          (println (format "  kubectl get flinkdeployment %s -n %s" deployment-name namespace))
          (println (format "  kubectl logs -f -l app=%s -n %s" deployment-name namespace))
          (println)
          (println "Note: The JAR file must be accessible to the Flink containers.")
          (println "      For production, upload the JAR to an artifact server and update jarURI.")

          {:deployment-name deployment-name
           :namespace namespace
           :yaml-file (.getAbsolutePath yaml-file)})
        (do
          (println "Deployment failed!")
          (println err)
          (System/exit 1))))))
