(ns flink-clj.cli.deploy.standalone
  "Deploy Flink jobs to standalone clusters.

  This handler uploads JARs and submits jobs to running Flink clusters
  using the REST API."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [flink-clj.rest :as rest])
  (:import [java.io File]))

;; =============================================================================
;; Cluster Connection
;; =============================================================================

(defn- connect-to-cluster
  "Connect to Flink cluster and verify it's running."
  [host port]
  (let [base-url (format "http://%s:%d" host port)
        client (rest/client base-url)]
    (try
      (let [overview (rest/cluster-overview client)]
        (println (format "Connected to Flink cluster at %s:%d" host port))
        (println (format "  Flink version: %s" (:flink-version overview)))
        (println (format "  TaskManagers: %d" (:taskmanagers overview)))
        (println (format "  Slots available: %d/%d"
                         (:slots-available overview)
                         (:slots-total overview)))
        (println)
        client)
      (catch Exception e
        (println (format "Error: Cannot connect to Flink cluster at %s:%d" host port))
        (println (format "       %s" (.getMessage e)))
        (println)
        (println "Make sure the Flink cluster is running and accessible.")
        (System/exit 1)))))

;; =============================================================================
;; Deployment
;; =============================================================================

(defn deploy!
  "Deploy JAR to standalone Flink cluster."
  [^File jar {:keys [host port parallelism main savepoint args name]}]
  (let [client (connect-to-cluster host port)
        job-name (or name (.getName jar))]

    ;; Upload JAR
    (println "Uploading JAR...")
    (let [jar-id (rest/upload-jar! client (.getAbsolutePath jar))]
      (println (format "  JAR ID: %s" jar-id))
      (println)

      ;; Submit job
      (println "Submitting job...")
      (let [submit-opts (cond-> {:parallelism parallelism}
                          main (assoc :entry-class main)
                          savepoint (assoc :savepoint-path savepoint)
                          (not (str/blank? args)) (assoc :program-args args))
            job-id (rest/submit-jar! client jar-id submit-opts)]
        (println (format "  Job ID: %s" job-id))
        (println)

        ;; Get job status
        (Thread/sleep 1000)  ; Give it a moment to start
        (let [job-details (rest/job-details client job-id)]
          (println (format "Job '%s' submitted successfully!" job-name))
          (println)
          (println (format "  Status: %s" (:state job-details)))
          (println (format "  Web UI: http://%s:%d/#/jobs/%s" host port job-id))
          (println)
          (println "Monitor with:")
          (println (format "  flink-clj jobs --host %s --port %d" host port))
          (println (format "  flink-clj jobs info %s --host %s --port %d" job-id host port))

          {:jar-id jar-id
           :job-id job-id
           :status (:state job-details)})))))
