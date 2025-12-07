(ns flink-clj.savepoint
  "Savepoint management utilities for Flink jobs.

  Savepoints are consistent snapshots of job state that enable:
  - Job migration between clusters
  - Flink version upgrades
  - A/B testing and rollbacks
  - Debugging and auditing

  This namespace provides utilities for triggering and managing savepoints
  programmatically via the JobClient API.

  Example:
    (require '[flink-clj.savepoint :as sp])

    ;; Trigger a savepoint from a running job
    (let [job-client (flink/execute-async env \"My Job\")]
      (sp/trigger job-client \"/path/to/savepoints\"))"
  (:import [org.apache.flink.core.execution JobClient]
           [java.util.concurrent CompletableFuture TimeUnit]))

;; =============================================================================
;; Savepoint Trigger
;; =============================================================================

(defn trigger
  "Trigger a savepoint for a running job.

  job-client is obtained from execute-async.
  savepoint-dir is the target directory for the savepoint.

  Options:
    :cancel? - If true, cancel the job after taking savepoint (default: false)
    :timeout - Timeout in milliseconds (default: 60000)

  Returns a CompletableFuture that completes with the savepoint path.

  Example:
    (let [future (trigger job-client \"/savepoints\")]
      (println \"Savepoint at:\" @future))"
  ([^JobClient job-client ^String savepoint-dir]
   (trigger job-client savepoint-dir {}))
  ([^JobClient job-client ^String savepoint-dir {:keys [cancel? timeout]
                                                   :or {cancel? false
                                                        timeout 60000}}]
   (if cancel?
     (.stopWithSavepoint job-client false savepoint-dir)
     (.triggerSavepoint job-client savepoint-dir))))

(defn trigger-sync
  "Trigger a savepoint and wait for completion.

  Returns the savepoint path.

  Example:
    (let [path (trigger-sync job-client \"/savepoints\")]
      (println \"Saved to:\" path))"
  ([^JobClient job-client ^String savepoint-dir]
   (trigger-sync job-client savepoint-dir {}))
  ([^JobClient job-client ^String savepoint-dir {:keys [cancel? timeout]
                                                   :or {cancel? false
                                                        timeout 60000}}]
   (let [future (trigger job-client savepoint-dir {:cancel? cancel?})]
     (.get ^CompletableFuture future timeout TimeUnit/MILLISECONDS))))

;; =============================================================================
;; Stop with Savepoint
;; =============================================================================

(defn stop-with-savepoint
  "Stop a job with a savepoint.

  This is the preferred way to gracefully stop a job while preserving state.
  The job will finish processing in-flight data before taking the savepoint.

  Options:
    :drain? - If true, wait for all sources to finish (default: false)
    :timeout - Timeout in milliseconds (default: 60000)

  Returns a CompletableFuture that completes with the savepoint path.

  Example:
    (stop-with-savepoint job-client \"/savepoints\" {:drain? true})"
  ([^JobClient job-client ^String savepoint-dir]
   (stop-with-savepoint job-client savepoint-dir {}))
  ([^JobClient job-client ^String savepoint-dir {:keys [drain? timeout]
                                                   :or {drain? false
                                                        timeout 60000}}]
   (.stopWithSavepoint job-client drain? savepoint-dir)))

(defn stop-with-savepoint-sync
  "Stop a job with savepoint and wait for completion.

  Returns the savepoint path.

  Example:
    (let [path (stop-with-savepoint-sync job-client \"/savepoints\")]
      (println \"Final savepoint:\" path))"
  ([^JobClient job-client ^String savepoint-dir]
   (stop-with-savepoint-sync job-client savepoint-dir {}))
  ([^JobClient job-client ^String savepoint-dir {:keys [drain? timeout]
                                                    :or {drain? false
                                                         timeout 60000}}]
   (let [future (stop-with-savepoint job-client savepoint-dir {:drain? drain?})]
     (.get ^CompletableFuture future timeout TimeUnit/MILLISECONDS))))

;; =============================================================================
;; Job Status
;; =============================================================================

(defn job-id
  "Get the job ID from a JobClient.

  Example:
    (job-id job-client)  ;=> #uuid \"...\""
  [^JobClient job-client]
  (.getJobID job-client))

(defn job-status
  "Get the current status of a job.

  Returns a CompletableFuture that completes with the JobStatus.

  Example:
    @(job-status job-client)  ;=> :running"
  [^JobClient job-client]
  (-> (.getJobStatus job-client)
      (.thenApply (fn [status]
                    (keyword (.toLowerCase (.name status)))))))

(defn job-status-sync
  "Get the current status of a job (blocking).

  Example:
    (job-status-sync job-client)  ;=> :running"
  ([^JobClient job-client]
   (job-status-sync job-client 30000))
  ([^JobClient job-client timeout-ms]
   (.get ^CompletableFuture (job-status job-client) timeout-ms TimeUnit/MILLISECONDS)))

;; =============================================================================
;; Job Cancellation
;; =============================================================================

(defn cancel
  "Cancel a running job (without savepoint).

  Use stop-with-savepoint if you want to preserve state.

  Returns a CompletableFuture that completes when cancellation is acknowledged.

  Example:
    @(cancel job-client)"
  [^JobClient job-client]
  (.cancel job-client))

(defn cancel-sync
  "Cancel a running job and wait for acknowledgment.

  Example:
    (cancel-sync job-client)"
  ([^JobClient job-client]
   (cancel-sync job-client 30000))
  ([^JobClient job-client timeout-ms]
   (.get ^CompletableFuture (cancel job-client) timeout-ms TimeUnit/MILLISECONDS)))

;; =============================================================================
;; Job Result
;; =============================================================================

(defn job-result
  "Get the job execution result (waits for completion).

  Returns a CompletableFuture that completes with the JobExecutionResult.

  Example:
    (let [result @(job-result job-client)]
      (println \"Job finished in\" (.getNetRuntime result) \"ms\"))"
  [^JobClient job-client]
  (.getJobExecutionResult job-client))

(defn await-termination
  "Wait for a job to terminate and return the result.

  Example:
    (await-termination job-client 300000)  ; 5 minute timeout"
  ([^JobClient job-client]
   (.get ^CompletableFuture (job-result job-client)))
  ([^JobClient job-client timeout-ms]
   (.get ^CompletableFuture (job-result job-client) timeout-ms TimeUnit/MILLISECONDS)))

;; =============================================================================
;; Environment Configuration for Restore
;; =============================================================================

(defn with-savepoint
  "Configure execution environment to restore from a savepoint.

  This modifies the environment's configuration to use the specified
  savepoint when starting the job.

  Options:
    :allow-non-restored-state? - Allow state that cannot be restored (default: false)

  Example:
    (-> (env/create-env)
        (sp/with-savepoint \"/path/to/savepoint\" {:allow-non-restored-state? true})
        ...)"
  ([env savepoint-path]
   (with-savepoint env savepoint-path {}))
  ([env savepoint-path {:keys [allow-non-restored-state?]
                         :or {allow-non-restored-state? false}}]
   ;; Use reflection to avoid compile-time dependency on version-specific class
   (try
     (let [config (.getConfiguration env)
           opts-class (Class/forName "org.apache.flink.runtime.jobgraph.SavepointConfigOptions")
           path-field (.getField opts-class "SAVEPOINT_PATH")
           ignore-field (.getField opts-class "SAVEPOINT_IGNORE_UNCLAIMED_STATE")
           path-opt (.get path-field nil)
           ignore-opt (.get ignore-field nil)]
       (.set config path-opt savepoint-path)
       (.set config ignore-opt (boolean allow-non-restored-state?))
       env)
     (catch ClassNotFoundException _
       ;; Try Flink 2.x API
       (try
         (let [config (.getConfiguration env)
               opts-class (Class/forName "org.apache.flink.configuration.CheckpointingOptions")
               path-field (.getField opts-class "SAVEPOINT_DIRECTORY")
               path-opt (.get path-field nil)]
           (.set config path-opt savepoint-path)
           env)
         (catch Exception e
           (throw (ex-info "Could not configure savepoint restore"
                           {:savepoint-path savepoint-path
                            :error (.getMessage e)}))))))))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn savepoint-info
  "Get information about a job for savepoint operations.

  Returns a map with job details.

  Example:
    (savepoint-info job-client)"
  [^JobClient job-client]
  {:job-id (str (job-id job-client))
   :status (job-status-sync job-client)})

(defn wait-until-running
  "Wait until a job reaches RUNNING status.

  Useful after submitting a job to ensure it started successfully.

  Options:
    :timeout - Maximum wait time in ms (default: 60000)
    :poll-interval - Check interval in ms (default: 1000)

  Example:
    (let [client (flink/execute-async env \"My Job\")]
      (wait-until-running client)
      (println \"Job is now running\"))"
  ([^JobClient job-client]
   (wait-until-running job-client {}))
  ([^JobClient job-client {:keys [timeout poll-interval]
                            :or {timeout 60000
                                 poll-interval 1000}}]
   (let [start (System/currentTimeMillis)]
     (loop []
       (let [elapsed (- (System/currentTimeMillis) start)]
         (when (> elapsed timeout)
           (throw (ex-info "Timeout waiting for job to start"
                           {:job-id (str (job-id job-client))
                            :timeout timeout})))
         (let [status (job-status-sync job-client poll-interval)]
           (if (= :running status)
             status
             (do
               (Thread/sleep poll-interval)
               (recur)))))))))

(defn ensure-job-running
  "Ensure a job is running, throwing if it's in a terminal state.

  Example:
    (ensure-job-running job-client)"
  [^JobClient job-client]
  (let [status (job-status-sync job-client)]
    (case status
      :running true
      (:finished :canceled :failed)
      (throw (ex-info "Job is not running"
                      {:job-id (str (job-id job-client))
                       :status status}))
      ;; Other states (created, restarting, etc.) - wait a bit
      (do
        (Thread/sleep 1000)
        (ensure-job-running job-client)))))
