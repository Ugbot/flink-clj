(ns flink-clj.rest
  "REST API client for Flink cluster management.

  This namespace provides functions to interact with the Flink REST API
  for job management, monitoring, and cluster operations.

  The Flink REST API is available at http://<jobmanager>:8081 by default.

  Example:
    (require '[flink-clj.rest :as rest])

    ;; Create a client
    (def client (rest/client \"http://localhost:8081\"))

    ;; List all jobs
    (rest/list-jobs client)

    ;; Get job details
    (rest/job-details client \"<job-id>\")

    ;; Cancel a job
    (rest/cancel-job! client \"<job-id>\")

    ;; Trigger a savepoint
    (rest/trigger-savepoint! client \"<job-id>\" \"/path/to/savepoints\")"
  (:require [clojure.string :as str])
  (:import [java.net URL HttpURLConnection]
           [java.io BufferedReader InputStreamReader OutputStream]))

;; =============================================================================
;; HTTP Client Utilities
;; =============================================================================

(defn- parse-json
  "Simple JSON parsing without external dependencies."
  [s]
  (when s
    (read-string
      (-> s
          (str/replace #"\"([^\"]+)\":" "\":$1\" ")
          (str/replace #"null" "nil")
          (str/replace #"true" "true")
          (str/replace #"false" "false")
          (str/replace #"\{" "{")
          (str/replace #"\}" "}")
          (str/replace #"\[" "[")
          (str/replace #"\]" "]")))))

(defn- to-json
  "Simple JSON serialization without external dependencies."
  [obj]
  (cond
    (nil? obj) "null"
    (string? obj) (str "\"" obj "\"")
    (keyword? obj) (str "\"" (name obj) "\"")
    (number? obj) (str obj)
    (boolean? obj) (str obj)
    (map? obj) (str "{"
                    (str/join ","
                      (map (fn [[k v]]
                             (str "\"" (if (keyword? k) (name k) k) "\":" (to-json v)))
                           obj))
                    "}")
    (sequential? obj) (str "[" (str/join "," (map to-json obj)) "]")
    :else (str "\"" obj "\"")))

(defn- http-request
  "Make an HTTP request to the Flink REST API."
  [method url & [{:keys [body headers timeout]
                  :or {timeout 30000}}]]
  (let [conn (doto ^HttpURLConnection (.openConnection (URL. url))
               (.setRequestMethod (name method))
               (.setConnectTimeout timeout)
               (.setReadTimeout timeout)
               (.setDoInput true))]
    ;; Set headers
    (doseq [[k v] (merge {"Accept" "application/json"} headers)]
      (.setRequestProperty conn k v))
    ;; Send body if present
    (when body
      (.setDoOutput conn true)
      (.setRequestProperty conn "Content-Type" "application/json")
      (with-open [os (.getOutputStream conn)]
        (.write os (.getBytes (if (string? body) body (to-json body)) "UTF-8"))))
    ;; Read response
    (let [status (.getResponseCode conn)
          stream (if (>= status 400)
                   (.getErrorStream conn)
                   (.getInputStream conn))
          response (when stream
                     (with-open [reader (BufferedReader. (InputStreamReader. stream "UTF-8"))]
                       (str/join "\n" (line-seq reader))))]
      {:status status
       :body response
       :parsed (when (and response (not (str/blank? response)))
                 (try
                   (read-string
                     (-> response
                         (str/replace #"\"([^\"\\]+)\":" ":$1 ")
                         (str/replace #"null" "nil")
                         (str/replace #":true" " true")
                         (str/replace #":false" " false")
                         (str/replace #",\s*}" "}")
                         (str/replace #",\s*\]" "]")))
                   (catch Exception e
                     {:raw response :parse-error (.getMessage e)})))})))

(defn- get-request [url] (http-request :GET url))
(defn- post-request [url body] (http-request :POST url {:body body}))
(defn- patch-request [url body] (http-request :PATCH url {:body body}))
(defn- delete-request [url] (http-request :DELETE url))

;; =============================================================================
;; Client
;; =============================================================================

(defn client
  "Create a Flink REST API client.

  Arguments:
    base-url - Base URL of the Flink JobManager (e.g., \"http://localhost:8081\")
    opts     - Optional configuration:
      :timeout - Request timeout in milliseconds (default: 30000)

  Example:
    (def client (rest/client \"http://localhost:8081\"))
    (def client (rest/client \"http://flink-jobmanager:8081\" {:timeout 60000}))"
  ([base-url]
   (client base-url {}))
  ([base-url opts]
   {:base-url (str/replace base-url #"/$" "")
    :timeout (:timeout opts 30000)}))

(defn- api-url
  "Build API URL from client and path."
  [client path]
  (str (:base-url client) path))

;; =============================================================================
;; Cluster Information
;; =============================================================================

(defn cluster-overview
  "Get cluster overview including slots, task managers, and jobs.

  Returns:
    {:taskmanagers N
     :slots-total N
     :slots-available N
     :jobs-running N
     :jobs-finished N
     :jobs-cancelled N
     :jobs-failed N
     :flink-version \"X.X.X\"
     :flink-commit \"...\"}

  Example:
    (rest/cluster-overview client)"
  [client]
  (:parsed (get-request (api-url client "/overview"))))

(defn config
  "Get cluster configuration.

  Example:
    (rest/config client)"
  [client]
  (:parsed (get-request (api-url client "/config"))))

(defn task-managers
  "List all TaskManagers in the cluster.

  Returns a vector of TaskManager info maps.

  Example:
    (rest/task-managers client)"
  [client]
  (get-in (get-request (api-url client "/taskmanagers"))
          [:parsed :taskmanagers]))

(defn task-manager-details
  "Get details for a specific TaskManager.

  Arguments:
    tm-id - TaskManager ID

  Example:
    (rest/task-manager-details client \"<tm-id>\")"
  [client tm-id]
  (:parsed (get-request (api-url client (str "/taskmanagers/" tm-id)))))

(defn task-manager-metrics
  "Get metrics for a specific TaskManager.

  Arguments:
    tm-id - TaskManager ID
    metrics - (optional) Comma-separated metric names to fetch

  Example:
    (rest/task-manager-metrics client \"<tm-id>\")
    (rest/task-manager-metrics client \"<tm-id>\" \"Status.JVM.Memory.Heap.Used\")"
  ([client tm-id]
   (:parsed (get-request (api-url client (str "/taskmanagers/" tm-id "/metrics")))))
  ([client tm-id metrics]
   (:parsed (get-request (api-url client (str "/taskmanagers/" tm-id "/metrics?get=" metrics))))))

;; =============================================================================
;; Job Management
;; =============================================================================

(defn list-jobs
  "List all jobs in the cluster.

  Returns a map with :jobs vector containing job info.

  Example:
    (rest/list-jobs client)
    ;; => {:jobs [{:id \"...\" :status \"RUNNING\"} ...]}"
  [client]
  (:parsed (get-request (api-url client "/jobs"))))

(defn jobs-overview
  "Get overview of all jobs with details.

  Returns more details than list-jobs, including task counts.

  Example:
    (rest/jobs-overview client)"
  [client]
  (:parsed (get-request (api-url client "/jobs/overview"))))

(defn job-details
  "Get detailed information about a job.

  Arguments:
    job-id - Job ID

  Returns job details including:
    :jid - Job ID
    :name - Job name
    :state - Job state (RUNNING, FINISHED, FAILED, etc.)
    :start-time - Start timestamp
    :vertices - Task/operator details

  Example:
    (rest/job-details client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id)))))

(defn job-status
  "Get the current status of a job.

  Arguments:
    job-id - Job ID

  Returns:
    {:status \"RUNNING\"} or similar

  Example:
    (rest/job-status client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id "/status")))))

(defn job-plan
  "Get the execution plan (DAG) of a job.

  Arguments:
    job-id - Job ID

  Example:
    (rest/job-plan client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id "/plan")))))

(defn job-config
  "Get the configuration of a job.

  Arguments:
    job-id - Job ID

  Example:
    (rest/job-config client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id "/config")))))

(defn job-exceptions
  "Get exceptions that occurred in a job.

  Arguments:
    job-id - Job ID

  Example:
    (rest/job-exceptions client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id "/exceptions")))))

(defn job-metrics
  "Get metrics for a job.

  Arguments:
    job-id  - Job ID
    metrics - (optional) Comma-separated metric names

  Example:
    (rest/job-metrics client \"<job-id>\")
    (rest/job-metrics client \"<job-id>\" \"numRecordsIn,numRecordsOut\")"
  ([client job-id]
   (:parsed (get-request (api-url client (str "/jobs/" job-id "/metrics")))))
  ([client job-id metrics]
   (:parsed (get-request (api-url client (str "/jobs/" job-id "/metrics?get=" metrics))))))

(defn job-checkpoints
  "Get checkpoint statistics for a job.

  Arguments:
    job-id - Job ID

  Example:
    (rest/job-checkpoints client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id "/checkpoints")))))

(defn job-checkpoint-config
  "Get checkpoint configuration for a job.

  Arguments:
    job-id - Job ID

  Example:
    (rest/job-checkpoint-config client \"<job-id>\")"
  [client job-id]
  (:parsed (get-request (api-url client (str "/jobs/" job-id "/checkpoints/config")))))

;; =============================================================================
;; Job Control
;; =============================================================================

(defn cancel-job!
  "Cancel a running job.

  Arguments:
    job-id - Job ID to cancel

  Example:
    (rest/cancel-job! client \"<job-id>\")"
  [client job-id]
  (:parsed (patch-request (api-url client (str "/jobs/" job-id)) {})))

(defn stop-job!
  "Stop a job with a savepoint.

  Arguments:
    job-id - Job ID to stop
    opts   - Options map:
      :targetDirectory - Savepoint directory (required)
      :drain - Whether to drain the job before stopping

  Example:
    (rest/stop-job! client \"<job-id>\" {:targetDirectory \"/savepoints\"})"
  [client job-id opts]
  (:parsed (post-request
             (api-url client (str "/jobs/" job-id "/stop"))
             opts)))

(defn trigger-savepoint!
  "Trigger a savepoint for a running job.

  Arguments:
    job-id          - Job ID
    target-directory - Directory to store the savepoint
    opts            - Options map:
      :cancel-job - If true, cancel the job after savepoint

  Returns:
    {:request-id \"...\"} - Use get-savepoint-status to check progress

  Example:
    (rest/trigger-savepoint! client \"<job-id>\" \"/savepoints\")
    (rest/trigger-savepoint! client \"<job-id>\" \"/savepoints\" {:cancel-job true})"
  ([client job-id target-directory]
   (trigger-savepoint! client job-id target-directory {}))
  ([client job-id target-directory opts]
   (:parsed (post-request
              (api-url client (str "/jobs/" job-id "/savepoints"))
              (merge {:targetDirectory target-directory}
                     (when (:cancel-job opts)
                       {:cancelJob true}))))))

(defn get-savepoint-status
  "Get the status of a savepoint operation.

  Arguments:
    job-id     - Job ID
    trigger-id - Trigger ID from trigger-savepoint!

  Returns:
    {:status {:id \"...\"} :operation {:location \"...\"}}

  Example:
    (rest/get-savepoint-status client \"<job-id>\" \"<trigger-id>\")"
  [client job-id trigger-id]
  (:parsed (get-request
             (api-url client (str "/jobs/" job-id "/savepoints/" trigger-id)))))

(defn rescale-job!
  "Rescale a running job to a new parallelism.

  Arguments:
    job-id      - Job ID
    parallelism - New parallelism

  Note: This requires the job to support rescaling.

  Example:
    (rest/rescale-job! client \"<job-id>\" 4)"
  [client job-id parallelism]
  (:parsed (patch-request
             (api-url client (str "/jobs/" job-id "/rescaling"))
             {:parallelism parallelism})))

;; =============================================================================
;; Job Submission
;; =============================================================================

(defn uploaded-jars
  "List uploaded JAR files.

  Example:
    (rest/uploaded-jars client)"
  [client]
  (:parsed (get-request (api-url client "/jars"))))

(defn delete-jar!
  "Delete an uploaded JAR file.

  Arguments:
    jar-id - JAR ID (filename)

  Example:
    (rest/delete-jar! client \"my-job.jar\")"
  [client jar-id]
  (:parsed (delete-request (api-url client (str "/jars/" jar-id)))))

(defn run-jar!
  "Run an uploaded JAR file.

  Arguments:
    jar-id - JAR ID (filename)
    opts   - Options map:
      :entry-class - Main class (optional if manifest specifies it)
      :program-args - Program arguments string
      :parallelism - Default parallelism
      :allowNonRestoredState - Allow non-restored state
      :savepointPath - Path to savepoint to restore from

  Returns:
    {:jobid \"...\"}

  Example:
    (rest/run-jar! client \"my-job.jar\")
    (rest/run-jar! client \"my-job.jar\" {:entry-class \"com.example.MyJob\"
                                           :parallelism 4})"
  ([client jar-id]
   (run-jar! client jar-id {}))
  ([client jar-id opts]
   (let [params (cond-> []
                  (:entry-class opts) (conj (str "entry-class=" (:entry-class opts)))
                  (:program-args opts) (conj (str "program-args=" (:program-args opts)))
                  (:parallelism opts) (conj (str "parallelism=" (:parallelism opts)))
                  (:allowNonRestoredState opts) (conj "allowNonRestoredState=true")
                  (:savepointPath opts) (conj (str "savepointPath=" (:savepointPath opts))))
         query (when (seq params) (str "?" (str/join "&" params)))]
     (:parsed (post-request
                (api-url client (str "/jars/" jar-id "/run" query))
                {})))))

(defn jar-plan
  "Get the execution plan for a JAR without running it.

  Arguments:
    jar-id - JAR ID (filename)
    opts   - Same as run-jar! options

  Example:
    (rest/jar-plan client \"my-job.jar\" {:entry-class \"com.example.MyJob\"})"
  ([client jar-id]
   (jar-plan client jar-id {}))
  ([client jar-id opts]
   (let [params (cond-> []
                  (:entry-class opts) (conj (str "entry-class=" (:entry-class opts)))
                  (:program-args opts) (conj (str "program-args=" (:program-args opts)))
                  (:parallelism opts) (conj (str "parallelism=" (:parallelism opts))))
         query (when (seq params) (str "?" (str/join "&" params)))]
     (:parsed (post-request
                (api-url client (str "/jars/" jar-id "/plan" query))
                {})))))

;; =============================================================================
;; Vertex/Task Metrics
;; =============================================================================

(defn vertex-details
  "Get details for a specific vertex (operator) in a job.

  Arguments:
    job-id    - Job ID
    vertex-id - Vertex ID

  Example:
    (rest/vertex-details client \"<job-id>\" \"<vertex-id>\")"
  [client job-id vertex-id]
  (:parsed (get-request
             (api-url client (str "/jobs/" job-id "/vertices/" vertex-id)))))

(defn vertex-metrics
  "Get metrics for a specific vertex.

  Arguments:
    job-id    - Job ID
    vertex-id - Vertex ID
    metrics   - (optional) Comma-separated metric names

  Example:
    (rest/vertex-metrics client \"<job-id>\" \"<vertex-id>\")"
  ([client job-id vertex-id]
   (:parsed (get-request
              (api-url client (str "/jobs/" job-id "/vertices/" vertex-id "/metrics")))))
  ([client job-id vertex-id metrics]
   (:parsed (get-request
              (api-url client (str "/jobs/" job-id "/vertices/" vertex-id "/metrics?get=" metrics))))))

(defn vertex-watermarks
  "Get watermark information for a vertex.

  Arguments:
    job-id    - Job ID
    vertex-id - Vertex ID

  Example:
    (rest/vertex-watermarks client \"<job-id>\" \"<vertex-id>\")"
  [client job-id vertex-id]
  (:parsed (get-request
             (api-url client (str "/jobs/" job-id "/vertices/" vertex-id "/watermarks")))))

(defn vertex-backpressure
  "Get backpressure information for a vertex.

  Arguments:
    job-id    - Job ID
    vertex-id - Vertex ID

  Example:
    (rest/vertex-backpressure client \"<job-id>\" \"<vertex-id>\")"
  [client job-id vertex-id]
  (:parsed (get-request
             (api-url client (str "/jobs/" job-id "/vertices/" vertex-id "/backpressure")))))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn wait-for-job-status
  "Wait for a job to reach a specific status.

  Arguments:
    job-id         - Job ID
    target-status  - Target status (e.g., \"RUNNING\", \"FINISHED\")
    opts           - Options:
      :timeout-ms  - Timeout in milliseconds (default: 60000)
      :poll-ms     - Poll interval in milliseconds (default: 1000)

  Returns the job status or throws on timeout.

  Example:
    (rest/wait-for-job-status client \"<job-id>\" \"RUNNING\")"
  ([client job-id target-status]
   (wait-for-job-status client job-id target-status {}))
  ([client job-id target-status {:keys [timeout-ms poll-ms]
                                  :or {timeout-ms 60000 poll-ms 1000}}]
   (let [start (System/currentTimeMillis)
         target-set (if (set? target-status) target-status #{target-status})]
     (loop []
       (let [elapsed (- (System/currentTimeMillis) start)]
         (when (> elapsed timeout-ms)
           (throw (ex-info "Timeout waiting for job status"
                           {:job-id job-id
                            :target-status target-status
                            :timeout-ms timeout-ms})))
         (let [status (job-status client job-id)
               current (:status status)]
           (if (target-set current)
             status
             (do
               (Thread/sleep poll-ms)
               (recur)))))))))

(defn wait-for-savepoint
  "Wait for a savepoint to complete.

  Arguments:
    job-id     - Job ID
    trigger-id - Trigger ID from trigger-savepoint!
    opts       - Options:
      :timeout-ms - Timeout in milliseconds (default: 300000)
      :poll-ms    - Poll interval in milliseconds (default: 1000)

  Returns savepoint info including location.

  Example:
    (let [result (rest/trigger-savepoint! client job-id \"/savepoints\")]
      (rest/wait-for-savepoint client job-id (:request-id result)))"
  ([client job-id trigger-id]
   (wait-for-savepoint client job-id trigger-id {}))
  ([client job-id trigger-id {:keys [timeout-ms poll-ms]
                               :or {timeout-ms 300000 poll-ms 1000}}]
   (let [start (System/currentTimeMillis)]
     (loop []
       (let [elapsed (- (System/currentTimeMillis) start)]
         (when (> elapsed timeout-ms)
           (throw (ex-info "Timeout waiting for savepoint"
                           {:job-id job-id
                            :trigger-id trigger-id
                            :timeout-ms timeout-ms})))
         (let [status (get-savepoint-status client job-id trigger-id)
               state (get-in status [:status :id])]
           (cond
             (= state "COMPLETED") status
             (= state "FAILED") (throw (ex-info "Savepoint failed" status))
             :else (do
                     (Thread/sleep poll-ms)
                     (recur)))))))))

(defn running-jobs
  "Get a list of currently running jobs.

  Example:
    (rest/running-jobs client)"
  [client]
  (let [jobs (:jobs (list-jobs client))]
    (clojure.core/filter #(= "RUNNING" (:status %)) jobs)))

(defn finished-jobs
  "Get a list of finished jobs.

  Example:
    (rest/finished-jobs client)"
  [client]
  (let [jobs (:jobs (list-jobs client))]
    (clojure.core/filter #(= "FINISHED" (:status %)) jobs)))

(defn failed-jobs
  "Get a list of failed jobs.

  Example:
    (rest/failed-jobs client)"
  [client]
  (let [jobs (:jobs (list-jobs client))]
    (clojure.core/filter #(= "FAILED" (:status %)) jobs)))
