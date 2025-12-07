(ns flink-clj.shell
  "Interactive REPL environment for flink-clj.

  Similar to PyFlink (st_env, bt_env) and Flink Scala shell (senv, benv).

  Usage:
    ;; Local mode (embedded cluster)
    (start!)
    (start! {:web-ui true})           ; With Web UI at localhost:8081
    (start! {:parallelism 4})

    ;; Remote mode (connect to existing cluster)
    (start! {:mode :remote
             :host \"flink-cluster\"
             :port 8081})

    ;; Pre-configured environments available after start!
    env       ; StreamExecutionEnvironment
    table-env ; StreamTableEnvironment (if Table API available)

    ;; Interactive helpers
    (collect stream)  ; Execute and collect results
    (take-n stream 10) ; Collect first n results
    (jobs)            ; List jobs (remote mode)
    (cancel! job-id)  ; Cancel a job
    (desc stream)     ; Describe stream type/parallelism
    (explain env)     ; Print execution plan
    (help)            ; Show available commands
    (stop!)           ; Cleanup"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as core]
            [flink-clj.table :as table]
            [flink-clj.rest :as rest]
            [flink-clj.test-utils :as tu]
            [clojure.string :as str])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api.datastream DataStream]))

;; =============================================================================
;; Shell State
;; =============================================================================

(defonce ^:private shell-state
  (atom {:running? false
         :mode nil           ; :local or :remote
         :env nil            ; StreamExecutionEnvironment
         :table-env nil      ; StreamTableEnvironment (if available)
         :rest-client nil    ; REST client (remote mode only)
         :options {}
         :web-ui-url nil}))

;; Dynamic vars for current environments (for external access)
(def ^:dynamic env nil)
(def ^:dynamic table-env nil)
(def ^:dynamic rest-client nil)

;; =============================================================================
;; Lifecycle
;; =============================================================================

(defn- create-local-env-with-options
  "Create local environment with shell options."
  [{:keys [parallelism web-ui web-ui-port]}]
  (let [opts (cond-> {}
               parallelism (assoc :parallelism parallelism)
               (and web-ui web-ui-port) (assoc :web-ui-port web-ui-port)
               (and web-ui (not web-ui-port)) (assoc :web-ui-port 8081))]
    (env/create-local-env opts)))

(defn- create-remote-env-with-options
  "Create remote environment with shell options."
  [{:keys [host port parallelism jars]}]
  (env/create-remote-env host port
                         (cond-> {}
                           parallelism (assoc :parallelism parallelism)
                           jars (assoc :jars jars))))

(defn- create-table-env-if-available
  "Create table environment if Table API is available."
  [stream-env]
  (when (table/table-api-available?)
    (try
      (table/create-table-env stream-env)
      (catch Exception e
        (println "Warning: Could not create table environment:" (.getMessage e))
        nil))))

(defn- validate-remote-cluster
  "Validate that remote cluster is reachable."
  [host port]
  (let [client (rest/client (format "http://%s:%d" host port))]
    (try
      (rest/cluster-overview client)
      client
      (catch Exception e
        (throw (ex-info (format "Cannot connect to Flink cluster at %s:%d" host port)
                        {:host host :port port :cause (.getMessage e)}))))))

(defn start!
  "Start the flink-clj shell.

  Options:
    :mode        - :local (default) or :remote
    :parallelism - Default parallelism (default: 2 for local)
    :web-ui      - Enable web UI in local mode (default: false)
    :web-ui-port - Web UI port (default: 8081 when web-ui is true)
    :host        - Remote cluster hostname (required for :remote mode)
    :port        - Remote cluster port (default: 8081)
    :jars        - Vector of JAR paths to ship (remote mode)

  Examples:
    (start!)                              ; Local mode
    (start! {:web-ui true})               ; Local with Web UI
    (start! {:parallelism 4 :web-ui true :web-ui-port 8082})
    (start! {:mode :remote :host \"flink.company.com\" :port 8081})"
  ([] (start! {}))
  ([opts]
   (if (:running? @shell-state)
     (do
       (println "Shell already running. Call (restart!) to restart with new options.")
       nil)
     (let [mode (or (:mode opts) :local)
         parallelism (or (:parallelism opts) (when (= mode :local) 2))
         opts (assoc opts :mode mode :parallelism parallelism)]

     (println (format "Starting flink-clj shell (%s mode)..." (name mode)))

     (let [stream-env (case mode
                        :local (create-local-env-with-options opts)
                        :remote (do
                                  (when-not (:host opts)
                                    (throw (ex-info "Remote mode requires :host" {:opts opts})))
                                  (create-remote-env-with-options opts)))

           _ (core/register-clojure-types! stream-env)

           tbl-env (create-table-env-if-available stream-env)

           rest-cli (when (= mode :remote)
                      (validate-remote-cluster (:host opts) (or (:port opts) 8081)))

           web-url (when (and (= mode :local) (:web-ui opts))
                     (format "http://localhost:%d" (or (:web-ui-port opts) 8081)))]

       ;; Update state
       (reset! shell-state
               {:running? true
                :mode mode
                :env stream-env
                :table-env tbl-env
                :rest-client rest-cli
                :options opts
                :web-ui-url web-url})

       ;; Update dynamic bindings
       (alter-var-root #'env (constantly stream-env))
       (alter-var-root #'table-env (constantly tbl-env))
       (alter-var-root #'rest-client (constantly rest-cli))

       ;; Print status
       (println)
       (when web-url
         (println (format "Web UI: %s" web-url)))
       (println)
       (println "Environments ready:")
       (println (format "  env       - StreamExecutionEnvironment (parallelism=%d)"
                        (or parallelism (.getParallelism stream-env))))
       (when tbl-env
         (println "  table-env - StreamTableEnvironment"))
       (when rest-cli
         (let [overview (rest/cluster-overview rest-cli)]
           (println (format "  rest      - REST client (TaskManagers: %d, Slots: %d)"
                            (:taskmanagers overview)
                            (:slots-total overview)))))
       (println)
       nil)))))

(defn stop!
  "Stop the flink-clj shell and cleanup resources."
  []
  (if-not (:running? @shell-state)
    (do
      (println "Shell not running.")
      nil)
    (do
      (println "Stopping shell...")

      ;; Reset state
      (reset! shell-state
              {:running? false
               :mode nil
               :env nil
               :table-env nil
               :rest-client nil
               :options {}
               :web-ui-url nil})

      ;; Clear dynamic bindings
      (alter-var-root #'env (constantly nil))
      (alter-var-root #'table-env (constantly nil))
      (alter-var-root #'rest-client (constantly nil))

      (println "Shell stopped.")
      nil)))

(defn restart!
  "Restart the shell with new options."
  ([] (restart! (:options @shell-state)))
  ([opts]
   (when (:running? @shell-state)
     (stop!))
   (start! opts)))

(defn status
  "Get current shell status."
  []
  (let [{:keys [running? mode options web-ui-url]} @shell-state]
    {:running? running?
     :mode mode
     :parallelism (:parallelism options)
     :web-ui-url web-ui-url
     :host (:host options)
     :port (:port options)}))

;; =============================================================================
;; Execution Helpers
;; =============================================================================

(defn- ensure-running!
  "Ensure shell is running, throw if not."
  []
  (when-not (:running? @shell-state)
    (throw (ex-info "Shell not running. Call (start!) first." {}))))

(defn collect
  "Execute stream and collect all results.

  Options:
    :limit   - Max results to collect (default: 10000)
    :timeout - Timeout in ms (default: no timeout)

  IMPORTANT: Blocks until job completes. Use with bounded sources only.

  Example:
    (-> (f/source env (f/collection [1 2 3]))
        (f/map inc)
        (collect))
    ;=> [2 3 4]"
  ([stream] (collect stream {}))
  ([stream {:keys [limit] :or {limit 10000}}]
   (ensure-running!)
   (let [results (tu/collect-results stream)]
     (if (and limit (> (count results) limit))
       (vec (take limit results))
       results))))

(defn take-n
  "Execute stream and collect first n results.

  Example:
    (take-n my-stream 10)"
  [stream n]
  (ensure-running!)
  (tu/collect-n stream n))

(defn run
  "Execute the job synchronously.

  Returns JobExecutionResult."
  ([stream-env] (run stream-env "flink-clj interactive job"))
  ([stream-env job-name]
   (ensure-running!)
   (core/execute stream-env job-name)))

(defn run-async
  "Execute the job asynchronously.

  Returns JobClient for monitoring/control."
  ([stream-env] (run-async stream-env "flink-clj interactive job"))
  ([stream-env job-name]
   (ensure-running!)
   (core/execute-async stream-env job-name)))

;; =============================================================================
;; Job Management (Remote Mode)
;; =============================================================================

(defn jobs
  "List all jobs in the cluster.

  Options:
    :status - Filter by status (:running, :finished, :failed, :canceled)

  Returns vector of job info maps.

  Example:
    (jobs)
    (jobs {:status :running})"
  ([] (jobs {}))
  ([{:keys [status]}]
   (ensure-running!)
   (when-not (:rest-client @shell-state)
     (throw (ex-info "jobs function requires remote mode" {})))
   (let [all-jobs (:jobs (rest/list-jobs (:rest-client @shell-state)))]
     (if status
       (filter #(= (name status) (str/lower-case (:status %))) all-jobs)
       all-jobs))))

(defn job-info
  "Get detailed information about a job.

  Example:
    (job-info \"abc123...\")"
  [job-id]
  (ensure-running!)
  (when-not (:rest-client @shell-state)
    (throw (ex-info "job-info requires remote mode" {})))
  (rest/job-details (:rest-client @shell-state) job-id))

(defn cancel!
  "Cancel a running job.

  Example:
    (cancel! \"abc123...\")"
  [job-id]
  (ensure-running!)
  (when-not (:rest-client @shell-state)
    (throw (ex-info "cancel! requires remote mode" {})))
  (rest/cancel-job! (:rest-client @shell-state) job-id)
  (println (format "Cancellation requested for job %s" job-id))
  nil)

;; =============================================================================
;; Introspection
;; =============================================================================

(defn desc
  "Describe a DataStream - show type info, parallelism, name.

  Example:
    (desc my-stream)
    ;=> {:type \"GenericType<Object>\" :parallelism 2 :name \"Map\"}"
  [^DataStream stream]
  (let [transform (.getTransformation stream)]
    {:type (str (.getType stream))
     :parallelism (.getParallelism transform)
     :name (.getName transform)
     :id (.getId transform)}))

(defn explain
  "Print the execution plan for the environment.

  Example:
    (explain env)"
  [^StreamExecutionEnvironment stream-env]
  (println (.getExecutionPlan stream-env)))

(defn web-ui-url
  "Get the Web UI URL if enabled."
  []
  (:web-ui-url @shell-state))

;; =============================================================================
;; Help
;; =============================================================================

(defn help
  "Print help for flink-clj shell commands."
  []
  (println "
=== flink-clj Shell Commands ===

LIFECYCLE:
  (start!)                     Start shell (local mode)
  (start! {:web-ui true})      Start with Web UI
  (start! {:mode :remote       Connect to remote cluster
           :host \"hostname\"
           :port 8081})
  (stop!)                      Stop shell
  (restart!)                   Restart with same options
  (restart! opts)              Restart with new options
  (status)                     Show shell status

ENVIRONMENTS (after start!):
  env                          StreamExecutionEnvironment
  table-env                    StreamTableEnvironment

EXECUTION:
  (collect stream)             Execute and collect all results
  (take-n stream n)            Collect first n results
  (run env \"job name\")         Execute synchronously
  (run-async env \"name\")       Execute asynchronously

JOB MANAGEMENT (remote mode):
  (jobs)                       List all jobs
  (jobs {:status :running})    Filter by status
  (job-info \"job-id\")          Get job details
  (cancel! \"job-id\")           Cancel a job

INTROSPECTION:
  (desc stream)                Describe stream type/parallelism
  (explain env)                Print execution plan
  (web-ui-url)                 Get Web UI URL

EXAMPLE SESSION:
  (start! {:web-ui true})
  (require '[flink-clj.dsl :as f])

  (defn double-it [x] (* x 2))

  (-> (f/source env (f/collection [1 2 3 4 5]))
      (f/map double-it)
      (f/filter even?)
      (collect))
  ;=> [2 4 6 8 10]

  (stop!)
"))
