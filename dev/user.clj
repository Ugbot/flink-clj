(ns user
  "Development namespace for flink-clj REPL-driven development.

  Start a REPL:
    lein shell              ; Flink 1.20
    lein shell-2.x          ; Flink 2.x
    (or: lein with-profile +dev,+flink-1.20 repl)

  Quick start:
    (start!)                  ; Initialize local Flink
    (start! {:web-ui true})   ; With Web UI at localhost:8081
    env                       ; StreamExecutionEnvironment (after start!)
    table-env                 ; StreamTableEnvironment (after start!)
    (stop!)                   ; Cleanup

  See (help) for all commands."
  (:require [clojure.tools.namespace.repl :refer [refresh refresh-all]]))

;; Placeholder vars - will be bound after (start!)
(def ^:dynamic env nil)
(def ^:dynamic table-env nil)
(def ^:dynamic flink-rest nil)  ; REST client for remote mode

;; =============================================================================
;; Lazy Shell Loading
;; =============================================================================

(defonce ^:private shell-loaded? (atom false))

(defn- ensure-shell!
  "Lazily load shell namespace to keep REPL startup fast."
  []
  (when-not @shell-loaded?
    (require '[flink-clj.shell :as shell])
    (require '[flink-clj.dsl :as f])
    (reset! shell-loaded? true)))

;; =============================================================================
;; Shell Lifecycle
;; =============================================================================

(defn start!
  "Start flink-clj shell.

  Options:
    :mode        - :local (default) or :remote
    :parallelism - Default parallelism (default: 2 for local)
    :web-ui      - Enable web UI in local mode (default: false)
    :web-ui-port - Web UI port (default: 8081)
    :host        - Remote cluster hostname (required for :remote)
    :port        - Remote cluster port (default: 8081)
    :jars        - Vector of JAR paths to ship (remote mode)

  Examples:
    (start!)                              ; Local mode
    (start! {:web-ui true})               ; Local with Web UI
    (start! {:mode :remote :host \"flink.company.com\" :port 8081})"
  ([] (start! {}))
  ([opts]
   (ensure-shell!)
   ((resolve 'flink-clj.shell/start!) opts)
   ;; Bind environments to user namespace for convenience
   (alter-var-root #'env (constantly @(resolve 'flink-clj.shell/env)))
   (alter-var-root #'table-env (constantly @(resolve 'flink-clj.shell/table-env)))
   (when-let [r @(resolve 'flink-clj.shell/rest-client)]
     (alter-var-root #'flink-rest (constantly r)))
   nil))

(defn stop!
  "Stop the flink-clj shell and cleanup resources."
  []
  (ensure-shell!)
  ((resolve 'flink-clj.shell/stop!))
  (alter-var-root #'env (constantly nil))
  (alter-var-root #'table-env (constantly nil))
  (alter-var-root #'flink-rest (constantly nil))
  nil)

(defn restart!
  "Restart the shell with new options."
  ([] (restart! nil))
  ([opts]
   (ensure-shell!)
   (if opts
     ((resolve 'flink-clj.shell/restart!) opts)
     ((resolve 'flink-clj.shell/restart!)))
   ;; Re-bind environments
   (alter-var-root #'env (constantly @(resolve 'flink-clj.shell/env)))
   (alter-var-root #'table-env (constantly @(resolve 'flink-clj.shell/table-env)))
   (when-let [r @(resolve 'flink-clj.shell/rest-client)]
     (alter-var-root #'flink-rest (constantly r)))
   nil))

(defn status
  "Get current shell status."
  []
  (ensure-shell!)
  ((resolve 'flink-clj.shell/status)))

;; =============================================================================
;; Execution Helpers
;; =============================================================================

(defn collect
  "Execute stream and collect all results.

  IMPORTANT: Blocks until job completes. Use with bounded sources only.

  Example:
    (-> (f/source env (f/collection [1 2 3]))
        (f/map inc)
        (collect))
    ;=> [2 3 4]"
  ([stream] (collect stream {}))
  ([stream opts]
   (ensure-shell!)
   ((resolve 'flink-clj.shell/collect) stream opts)))

(defn take-n
  "Execute stream and collect first n results.

  Example:
    (take-n my-stream 10)"
  [stream n]
  (ensure-shell!)
  ((resolve 'flink-clj.shell/take-n) stream n))

(defn run
  "Execute the job synchronously. Returns JobExecutionResult."
  ([env-arg] (run env-arg "flink-clj interactive job"))
  ([env-arg job-name]
   (ensure-shell!)
   ((resolve 'flink-clj.shell/run) env-arg job-name)))

(defn run-async
  "Execute the job asynchronously. Returns JobClient."
  ([env-arg] (run-async env-arg "flink-clj interactive job"))
  ([env-arg job-name]
   (ensure-shell!)
   ((resolve 'flink-clj.shell/run-async) env-arg job-name)))

;; =============================================================================
;; Job Management (Remote Mode)
;; =============================================================================

(defn jobs
  "List all jobs in the cluster (remote mode only).

  Example:
    (jobs)
    (jobs {:status :running})"
  ([] (jobs {}))
  ([opts]
   (ensure-shell!)
   ((resolve 'flink-clj.shell/jobs) opts)))

(defn job-info
  "Get detailed information about a job (remote mode only)."
  [job-id]
  (ensure-shell!)
  ((resolve 'flink-clj.shell/job-info) job-id))

(defn cancel!
  "Cancel a running job (remote mode only)."
  [job-id]
  (ensure-shell!)
  ((resolve 'flink-clj.shell/cancel!) job-id))

;; =============================================================================
;; Introspection
;; =============================================================================

(defn desc
  "Describe a DataStream - show type info, parallelism, name.

  Example:
    (desc my-stream)"
  [stream]
  (ensure-shell!)
  ((resolve 'flink-clj.shell/desc) stream))

(defn explain
  "Print the execution plan for the environment."
  [env-arg]
  (ensure-shell!)
  ((resolve 'flink-clj.shell/explain) env-arg))

(defn web-ui-url
  "Get the Web UI URL if enabled."
  []
  (ensure-shell!)
  ((resolve 'flink-clj.shell/web-ui-url)))

(defn help
  "Print help for flink-clj shell commands."
  []
  (ensure-shell!)
  ((resolve 'flink-clj.shell/help)))

;; =============================================================================
;; Development Utilities
;; =============================================================================

(defn reset
  "Reload all changed namespaces."
  []
  (refresh))

(defn init
  "Load flink-clj namespaces manually (alternative to start!)."
  []
  (require '[flink-clj.core :as flink]
           '[flink-clj.env :as env]
           '[flink-clj.stream :as stream]
           '[flink-clj.keyed :as keyed]
           '[flink-clj.sink :as sink]
           '[flink-clj.dsl :as f])
  (println "flink-clj loaded. Use flink/, env/, stream/, keyed/, sink/, f/ prefixes.")
  (println "Or call (start!) for interactive shell with pre-configured environments."))

;; =============================================================================
;; Startup Banner
;; =============================================================================

(println)
(println "=== flink-clj REPL ===")
(println "(start!)              - Initialize local Flink")
(println "(start! {:web-ui true}) - With Web UI")
(println "(help)                - Show all commands")
(println)
