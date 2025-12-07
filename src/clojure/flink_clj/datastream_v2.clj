(ns flink-clj.datastream-v2
  "EXPERIMENTAL: DataStream V2 API support for Flink 2.x.

  DataStream V2 is a new experimental API introduced in Flink 2.0 (FLIP-408)
  that aims to gradually replace the original DataStream API. It provides:

  - Explicit partitioning concepts (NonKeyedPartitionStream, KeyedPartitionStream)
  - Async-first state access
  - Simplified ProcessFunction interfaces
  - Better separation of concerns

  WARNING: This API is experimental and subject to change. Not recommended
  for production use until it stabilizes.

  Basic usage:
    (require '[flink-clj.datastream-v2 :as v2])

    (defn my-process [record output ctx]
      (.collect output (process record)))

    (-> (v2/execution-env)
        (v2/from-collection [1 2 3])
        (v2/process #'my-process)
        (v2/execute \"My V2 Job\"))

  See also:
  - https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream-v2/overview/
  - FLIP-408: https://cwiki.apache.org/confluence/display/FLINK/FLIP-408"
  (:refer-clojure :exclude [shuffle])
  (:require [flink-clj.version :as v]
            [flink-clj.impl.functions :as impl]))

;; =============================================================================
;; Version and Availability Checks
;; =============================================================================

(defn- v2-api-available?
  "Check if DataStream V2 API is available (Flink 2.x)."
  []
  (v/datastream-v2-available?))

(defn- ensure-v2-available!
  "Throw if DataStream V2 is not available."
  [feature-name]
  (when-not (v2-api-available?)
    (throw (ex-info (str feature-name " requires DataStream V2 API (Flink 2.x)")
                    {:flink-version (v/flink-minor-version)
                     :hint "DataStream V2 is an experimental API available in Flink 2.x"}))))

(defn v2-available?
  "Check if DataStream V2 API is available.

  Returns true if running on Flink 2.x with V2 API classes present."
  []
  (v2-api-available?))

(defn v2-info
  "Get information about DataStream V2 availability.

  Returns a map with:
    :available - Boolean indicating if V2 is available
    :version - Flink version string
    :status - :experimental or :unavailable"
  []
  {:available (v2-available?)
   :version (v/flink-minor-version)
   :status (if (v2-available?) :experimental :unavailable)})

;; =============================================================================
;; ExecutionEnvironment (V2)
;; =============================================================================

(defn execution-env
  "Get a DataStream V2 ExecutionEnvironment.

  Unlike the classic StreamExecutionEnvironment, this returns the V2 variant
  from org.apache.flink.datastream.api.ExecutionEnvironment.

  Example:
    (def env (v2/execution-env))"
  []
  (ensure-v2-available! "execution-env")
  (let [env-class (Class/forName "org.apache.flink.datastream.api.ExecutionEnvironment")
        get-instance (.getMethod env-class "getInstance" (into-array Class []))]
    (.invoke get-instance nil (into-array Object []))))

;; =============================================================================
;; Source Operations
;; =============================================================================

(defn from-collection
  "Create a NonKeyedPartitionStream from a collection.

  Uses DataStreamV2SourceUtils.fromData internally.

  Example:
    (from-collection env [1 2 3 4 5])"
  [env coll]
  (ensure-v2-available! "from-collection")
  (let [source-utils-class (Class/forName "org.apache.flink.datastream.api.extension.eventtime.sources.DataStreamV2SourceUtils")
        from-data (.getMethod source-utils-class "fromData"
                              (into-array Class [java.util.Collection]))
        source (.invoke from-data nil (into-array Object [(vec coll)]))
        from-source (.getMethod (.getClass env) "fromSource"
                                (into-array Class [Object String]))]
    (.invoke from-source env (into-array Object [source "collection-source"]))))

(defn from-source
  "Create a NonKeyedPartitionStream from a V2 Source.

  Example:
    (from-source env my-v2-source \"source-name\")"
  [env source source-name]
  (ensure-v2-available! "from-source")
  (let [from-source-method (.getMethod (.getClass env) "fromSource"
                                       (into-array Class [Object String]))]
    (.invoke from-source-method env (into-array Object [source source-name]))))

;; =============================================================================
;; Stream Processing
;; =============================================================================

(defn process
  "Apply a OneInputStreamProcessFunction to a stream.

  The process function receives:
    record - The input record
    output - A Collector for emitting results
    ctx    - ProcessFunction context

  Example:
    (defn my-process [record output ctx]
      (.collect output (* record 2)))

    (v2/process stream #'my-process)"
  [stream f]
  (ensure-v2-available! "process")
  ;; For V2 API, we need to create a Java wrapper that implements
  ;; OneInputStreamProcessFunction and delegates to the Clojure function
  (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var f))
        ;; Load the function at runtime
        clj-fn (do
                 (require (symbol ns-str))
                 (resolve (symbol ns-str fn-name)))
        ;; Create the process function wrapper
        process-fn-class (Class/forName "org.apache.flink.datastream.api.function.OneInputStreamProcessFunction")
        wrapper (reify
                  org.apache.flink.datastream.api.function.OneInputStreamProcessFunction
                  (processRecord [_ record output ctx]
                    (clj-fn record output ctx)))
        ;; Call process on the stream
        process-method (.getMethod (.getClass stream) "process"
                                   (into-array Class [process-fn-class]))]
    (.invoke process-method stream (into-array Object [wrapper]))))

(defn key-by
  "Partition a NonKeyedPartitionStream by key to create a KeyedPartitionStream.

  key-fn can be a keyword or var.

  Example:
    (v2/key-by stream :user-id)
    (v2/key-by stream #'get-key)"
  [stream key-fn]
  (ensure-v2-available! "key-by")
  (let [key-selector-fn (if (keyword? key-fn)
                          (fn [x] (get x key-fn))
                          (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var key-fn))]
                            (do
                              (require (symbol ns-str))
                              (resolve (symbol ns-str fn-name)))))
        key-selector-class (Class/forName "org.apache.flink.api.java.functions.KeySelector")
        selector (reify
                   org.apache.flink.api.java.functions.KeySelector
                   (getKey [_ value]
                     (if (keyword? key-fn)
                       (key-fn value)
                       (key-selector-fn value))))
        key-by-method (.getMethod (.getClass stream) "keyBy"
                                  (into-array Class [key-selector-class]))]
    (.invoke key-by-method stream (into-array Object [selector]))))

;; =============================================================================
;; Partitioning Operations
;; =============================================================================

(defn shuffle
  "Shuffle elements randomly across partitions.

  Example:
    (v2/shuffle stream)"
  [stream]
  (ensure-v2-available! "shuffle")
  (let [shuffle-method (.getMethod (.getClass stream) "shuffle" (into-array Class []))]
    (.invoke shuffle-method stream (into-array Object []))))

(defn broadcast
  "Broadcast stream to all downstream partitions.

  Example:
    (v2/broadcast stream)"
  [stream]
  (ensure-v2-available! "broadcast")
  (let [broadcast-method (.getMethod (.getClass stream) "broadcast" (into-array Class []))]
    (.invoke broadcast-method stream (into-array Object []))))

(defn global
  "Merge all partitions into a single partition.

  Example:
    (v2/global stream)"
  [stream]
  (ensure-v2-available! "global")
  (let [global-method (.getMethod (.getClass stream) "global" (into-array Class []))]
    (.invoke global-method stream (into-array Object []))))

;; =============================================================================
;; Sink Operations
;; =============================================================================

(defn to-sink
  "Send stream to a V2 Sink.

  Example:
    (v2/to-sink stream my-sink)"
  [stream sink]
  (ensure-v2-available! "to-sink")
  (let [to-sink-method (.getMethod (.getClass stream) "toSink"
                                   (into-array Class [Object]))]
    (.invoke to-sink-method stream (into-array Object [sink]))))

(defn print-sink
  "Create a print sink for debugging.

  Example:
    (v2/to-sink stream (v2/print-sink))"
  []
  (ensure-v2-available! "print-sink")
  (let [sink-utils-class (Class/forName "org.apache.flink.datastream.api.extension.eventtime.sinks.DataStreamV2SinkUtils")
        print-method (.getMethod sink-utils-class "print" (into-array Class []))]
    (.invoke print-method nil (into-array Object []))))

;; =============================================================================
;; Execution
;; =============================================================================

(defn execute
  "Execute the V2 DataStream job.

  Example:
    (v2/execute env \"My Job\")"
  ([env]
   (execute env "Flink Job"))
  ([env job-name]
   (ensure-v2-available! "execute")
   (let [execute-method (.getMethod (.getClass env) "execute"
                                    (into-array Class [String]))]
     (.invoke execute-method env (into-array Object [job-name])))))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defmacro with-v2-env
  "Execute body with a V2 ExecutionEnvironment bound to env-sym.

  Example:
    (v2/with-v2-env [env]
      (-> env
          (v2/from-collection [1 2 3])
          (v2/process #'double-it)
          (v2/to-sink (v2/print-sink)))
      (v2/execute env \"Test\"))"
  [[env-sym] & body]
  `(let [~env-sym (execution-env)]
     ~@body))
