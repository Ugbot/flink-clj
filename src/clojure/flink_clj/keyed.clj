(ns flink-clj.keyed
  "KeyedStream operations.

  Functions for partitioning streams by key and performing
  keyed aggregations.

  Type hints:
    Use :key-type and :returns options for efficient native serialization.

    (require '[flink-clj.types :as t])
    (-> stream
        (key-by :user-id {:key-type :string})
        (flink-reduce my-reducer {:returns t/LONG}))

  Async State (Flink 2.x only):
    Enable async state for non-blocking state access in process functions.

    (require '[flink-clj.keyed :as k]
             '[flink-clj.process :as p]
             '[flink-clj.state :as state])

    (-> stream
        (k/key-by :user-id)
        (k/enable-async-state)  ; Flink 2.x only
        (p/keyed-process {:process my-async-process-fn}))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types]
            [flink-clj.version :as v])
  (:import [org.apache.flink.streaming.api.datastream DataStream KeyedStream SingleOutputStreamOperator]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation.
  Accepts TypeInformation directly or a spec for from-spec."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

(defn- apply-returns
  "Apply :returns type hint to operator if provided."
  [^SingleOutputStreamOperator op returns]
  (if returns
    (.returns op ^TypeInformation (resolve-type-info returns))
    op))

(defn key-by
  "Partition stream by key.

  key-fn can be:
  - A keyword: (key-by stream :user-id)
  - A var: (key-by stream my-key-fn)
  - Built-in: first, second, etc.

  Options:
    :key-type - TypeInformation or spec for the key type
                Enables efficient key serialization.

  Example:
    (key-by stream :user-id)
    (key-by stream :user-id {:key-type :string})
    (key-by stream first)  ; for tuples/vectors"
  ([^DataStream stream key-fn]
   (key-by stream key-fn nil))
  ([^DataStream stream key-fn {:keys [key-type]}]
   (let [selector (impl/make-key-selector key-fn)]
     (if key-type
       (.keyBy stream selector ^TypeInformation (resolve-type-info key-type))
       (.keyBy stream selector)))))

(defn flink-reduce
  "Apply a rolling reduce function on keyed stream.

  reduce-fn takes two arguments and returns the combined result.
  Must be a var (defined with defn).

  Options:
    :returns - TypeInformation or spec for output type
               Enables efficient native serialization.

  Example:
    (defn sum-counts [[w1 c1] [w2 c2]]
      [w1 (+ c1 c2)])

    (-> stream
        (key-by first)
        (flink-reduce sum-counts))

    ;; With type hint
    (-> stream
        (key-by first {:key-type :string})
        (flink-reduce sum-counts {:returns [:tuple :string :long]}))"
  ([^KeyedStream stream reduce-fn]
   (flink-reduce stream reduce-fn nil))
  ([^KeyedStream stream reduce-fn {:keys [returns]}]
   (let [reducer (impl/make-reduce-function reduce-fn)]
     (-> (.reduce stream reducer)
         (apply-returns returns)))))

(defn sum
  "Rolling sum on a field (for tuples/POJOs).

  field can be:
  - An integer index (for tuples)
  - A string field name (for POJOs)

  Example:
    (sum keyed-stream 1)  ; Sum field at index 1
    (sum keyed-stream \"count\")  ; Sum 'count' field"
  [^KeyedStream stream field]
  (if (integer? field)
    (.sum stream (int field))
    (.sum stream (str field))))

(defn flink-min
  "Rolling minimum on a field.

  Returns the element with the minimum value for the field.

  Example:
    (flink-min keyed-stream 1)
    (flink-min keyed-stream \"timestamp\")"
  [^KeyedStream stream field]
  (if (integer? field)
    (.min stream (int field))
    (.min stream (str field))))

(defn flink-max
  "Rolling maximum on a field.

  Returns the element with the maximum value for the field.

  Example:
    (flink-max keyed-stream 1)
    (flink-max keyed-stream \"value\")"
  [^KeyedStream stream field]
  (if (integer? field)
    (.max stream (int field))
    (.max stream (str field))))

(defn min-by
  "Rolling minimum by field - returns the entire element.

  Unlike min which may combine fields, min-by returns the complete
  element that has the minimum value for the specified field.

  Example:
    (min-by keyed-stream \"timestamp\")"
  [^KeyedStream stream field]
  (if (integer? field)
    (.minBy stream (int field))
    (.minBy stream (str field))))

(defn max-by
  "Rolling maximum by field - returns the entire element.

  Unlike max which may combine fields, max-by returns the complete
  element that has the maximum value for the specified field.

  Example:
    (max-by keyed-stream \"value\")"
  [^KeyedStream stream field]
  (if (integer? field)
    (.maxBy stream (int field))
    (.maxBy stream (str field))))

;; =============================================================================
;; Async State Support (Flink 2.x only)
;; =============================================================================

(defn- async-state-available?
  "Check if async state is available (Flink 2.x with State V2 API)."
  []
  (and (v/flink-2?)
       (try
         (Class/forName "org.apache.flink.api.common.state.v2.StateFuture")
         true
         (catch ClassNotFoundException _ false))))

(defn enable-async-state
  "Enable asynchronous state access for the keyed stream.

  When enabled, process functions can use async state operations
  (async-value, async-update!, etc.) for non-blocking state access.
  This can significantly improve throughput for state-heavy applications.

  Requires:
  - Flink 2.x (State V2 API)
  - ForSt or async-enabled state backend

  IMPORTANT: When async state is enabled, only use State V2 async APIs
  (async-value, async-update!, etc.) in your process function.
  Do not mix synchronous and asynchronous state access.

  Example:
    (require '[flink-clj.keyed :as k]
             '[flink-clj.process :as p]
             '[flink-clj.state :as state])

    (defn my-process [ctx element out]
      (let [count-state (.getState (.getRuntimeContext ctx) count-desc)]
        (-> (state/async-value count-state)
            (state/then-apply #(or % 0))
            (state/then-apply inc)
            (state/then-accept
              (fn [new-count]
                (state/async-update! count-state new-count)
                (.collect out {:element element :count new-count}))))))

    (-> stream
        (k/key-by :user-id)
        (k/enable-async-state)
        (p/keyed-process {:process #'my-process}))"
  [^KeyedStream stream]
  (if (async-state-available?)
    (.enableAsyncState stream)
    (throw (ex-info "enable-async-state requires Flink 2.x with State V2 API"
                    {:flink-version (v/flink-minor-version)
                     :hint "This feature is only available in Flink 2.x"}))))
