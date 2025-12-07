(ns flink-clj.stream
  "DataStream transformation operations.

  All functions take a DataStream as the first argument and return a DataStream,
  enabling thread-first composition with ->.

  Type hints:
    Use :returns option with TypeInformation or spec to enable efficient native
    serialization instead of falling back to Nippy/Kryo.

    (require '[flink-clj.types :as t])
    (-> stream
        (flink-map transform-fn {:returns t/LONG})
        (flink-filter pred-fn)
        (flat-map expand-fn {:returns [:list :string]}))

  Example:
    (-> stream
        (flink-map transform-fn)
        (flink-filter pred-fn)
        (flat-map expand-fn))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.datastream DataStream SingleOutputStreamOperator]
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

(defn flink-map
  "Apply a function to each element of the stream.

  f must be a var (defined with defn) for proper serialization.

  Options:
    :returns - TypeInformation or spec for output type
               Enables efficient native serialization.

  Example:
    (defn double-value [x] (* x 2))
    (flink-map stream double-value)
    (flink-map stream double-value {:returns :long})"
  ([^DataStream stream f]
   (flink-map stream f nil))
  ([^DataStream stream f {:keys [returns]}]
   (let [wrapper (impl/make-map-function f)]
     (-> (.map stream wrapper)
         (apply-returns returns)))))

(defn flink-filter
  "Keep only elements where (pred element) is truthy.

  pred must be a var (defined with defn) for proper serialization.

  Options:
    :returns - TypeInformation or spec for element type
               Usually not needed (type propagates from input).

  Example:
    (defn even-value? [x] (even? (:value x)))
    (flink-filter stream even-value?)"
  ([^DataStream stream pred]
   (flink-filter stream pred nil))
  ([^DataStream stream pred {:keys [returns]}]
   (let [wrapper (impl/make-filter-function pred)]
     (-> (.filter stream wrapper)
         (apply-returns returns)))))

(defn flat-map
  "Apply a function that returns a sequence to each element.

  Each element in the returned sequence becomes a separate output element.
  f must be a var (defined with defn) for proper serialization.

  Options:
    :returns - TypeInformation or spec for output element type
               Enables efficient native serialization.

  Example:
    (defn tokenize [line] (str/split line #\"\\s+\"))
    (flat-map stream tokenize)
    (flat-map stream tokenize {:returns :string})"
  ([^DataStream stream f]
   (flat-map stream f nil))
  ([^DataStream stream f {:keys [returns]}]
   (let [wrapper (impl/make-flat-map-function f)]
     (-> (.flatMap stream wrapper)
         (apply-returns returns)))))

(defn returns
  "Set the output TypeInformation for an operator.

  Use this to specify the output type for efficient native serialization
  instead of falling back to Nippy/Kryo.

  type-info can be:
  - A TypeInformation instance (from flink-clj.types)
  - A spec keyword like :string, :long, :double
  - A spec vector like [:row :id :int :name :string]

  Example:
    (require '[flink-clj.types :as t])

    ;; Using TypeInformation directly
    (-> stream
        (map my-fn)
        (returns t/LONG))

    ;; Using spec
    (-> stream
        (map my-fn)
        (returns [:row :word :string :count :long]))"
  [^SingleOutputStreamOperator stream type-info]
  (.returns stream ^TypeInformation (resolve-type-info type-info)))

(defn union
  "Combine this stream with one or more other streams.

  All streams must have the same type.

  Example:
    (union stream1 stream2 stream3)"
  [^DataStream stream & streams]
  (.union stream (into-array DataStream streams)))

(defn flink-name
  "Set a name for this operator (shown in Flink UI).

  Example:
    (-> stream
        (flink-map my-fn)
        (flink-name \"My Transform\"))"
  [^SingleOutputStreamOperator stream ^String op-name]
  (.name stream op-name))

(defn uid
  "Set a unique ID for this operator.

  Important for state recovery - the same UID should be used across job restarts.

  Example:
    (-> stream
        (map my-fn)
        (uid \"my-transform-v1\"))"
  [^SingleOutputStreamOperator stream ^String id]
  (.uid stream id))

(defn set-parallelism
  "Set the parallelism for this operator.

  Example:
    (-> stream
        (map heavy-fn)
        (set-parallelism 8))"
  [^SingleOutputStreamOperator stream parallelism]
  (.setParallelism stream parallelism))

(defn rebalance
  "Round-robin redistribute elements across parallel instances.

  Use when upstream operators are skewed and you want even distribution.

  Example:
    (-> stream
        (rebalance)
        (map compute-intensive-fn))"
  [^DataStream stream]
  (.rebalance stream))

(defn broadcast
  "Broadcast all elements to all downstream parallel instances.

  Use carefully - this replicates all data.

  Example:
    (broadcast stream)"
  [^DataStream stream]
  (.broadcast stream))

(defn flink-shuffle
  "Randomly redistribute elements.

  Example:
    (flink-shuffle stream)"
  [^DataStream stream]
  (.shuffle stream))

(defn rescale
  "Rescale partitioning - round-robin to subset of downstream.

  More efficient than rebalance when parallelism changes.

  Example:
    (rescale stream)"
  [^DataStream stream]
  (.rescale stream))

(defn forward
  "Forward elements to the same downstream instance.

  Only works when parallelism matches.

  Example:
    (forward stream)"
  [^DataStream stream]
  (.forward stream))

(defn global
  "Send all elements to a single downstream instance.

  Use carefully - creates a bottleneck.

  Example:
    (global stream)"
  [^DataStream stream]
  (.global stream))

(defn flink-print
  "Print stream elements to stdout (for debugging).

  Example:
    (flink-print stream)
    (flink-print stream \"prefix: \")"
  ([^DataStream stream]
   (.print stream))
  ([^DataStream stream ^String prefix]
   (-> stream
       (.print)
       (.setParallelism 1)
       (.name (str "Print: " prefix)))))

(defn get-side-output
  "Get a side output stream by its OutputTag.

  Used to retrieve data sent to side outputs via ProcessFunction
  or other operators that support side outputs.

  Example:
    (require '[flink-clj.process :as p])

    ;; Create an output tag
    (def late-data-tag (p/output-tag \"late-data\" :string))

    ;; In your process function, emit to side output:
    ;; (.output ctx late-data-tag value)

    ;; Retrieve the side output stream
    (def late-stream (get-side-output main-stream late-data-tag))"
  [^SingleOutputStreamOperator stream output-tag]
  (.getSideOutput stream output-tag))

;; =============================================================================
;; Operator Chaining Control
;; =============================================================================

(defn start-new-chain
  "Start a new operator chain from this operator.

  Operator chaining combines multiple operators into a single task
  for better performance. Use this to break the chain at specific points.

  Example:
    (-> stream
        (flink-map heavy-compute)
        (start-new-chain)
        (flink-filter pred))"
  [^SingleOutputStreamOperator stream]
  (.startNewChain stream))

(defn disable-chaining
  "Disable operator chaining for this operator.

  The operator won't be chained with preceding or subsequent operators.
  Useful for debugging or ensuring operator isolation.

  Example:
    (-> stream
        (flink-map heavy-compute)
        (disable-chaining)
        (flink-filter pred))"
  [^SingleOutputStreamOperator stream]
  (.disableChaining stream))

(defn slot-sharing-group
  "Set the slot sharing group for this operator.

  Operators in the same slot sharing group can share task slots.
  Use to isolate operators that need dedicated resources.

  Example:
    (-> stream
        (flink-map compute-fn)
        (slot-sharing-group \"heavy-compute\"))"
  [^SingleOutputStreamOperator stream ^String group-name]
  (.slotSharingGroup stream group-name))

;; =============================================================================
;; Custom Partitioning
;; =============================================================================

(defn partition-custom
  "Partition stream elements using a custom partitioner.

  partitioner-fn takes an element and returns an integer partition index.
  The partition index should be in range [0, num-partitions).

  key-fn extracts the key to partition on (keyword or var).

  Options:
    :key-type - TypeInformation for the key

  Example:
    (defn my-partitioner [key num-partitions]
      (mod (hash key) num-partitions))

    ;; Partition by user-id using custom logic
    (partition-custom stream :user-id #'my-partitioner)"
  ([^DataStream stream key-fn partitioner-fn]
   (partition-custom stream key-fn partitioner-fn nil))
  ([^DataStream stream key-fn partitioner-fn {:keys [key-type]}]
   (let [key-selector (impl/make-key-selector key-fn)
         partitioner (reify org.apache.flink.api.common.functions.Partitioner
                       (partition [_ key num-partitions]
                         (int (partitioner-fn key num-partitions))))]
     (if key-type
       (.partitionCustom stream partitioner key-selector
                         ^TypeInformation (resolve-type-info key-type))
       (.partitionCustom stream partitioner key-selector)))))
