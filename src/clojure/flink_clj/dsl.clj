(ns flink-clj.dsl
  "Idiomatic Clojure DSL for Flink pipelines.

  This namespace provides syntactic sugar that makes flink-clj feel more
  like native Clojure:

  - No #' required - pass functions directly
  - Transducer-style composition with `pipeline`
  - Conditional pipeline steps with `when->` and `if->`
  - Destructuring-friendly aggregations
  - `into` style sinks

  Example:
    (require '[flink-clj.dsl :as f])

    ;; No more #' - just pass the function
    (defn double-it [x] (* x 2))

    (f/run
      (f/source (f/collection [1 2 3 4 5]))
      (f/pipeline
        (f/map double-it)
        (f/filter even?)
        (f/key-by identity)
        (f/reduce +))
      (f/print))"
  (:refer-clojure :exclude [map filter reduce print])
  (:require [flink-clj.core :as core]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.window :as window]
            [flink-clj.connectors.kafka :as kafka]
            [flink-clj.connectors.hybrid :as hybrid]
            [flink-clj.types :as types]
            [clojure.string :as str])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api.datastream DataStream KeyedStream]))

;; =============================================================================
;; Auto-resolve vars from symbols/functions
;; =============================================================================

(defn- fn->var
  "Try to resolve a function to its var by parsing the class name.
  Clojure compiles functions to classes like 'clojure.core$first' or 'my.ns$my_fn'.
  Returns nil if cannot be resolved."
  [f]
  (try
    (let [class-name (.getName (class f))
          ;; Class name format: namespace$fn_name or namespace$fn_name__1234
          [_ ns-part fn-part] (re-matches #"(.+)\$([^_]+(?:_[^_]+)*)(?:__\d+)?" class-name)]
      (when (and ns-part fn-part)
        (let [ns-name (str/replace ns-part "_" "-")
              fn-name (str/replace fn-part "_" "-")]
          (when-let [ns-obj (find-ns (symbol ns-name))]
            (ns-resolve ns-obj (symbol fn-name))))))
    (catch Exception _ nil)))

(defn- resolve-fn
  "Resolve a function to a var for Flink serialization.

  Accepts:
  - Var: returned as-is
  - Symbol: resolved to var
  - Keyword: returned as-is (for key selectors)
  - IFn: tries to resolve from metadata or class name

  This eliminates the need for #' syntax in most cases."
  [f]
  (cond
    (var? f) f
    (keyword? f) f
    (symbol? f) (or (resolve f)
                    (throw (ex-info (str "Cannot resolve symbol: " f)
                                    {:symbol f})))
    ;; Try to get the var from a function's metadata or class name
    (fn? f) (or
              ;; First try metadata (user-defined functions)
              (when-let [fn-name (-> f meta :name)]
                (when-let [fn-ns (-> f meta :ns)]
                  (ns-resolve fn-ns fn-name)))
              ;; Then try resolving from class name (clojure.core functions)
              (fn->var f)
              ;; Finally give up
              (throw (ex-info
                       "Cannot serialize anonymous function. Define with defn."
                       {:hint "Use (defn my-fn [x] ...) then pass my-fn"})))
    :else f))

(defmacro defn-stream
  "Define a function that can be used in Flink pipelines.

  Like defn, but ensures the function can be resolved for serialization.

  Example:
    (defn-stream double-it [x] (* x 2))
    (f/map stream double-it)  ; No #' needed"
  [name & fdecl]
  `(defn ~name ~@fdecl))

;; =============================================================================
;; Pipeline Composition
;; =============================================================================

(defn pipeline
  "Compose multiple stream operations into a single transformation.

  Returns a function that applies all operations in sequence.

  Example:
    (def my-transform
      (pipeline
        (map parse-event)
        (filter valid?)
        (map enrich)))

    (-> stream my-transform)"
  [& ops]
  (fn [stream]
    (clojure.core/reduce (fn [s op] (op s)) stream ops)))

(defmacro defpipeline
  "Define a named pipeline composition.

  Example:
    (defpipeline word-count-transform
      (flat-map tokenize)
      (key-by first)
      (reduce sum-counts))

    ;; Use it
    (-> stream word-count-transform)"
  [name & ops]
  `(def ~name (pipeline ~@ops)))

;; =============================================================================
;; Conditional Pipeline Steps
;; =============================================================================

(defmacro when->
  "Conditional pipeline step. Applies op only when condition is true.

  Example:
    (-> stream
        (f/map parse)
        (f/when-> should-filter?
          (f/filter valid?))
        (f/map output))"
  [stream condition & ops]
  `(if ~condition
     (-> ~stream ~@ops)
     ~stream))

(defmacro if->
  "Conditional pipeline branching.

  Example:
    (-> stream
        (f/if-> use-windowing?
          (f/windowed (f/tumbling 10 :seconds))
          identity))"
  [stream condition then-op else-op]
  `(if ~condition
     (~then-op ~stream)
     (~else-op ~stream)))

(defmacro cond->stream
  "Multiple conditional pipeline steps.

  Example:
    (-> stream
        (f/cond->stream
          should-filter? (f/filter valid?)
          should-enrich? (f/map enrich)
          debug-mode?    (f/map log-event)))"
  [stream & clauses]
  (let [pairs (partition 2 clauses)]
    `(-> ~stream
         ~@(for [[condition op] pairs]
             `(when-> ~condition ~op)))))

;; =============================================================================
;; Stream Operations (no #' needed)
;; =============================================================================

(defn map
  "Transform each element. No #' needed.

  Example:
    (defn double-it [x] (* x 2))
    (f/map stream double-it)"
  ([f] (fn [stream] (map stream f)))
  ([stream f]
   (stream/flink-map stream (resolve-fn f)))
  ([stream f opts]
   (stream/flink-map stream (resolve-fn f) opts)))

(defn filter
  "Keep elements matching predicate. No #' needed.

  Example:
    (defn valid? [x] (pos? (:value x)))
    (f/filter stream valid?)"
  ([pred] (fn [stream] (filter stream pred)))
  ([stream pred]
   (stream/flink-filter stream (resolve-fn pred)))
  ([stream pred opts]
   (stream/flink-filter stream (resolve-fn pred) opts)))

(defn flat-map
  "Transform each element to zero or more elements. No #' needed.

  Example:
    (defn tokenize [s] (str/split s #\" \"))
    (f/flat-map stream tokenize)"
  ([f] (fn [stream] (flat-map stream f)))
  ([stream f]
   (stream/flat-map stream (resolve-fn f)))
  ([stream f opts]
   (stream/flat-map stream (resolve-fn f) opts)))

(defn key-by
  "Partition stream by key.

  Example:
    (f/key-by stream :user-id)
    (f/key-by stream first)"
  ([key-fn] (fn [stream] (key-by stream key-fn)))
  ([stream key-fn]
   (keyed/key-by stream (resolve-fn key-fn)))
  ([stream key-fn opts]
   (keyed/key-by stream (resolve-fn key-fn) opts)))

(defn reduce
  "Rolling reduce on keyed stream. No #' needed.

  Example:
    (defn sum-counts [[k c1] [_ c2]] [k (+ c1 c2)])
    (-> stream
        (f/key-by first)
        (f/reduce sum-counts))"
  ([f] (fn [stream] (reduce stream f)))
  ([stream f]
   (if (instance? KeyedStream stream)
     (keyed/flink-reduce stream (resolve-fn f))
     (window/flink-reduce stream (resolve-fn f))))
  ([stream f opts]
   (if (instance? KeyedStream stream)
     (keyed/flink-reduce stream (resolve-fn f) opts)
     (window/flink-reduce stream (resolve-fn f) opts))))

;; =============================================================================
;; Aggregations with Clojure maps
;; =============================================================================

(defn aggregate
  "Aggregate with a spec map. Feels like Clojure reducers.

  Example:
    (f/aggregate stream
      {:init {}
       :add (fn [acc x] (update acc :count (fnil inc 0)))
       :result identity
       :merge (fn [a b] (merge-with + a b))})"
  ([spec] (fn [stream] (aggregate stream spec)))
  ([stream {:keys [init add result merge] :as spec}]
   (let [resolved-spec {:create-accumulator (if (fn? init)
                                               (resolve-fn init)
                                               (constantly init))
                        :add (resolve-fn add)
                        :get-result (resolve-fn result)
                        :merge (when merge (resolve-fn merge))}]
     (window/aggregate stream resolved-spec))))

;; =============================================================================
;; Windows (cleaner syntax)
;; =============================================================================

(defn tumbling
  "Create tumbling window specification.

  Example:
    (f/tumbling 10 :seconds)
    (f/tumbling 1 :minutes)"
  [n unit]
  {:type :tumbling
   :size (case unit
           (:ms :milliseconds) (window/milliseconds n)
           (:s :seconds :second) (window/seconds n)
           (:m :minutes :minute) (window/minutes n)
           (:h :hours :hour) (window/hours n)
           (:d :days :day) (window/days n))})

(defn sliding
  "Create sliding window specification.

  Example:
    (f/sliding 1 :minutes 10 :seconds)"
  [size size-unit slide slide-unit]
  {:type :sliding
   :size (tumbling size size-unit)
   :slide (tumbling slide slide-unit)})

(defn session
  "Create session window specification.

  Example:
    (f/session 5 :minutes)"
  [gap unit]
  {:type :session
   :gap (tumbling gap unit)})

(defn count-tumbling
  "Create count-based tumbling window specification.

  Example:
    (f/count-tumbling 100)  ; Window every 100 elements"
  [size]
  {:type :count-tumbling
   :size size})

(defn count-sliding
  "Create count-based sliding window specification.

  Example:
    (f/count-sliding 100 10)  ; Window of 100, slide by 10"
  [size slide]
  {:type :count-sliding
   :size size
   :slide slide})

(defn dynamic-session
  "Create dynamic gap session window specification.

  gap-fn takes an element and returns gap duration in milliseconds.

  Example:
    (defn get-gap [event]
      (case (:priority event)
        :high 5000
        :low 60000
        30000))

    (f/dynamic-session get-gap)"
  [gap-fn]
  {:type :dynamic-session
   :gap-fn gap-fn})

(defn windowed
  "Apply window to keyed stream.

  Example:
    (-> stream
        (f/key-by :user-id)
        (f/windowed (f/tumbling 10 :seconds))
        (f/reduce merge-fn))"
  ([window-spec] (fn [stream] (windowed stream window-spec)))
  ([stream {:keys [type size slide gap gap-fn] :as spec}]
   (case type
     :tumbling (window/tumbling-processing-time stream (:size size))
     :sliding (window/sliding-processing-time stream
                                               (:size (:size spec))
                                               (:size (:slide spec)))
     :session (window/session-processing-time stream (:size (:gap spec)))
     :count-tumbling (window/count-window stream size)
     :count-sliding (window/count-window-sliding stream size slide)
     :dynamic-session (window/dynamic-session-processing-time stream (resolve-fn gap-fn)))))

;; =============================================================================
;; Sources
;; =============================================================================

(defn collection
  "Create source spec from collection.

  Example:
    (f/source env (f/collection [1 2 3 4 5]))"
  [coll]
  {:type :collection :data coll})

(defn kafka-source
  "Create Kafka source spec.

  Example:
    (f/source env (f/kafka-source
                    {:servers \"localhost:9092\"
                     :topic \"events\"
                     :group \"my-group\"}))"
  [{:keys [servers topic topics group from format]
    :or {from :latest format :string}}]
  {:type :kafka
   :config {:bootstrap-servers servers
            :topics (or topics [topic])
            :group-id group
            :starting-offsets from
            :value-format format}})

(defn hybrid-source
  "Create a hybrid source that chains bounded then unbounded sources.

  Example:
    (f/source env (f/hybrid-source
                    {:bounded file-source
                     :unbounded kafka-source}))"
  [{:keys [bounded unbounded]}]
  {:type :hybrid
   :config {:bounded bounded
            :unbounded unbounded}})

(defn source
  "Create a source stream.

  Example:
    (f/source env (f/collection [1 2 3]))
    (f/source env (f/kafka-source {...}))
    (f/source env (f/hybrid-source {:bounded ... :unbounded ...}))"
  [env spec]
  (case (:type spec)
    :collection (core/from-collection env (:data spec) (:opts spec))
    :kafka (let [src (kafka/source (:config spec))]
             (core/from-source env src "Kafka Source"))
    :hybrid (let [src (hybrid/backfill-then-stream (:config spec))]
              (core/from-source env src "Hybrid Source"))))

;; =============================================================================
;; Sinks
;; =============================================================================

(defn into-kafka
  "Send stream to Kafka topic.

  Example:
    (-> stream
        (f/map to-string)
        (f/into-kafka {:servers \"localhost:9092\"
                       :topic \"output\"}))"
  ([config] (fn [stream] (into-kafka stream config)))
  ([stream {:keys [servers topic format guarantee]
            :or {format :string guarantee :at-least-once}}]
   (let [sink (kafka/sink {:bootstrap-servers servers
                           :topic topic
                           :value-format format
                           :delivery-guarantee guarantee})]
     (core/to-sink stream sink "Kafka Sink"))))

(defn print
  "Print stream to stdout.

  Example:
    (-> stream (f/print))"
  ([] (fn [stream] (print stream)))
  ([stream]
   (stream/flink-print stream)))

;; =============================================================================
;; Environment & Execution
;; =============================================================================

(defn env
  "Create execution environment.

  Example:
    (f/env)
    (f/env {:parallelism 4})"
  ([] (env {}))
  ([opts]
   (-> (env/create-env opts)
       (core/register-clojure-types!))))

(defn run
  "Execute the pipeline.

  Example:
    (f/run env \"My Job\")"
  ([env]
   (core/execute env))
  ([env job-name]
   (core/execute env job-name)))

;; =============================================================================
;; Let-style bindings for complex pipelines
;; =============================================================================

(defmacro let-streams
  "Bind intermediate streams for complex pipelines.

  Example:
    (f/let-streams [parsed (f/map raw-stream parse-event)
                    valid (f/filter parsed valid?)
                    enriched (f/map valid enrich)]
      (f/print enriched))"
  [bindings & body]
  `(let ~bindings ~@body))

;; =============================================================================
;; Tapping / Side effects
;; =============================================================================

(defn tap
  "Apply side effect without changing stream (for logging/debugging).

  Example:
    (-> stream
        (f/tap #(println \"Processing:\" %))
        (f/map transform))"
  ([f] (fn [stream] (tap stream f)))
  ([stream f]
   (let [tap-fn (fn [x] (f x) x)]
     (stream/flink-map stream (resolve-fn tap-fn)))))

;; =============================================================================
;; Naming and Configuration
;; =============================================================================

(defn named
  "Name the preceding operator (shown in Flink UI).

  Example:
    (-> stream
        (f/map transform)
        (f/named \"Parse Events\"))"
  ([op-name] (fn [stream] (named stream op-name)))
  ([stream op-name]
   (stream/flink-name stream op-name)))

(defn with-parallelism
  "Set parallelism for preceding operator.

  Example:
    (-> stream
        (f/map heavy-computation)
        (f/with-parallelism 8))"
  ([n] (fn [stream] (with-parallelism stream n)))
  ([stream n]
   (stream/set-parallelism stream n)))

(defn with-uid
  "Set UID for state recovery.

  Example:
    (-> stream
        (f/reduce aggregate-fn)
        (f/with-uid \"aggregation-v1\"))"
  ([uid] (fn [stream] (with-uid stream uid)))
  ([stream uid]
   (stream/uid stream uid)))
