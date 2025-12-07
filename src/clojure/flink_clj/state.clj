(ns flink-clj.state
  "State management for stateful stream processing.

  Flink provides several state primitives for keyed streams:
  - ValueState: Single value per key
  - ListState: List of values per key
  - MapState: Map of key-value pairs per key
  - ReducingState: Aggregated value per key
  - AggregatingState: Custom aggregation per key

  State is accessed via state descriptors in process functions.

  Example:
    (require '[flink-clj.state :as state])

    ;; Create state descriptors
    (def count-state (state/value-state \"count\" :long))
    (def items-state (state/list-state \"items\" :string))
    (def lookup-state (state/map-state \"lookup\" :string :long))

    ;; Use in a process function
    (defn my-process [ctx element]
      (let [count-val (state/get-value ctx count-state)
            new-count (inc (or count-val 0))]
        (state/update-value! ctx count-state new-count)
        new-count))"
  (:require [flink-clj.types :as types])
  (:import [org.apache.flink.api.common.state
            ValueStateDescriptor ListStateDescriptor MapStateDescriptor
            ReducingStateDescriptor AggregatingStateDescriptor
            StateTtlConfig StateTtlConfig$Builder
            StateTtlConfig$UpdateType StateTtlConfig$StateVisibility]
           [org.apache.flink.api.common.typeinfo TypeInformation]
           [java.time Duration]))

;; =============================================================================
;; Type Resolution Helper
;; =============================================================================

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (cond
    (instance? TypeInformation type-hint) type-hint
    (nil? type-hint) types/CLOJURE
    :else (types/from-spec type-hint)))

;; =============================================================================
;; TTL Configuration
;; =============================================================================

(defn- ->duration
  "Convert a TTL spec to java.time.Duration."
  [ttl]
  (cond
    (instance? Duration ttl) ttl
    (vector? ttl) (let [[n unit] ttl]
                    (case unit
                      (:ms :milliseconds) (Duration/ofMillis n)
                      (:s :seconds) (Duration/ofSeconds n)
                      (:m :minutes) (Duration/ofMinutes n)
                      (:h :hours) (Duration/ofHours n)
                      (:d :days) (Duration/ofDays n)))
    :else (throw (ex-info "Invalid TTL" {:ttl ttl}))))

(defn- create-ttl-builder
  "Create a TTL builder, handling version differences.
  Flink 1.x uses Time, Flink 2.x uses Duration."
  [duration]
  (try
    ;; Try Flink 2.x API first (accepts Duration directly)
    (let [method (.getMethod StateTtlConfig "newBuilder" (into-array Class [Duration]))]
      (.invoke method nil (into-array Object [duration])))
    (catch NoSuchMethodException _
      ;; Fall back to Flink 1.x API (uses Time)
      (let [time-class (Class/forName "org.apache.flink.api.common.time.Time")
            millis (.toMillis duration)
            of-millis (.getMethod time-class "milliseconds" (into-array Class [Long/TYPE]))
            time-obj (.invoke of-millis nil (into-array Object [(long millis)]))
            builder-method (.getMethod StateTtlConfig "newBuilder" (into-array Class [time-class]))]
        (.invoke builder-method nil (into-array Object [time-obj]))))))

(defn ttl-config
  "Create a StateTtlConfig for state expiration.

  Options:
    :ttl - Time-to-live duration (required)
           Can be [n :unit] vector or java.time.Duration
    :update-type - When to reset TTL:
                   :on-create-and-write (default)
                   :on-read-and-write
                   :disabled
    :visibility - Which state to return:
                  :never-return-expired (default)
                  :return-expired-if-not-cleaned
    :cleanup - Cleanup strategy (optional map):
               :full-snapshot true - Clean on full snapshot
               :incremental-cleanup {:access-count n} - Clean during access
               :rocksdb-compaction-filter true - Clean during RocksDB compaction

  Example:
    (ttl-config {:ttl [1 :hours]
                 :update-type :on-read-and-write})

    (ttl-config {:ttl [30 :minutes]
                 :cleanup {:incremental-cleanup {:access-count 1000}}})"
  [{:keys [ttl update-type visibility cleanup]
    :or {update-type :on-create-and-write
         visibility :never-return-expired}}]
  (let [duration (->duration ttl)
        builder (-> (create-ttl-builder duration)
                    (.setUpdateType
                      (case update-type
                        :on-create-and-write StateTtlConfig$UpdateType/OnCreateAndWrite
                        :on-read-and-write StateTtlConfig$UpdateType/OnReadAndWrite
                        :disabled StateTtlConfig$UpdateType/Disabled))
                    (.setStateVisibility
                      (case visibility
                        :never-return-expired StateTtlConfig$StateVisibility/NeverReturnExpired
                        :return-expired-if-not-cleaned StateTtlConfig$StateVisibility/ReturnExpiredIfNotCleanedUp)))]
    ;; Apply cleanup strategies
    (when cleanup
      (when (:full-snapshot cleanup)
        (.cleanupFullSnapshot builder))
      (when-let [inc-cleanup (:incremental-cleanup cleanup)]
        (.cleanupIncrementally builder
                               (:access-count inc-cleanup 1000)
                               (boolean (:run-on-access inc-cleanup true))))
      (when (:rocksdb-compaction-filter cleanup)
        (.cleanupInRocksdbCompactFilter builder)))
    (.build builder)))

;; =============================================================================
;; State Descriptors
;; =============================================================================

(defn value-state
  "Create a ValueStateDescriptor for single-value state.

  Options:
    :default - Default value when state is empty
    :ttl - TTL configuration (see ttl-config)

  Example:
    (value-state \"count\" :long)
    (value-state \"count\" :long {:default 0})
    (value-state \"data\" [:row :id :int :name :string] {:ttl {:ttl [1 :hours]}})"
  ([name type-spec]
   (value-state name type-spec nil))
  ([name type-spec {:keys [default ttl]}]
   (let [type-info (resolve-type-info type-spec)
         desc (if default
                (ValueStateDescriptor. ^String name ^TypeInformation type-info default)
                (ValueStateDescriptor. ^String name ^TypeInformation type-info))]
     (when ttl
       (.enableTimeToLive desc (if (map? ttl) (ttl-config ttl) ttl)))
     desc)))

(defn list-state
  "Create a ListStateDescriptor for list-valued state.

  Example:
    (list-state \"items\" :string)
    (list-state \"events\" [:row :ts :long :data :string])"
  ([name element-type]
   (list-state name element-type nil))
  ([name element-type {:keys [ttl]}]
   (let [type-info (resolve-type-info element-type)
         desc (ListStateDescriptor. ^String name ^TypeInformation type-info)]
     (when ttl
       (.enableTimeToLive desc (if (map? ttl) (ttl-config ttl) ttl)))
     desc)))

(defn map-state
  "Create a MapStateDescriptor for map-valued state.

  Example:
    (map-state \"lookup\" :string :long)
    (map-state \"cache\" :string [:row :value :double :ts :long])"
  ([name key-type value-type]
   (map-state name key-type value-type nil))
  ([name key-type value-type {:keys [ttl]}]
   (let [key-type-info (resolve-type-info key-type)
         value-type-info (resolve-type-info value-type)
         desc (MapStateDescriptor. ^String name
                                   ^TypeInformation key-type-info
                                   ^TypeInformation value-type-info)]
     (when ttl
       (.enableTimeToLive desc (if (map? ttl) (ttl-config ttl) ttl)))
     desc)))

(defn reducing-state
  "Create a ReducingStateDescriptor for automatically reduced state.

  reduce-fn must be a var (defined with defn) that takes two values
  and returns a combined result.

  Example:
    (defn sum [a b] (+ a b))
    (reducing-state \"total\" :long sum)"
  ([name type-spec reduce-fn]
   (reducing-state name type-spec reduce-fn nil))
  ([name type-spec reduce-fn {:keys [ttl]}]
   (let [type-info (resolve-type-info type-spec)
         ;; Note: This requires a ReduceFunction Java wrapper
         ;; For now, we'll create the descriptor with the type info
         ;; The actual ReduceFunction will be set up in the process function
         desc (ReducingStateDescriptor.
                ^String name
                (reify org.apache.flink.api.common.functions.ReduceFunction
                  (reduce [_ v1 v2]
                    (reduce-fn v1 v2)))
                ^TypeInformation type-info)]
     (when ttl
       (.enableTimeToLive desc (if (map? ttl) (ttl-config ttl) ttl)))
     desc)))

(defn aggregating-state
  "Create an AggregatingStateDescriptor for custom aggregation state.

  Unlike ReducingState (which requires same input/output type),
  AggregatingState supports different accumulator and output types.

  agg-spec is a map with:
    :create-accumulator - fn [] -> initial accumulator (required)
    :add - fn [acc input] -> updated accumulator (required)
    :get-result - fn [acc] -> output value (required)
    :merge - fn [acc1 acc2] -> merged accumulator (optional)

  Options:
    :acc-type - TypeInformation for accumulator (default: CLOJURE)
    :ttl - TTL configuration

  Example:
    ;; Average aggregator
    (defn create-avg-acc [] {:sum 0 :count 0})
    (defn add-to-avg [acc x]
      (-> acc
          (update :sum + x)
          (update :count inc)))
    (defn get-avg [acc] (/ (:sum acc) (:count acc)))
    (defn merge-avg [a b]
      {:sum (+ (:sum a) (:sum b))
       :count (+ (:count a) (:count b))})

    (aggregating-state \"average\" nil  ; nil = use CLOJURE type
      {:create-accumulator #'create-avg-acc
       :add #'add-to-avg
       :get-result #'get-avg
       :merge #'merge-avg})"
  ([name acc-type agg-spec]
   (aggregating-state name acc-type agg-spec nil))
  ([name acc-type agg-spec {:keys [ttl]}]
   (let [{:keys [create-accumulator add get-result merge]} agg-spec
         acc-type-info (resolve-type-info acc-type)
         agg-fn (reify org.apache.flink.api.common.functions.AggregateFunction
                  (createAccumulator [_]
                    (create-accumulator))
                  (add [_ value acc]
                    (add acc value))
                  (getResult [_ acc]
                    (get-result acc))
                  (merge [_ acc1 acc2]
                    (if merge
                      (merge acc1 acc2)
                      (throw (UnsupportedOperationException.
                               "merge not implemented for this aggregator")))))
         desc (AggregatingStateDescriptor.
                ^String name
                agg-fn
                ^TypeInformation acc-type-info)]
     (when ttl
       (.enableTimeToLive desc (if (map? ttl) (ttl-config ttl) ttl)))
     desc)))

;; =============================================================================
;; State Access Functions (for use in process functions)
;; =============================================================================

(defn get-state
  "Get a state object from the runtime context.

  This is typically called in the open() method of a process function
  to obtain a reference to the state.

  Example:
    ;; In process function setup
    (def my-state-ref (atom nil))
    (reset! my-state-ref (state/get-state ctx (state/value-state \"count\" :long)))"
  [runtime-ctx state-descriptor]
  (cond
    (instance? ValueStateDescriptor state-descriptor)
    (.getState runtime-ctx state-descriptor)

    (instance? ListStateDescriptor state-descriptor)
    (.getListState runtime-ctx state-descriptor)

    (instance? MapStateDescriptor state-descriptor)
    (.getMapState runtime-ctx state-descriptor)

    (instance? ReducingStateDescriptor state-descriptor)
    (.getReducingState runtime-ctx state-descriptor)

    (instance? AggregatingStateDescriptor state-descriptor)
    (.getAggregatingState runtime-ctx state-descriptor)

    :else
    (throw (ex-info "Unknown state descriptor type"
                    {:descriptor state-descriptor
                     :type (type state-descriptor)}))))

;; =============================================================================
;; ValueState Operations
;; =============================================================================

(defn value
  "Get the current value from a ValueState.

  Returns nil if the state is empty."
  [state]
  (.value state))

(defn update!
  "Update the value in a ValueState."
  [state new-value]
  (.update state new-value))

(defn clear!
  "Clear the state (works for all state types)."
  [state]
  (.clear state))

;; =============================================================================
;; ListState Operations
;; =============================================================================

(defn get-list
  "Get all values from a ListState as a Clojure sequence."
  [state]
  (when-let [iterable (.get state)]
    (iterator-seq (.iterator iterable))))

(defn add!
  "Add a value to a ListState."
  [state value]
  (.add state value))

(defn add-all!
  "Add multiple values to a ListState."
  [state values]
  (.addAll state (vec values)))

(defn update-list!
  "Replace all values in a ListState."
  [state values]
  (.update state (vec values)))

;; =============================================================================
;; MapState Operations
;; =============================================================================

(defn get-map-value
  "Get a value from a MapState by key."
  [state key]
  (.get state key))

(defn put!
  "Put a key-value pair into a MapState."
  [state key value]
  (.put state key value))

(defn put-all!
  "Put multiple key-value pairs into a MapState."
  [state m]
  (.putAll state m))

(defn remove-key!
  "Remove a key from a MapState."
  [state key]
  (.remove state key))

(defn contains-key?
  "Check if a MapState contains a key."
  [state key]
  (.contains state key))

(defn map-keys
  "Get all keys from a MapState."
  [state]
  (when-let [keys (.keys state)]
    (iterator-seq (.iterator keys))))

(defn map-values
  "Get all values from a MapState."
  [state]
  (when-let [values (.values state)]
    (iterator-seq (.iterator values))))

(defn map-entries
  "Get all entries from a MapState as [key value] pairs."
  [state]
  (when-let [entries (.entries state)]
    (for [entry (iterator-seq (.iterator entries))]
      [(.getKey entry) (.getValue entry)])))

(defn is-empty?
  "Check if a MapState is empty."
  [state]
  (.isEmpty state))

;; =============================================================================
;; ReducingState Operations
;; =============================================================================

(defn reducing-add!
  "Add a value to a ReducingState (automatically reduced)."
  [state value]
  (.add state value))

(defn reducing-get
  "Get the current reduced value from a ReducingState."
  [state]
  (.get state))

;; =============================================================================
;; AggregatingState Operations
;; =============================================================================

(defn aggregating-add!
  "Add a value to an AggregatingState.

  The value is accumulated using the add function from the aggregator spec."
  [state value]
  (.add state value))

(defn aggregating-get
  "Get the current aggregated result from an AggregatingState.

  Returns the result of applying get-result to the current accumulator."
  [state]
  (.get state))

;; =============================================================================
;; Async State Access (Flink 2.0.1+)
;; =============================================================================
;;
;; Async state operations return StateFuture objects that can be composed
;; for non-blocking state access. This significantly improves throughput
;; for state-heavy applications.
;;
;; Example:
;;   (-> (async-value my-state)
;;       (then-apply #(or % 0))
;;       (then-apply inc)
;;       (then-accept #(.collect out %)))

(defn- state-future-available?
  "Check if async state (StateFuture) is available at runtime."
  []
  (try
    (Class/forName "org.apache.flink.api.common.state.v2.StateFuture")
    true
    (catch ClassNotFoundException _ false)))

(defn async-value
  "Get value from a ValueState asynchronously.

  Returns a StateFuture that can be composed with then-apply, then-compose, etc.
  Only available in Flink 2.0.1+ with async state support.

  Example:
    (-> (async-value my-state)
        (then-apply #(or % 0))
        (then-apply inc)
        (then-accept #(.collect out %)))"
  [state]
  (when-not (state-future-available?)
    (throw (ex-info "async-value requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/value instead"})))
  (.asyncValue state))

(defn async-update!
  "Update a ValueState asynchronously.

  Returns a StateFuture<Void> for chaining.

  Example:
    (-> (async-update! state new-value)
        (then-run #(log/info \"State updated\")))"
  [state new-value]
  (when-not (state-future-available?)
    (throw (ex-info "async-update! requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/update! instead"})))
  (.asyncUpdate state new-value))

(defn async-clear!
  "Clear state asynchronously.

  Returns a StateFuture<Void>."
  [state]
  (when-not (state-future-available?)
    (throw (ex-info "async-clear! requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/clear! instead"})))
  (.asyncClear state))

;; ListState async operations

(defn async-get-list
  "Get all values from a ListState asynchronously.

  Returns StateFuture<Iterable<T>>."
  [state]
  (when-not (state-future-available?)
    (throw (ex-info "async-get-list requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/get-list instead"})))
  (.asyncGet state))

(defn async-add!
  "Add a value to a ListState asynchronously.

  Returns StateFuture<Void>."
  [state value]
  (when-not (state-future-available?)
    (throw (ex-info "async-add! requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/add! instead"})))
  (.asyncAdd state value))

;; MapState async operations

(defn async-get-map-value
  "Get a value from a MapState by key asynchronously.

  Returns StateFuture<V>."
  [state key]
  (when-not (state-future-available?)
    (throw (ex-info "async-get-map-value requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/get-map-value instead"})))
  (.asyncGet state key))

(defn async-put!
  "Put a key-value pair into a MapState asynchronously.

  Returns StateFuture<Void>."
  [state key value]
  (when-not (state-future-available?)
    (throw (ex-info "async-put! requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/put! instead"})))
  (.asyncPut state key value))

(defn async-contains?
  "Check if a MapState contains a key asynchronously.

  Returns StateFuture<Boolean>."
  [state key]
  (when-not (state-future-available?)
    (throw (ex-info "async-contains? requires Flink 2.0.1+ with async state support"
                    {:hint "Use synchronous state/contains-key? instead"})))
  (.asyncContains state key))

;; =============================================================================
;; StateFuture Composition Helpers
;; =============================================================================

(defn then-apply
  "Apply a function to the result of a StateFuture.

  Returns a new StateFuture with the transformed result.

  Example:
    (-> (async-value my-state)
        (then-apply #(* % 2))
        (then-apply #(+ % 1)))"
  [state-future f]
  (.thenApply state-future
    (reify java.util.function.Function
      (apply [_ v] (f v)))))

(defn then-compose
  "Chain StateFutures together.

  The function receives the result and must return a new StateFuture.
  Use this when the next operation also returns a StateFuture.

  Example:
    (-> (async-value state1)
        (then-compose #(async-get-map-value state2 %)))"
  [state-future f]
  (.thenCompose state-future
    (reify java.util.function.Function
      (apply [_ v] (f v)))))

(defn then-accept
  "Consume the result of a StateFuture (for side effects).

  Returns a StateFuture<Void>. Use at the end of a chain
  to emit results via the Collector.

  Example:
    (-> (async-value my-state)
        (then-accept #(.collect out %)))"
  [state-future consumer]
  (.thenAccept state-future
    (reify java.util.function.Consumer
      (accept [_ v] (consumer v)))))

(defn then-run
  "Run an action after StateFuture completes (ignores result).

  Example:
    (-> (async-update! state new-val)
        (then-run #(println \"Updated\")))"
  [state-future runnable]
  (.thenRun state-future
    (reify Runnable
      (run [_] (runnable)))))

(defn combine
  "Combine two StateFutures into one.

  Both futures execute concurrently. When both complete,
  the combine-fn is called with both results.

  Example:
    (combine (async-value count-state)
             (async-value sum-state)
             (fn [cnt sum] {:count cnt :sum sum :avg (/ sum cnt)}))"
  [future1 future2 combine-fn]
  (.thenCombine future1 future2
    (reify java.util.function.BiFunction
      (apply [_ v1 v2] (combine-fn v1 v2)))))
