(ns flink-clj.watermark
  "Watermark strategies for event-time processing.

  Watermarks are markers that indicate progress of event time in a stream.
  They allow Flink to reason about completeness of time-based windows.

  Example:
    (require '[flink-clj.watermark :as wm])

    ;; For strictly ascending timestamps
    (def strategy (wm/for-monotonous-timestamps))

    ;; For out-of-order events (up to 5 seconds late)
    (def strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                      (wm/with-timestamp-assigner :event-time)))

    ;; Use with from-source
    (from-source env source \"Events\" {:watermark-strategy strategy})

    ;; Apply to existing stream
    (-> stream
        (wm/assign-timestamps-and-watermarks
          (-> (wm/for-bounded-out-of-orderness [5 :seconds])
              (wm/with-timestamp-assigner :event-time))))

    ;; Custom watermark generator
    (-> (wm/custom-generator {:on-event my-on-event :on-periodic my-emit})
        (wm/with-timestamp-assigner :timestamp))"
  (:require [flink-clj.impl.functions :as impl])
  (:import [org.apache.flink.streaming.api.datastream DataStream SingleOutputStreamOperator]
           [org.apache.flink.api.common.eventtime
            WatermarkStrategy WatermarkGenerator WatermarkOutput Watermark
            TimestampAssigner TimestampAssignerSupplier WatermarkGeneratorSupplier]
           [java.time Duration]
           [flink_clj CljWatermarkGenerator]))

;; =============================================================================
;; Duration Helpers
;; =============================================================================

(defn- to-duration
  "Convert duration spec to java.time.Duration."
  [spec]
  (cond
    (instance? Duration spec) spec
    (number? spec) (Duration/ofMillis spec)
    (vector? spec)
    (let [[n unit] spec]
      (case unit
        (:ms :milliseconds) (Duration/ofMillis n)
        (:s :seconds) (Duration/ofSeconds n)
        (:m :minutes) (Duration/ofMinutes n)
        (:h :hours) (Duration/ofHours n)
        (:d :days) (Duration/ofDays n)
        (throw (ex-info "Unknown duration unit" {:unit unit}))))
    :else
    (throw (ex-info "Invalid duration spec" {:spec spec}))))

;; =============================================================================
;; Built-in Watermark Strategies
;; =============================================================================

(defn no-watermarks
  "Create a WatermarkStrategy that doesn't generate watermarks.

  Use for processing-time semantics or when timestamps don't matter.

  Example:
    (no-watermarks)"
  []
  (WatermarkStrategy/noWatermarks))

(defn for-monotonous-timestamps
  "Create a WatermarkStrategy for strictly ascending timestamps.

  Assumes timestamps never decrease. Efficient but doesn't handle
  out-of-order events.

  Example:
    (for-monotonous-timestamps)"
  []
  (WatermarkStrategy/forMonotonousTimestamps))

(defn for-bounded-out-of-orderness
  "Create a WatermarkStrategy that allows events to be late.

  max-delay specifies the maximum expected out-of-orderness.
  Events arriving later than max-delay after the watermark are dropped.

  max-delay can be:
  - A number (milliseconds)
  - A Duration
  - A vector like [5 :seconds] or [100 :ms]

  Example:
    (for-bounded-out-of-orderness 5000)                ; 5 seconds
    (for-bounded-out-of-orderness [5 :seconds])        ; 5 seconds
    (for-bounded-out-of-orderness (Duration/ofSeconds 5))"
  [max-delay]
  (WatermarkStrategy/forBoundedOutOfOrderness (to-duration max-delay)))

(defn for-generator
  "Create a WatermarkStrategy from a custom watermark generator function.

  generator-fn is called with (env context) and should return a WatermarkGenerator.
  For simple cases, use the built-in strategies instead.

  Example:
    (for-generator
      (fn [ctx]
        (reify WatermarkGenerator
          (onEvent [_ event timestamp output]
            ;; Handle event
            )
          (onPeriodicEmit [_ output]
            ;; Emit watermark periodically
            ))))"
  [generator-fn]
  (WatermarkStrategy/forGenerator
    (reify org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier
      (createWatermarkGenerator [_ ctx]
        (generator-fn ctx)))))

;; =============================================================================
;; Timestamp Assigners
;; =============================================================================

(defn with-timestamp-assigner
  "Add a timestamp assigner to a WatermarkStrategy.

  extract-fn can be:
  - A keyword: extracts that field from the element (for maps)
  - A function: called with (element previous-timestamp) -> long timestamp

  Note: For functions, they should be top-level vars for serialization.

  Example:
    ;; Using keyword for maps
    (-> (for-bounded-out-of-orderness [5 :seconds])
        (with-timestamp-assigner :timestamp))

    ;; Using function
    (defn extract-ts [event _] (:event-time event))
    (-> (for-bounded-out-of-orderness [5 :seconds])
        (with-timestamp-assigner extract-ts))"
  [^WatermarkStrategy strategy extract-fn]
  (let [assigner (cond
                   ;; Keyword - extract field from map
                   (keyword? extract-fn)
                   (reify TimestampAssigner
                     (extractTimestamp [_ element _]
                       (long (get element extract-fn))))

                   ;; Function - call directly
                   (fn? extract-fn)
                   (reify TimestampAssigner
                     (extractTimestamp [_ element prev-ts]
                       (long (extract-fn element prev-ts))))

                   :else
                   (throw (ex-info "extract-fn must be keyword or function"
                                   {:extract-fn extract-fn})))]
    (.withTimestampAssigner strategy
      (reify TimestampAssignerSupplier
        (createTimestampAssigner [_ _ctx]
          assigner)))))

(defn with-idleness
  "Configure the strategy to handle idle sources.

  When a source partition is idle for longer than the timeout,
  it's marked as temporarily idle and won't hold back watermarks.

  Example:
    (-> (for-bounded-out-of-orderness [5 :seconds])
        (with-idleness [1 :minutes]))"
  [^WatermarkStrategy strategy timeout]
  (.withIdleness strategy (to-duration timeout)))

;; =============================================================================
;; Watermark Utilities
;; =============================================================================

(defn watermark
  "Create a Watermark with the given timestamp.

  Example:
    (watermark 1640000000000)"
  [timestamp]
  (Watermark. timestamp))

(def ^Watermark max-watermark
  "The maximum watermark, indicating end of stream."
  Watermark/MAX_WATERMARK)

;; =============================================================================
;; Assign Watermarks to DataStream
;; =============================================================================

(defn assign-timestamps-and-watermarks
  "Assign timestamps and watermarks to a DataStream.

  Use this when you need to apply watermark strategy after the source,
  for example after filtering or transforming data.

  Note: Prefer assigning watermarks at the source when possible.
  This method overwrites any existing timestamps/watermarks.

  Example:
    (-> stream
        (flink-filter valid-event?)
        (assign-timestamps-and-watermarks
          (-> (for-bounded-out-of-orderness [5 :seconds])
              (with-timestamp-assigner :event-time))))"
  [^DataStream stream ^WatermarkStrategy strategy]
  (.assignTimestampsAndWatermarks stream strategy))

;; =============================================================================
;; Custom Watermark Generators
;; =============================================================================

(defn custom-generator
  "Create a WatermarkStrategy with custom Clojure functions.

  Uses CljWatermarkGenerator which delegates to Clojure functions.
  Functions must be top-level vars (defn) for serialization.

  handlers map:
    :on-event    - (fn [ctx] ...) -> long or nil
                   Called for each event. ctx contains:
                     :element          - The incoming element
                     :timestamp        - Event timestamp
                     :current-watermark - Current watermark value
                   Return: new watermark value (long) or nil for no update

    :on-periodic - (fn [ctx] ...) -> long or nil
                   Called periodically. ctx contains:
                     :current-watermark - Current watermark value
                   Return: watermark to emit (long) or nil for no emission

  Either handler can be nil:
  - If :on-event is nil, tracks max timestamp automatically
  - If :on-periodic is nil, emits current watermark periodically

  Example:
    ;; Track max timestamp with 1-second delay
    (defn on-event [{:keys [timestamp current-watermark]}]
      (max current-watermark (- timestamp 1000)))

    (defn on-periodic [{:keys [current-watermark]}]
      current-watermark)

    (def strategy
      (-> (custom-generator {:on-event #'on-event
                             :on-periodic #'on-periodic})
          (with-timestamp-assigner :event-time)))"
  [{:keys [on-event on-periodic]}]
  (let [[on-event-ns on-event-name] (when on-event (impl/fn->ns-name on-event))
        [on-periodic-ns on-periodic-name] (when on-periodic (impl/fn->ns-name on-periodic))
        ;; Use same namespace for both, or pick available one
        ns-str (or on-event-ns on-periodic-ns)]
    (WatermarkStrategy/forGenerator
      (reify WatermarkGeneratorSupplier
        (createWatermarkGenerator [_ _ctx]
          (CljWatermarkGenerator. ns-str on-event-name on-periodic-name))))))

(defn periodic-generator
  "Create a WatermarkStrategy that emits watermarks periodically.

  on-event-fn receives context map {:element :timestamp :current-watermark}
  and should return the new watermark value (or nil to not update).

  Watermarks are emitted at the interval configured in ExecutionConfig
  (default 200ms, configure with env/set-auto-watermark-interval!).

  Example:
    ;; Track max timestamp minus 5 seconds
    (defn track-max-minus-delay [{:keys [timestamp current-watermark]}]
      (max current-watermark (- timestamp 5000)))

    (def strategy
      (-> (periodic-generator #'track-max-minus-delay)
          (with-timestamp-assigner :event-time)))"
  [on-event-fn]
  (custom-generator {:on-event on-event-fn :on-periodic nil}))

(defn punctuated-generator
  "Create a WatermarkStrategy that emits watermarks on special events.

  on-event-fn receives context map {:element :timestamp :current-watermark}
  and should return a watermark value to emit immediately, or nil.

  Use for streams with explicit watermark markers or when every event
  should potentially trigger a watermark.

  WARNING: Emitting watermarks on every event can degrade performance.
  Only emit when necessary.

  Example:
    ;; Emit watermark when seeing a special marker event
    (defn check-marker [{:keys [element timestamp]}]
      (when (:is-watermark-marker element)
        timestamp))

    (def strategy
      (-> (punctuated-generator #'check-marker)
          (with-timestamp-assigner :event-time)))"
  [on-event-fn]
  (let [[ns-str fn-name] (impl/fn->ns-name on-event-fn)]
    (WatermarkStrategy/forGenerator
      (reify WatermarkGeneratorSupplier
        (createWatermarkGenerator [_ _ctx]
          ;; Create a punctuated generator that emits immediately on event
          (let [current-watermark (atom Long/MIN_VALUE)]
            (reify WatermarkGenerator
              (onEvent [_ element timestamp output]
                (let [clj-fn (impl/resolve-fn ns-str fn-name)
                      ctx {:element element
                           :timestamp timestamp
                           :current-watermark @current-watermark}
                      result (clj-fn ctx)]
                  (when result
                    (let [wm-value (long result)]
                      (when (> wm-value @current-watermark)
                        (reset! current-watermark wm-value)
                        (.emitWatermark output (Watermark. wm-value)))))))
              (onPeriodicEmit [_ _output]
                ;; Punctuated generators don't emit periodically
                nil))))))))

;; =============================================================================
;; Watermark Alignment (Multi-Source)
;; =============================================================================

(defn with-watermark-alignment
  "Configure watermark alignment across sources.

  When multiple sources emit watermarks at different rates, faster sources
  can get too far ahead, causing memory issues. Alignment pauses fast
  sources when they're too far ahead of slow ones.

  Parameters:
    group       - Alignment group name (sources in same group are aligned)
    max-drift   - Maximum allowed drift between sources (duration spec)
    update-interval - How often to check alignment (optional, duration spec)

  Example:
    ;; Align Kafka partitions, allow up to 30 seconds drift
    (-> (for-bounded-out-of-orderness [5 :seconds])
        (with-timestamp-assigner :event-time)
        (with-watermark-alignment \"kafka-group\" [30 :seconds]))

    ;; With custom update interval
    (-> (for-bounded-out-of-orderness [5 :seconds])
        (with-watermark-alignment \"kafka-group\" [30 :seconds] [1 :seconds]))"
  ([^WatermarkStrategy strategy group max-drift]
   (.withWatermarkAlignment strategy group (to-duration max-drift)))
  ([^WatermarkStrategy strategy group max-drift update-interval]
   (.withWatermarkAlignment strategy
                            group
                            (to-duration max-drift)
                            (to-duration update-interval))))

;; =============================================================================
;; Convenience Builders
;; =============================================================================

(defn create-strategy
  "Create a complete WatermarkStrategy with common options.

  Options:
    :max-out-of-orderness - Duration spec for bounded out-of-orderness
                            If not specified, uses monotonous timestamps
    :timestamp-field      - Keyword to extract timestamp from maps
    :timestamp-fn         - Function to extract timestamp (alternative to field)
    :idleness-timeout     - Duration spec for source idleness detection
    :alignment-group      - String name for watermark alignment
    :max-drift            - Duration spec for max drift (requires alignment-group)

  Example:
    ;; Simple strategy for maps with :event-time field
    (create-strategy {:max-out-of-orderness [5 :seconds]
                      :timestamp-field :event-time})

    ;; Full configuration
    (create-strategy {:max-out-of-orderness [5 :seconds]
                      :timestamp-fn extract-timestamp
                      :idleness-timeout [1 :minutes]
                      :alignment-group \"my-sources\"
                      :max-drift [30 :seconds]})"
  [{:keys [max-out-of-orderness timestamp-field timestamp-fn
           idleness-timeout alignment-group max-drift]}]
  (cond-> (if max-out-of-orderness
            (for-bounded-out-of-orderness max-out-of-orderness)
            (for-monotonous-timestamps))
    timestamp-field (with-timestamp-assigner timestamp-field)
    timestamp-fn (with-timestamp-assigner timestamp-fn)
    idleness-timeout (with-idleness idleness-timeout)
    (and alignment-group max-drift) (with-watermark-alignment alignment-group max-drift)))
