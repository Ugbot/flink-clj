(ns flink-clj.join
  "Join operations for combining two DataStreams.

  This namespace provides two types of joins:

  1. Window Join - joins elements from two streams within a common window
  2. Interval Join - joins keyed streams within time bounds

  Window Joins:
    Window joins combine elements that share a common key and fall within
    the same window (tumbling, sliding, or session).

    Example:
      (require '[flink-clj.join :as join])
      (require '[flink-clj.window :as w])

      (defn join-order-shipment [order shipment]
        {:order-id (:id order)
         :shipped-at (:timestamp shipment)})

      (-> (join/window-join orders shipments
            {:left-key :order-id
             :right-key :order-id
             :assigner (w/tumbling-event-time (w/seconds 60))
             :join-fn #'join-order-shipment})
          (sink/print))

  Interval Joins:
    Interval joins match elements from two keyed streams based on time
    bounds relative to the left stream's timestamp.

    Example:
      (defn enrich-click [click impression]
        (assoc click :impression impression))

      (-> (join/interval-join clicks impressions
            {:left-key :user-id
             :right-key :user-id
             :lower-bound [-5 :minutes]
             :upper-bound [0 :seconds]
             :join-fn #'enrich-click})
          (sink/print))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types]
            [flink-clj.window :as w])
  (:import [org.apache.flink.streaming.api.datastream DataStream KeyedStream]
           [org.apache.flink.streaming.api.windowing.assigners
            TumblingEventTimeWindows TumblingProcessingTimeWindows
            SlidingEventTimeWindows SlidingProcessingTimeWindows
            EventTimeSessionWindows ProcessingTimeSessionWindows]
           [java.time Duration]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

;; =============================================================================
;; Type Helpers
;; =============================================================================

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

(defn- to-duration
  "Convert various duration representations to java.time.Duration."
  [d]
  (cond
    (instance? Duration d) d
    (vector? d) (let [[n unit] d]
                  (case unit
                    (:ms :milliseconds) (Duration/ofMillis n)
                    (:s :seconds) (Duration/ofSeconds n)
                    (:m :minutes) (Duration/ofMinutes n)
                    (:h :hours) (Duration/ofHours n)
                    (:d :days) (Duration/ofDays n)))
    (number? d) (Duration/ofMillis d)
    :else (throw (ex-info "Cannot convert to Duration" {:value d}))))

;; =============================================================================
;; Window Join
;; =============================================================================

(defn window-join
  "Join two DataStreams within a common window.

  Performs an inner join: only elements with matching keys in both streams
  within the same window produce output.

  Arguments:
    stream1 - First DataStream
    stream2 - Second DataStream
    opts    - Join configuration map:
              :left-key    - Key selector for stream1 (keyword or var)
              :right-key   - Key selector for stream2 (keyword or var)
              :assigner    - Window assigner (from flink-clj.window)
              :join-fn     - Join function (var), receives [left right] -> result
              :key-type    - Optional TypeInformation for key
              :returns     - Optional TypeInformation for output

  Window Types:
    Use functions from flink-clj.window to create assigners:
    - (w/tumbling-event-time (w/seconds 10))
    - (w/sliding-event-time (w/minutes 1) (w/seconds 10))
    - (w/session-event-time (w/minutes 5))

  Example:
    (defn merge-events [left right]
      {:left-data left
       :right-data right
       :timestamp (max (:ts left) (:ts right))})

    (window-join stream1 stream2
      {:left-key :user-id
       :right-key :user-id
       :assigner (w/tumbling-event-time (w/seconds 30))
       :join-fn #'merge-events})"
  [^DataStream stream1 ^DataStream stream2
   {:keys [left-key right-key assigner join-fn key-type returns]}]
  (let [left-selector (impl/make-key-selector left-key)
        right-selector (impl/make-key-selector right-key)
        join-wrapper (impl/make-join-function join-fn)
        ;; Build the join
        joined (-> stream1
                   (.join stream2)
                   (.where left-selector)
                   (.equalTo right-selector)
                   (.window assigner)
                   (.apply join-wrapper))]
    (if returns
      (.returns joined ^TypeInformation (resolve-type-info returns))
      joined)))

(defn flat-window-join
  "Join two DataStreams within a common window, emitting multiple results per match.

  Like window-join but join-fn returns a sequence of results.

  Example:
    (defn expand-join [left right]
      [{:type :summary :left left :right right}
       {:type :left-only :data left}
       {:type :right-only :data right}])

    (flat-window-join stream1 stream2
      {:left-key :id
       :right-key :id
       :assigner (w/tumbling-event-time (w/seconds 10))
       :join-fn #'expand-join})"
  [^DataStream stream1 ^DataStream stream2
   {:keys [left-key right-key assigner join-fn key-type returns]}]
  (let [left-selector (impl/make-key-selector left-key)
        right-selector (impl/make-key-selector right-key)
        join-wrapper (impl/make-flat-join-function join-fn)
        joined (-> stream1
                   (.join stream2)
                   (.where left-selector)
                   (.equalTo right-selector)
                   (.window assigner)
                   (.apply join-wrapper))]
    (if returns
      (.returns joined ^TypeInformation (resolve-type-info returns))
      joined)))

;; =============================================================================
;; Interval Join
;; =============================================================================

(defn interval-join
  "Join two keyed streams based on time intervals.

  Interval joins match elements where:
    right.timestamp ∈ [left.timestamp + lower-bound, left.timestamp + upper-bound]

  This is useful for scenarios like:
  - Matching clicks to impressions within a time window
  - Correlating orders with shipments
  - Joining events that are 'close' in time

  Arguments:
    stream1 - First KeyedStream (or DataStream, will be keyed)
    stream2 - Second KeyedStream (or DataStream, will be keyed)
    opts    - Join configuration map:
              :left-key     - Key selector for stream1 (if not already keyed)
              :right-key    - Key selector for stream2 (if not already keyed)
              :lower-bound  - Lower time bound [n :unit] or Duration (can be negative)
              :upper-bound  - Upper time bound [n :unit] or Duration
              :join-fn      - Join function (var), receives [left right] -> result
              :flat?        - If true, treat result as sequence (default: false)
              :lower-exclusive - Make lower bound exclusive (default: false)
              :upper-exclusive - Make upper bound exclusive (default: false)
              :key-type     - Optional TypeInformation for key
              :returns      - Optional TypeInformation for output

  Example:
    ;; Match impressions to clicks within 10 minutes before the click
    (defn enrich-click [click impression]
      (assoc click :impression-id (:id impression)))

    (interval-join clicks impressions
      {:left-key :user-id
       :right-key :user-id
       :lower-bound [-10 :minutes]  ; impression can be up to 10 min before click
       :upper-bound [0 :seconds]    ; impression must be at or before click
       :join-fn #'enrich-click})

    ;; Match orders with shipments within 7 days after order
    (interval-join orders shipments
      {:left-key :order-id
       :right-key :order-id
       :lower-bound [0 :seconds]    ; shipment at or after order
       :upper-bound [7 :days]       ; shipment within 7 days
       :join-fn #'match-order-shipment})"
  [stream1 stream2
   {:keys [left-key right-key lower-bound upper-bound join-fn
           flat? lower-exclusive upper-exclusive key-type returns]}]
  (let [;; Key the streams if necessary
        keyed1 (if (instance? KeyedStream stream1)
                 stream1
                 (if key-type
                   (.keyBy ^DataStream stream1
                           (impl/make-key-selector left-key)
                           ^TypeInformation (resolve-type-info key-type))
                   (.keyBy ^DataStream stream1
                           (impl/make-key-selector left-key))))
        keyed2 (if (instance? KeyedStream stream2)
                 stream2
                 (if key-type
                   (.keyBy ^DataStream stream2
                           (impl/make-key-selector right-key)
                           ^TypeInformation (resolve-type-info key-type))
                   (.keyBy ^DataStream stream2
                           (impl/make-key-selector right-key))))
        ;; Convert bounds to Duration
        lower-dur (to-duration lower-bound)
        upper-dur (to-duration upper-bound)
        ;; Create the join function wrapper
        process-fn (impl/make-process-join-function join-fn (boolean flat?))
        ;; Build the interval join
        interval-joined (-> keyed1
                            (.intervalJoin keyed2)
                            (.between lower-dur upper-dur)
                            (cond->
                              lower-exclusive (.lowerBoundExclusive)
                              upper-exclusive (.upperBoundExclusive))
                            (.process process-fn))]
    (if returns
      (.returns interval-joined ^TypeInformation (resolve-type-info returns))
      interval-joined)))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn tumbling-window-join
  "Convenience function for tumbling window join.

  Example:
    (tumbling-window-join orders shipments
      {:left-key :order-id
       :right-key :order-id
       :size (w/minutes 5)
       :time-type :event-time
       :join-fn #'merge-order-shipment})"
  [stream1 stream2
   {:keys [left-key right-key size time-type join-fn returns]
    :or {time-type :event-time}}]
  (let [assigner (case time-type
                   :event-time (TumblingEventTimeWindows/of (w/to-time size))
                   :processing-time (TumblingProcessingTimeWindows/of (w/to-time size)))]
    (window-join stream1 stream2
                 {:left-key left-key
                  :right-key right-key
                  :assigner assigner
                  :join-fn join-fn
                  :returns returns})))

(defn sliding-window-join
  "Convenience function for sliding window join.

  Example:
    (sliding-window-join clicks impressions
      {:left-key :user-id
       :right-key :user-id
       :size (w/minutes 10)
       :slide (w/minutes 1)
       :time-type :event-time
       :join-fn #'correlate-events})"
  [stream1 stream2
   {:keys [left-key right-key size slide time-type join-fn returns]
    :or {time-type :event-time}}]
  (let [assigner (case time-type
                   :event-time (SlidingEventTimeWindows/of
                                 (w/to-time size) (w/to-time slide))
                   :processing-time (SlidingProcessingTimeWindows/of
                                      (w/to-time size) (w/to-time slide)))]
    (window-join stream1 stream2
                 {:left-key left-key
                  :right-key right-key
                  :assigner assigner
                  :join-fn join-fn
                  :returns returns})))

(defn session-window-join
  "Convenience function for session window join.

  Example:
    (session-window-join user-actions page-views
      {:left-key :user-id
       :right-key :user-id
       :gap (w/minutes 30)
       :time-type :event-time
       :join-fn #'combine-session-events})"
  [stream1 stream2
   {:keys [left-key right-key gap time-type join-fn returns]
    :or {time-type :event-time}}]
  (let [assigner (case time-type
                   :event-time (EventTimeSessionWindows/withGap (w/to-time gap))
                   :processing-time (ProcessingTimeSessionWindows/withGap (w/to-time gap)))]
    (window-join stream1 stream2
                 {:left-key left-key
                  :right-key right-key
                  :assigner assigner
                  :join-fn join-fn
                  :returns returns})))

;; =============================================================================
;; CoGroup
;; =============================================================================

(defn cogroup
  "CoGroup two DataStreams within a common window.

  Unlike join, cogroup provides access to ALL elements from both streams
  that fall within the same window and have the same key. This enables:
  - Outer joins (left, right, full)
  - Complex aggregations across streams
  - Custom join logic

  The cogroup-fn receives a map:
    {:left  [...]   ; All elements from stream1 with this key in window
     :right [...]}  ; All elements from stream2 with this key in window

  Arguments:
    stream1 - First DataStream
    stream2 - Second DataStream
    opts    - CoGroup configuration map:
              :left-key    - Key selector for stream1 (keyword or var)
              :right-key   - Key selector for stream2 (keyword or var)
              :assigner    - Window assigner (from flink-clj.window)
              :cogroup-fn  - CoGroup function (var), receives {:left [...] :right [...]}
              :flat?       - If true, treat result as sequence (default: false)
              :key-type    - Optional TypeInformation for key
              :returns     - Optional TypeInformation for output

  Example - Full outer join:
    (defn full-outer-join [{:keys [left right]}]
      (cond
        (and (seq left) (seq right))
        (for [l left, r right]
          {:type :matched :left l :right r})

        (seq left)
        (for [l left]
          {:type :left-only :left l})

        (seq right)
        (for [r right]
          {:type :right-only :right r})))

    (cogroup orders shipments
      {:left-key :order-id
       :right-key :order-id
       :assigner (w/tumbling-event-time (w/hours 1))
       :cogroup-fn #'full-outer-join
       :flat? true})

  Example - Aggregate across streams:
    (defn summarize-streams [{:keys [left right]}]
      {:left-count (count left)
       :right-count (count right)
       :left-sum (reduce + 0 (map :value left))
       :right-sum (reduce + 0 (map :value right))})

    (cogroup stream1 stream2
      {:left-key :id
       :right-key :id
       :assigner (w/tumbling-event-time (w/minutes 5))
       :cogroup-fn #'summarize-streams})"
  [^DataStream stream1 ^DataStream stream2
   {:keys [left-key right-key assigner cogroup-fn flat? key-type returns]}]
  (let [left-selector (impl/make-key-selector left-key)
        right-selector (impl/make-key-selector right-key)
        cogroup-wrapper (impl/make-cogroup-function cogroup-fn (boolean flat?))
        ;; Build the cogroup
        cogrouped (-> stream1
                      (.coGroup stream2)
                      (.where left-selector)
                      (.equalTo right-selector)
                      (.window assigner)
                      (.apply cogroup-wrapper))]
    (if returns
      (.returns cogrouped ^TypeInformation (resolve-type-info returns))
      cogrouped)))

;; =============================================================================
;; Outer Joins (built on CoGroup)
;; =============================================================================

(defn left-outer-join
  "Left outer join: all left elements, matched right elements where available.

  Unmatched left elements are emitted with nil for the right side.

  Arguments:
    stream1 - Left DataStream (all elements emitted)
    stream2 - Right DataStream (matched elements only)
    opts    - Join configuration:
              :left-key  - Key selector for stream1
              :right-key - Key selector for stream2
              :assigner  - Window assigner
              :join-fn   - Function receiving [left right-or-nil] -> result
              :returns   - Optional TypeInformation for output

  Example:
    (defn join-with-optional-details [order shipment]
      {:order order
       :shipment shipment  ; may be nil
       :shipped? (some? shipment)})

    (left-outer-join orders shipments
      {:left-key :order-id
       :right-key :order-id
       :assigner (w/tumbling-event-time (w/days 1))
       :join-fn #'join-with-optional-details})"
  [^DataStream stream1 ^DataStream stream2
   {:keys [left-key right-key assigner join-fn key-type returns]}]
  (let [;; Create a cogroup function that implements left outer join
        left-outer-fn (fn [{:keys [left right]}]
                        (let [join-var (if (var? join-fn) join-fn (resolve join-fn))
                              join-impl @join-var]
                          (if (seq right)
                            ;; Matched - emit all combinations
                            (for [l left, r right]
                              (join-impl l r))
                            ;; Unmatched - emit left with nil
                            (for [l left]
                              (join-impl l nil)))))
        ;; We need to define this function at top level for serialization
        ;; Using a trick: store join-fn reference in metadata
        wrapper-ns "flink-clj.join"
        wrapper-name (str "left-outer-join-wrapper-" (System/nanoTime))]
    ;; Create the wrapper function dynamically
    (intern (find-ns 'flink-clj.join)
            (symbol wrapper-name)
            (fn [{:keys [left right]}]
              (let [join-impl @join-fn]
                (if (seq right)
                  (for [l left, r right]
                    (join-impl l r))
                  (for [l left]
                    (join-impl l nil))))))
    ;; Use cogroup with the wrapper
    (cogroup stream1 stream2
             {:left-key left-key
              :right-key right-key
              :assigner assigner
              :cogroup-fn (ns-resolve 'flink-clj.join (symbol wrapper-name))
              :flat? true
              :key-type key-type
              :returns returns})))

(defn right-outer-join
  "Right outer join: all right elements, matched left elements where available.

  Unmatched right elements are emitted with nil for the left side.

  Arguments:
    stream1 - Left DataStream (matched elements only)
    stream2 - Right DataStream (all elements emitted)
    opts    - Join configuration:
              :left-key  - Key selector for stream1
              :right-key - Key selector for stream2
              :assigner  - Window assigner
              :join-fn   - Function receiving [left-or-nil right] -> result
              :returns   - Optional TypeInformation for output

  Example:
    (right-outer-join orders shipments
      {:left-key :order-id
       :right-key :order-id
       :assigner (w/tumbling-event-time (w/days 1))
       :join-fn #'join-shipment-with-order})"
  [^DataStream stream1 ^DataStream stream2
   {:keys [left-key right-key assigner join-fn key-type returns]}]
  (let [wrapper-name (str "right-outer-join-wrapper-" (System/nanoTime))]
    (intern (find-ns 'flink-clj.join)
            (symbol wrapper-name)
            (fn [{:keys [left right]}]
              (let [join-impl @join-fn]
                (if (seq left)
                  (for [l left, r right]
                    (join-impl l r))
                  (for [r right]
                    (join-impl nil r))))))
    (cogroup stream1 stream2
             {:left-key left-key
              :right-key right-key
              :assigner assigner
              :cogroup-fn (ns-resolve 'flink-clj.join (symbol wrapper-name))
              :flat? true
              :key-type key-type
              :returns returns})))

(defn full-outer-join
  "Full outer join: all elements from both streams.

  Matched elements are joined, unmatched elements are emitted with nil.

  Arguments:
    stream1 - Left DataStream
    stream2 - Right DataStream
    opts    - Join configuration:
              :left-key  - Key selector for stream1
              :right-key - Key selector for stream2
              :assigner  - Window assigner
              :join-fn   - Function receiving [left-or-nil right-or-nil] -> result
              :returns   - Optional TypeInformation for output

  Example:
    (defn merge-or-standalone [left right]
      (cond
        (and left right) {:type :matched :left left :right right}
        left {:type :left-only :data left}
        right {:type :right-only :data right}))

    (full-outer-join orders shipments
      {:left-key :order-id
       :right-key :order-id
       :assigner (w/tumbling-event-time (w/days 1))
       :join-fn #'merge-or-standalone})"
  [^DataStream stream1 ^DataStream stream2
   {:keys [left-key right-key assigner join-fn key-type returns]}]
  (let [wrapper-name (str "full-outer-join-wrapper-" (System/nanoTime))]
    (intern (find-ns 'flink-clj.join)
            (symbol wrapper-name)
            (fn [{:keys [left right]}]
              (let [join-impl @join-fn]
                (cond
                  (and (seq left) (seq right))
                  (for [l left, r right]
                    (join-impl l r))

                  (seq left)
                  (for [l left]
                    (join-impl l nil))

                  (seq right)
                  (for [r right]
                    (join-impl nil r))

                  :else []))))
    (cogroup stream1 stream2
             {:left-key left-key
              :right-key right-key
              :assigner assigner
              :cogroup-fn (ns-resolve 'flink-clj.join (symbol wrapper-name))
              :flat? true
              :key-type key-type
              :returns returns})))
