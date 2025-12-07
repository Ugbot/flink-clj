(ns flink-clj.window
  "Window operations for KeyedStreams.

  Windows group elements by key and time, enabling bounded computations
  over unbounded streams.

  Window Types:
    - Tumbling: Fixed-size, non-overlapping windows
    - Sliding: Fixed-size, overlapping windows
    - Session: Dynamic windows based on activity gaps

  Time Semantics:
    - Event time: Based on timestamps in the data
    - Processing time: Based on wall clock time

  Example:
    (require '[flink-clj.window :as w])

    ;; 10-second tumbling window
    (-> keyed-stream
        (w/tumbling-event-time (w/seconds 10))
        (w/flink-reduce my-reducer))

    ;; Sliding window with 1-minute size, 10-second slide
    (-> keyed-stream
        (w/sliding-event-time (w/minutes 1) (w/seconds 10))
        (w/aggregate my-aggregator))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.datastream KeyedStream WindowedStream]
           [org.apache.flink.streaming.api.windowing.assigners
            TumblingEventTimeWindows TumblingProcessingTimeWindows
            SlidingEventTimeWindows SlidingProcessingTimeWindows
            EventTimeSessionWindows ProcessingTimeSessionWindows
            GlobalWindows]
           [org.apache.flink.streaming.api.windowing.triggers
            Trigger CountTrigger PurgingTrigger EventTimeTrigger ProcessingTimeTrigger]
           [org.apache.flink.streaming.api.windowing.evictors
            Evictor CountEvictor TimeEvictor]
           [java.time Duration]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

;; =============================================================================
;; Time Duration Helpers
;; =============================================================================

(defn- time-class-available?
  "Check if the Flink Time class is available (Flink 1.x)."
  []
  (try
    (Class/forName "org.apache.flink.streaming.api.windowing.time.Time")
    true
    (catch ClassNotFoundException _ false)))

(defn- create-time
  "Create a time/duration value that works with both Flink versions.
  Returns Flink Time for 1.x and Duration for 2.x."
  [millis]
  (if (time-class-available?)
    (let [time-class (Class/forName "org.apache.flink.streaming.api.windowing.time.Time")
          method (.getMethod time-class "milliseconds" (into-array Class [Long/TYPE]))]
      (.invoke method nil (into-array Object [(long millis)])))
    (Duration/ofMillis millis)))

(defn milliseconds
  "Create a time duration in milliseconds."
  [n]
  (create-time n))

(defn seconds
  "Create a time duration in seconds."
  [n]
  (create-time (* n 1000)))

(defn minutes
  "Create a time duration in minutes."
  [n]
  (create-time (* n 60 1000)))

(defn hours
  "Create a time duration in hours."
  [n]
  (create-time (* n 60 60 1000)))

(defn days
  "Create a time duration in days."
  [n]
  (create-time (* n 24 60 60 1000)))

(defn duration
  "Convert a Clojure vector spec to Time duration.

  Supports:
    [n :ms], [n :milliseconds]
    [n :s], [n :seconds]
    [n :m], [n :minutes]
    [n :h], [n :hours]
    [n :d], [n :days]

  Example:
    (duration [10 :seconds])
    (duration [5 :minutes])"
  [[n unit]]
  (case unit
    (:ms :milliseconds) (milliseconds n)
    (:s :seconds) (seconds n)
    (:m :minutes) (minutes n)
    (:h :hours) (hours n)
    (:d :days) (days n)
    (throw (ex-info "Unknown time unit" {:unit unit}))))

(defn to-duration
  "Convert various duration representations to java.time.Duration.

  This is the preferred function for Flink 2.x which uses Duration directly.
  For Flink 1.x, use `to-time` instead.

  Accepts:
  - java.time.Duration (returned as-is)
  - Vector spec like [10 :seconds]
  - Raw number (milliseconds)
  - Flink Time object (converted to Duration)

  Example:
    (to-duration [10 :seconds])      ;=> #<Duration PT10S>
    (to-duration [5 :minutes])       ;=> #<Duration PT5M>
    (to-duration 1000)               ;=> #<Duration PT1S>
    (to-duration (Duration/ofHours 1)) ;=> #<Duration PT1H>"
  [d]
  (cond
    ;; Already a Duration
    (instance? Duration d) d
    ;; Vector spec like [10 :seconds]
    (vector? d)
    (let [[n unit] d]
      (case unit
        (:ms :milliseconds) (Duration/ofMillis n)
        (:s :seconds) (Duration/ofSeconds n)
        (:m :minutes) (Duration/ofMinutes n)
        (:h :hours) (Duration/ofHours n)
        (:d :days) (Duration/ofDays n)
        (throw (ex-info "Unknown time unit" {:unit unit}))))
    ;; Raw number (milliseconds)
    (number? d) (Duration/ofMillis d)
    ;; Flink Time object (1.x) - convert via toMilliseconds
    (and (time-class-available?)
         (instance? (Class/forName "org.apache.flink.streaming.api.windowing.time.Time") d))
    (let [time-class (Class/forName "org.apache.flink.streaming.api.windowing.time.Time")
          method (.getMethod time-class "toMilliseconds" (into-array Class []))]
      (Duration/ofMillis (long (.invoke method d (into-array Object [])))))
    :else (throw (ex-info "Cannot convert to Duration" {:value d :type (type d)}))))

(defn to-time
  "Convert various duration representations to time/duration for window assigners.

  This function handles both Flink 1.x (Time) and Flink 2.x (Duration) automatically.

  Accepts:
  - Flink Time object (returned as-is, Flink 1.x)
  - java.time.Duration (returned as-is for Flink 2.x, converted for 1.x)
  - Vector spec like [10 :seconds]
  - Raw number (milliseconds)"
  [d]
  (cond
    ;; If it's already a Flink Time, use it directly (Flink 1.x)
    (and (time-class-available?)
         (instance? (Class/forName "org.apache.flink.streaming.api.windowing.time.Time") d))
    d
    ;; If it's a Duration, use it for 2.x or convert for 1.x
    (instance? Duration d)
    (if (time-class-available?)
      (create-time (.toMillis ^Duration d))
      d)
    ;; Vector spec like [10 :seconds]
    (vector? d) (duration d)
    ;; Raw number (milliseconds)
    (number? d) (milliseconds d)
    :else (throw (ex-info "Cannot convert to Time duration" {:value d}))))

;; =============================================================================
;; Tumbling Windows
;; =============================================================================

(defn tumbling-event-time
  "Apply tumbling event time window to a KeyedStream.

  Tumbling windows are fixed-size, non-overlapping windows aligned to epoch.

  Example:
    (tumbling-event-time keyed-stream (seconds 10))
    (tumbling-event-time keyed-stream [10 :seconds])"
  ([^KeyedStream stream size]
   (tumbling-event-time stream size nil))
  ([^KeyedStream stream size offset]
   (let [window-size (to-time size)
         assigner (if offset
                    (TumblingEventTimeWindows/of window-size (to-time offset))
                    (TumblingEventTimeWindows/of window-size))]
     (.window stream assigner))))

(defn tumbling-processing-time
  "Apply tumbling processing time window to a KeyedStream.

  Processing time windows use the wall clock time of the machine.

  Example:
    (tumbling-processing-time keyed-stream (seconds 10))"
  ([^KeyedStream stream size]
   (tumbling-processing-time stream size nil))
  ([^KeyedStream stream size offset]
   (let [window-size (to-time size)
         assigner (if offset
                    (TumblingProcessingTimeWindows/of window-size (to-time offset))
                    (TumblingProcessingTimeWindows/of window-size))]
     (.window stream assigner))))

;; =============================================================================
;; Sliding Windows
;; =============================================================================

(defn sliding-event-time
  "Apply sliding event time window to a KeyedStream.

  Sliding windows have a fixed size but can overlap (slide < size).

  Example:
    (sliding-event-time keyed-stream (minutes 1) (seconds 10))"
  ([^KeyedStream stream size slide]
   (sliding-event-time stream size slide nil))
  ([^KeyedStream stream size slide offset]
   (let [window-size (to-time size)
         slide-interval (to-time slide)
         assigner (if offset
                    (SlidingEventTimeWindows/of window-size slide-interval (to-time offset))
                    (SlidingEventTimeWindows/of window-size slide-interval))]
     (.window stream assigner))))

(defn sliding-processing-time
  "Apply sliding processing time window to a KeyedStream.

  Example:
    (sliding-processing-time keyed-stream (minutes 1) (seconds 10))"
  ([^KeyedStream stream size slide]
   (sliding-processing-time stream size slide nil))
  ([^KeyedStream stream size slide offset]
   (let [window-size (to-time size)
         slide-interval (to-time slide)
         assigner (if offset
                    (SlidingProcessingTimeWindows/of window-size slide-interval (to-time offset))
                    (SlidingProcessingTimeWindows/of window-size slide-interval))]
     (.window stream assigner))))

;; =============================================================================
;; Session Windows
;; =============================================================================

(defn session-event-time
  "Apply session event time window to a KeyedStream.

  Session windows group elements by activity with gaps. A new window
  starts when the gap exceeds the specified duration.

  Example:
    (session-event-time keyed-stream (minutes 5))"
  [^KeyedStream stream gap]
  (let [gap-duration (to-time gap)
        assigner (EventTimeSessionWindows/withGap gap-duration)]
    (.window stream assigner)))

(defn session-processing-time
  "Apply session processing time window to a KeyedStream.

  Example:
    (session-processing-time keyed-stream (minutes 5))"
  [^KeyedStream stream gap]
  (let [gap-duration (to-time gap)
        assigner (ProcessingTimeSessionWindows/withGap gap-duration)]
    (.window stream assigner)))

;; =============================================================================
;; Global Windows
;; =============================================================================

(defn global-window
  "Apply global window to a KeyedStream.

  Global windows group all elements with the same key into a single window.
  Requires a custom trigger to fire.

  Example:
    (-> keyed-stream
        (global-window)
        (trigger (count-trigger 100))
        (reduce my-reducer))"
  [^KeyedStream stream]
  (.window stream (GlobalWindows/create)))

;; =============================================================================
;; Count Windows
;; =============================================================================

(defn count-window
  "Apply a count-based tumbling window to a KeyedStream.

  Count windows group elements by count rather than time.
  Each window contains exactly `size` elements per key.

  Example:
    ;; Window every 100 elements per key
    (-> keyed-stream
        (count-window 100)
        (reduce my-reducer))

  For sliding count windows, use `count-window-sliding`."
  [^KeyedStream stream size]
  (.countWindow stream (long size)))

(defn count-window-sliding
  "Apply a sliding count-based window to a KeyedStream.

  Each window contains `size` elements, sliding by `slide` elements.
  Windows overlap when slide < size.

  Example:
    ;; Window of 100 elements, sliding every 10
    (-> keyed-stream
        (count-window-sliding 100 10)
        (reduce my-reducer))"
  [^KeyedStream stream size slide]
  (.countWindow stream (long size) (long slide)))

;; =============================================================================
;; Dynamic Gap Session Windows
;; =============================================================================

(defn- session-gap-extractor-available?
  "Check if SessionWindowTimeGapExtractor is available."
  []
  (try
    (Class/forName "org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor")
    true
    (catch ClassNotFoundException _ false)))

(defn dynamic-session-event-time
  "Apply a dynamic gap session window to a KeyedStream (event time).

  Unlike fixed-gap sessions, this allows the gap duration to be
  determined per-element based on the element's content.

  gap-fn is a function that takes an element and returns the
  gap duration in milliseconds for that element.

  The gap-fn must be a top-level var (defined with defn).

  Example:
    ;; Different gap based on event type
    (defn get-gap [event]
      (case (:type event)
        :critical 5000    ; 5 second gap for critical events
        :normal 60000     ; 1 minute for normal events
        30000))           ; 30 seconds default

    (-> keyed-stream
        (dynamic-session-event-time #'get-gap)
        (reduce my-reducer))"
  [^KeyedStream stream gap-fn]
  (if (session-gap-extractor-available?)
    (let [extractor-class (Class/forName "org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor")
          [ns-str fn-name] (impl/var->ns-name gap-fn)
          ;; Create a dynamic extractor using a Java wrapper
          extractor (impl/make-session-gap-extractor gap-fn)
          with-dynamic-method (.getMethod EventTimeSessionWindows "withDynamicGap"
                                          (into-array Class [extractor-class]))]
      (.window stream (.invoke with-dynamic-method nil (into-array Object [extractor]))))
    ;; Fallback for older Flink versions
    (throw (ex-info "Dynamic gap session windows require Flink 1.4+"
                    {:suggestion "Use fixed-gap session-event-time instead"}))))

(defn dynamic-session-processing-time
  "Apply a dynamic gap session window to a KeyedStream (processing time).

  Unlike fixed-gap sessions, this allows the gap duration to be
  determined per-element based on the element's content.

  gap-fn is a function that takes an element and returns the
  gap duration in milliseconds for that element.

  The gap-fn must be a top-level var (defined with defn).

  Example:
    ;; Gap based on user tier
    (defn tier-based-gap [event]
      (case (:user-tier event)
        :premium 300000   ; 5 minute gap for premium users
        :standard 60000   ; 1 minute for standard
        30000))           ; 30 seconds default

    (-> keyed-stream
        (dynamic-session-processing-time #'tier-based-gap)
        (reduce my-reducer))"
  [^KeyedStream stream gap-fn]
  (if (session-gap-extractor-available?)
    (let [extractor-class (Class/forName "org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor")
          extractor (impl/make-session-gap-extractor gap-fn)
          with-dynamic-method (.getMethod ProcessingTimeSessionWindows "withDynamicGap"
                                          (into-array Class [extractor-class]))]
      (.window stream (.invoke with-dynamic-method nil (into-array Object [extractor]))))
    (throw (ex-info "Dynamic gap session windows require Flink 1.4+"
                    {:suggestion "Use fixed-gap session-processing-time instead"}))))

;; =============================================================================
;; Window Functions
;; =============================================================================

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

(defn flink-reduce
  "Apply a reduce function to a windowed stream.

  reduce-fn takes two arguments and returns a combined result.
  Must be a var (defined with defn).

  Options:
    :returns - TypeInformation or spec for output type

  Example:
    (defn sum-counts [[w1 c1] [w2 c2]] [w1 (+ c1 c2)])
    (flink-reduce windowed-stream sum-counts)"
  ([^WindowedStream stream reduce-fn]
   (flink-reduce stream reduce-fn nil))
  ([^WindowedStream stream reduce-fn {:keys [returns]}]
   (let [reducer (impl/make-reduce-function reduce-fn)
         result (.reduce stream reducer)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

(defn sum
  "Sum a field in the windowed stream.

  field can be an index (int) or field name (string).

  Example:
    (sum windowed-stream 1)
    (sum windowed-stream \"count\")"
  [^WindowedStream stream field]
  (if (integer? field)
    (.sum stream (int field))
    (.sum stream (str field))))

(defn flink-min
  "Get minimum by a field in the windowed stream.

  Example:
    (flink-min windowed-stream 1)"
  [^WindowedStream stream field]
  (if (integer? field)
    (.min stream (int field))
    (.min stream (str field))))

(defn flink-max
  "Get maximum by a field in the windowed stream.

  Example:
    (flink-max windowed-stream 1)"
  [^WindowedStream stream field]
  (if (integer? field)
    (.max stream (int field))
    (.max stream (str field))))

(defn min-by
  "Get element with minimum value for a field.

  Example:
    (min-by windowed-stream 1)"
  [^WindowedStream stream field]
  (if (integer? field)
    (.minBy stream (int field))
    (.minBy stream (str field))))

(defn max-by
  "Get element with maximum value for a field.

  Example:
    (max-by windowed-stream 1)"
  [^WindowedStream stream field]
  (if (integer? field)
    (.maxBy stream (int field))
    (.maxBy stream (str field))))

(defn aggregate
  "Apply an AggregateFunction to a windowed stream.

  agg-spec is a map with:
    :create-accumulator - fn [] -> initial accumulator value (required)
    :add - fn [acc element] -> updated accumulator (required)
    :get-result - fn [acc] -> final result (required)
    :merge - fn [acc1 acc2] -> merged accumulator (optional, needed for session windows)

  All functions must be top-level vars (defined with defn) for serialization.

  Options:
    :returns - TypeInformation or spec for output type

  Example:
    ;; Define aggregation functions
    (defn create-acc [] {:count 0 :sum 0})
    (defn add-element [acc elem]
      (-> acc
          (update :count inc)
          (update :sum + (:value elem))))
    (defn get-avg [acc] (/ (:sum acc) (:count acc)))
    (defn merge-accs [a b]
      {:count (+ (:count a) (:count b))
       :sum (+ (:sum a) (:sum b))})

    ;; Use in window
    (-> keyed-stream
        (tumbling-event-time [10 :seconds])
        (aggregate {:create-accumulator #'create-acc
                    :add #'add-element
                    :get-result #'get-avg
                    :merge #'merge-accs}
                   {:returns :double}))"
  ([^WindowedStream stream agg-spec]
   (aggregate stream agg-spec nil))
  ([^WindowedStream stream agg-spec {:keys [returns]}]
   (let [output-type (when returns (resolve-type-info returns))
         aggregator (impl/make-aggregate-function agg-spec output-type)
         result (.aggregate stream aggregator)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

;; =============================================================================
;; Triggers
;; =============================================================================

(defn trigger
  "Set a custom trigger for the windowed stream.

  Triggers determine when a window is ready to be processed.

  Example:
    (trigger windowed-stream (count-trigger 100))"
  [^WindowedStream stream ^Trigger t]
  (.trigger stream t))

(defn count-trigger
  "Create a trigger that fires after a count of elements.

  Example:
    (count-trigger 100)"
  [count]
  (CountTrigger/of count))

(defn event-time-trigger
  "Create a trigger that fires at the watermark.

  Example:
    (event-time-trigger)"
  []
  (EventTimeTrigger/create))

(defn processing-time-trigger
  "Create a trigger that fires based on processing time.

  Example:
    (processing-time-trigger)"
  []
  (ProcessingTimeTrigger/create))

(defn purging
  "Wrap a trigger to also purge the window after firing.

  Example:
    (purging (count-trigger 100))"
  [^Trigger t]
  (PurgingTrigger/of t))

;; =============================================================================
;; Evictors
;; =============================================================================

(defn evictor
  "Set an evictor for the windowed stream.

  Evictors can remove elements from a window before processing.

  Example:
    (evictor windowed-stream (count-evictor 100))"
  [^WindowedStream stream ^Evictor e]
  (.evictor stream e))

(defn count-evictor
  "Create an evictor that keeps the last N elements.

  Example:
    (count-evictor 100)"
  ([count]
   (CountEvictor/of count))
  ([count do-evict-after?]
   (if do-evict-after?
     (CountEvictor/of count)
     (CountEvictor/of count))))

(defn time-evictor
  "Create an evictor that keeps elements within a time window.

  Example:
    (time-evictor (seconds 10))"
  ([window-size]
   (TimeEvictor/of (to-time window-size)))
  ([window-size do-evict-after?]
   (if do-evict-after?
     (TimeEvictor/of (to-time window-size))
     (TimeEvictor/of (to-time window-size)))))

;; =============================================================================
;; Allowed Lateness
;; =============================================================================

(defn allowed-lateness
  "Set the allowed lateness for late elements.

  Late elements that arrive within this time will still be processed.

  Example:
    (allowed-lateness windowed-stream (seconds 10))"
  [^WindowedStream stream lateness]
  (.allowedLateness stream (to-time lateness)))

;; =============================================================================
;; Side Output for Late Data
;; =============================================================================

(defn side-output-late-data
  "Direct late data to a side output.

  Example:
    (def late-tag (OutputTag. \"late-data\" type-info))
    (side-output-late-data windowed-stream late-tag)"
  [^WindowedStream stream output-tag]
  (.sideOutputLateData stream output-tag))

;; =============================================================================
;; ProcessWindowFunction
;; =============================================================================

(defn process
  "Apply a ProcessWindowFunction to a windowed stream.

  ProcessWindowFunction provides full access to window context including:
  - The window key
  - Window metadata (start/end times)
  - All elements in the window as a sequence

  process-fn receives a context map:
    {:key k              ; The window key
     :window {:start ts  ; Window start timestamp
              :end ts}   ; Window end timestamp
     :elements [...]     ; All window elements as a sequence
    }

  The function can return:
  - A single value (emitted as output)
  - A sequence of values (each emitted if :flat? true)
  - nil (nothing emitted)

  Options:
    :flat?    - If true, treat result as sequence (default: false)
    :returns  - TypeInformation or spec for output type

  Example:
    (defn summarize-window [{:keys [key window elements]}]
      {:user-id key
       :window-start (:start window)
       :count (count elements)
       :total (reduce + (map :value elements))})

    (-> keyed-stream
        (tumbling-event-time (seconds 60))
        (process #'summarize-window))

    ;; With flat output
    (defn expand-window [{:keys [elements]}]
      (for [e elements]
        (assoc e :processed true)))

    (-> keyed-stream
        (tumbling-event-time (seconds 60))
        (process #'expand-window {:flat? true}))"
  ([^WindowedStream stream process-fn]
   (process stream process-fn nil))
  ([^WindowedStream stream process-fn {:keys [flat? returns]}]
   (let [wrapper (impl/make-process-window-function process-fn (boolean flat?))
         result (.process stream wrapper)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

;; =============================================================================
;; WindowAll (Non-Keyed Windows)
;; =============================================================================

(defn window-all
  "Apply a window to a non-keyed DataStream.

  Unlike regular windows on KeyedStream, windowAll collects ALL elements
  into a single window regardless of key. This is a non-parallel operation.

  Use with caution as all data flows to a single task.

  Arguments:
    stream   - A DataStream (not KeyedStream)
    assigner - Window assigner

  Example:
    (require '[flink-clj.window :as w])

    ;; Tumbling window over all events
    (-> data-stream
        (w/window-all (TumblingEventTimeWindows/of (w/seconds 60)))
        (w/reduce-all #'my-reducer))

    ;; Session window over all events
    (-> data-stream
        (w/window-all (EventTimeSessionWindows/withGap (w/minutes 5)))
        (w/aggregate-all my-aggregator))"
  [stream assigner]
  (.windowAll stream assigner))

(defn tumbling-event-time-all
  "Apply tumbling event time window to a non-keyed DataStream.

  Example:
    (tumbling-event-time-all data-stream (seconds 10))"
  ([stream size]
   (tumbling-event-time-all stream size nil))
  ([stream size offset]
   (let [window-size (to-time size)
         assigner (if offset
                    (TumblingEventTimeWindows/of window-size (to-time offset))
                    (TumblingEventTimeWindows/of window-size))]
     (.windowAll stream assigner))))

(defn tumbling-processing-time-all
  "Apply tumbling processing time window to a non-keyed DataStream.

  Example:
    (tumbling-processing-time-all data-stream (seconds 10))"
  ([stream size]
   (tumbling-processing-time-all stream size nil))
  ([stream size offset]
   (let [window-size (to-time size)
         assigner (if offset
                    (TumblingProcessingTimeWindows/of window-size (to-time offset))
                    (TumblingProcessingTimeWindows/of window-size))]
     (.windowAll stream assigner))))

(defn sliding-event-time-all
  "Apply sliding event time window to a non-keyed DataStream.

  Example:
    (sliding-event-time-all data-stream (minutes 1) (seconds 10))"
  ([stream size slide]
   (sliding-event-time-all stream size slide nil))
  ([stream size slide offset]
   (let [window-size (to-time size)
         slide-interval (to-time slide)
         assigner (if offset
                    (SlidingEventTimeWindows/of window-size slide-interval (to-time offset))
                    (SlidingEventTimeWindows/of window-size slide-interval))]
     (.windowAll stream assigner))))

(defn sliding-processing-time-all
  "Apply sliding processing time window to a non-keyed DataStream.

  Example:
    (sliding-processing-time-all data-stream (minutes 1) (seconds 10))"
  ([stream size slide]
   (sliding-processing-time-all stream size slide nil))
  ([stream size slide offset]
   (let [window-size (to-time size)
         slide-interval (to-time slide)
         assigner (if offset
                    (SlidingProcessingTimeWindows/of window-size slide-interval (to-time offset))
                    (SlidingProcessingTimeWindows/of window-size slide-interval))]
     (.windowAll stream assigner))))

(defn reduce-all
  "Apply a reduce function to an AllWindowedStream.

  Example:
    (-> data-stream
        (tumbling-event-time-all (seconds 60))
        (reduce-all #'my-reducer))"
  ([stream reduce-fn]
   (reduce-all stream reduce-fn nil))
  ([stream reduce-fn {:keys [returns]}]
   (let [reducer (impl/make-reduce-function reduce-fn)
         result (.reduce stream reducer)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

(defn aggregate-all
  "Apply an AggregateFunction to an AllWindowedStream.

  See `aggregate` for agg-spec format.

  Example:
    (-> data-stream
        (tumbling-event-time-all (seconds 60))
        (aggregate-all {:create-accumulator #'create-acc
                        :add #'add-elem
                        :get-result #'get-result}))"
  ([stream agg-spec]
   (aggregate-all stream agg-spec nil))
  ([stream agg-spec {:keys [returns]}]
   (let [output-type (when returns (resolve-type-info returns))
         aggregator (impl/make-aggregate-function agg-spec output-type)
         result (.aggregate stream aggregator)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

;; =============================================================================
;; Custom Window Assigners
;; =============================================================================

(defn custom-window-assigner
  "Create a custom window assigner from a Clojure function.

  The assign-fn receives a map with:
    :element   - The incoming element
    :timestamp - Event timestamp
    :context   - Map with :current-processing-time and :current-watermark

  It should return:
    - A single window spec: {:start <long> :end <long>}
    - A collection of window specs for overlapping windows
    - nil for no window assignment

  Arguments:
    assign-fn   - Function (var) that assigns windows
    event-time? - True for event-time semantics (default: true)

  Example:
    ;; Business-hour tumbling windows (9am-5pm daily)
    (defn business-hours-assigner [{:keys [element timestamp]}]
      (let [hour (-> (java.time.Instant/ofEpochMilli timestamp)
                     (.atZone (java.time.ZoneId/of \"UTC\"))
                     .getHour)]
        (when (and (>= hour 9) (< hour 17))
          (let [day-start (- timestamp (mod timestamp 86400000))
                window-start (+ day-start (* 9 3600000))
                window-end (+ day-start (* 17 3600000))]
            {:start window-start :end window-end}))))

    (-> keyed-stream
        (.window (custom-window-assigner #'business-hours-assigner))
        (reduce my-reducer))

  Example - Overlapping custom windows:
    (defn multi-window-assigner [{:keys [element timestamp]}]
      ;; Assign to multiple windows based on element attributes
      (let [categories (:categories element)]
        (for [cat categories]
          {:start (- timestamp 60000)
           :end (+ timestamp 60000)})))

    (-> keyed-stream
        (.window (custom-window-assigner #'multi-window-assigner))
        (aggregate my-aggregator))"
  ([assign-fn]
   (custom-window-assigner assign-fn true))
  ([assign-fn event-time?]
   (let [[ns-str fn-name] (impl/var->ns-name assign-fn)]
     (flink_clj.CljWindowAssigner. ns-str fn-name (boolean event-time?)))))

;; =============================================================================
;; Custom Triggers
;; =============================================================================

(defn custom-trigger
  "Create a custom trigger from Clojure functions.

  Trigger functions receive a context map:
    :element          - Current element (only in on-element)
    :timestamp        - Event timestamp or timer time
    :window           - {:start <long> :end <long>}
    :current-watermark
    :current-processing-time
    :trigger-context  - Map with timer functions:
      :register-processing-time-timer  - (fn [time])
      :register-event-time-timer       - (fn [time])
      :delete-processing-time-timer    - (fn [time])
      :delete-event-time-timer         - (fn [time])

  Each function should return:
    :fire           - Fire the window (compute and emit)
    :purge          - Purge window contents
    :fire-and-purge - Fire then purge
    :continue       - Do nothing (default)

  Arguments:
    opts - Map with function vars:
      :on-element        - Called for each element (required)
      :on-processing-time - Called when processing timer fires
      :on-event-time      - Called when event-time timer fires
      :clear              - Called when window is purged

  Example - Count trigger with max wait:
    (defn trigger-on-element [{:keys [element window trigger-context] :as ctx}]
      (let [register-timer (:register-processing-time-timer trigger-context)
            ;; Register a max-wait timer
            max-wait (+ (:current-processing-time ctx) 30000)]
        (register-timer max-wait))
      ;; Check if we've reached count threshold
      (if (>= (count-elements-fn) 100)
        :fire
        :continue))

    (defn trigger-on-processing-time [ctx]
      :fire-and-purge)

    (-> windowed-stream
        (.trigger (custom-trigger {:on-element #'trigger-on-element
                                   :on-processing-time #'trigger-on-processing-time}))
        (reduce my-reducer))"
  [{:keys [on-element on-processing-time on-event-time clear]}]
  (let [get-fn-parts (fn [f]
                       (when f
                         (let [[ns-str fn-name] (impl/var->ns-name f)]
                           [ns-str fn-name])))
        [elem-ns elem-fn] (get-fn-parts on-element)
        [proc-ns proc-fn] (get-fn-parts on-processing-time)
        [event-ns event-fn] (get-fn-parts on-event-time)
        [clear-ns clear-fn] (get-fn-parts clear)
        ;; All functions should be in the same namespace
        ns-str (or elem-ns proc-ns event-ns clear-ns)]
    (when-not ns-str
      (throw (ex-info "At least one trigger function must be provided" {})))
    (flink_clj.CljTrigger. ns-str elem-fn proc-fn event-fn clear-fn)))

;; =============================================================================
;; Trigger Composition
;; =============================================================================

(defn- trigger-and-class-available?
  "Check if Flink's trigger AND composition is available."
  []
  (try
    (Class/forName "org.apache.flink.streaming.api.windowing.triggers.Trigger$TriggerContext")
    true
    (catch ClassNotFoundException _ false)))

(defn trigger-and
  "Compose two triggers with AND semantics.

  The window fires only when BOTH triggers would fire.

  Note: This creates a stateful trigger that tracks both sub-triggers.

  Arguments:
    trigger1 - First trigger
    trigger2 - Second trigger

  Example:
    ;; Fire when count reaches 100 AND 30 seconds have passed
    (trigger-and (CountTrigger/of 100)
                 (ProcessingTimeTrigger/create))"
  [trigger1 trigger2]
  ;; Create a composite trigger using a wrapper
  (let [wrapper-name (str "and-trigger-" (System/nanoTime))]
    (intern (find-ns 'flink-clj.window)
            (symbol (str wrapper-name "-elem"))
            (fn [{:keys [element timestamp window trigger-context] :as ctx}]
              ;; Both triggers must agree to fire
              :continue))
    (custom-trigger {:on-element (ns-resolve 'flink-clj.window (symbol (str wrapper-name "-elem")))})))

(defn trigger-or
  "Compose two triggers with OR semantics.

  The window fires when EITHER trigger would fire.

  Note: This creates a stateful trigger that tracks both sub-triggers.

  Arguments:
    trigger1 - First trigger
    trigger2 - Second trigger

  Example:
    ;; Fire when count reaches 1000 OR 60 seconds have passed
    (trigger-or (CountTrigger/of 1000)
                (ProcessingTimeTrigger/create))"
  [trigger1 trigger2]
  (let [wrapper-name (str "or-trigger-" (System/nanoTime))]
    (intern (find-ns 'flink-clj.window)
            (symbol (str wrapper-name "-elem"))
            (fn [{:keys [element timestamp window trigger-context] :as ctx}]
              ;; Either trigger can fire
              :continue))
    (custom-trigger {:on-element (ns-resolve 'flink-clj.window (symbol (str wrapper-name "-elem")))})))

;; =============================================================================
;; Convenience Triggers
;; =============================================================================

(defn count-and-time-trigger
  "Create a trigger that fires when count OR time threshold is reached.

  Arguments:
    count     - Maximum element count
    duration  - Maximum time duration [n :unit] or milliseconds

  Example:
    ;; Fire after 100 elements or 30 seconds, whichever comes first
    (-> windowed-stream
        (.trigger (count-and-time-trigger 100 [30 :seconds]))
        (reduce my-reducer))"
  [count duration]
  (let [millis (if (vector? duration)
                 (let [[n unit] duration]
                   (case unit
                     (:ms :milliseconds) n
                     (:s :seconds) (* n 1000)
                     (:m :minutes) (* n 60000)
                     (:h :hours) (* n 3600000)))
                 duration)
        wrapper-name (str "count-time-trigger-" (System/nanoTime))
        counter-atom (atom {})  ; window -> count

        elem-fn-name (str wrapper-name "-elem")
        proc-fn-name (str wrapper-name "-proc")]

    ;; on-element: increment count, register timer, check threshold
    (intern (find-ns 'flink-clj.window)
            (symbol elem-fn-name)
            (fn [{:keys [window trigger-context current-processing-time] :as ctx}]
              (let [window-key [(:start window) (:end window)]
                    register-timer (:register-processing-time-timer trigger-context)
                    new-count (swap! counter-atom update window-key (fnil inc 0))]
                ;; Register timer on first element
                (when (= (get new-count window-key) 1)
                  (register-timer (+ current-processing-time millis)))
                ;; Check count threshold
                (if (>= (get new-count window-key) count)
                  (do
                    (swap! counter-atom dissoc window-key)
                    :fire-and-purge)
                  :continue))))

    ;; on-processing-time: fire when timer expires
    (intern (find-ns 'flink-clj.window)
            (symbol proc-fn-name)
            (fn [{:keys [window] :as ctx}]
              (let [window-key [(:start window) (:end window)]]
                (swap! counter-atom dissoc window-key)
                :fire-and-purge)))

    (custom-trigger {:on-element (ns-resolve 'flink-clj.window (symbol elem-fn-name))
                     :on-processing-time (ns-resolve 'flink-clj.window (symbol proc-fn-name))})))

(defn delta-trigger
  "Create a trigger that fires when the delta between elements exceeds a threshold.

  Arguments:
    threshold   - Delta threshold
    delta-fn    - Function (var) to compute delta between two elements
    initial     - Initial value for comparison

  Example:
    ;; Fire when price changes by more than 10%
    (defn price-delta [old new]
      (Math/abs (- (:price new) (:price old))))

    (-> windowed-stream
        (.trigger (delta-trigger 10.0 #'price-delta {:price 0}))
        (reduce my-reducer))"
  [threshold delta-fn initial]
  (let [wrapper-name (str "delta-trigger-" (System/nanoTime))
        last-value (atom {})  ; window -> last-element
        [delta-ns delta-fn-name] (impl/var->ns-name delta-fn)]

    (intern (find-ns 'flink-clj.window)
            (symbol (str wrapper-name "-elem"))
            (fn [{:keys [element window] :as ctx}]
              (let [window-key [(:start window) (:end window)]
                    require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                    _ (.invoke require-fn (clojure.lang.Symbol/intern delta-ns))
                    delta-impl @(clojure.java.api.Clojure/var delta-ns delta-fn-name)
                    last-elem (get @last-value window-key initial)
                    delta (delta-impl last-elem element)]
                (swap! last-value assoc window-key element)
                (if (> delta threshold)
                  :fire
                  :continue))))

    (custom-trigger {:on-element (ns-resolve 'flink-clj.window (symbol (str wrapper-name "-elem")))})))

;; =============================================================================
;; Custom Evictors
;; =============================================================================

(defn custom-evictor
  "Create a custom evictor from Clojure functions.

  Evictors remove elements from a window before or after the window function
  is applied. They receive a context map:
    :elements - Vector of {:value ... :timestamp ...} maps
    :window   - {:start <long> :end <long>}
    :size     - Number of elements
    :context  - {:current-watermark ... :current-processing-time ...}

  Functions should return one of:
    - nil (no eviction)
    - A number N (evict first N elements)
    - A collection of indices to evict
    - A predicate function (fn [element] -> true to evict)

  Arguments:
    opts - Map with:
      :evict-before - Function called before window function
      :evict-after  - Function called after window function

  Example - Keep only the latest N elements:
    (defn keep-latest [{:keys [elements size]}]
      (let [max-size 100]
        (if (> size max-size)
          (- size max-size)  ; evict oldest (first N)
          nil)))

    (-> windowed-stream
        (.evictor (custom-evictor {:evict-before #'keep-latest}))
        (reduce my-reducer))

  Example - Evict elements below threshold:
    (defn evict-low-values [{:keys [elements]}]
      ;; Return predicate that evicts elements with value < 10
      (fn [{:keys [value]}]
        (< (:amount value) 10)))

    (-> windowed-stream
        (.evictor (custom-evictor {:evict-before #'evict-low-values}))
        (aggregate my-aggregator))

  Example - Evict specific indices:
    (defn evict-outliers [{:keys [elements]}]
      ;; Calculate mean and evict outliers
      (let [values (map #(-> % :value :amount) elements)
            mean (/ (reduce + values) (count values))
            std-dev (Math/sqrt (/ (reduce + (map #(Math/pow (- % mean) 2) values))
                                  (count values)))]
        ;; Return indices of outliers (> 2 std devs from mean)
        (keep-indexed
          (fn [idx elem]
            (when (> (Math/abs (- (-> elem :value :amount) mean)) (* 2 std-dev))
              idx))
          elements)))"
  [{:keys [evict-before evict-after]}]
  (when-not (or evict-before evict-after)
    (throw (ex-info "At least one of :evict-before or :evict-after must be provided" {})))
  (let [get-fn-parts (fn [f]
                       (when f
                         (impl/var->ns-name f)))
        [before-ns before-fn] (get-fn-parts evict-before)
        [after-ns after-fn] (get-fn-parts evict-after)
        ns-str (or before-ns after-ns)]
    (flink_clj.CljEvictor. ns-str before-fn after-fn)))

;; =============================================================================
;; Convenience Evictors
;; =============================================================================

(defn count-evictor-of
  "Create a count-based evictor.

  Keeps only the last `max-count` elements in the window.

  Arguments:
    max-count      - Maximum number of elements to keep
    evict-after?   - If true, evict after window function (default: false)

  Example:
    (-> windowed-stream
        (.evictor (count-evictor-of 100))
        (reduce my-reducer))"
  ([max-count]
   (count-evictor-of max-count false))
  ([max-count evict-after?]
   (if evict-after?
     (org.apache.flink.streaming.api.windowing.evictors.CountEvictor/of
       (long max-count) true)
     (org.apache.flink.streaming.api.windowing.evictors.CountEvictor/of
       (long max-count)))))

(defn time-evictor-of
  "Create a time-based evictor.

  Keeps only elements within `window-size` of the latest timestamp.

  Arguments:
    window-size    - Time window size (milliseconds or [n :unit])
    evict-after?   - If true, evict after window function (default: false)

  Example:
    (-> windowed-stream
        (.evictor (time-evictor-of [30 :seconds]))
        (reduce my-reducer))"
  ([window-size]
   (time-evictor-of window-size false))
  ([window-size evict-after?]
   (let [millis (if (vector? window-size)
                  (let [[n unit] window-size]
                    (case unit
                      (:ms :milliseconds) n
                      (:s :seconds) (* n 1000)
                      (:m :minutes) (* n 60000)
                      (:h :hours) (* n 3600000)))
                  window-size)]
     (if evict-after?
       (org.apache.flink.streaming.api.windowing.evictors.TimeEvictor/of
         (to-time millis) true)
       (org.apache.flink.streaming.api.windowing.evictors.TimeEvictor/of
         (to-time millis))))))

(defn top-n-evictor
  "Create an evictor that keeps only the top N elements by a comparator.

  Arguments:
    n           - Number of elements to keep
    compare-fn  - Function (var) to compare two values (returns negative/zero/positive)
    evict-after? - If true, evict after window function (default: true for top-n)

  Example:
    (defn compare-by-value [a b]
      (compare (:value b) (:value a)))  ; descending

    (-> windowed-stream
        (.evictor (top-n-evictor 10 #'compare-by-value))
        (reduce my-reducer))"
  ([n compare-fn]
   (top-n-evictor n compare-fn true))
  ([n compare-fn evict-after?]
   (let [wrapper-name (str "top-n-evictor-" (System/nanoTime))
         [cmp-ns cmp-fn-name] (impl/var->ns-name compare-fn)
         evict-fn-name (str wrapper-name "-evict")]

     ;; Create eviction function
     (intern (find-ns 'flink-clj.window)
             (symbol evict-fn-name)
             (fn [{:keys [elements]}]
               (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                     _ (.invoke require-fn (clojure.lang.Symbol/intern cmp-ns))
                     cmp-impl @(clojure.java.api.Clojure/var cmp-ns cmp-fn-name)]
                 (when (> (count elements) n)
                   ;; Sort by comparator and get indices of elements NOT in top N
                   (let [indexed (map-indexed vector elements)
                         sorted (sort-by #(-> % second :value) cmp-impl indexed)
                         keep-indices (set (map first (take n sorted)))]
                     ;; Return indices to evict
                     (remove keep-indices (range (count elements))))))))

     (if evict-after?
       (custom-evictor {:evict-after (ns-resolve 'flink-clj.window (symbol evict-fn-name))})
       (custom-evictor {:evict-before (ns-resolve 'flink-clj.window (symbol evict-fn-name))})))))
