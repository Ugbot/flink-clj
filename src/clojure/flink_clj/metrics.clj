(ns flink-clj.metrics
  "Custom metrics for monitoring Flink jobs.

  Flink provides four types of metrics:
  - Counter: Counts events (can be incremented/decremented)
  - Gauge: Reports a value at a point in time
  - Histogram: Measures distribution of values (requires flink-metrics-dropwizard)
  - Meter: Measures rate of events per second (requires flink-metrics-dropwizard)

  Metrics are registered through the RuntimeContext in RichFunctions.
  This namespace provides utilities for working with metrics from Clojure.

  Example:
    (require '[flink-clj.metrics :as m])

    ;; In a process function's open handler
    (defn setup-metrics [ctx]
      (let [counter (m/counter ctx \"my-group\" \"processed-count\")]
        {:counter counter}))

    ;; In the process function
    (defn process-with-metrics [{:keys [element]} metrics]
      (m/inc! (:counter metrics))
      (process element))"
  (:import [org.apache.flink.metrics Counter Gauge Histogram Meter MetricGroup]
           [org.apache.flink.api.common.functions RuntimeContext]))

;; =============================================================================
;; Metric Group Operations
;; =============================================================================

(defn metric-group
  "Get the metric group from a RuntimeContext.

  Example:
    (metric-group runtime-context)"
  [^RuntimeContext ctx]
  (.getMetricGroup ctx))

(defn add-group
  "Add a subgroup to a metric group.

  Example:
    (-> (metric-group ctx)
        (add-group \"my-operator\")
        (add-group \"counters\"))"
  [^MetricGroup group ^String name]
  (.addGroup group name))

(defn add-groups
  "Add multiple nested subgroups.

  Example:
    (add-groups (metric-group ctx) \"my-app\" \"operator\" \"counters\")"
  [^MetricGroup group & names]
  (reduce add-group group names))

;; =============================================================================
;; Counter
;; =============================================================================

(defn counter
  "Register or get a counter metric.

  A counter can be incremented and decremented.

  Example:
    (def my-counter (counter ctx \"events-processed\"))
    (inc! my-counter)
    (dec! my-counter)
    (add! my-counter 10)"
  ([^RuntimeContext ctx ^String name]
   (.counter (.getMetricGroup ctx) name))
  ([^RuntimeContext ctx ^String group-name ^String name]
   (-> (.getMetricGroup ctx)
       (.addGroup group-name)
       (.counter name))))

(defn inc!
  "Increment a counter by 1 or by a specific amount.

  Example:
    (inc! my-counter)
    (inc! my-counter 5)"
  ([^Counter counter]
   (.inc counter))
  ([^Counter counter ^long n]
   (.inc counter n)))

(defn dec!
  "Decrement a counter by 1 or by a specific amount.

  Example:
    (dec! my-counter)
    (dec! my-counter 5)"
  ([^Counter counter]
   (.dec counter))
  ([^Counter counter ^long n]
   (.dec counter n)))

(defn add!
  "Add a value to a counter (alias for inc! with amount).

  Example:
    (add! my-counter 10)"
  [^Counter counter ^long n]
  (.inc counter n))

(defn counter-value
  "Get the current value of a counter.

  Example:
    (counter-value my-counter)"
  [^Counter counter]
  (.getCount counter))

;; =============================================================================
;; Gauge
;; =============================================================================

(defn gauge
  "Register a gauge metric.

  A gauge reports a value at a point in time. The value-fn is called
  when the metric is scraped.

  value-fn should be a no-arg function returning the current value.

  Example:
    (def queue-size (atom 0))
    (gauge ctx \"queue-size\" #(deref queue-size))

    ;; With group
    (gauge ctx \"my-group\" \"queue-size\" #(deref queue-size))"
  ([^RuntimeContext ctx ^String name value-fn]
   (let [g (reify Gauge
             (getValue [_] (value-fn)))]
     (.gauge (.getMetricGroup ctx) name g)))
  ([^RuntimeContext ctx ^String group-name ^String name value-fn]
   (let [g (reify Gauge
             (getValue [_] (value-fn)))]
     (-> (.getMetricGroup ctx)
         (.addGroup group-name)
         (.gauge name g)))))

(defn gauge-from-atom
  "Register a gauge that reports the value of an atom.

  Convenience function for the common pattern of gauging an atom.

  Example:
    (def my-value (atom 0))
    (gauge-from-atom ctx \"current-value\" my-value)"
  ([^RuntimeContext ctx ^String name atom-ref]
   (gauge ctx name #(deref atom-ref)))
  ([^RuntimeContext ctx ^String group-name ^String name atom-ref]
   (gauge ctx group-name name #(deref atom-ref))))

;; =============================================================================
;; Histogram (requires flink-metrics-dropwizard)
;; =============================================================================

(defn- dropwizard-available?
  "Check if Dropwizard metrics wrapper is available."
  []
  (try
    (Class/forName "org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper")
    true
    (catch ClassNotFoundException _ false)))

(defn histogram
  "Register a histogram metric.

  A histogram measures the distribution of values over time.

  NOTE: Requires flink-metrics-dropwizard dependency.
  Add to project.clj: [org.apache.flink/flink-metrics-dropwizard \"<version>\"]

  Example:
    (def latency-hist (histogram ctx \"request-latency\"))
    (update-histogram! latency-hist 42)"
  ([^RuntimeContext ctx ^String name]
   (if (dropwizard-available?)
     (let [wrapper-class (Class/forName "org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper")
           hist-class (Class/forName "com.codahale.metrics.Histogram")
           reservoir-class (Class/forName "com.codahale.metrics.SlidingTimeWindowReservoir")
           time-unit java.util.concurrent.TimeUnit/SECONDS
           reservoir-ctor (.getConstructor reservoir-class (into-array Class [Long/TYPE java.util.concurrent.TimeUnit]))
           reservoir (.newInstance reservoir-ctor (into-array Object [(long 60) time-unit]))
           hist-ctor (.getConstructor hist-class (into-array Class [(Class/forName "com.codahale.metrics.Reservoir")]))
           hist (.newInstance hist-ctor (into-array Object [reservoir]))
           wrapper-ctor (.getConstructor wrapper-class (into-array Class [hist-class]))
           wrapper (.newInstance wrapper-ctor (into-array Object [hist]))]
       (.histogram (.getMetricGroup ctx) name wrapper))
     (throw (ex-info "Histogram requires flink-metrics-dropwizard dependency"
                     {:dependency "org.apache.flink/flink-metrics-dropwizard"}))))
  ([^RuntimeContext ctx ^String group-name ^String name]
   (if (dropwizard-available?)
     (let [group (-> (.getMetricGroup ctx) (.addGroup group-name))]
       (histogram {:metric-group group} name))
     (throw (ex-info "Histogram requires flink-metrics-dropwizard dependency"
                     {:dependency "org.apache.flink/flink-metrics-dropwizard"})))))

(defn update-histogram!
  "Update a histogram with a value.

  Example:
    (update-histogram! my-histogram 42)"
  [^Histogram histogram ^long value]
  (.update histogram value))

(defn histogram-count
  "Get the count of values in a histogram."
  [^Histogram histogram]
  (.getCount histogram))

;; =============================================================================
;; Meter (requires flink-metrics-dropwizard)
;; =============================================================================

(defn meter
  "Register a meter metric.

  A meter measures the rate of events over time (events per second).

  NOTE: Requires flink-metrics-dropwizard dependency.
  Add to project.clj: [org.apache.flink/flink-metrics-dropwizard \"<version>\"]

  Example:
    (def event-rate (meter ctx \"events-per-second\"))
    (mark! event-rate)"
  ([^RuntimeContext ctx ^String name]
   (if (dropwizard-available?)
     (let [wrapper-class (Class/forName "org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper")
           meter-class (Class/forName "com.codahale.metrics.Meter")
           meter (.newInstance meter-class)
           wrapper-ctor (.getConstructor wrapper-class (into-array Class [meter-class]))
           wrapper (.newInstance wrapper-ctor (into-array Object [meter]))]
       (.meter (.getMetricGroup ctx) name wrapper))
     (throw (ex-info "Meter requires flink-metrics-dropwizard dependency"
                     {:dependency "org.apache.flink/flink-metrics-dropwizard"}))))
  ([^RuntimeContext ctx ^String group-name ^String name]
   (if (dropwizard-available?)
     (let [group (-> (.getMetricGroup ctx) (.addGroup group-name))]
       (meter {:metric-group group} name))
     (throw (ex-info "Meter requires flink-metrics-dropwizard dependency"
                     {:dependency "org.apache.flink/flink-metrics-dropwizard"})))))

(defn mark!
  "Mark an event occurrence in a meter.

  Example:
    (mark! my-meter)
    (mark! my-meter 5)  ; mark 5 events"
  ([^Meter meter]
   (.markEvent meter))
  ([^Meter meter ^long n]
   (.markEvent meter n)))

(defn meter-rate
  "Get the current rate (events per second) from a meter."
  [^Meter meter]
  (.getRate meter))

(defn meter-count
  "Get the total count of events in a meter."
  [^Meter meter]
  (.getCount meter))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn timed
  "Time the execution of a function and update a histogram.

  Returns the result of the function.

  Example:
    (def latency (histogram ctx \"processing-latency\"))
    (timed latency #(expensive-computation x))"
  [^Histogram histogram f]
  (let [start (System/nanoTime)
        result (f)
        elapsed (/ (- (System/nanoTime) start) 1000000.0)]
    (update-histogram! histogram (long elapsed))
    result))

(defmacro with-timing
  "Macro version of timed for cleaner syntax.

  Example:
    (with-timing latency-histogram
      (expensive-computation x))"
  [histogram & body]
  `(timed ~histogram (fn [] ~@body)))

(defn counted
  "Wrap a function to automatically count invocations.

  Example:
    (def call-counter (counter ctx \"function-calls\"))
    (def counted-fn (counted call-counter my-fn))"
  [^Counter counter f]
  (fn [& args]
    (inc! counter)
    (apply f args)))

(defn metered
  "Wrap a function to automatically track invocation rate.

  Example:
    (def rate-meter (meter ctx \"function-rate\"))
    (def metered-fn (metered rate-meter my-fn))"
  [^Meter meter f]
  (fn [& args]
    (mark! meter)
    (apply f args)))

;; =============================================================================
;; Simple Metric Registration
;; =============================================================================

(defn register-counters!
  "Register multiple counters at once.

  Returns a map of keyword -> Counter.

  Example:
    (register-counters! ctx \"my-group\"
      [\"processed\" \"errors\" \"skipped\"])
    ;; Returns {:processed #<Counter>, :errors #<Counter>, :skipped #<Counter>}"
  [^RuntimeContext ctx ^String group-name counter-names]
  (let [group (-> (.getMetricGroup ctx) (.addGroup group-name))]
    (into {}
          (for [n counter-names]
            [(keyword n) (.counter group (name n))]))))

(defn register-gauges!
  "Register multiple gauges at once.

  Takes a map of name -> value-fn.
  Returns a map of keyword -> Gauge.

  Example:
    (register-gauges! ctx \"my-group\"
      {\"queue-size\" #(count @queue)
       \"active-users\" #(count @active)})"
  [^RuntimeContext ctx ^String group-name gauge-specs]
  (let [group (-> (.getMetricGroup ctx) (.addGroup group-name))]
    (into {}
          (for [[n f] gauge-specs]
            [(keyword n)
             (.gauge group (name n)
                     (reify Gauge (getValue [_] (f))))]))))

;; =============================================================================
;; Latency Tracking
;; =============================================================================

(defn latency-tracker
  "Create a latency tracking helper with histogram and counter.

  Returns a map with:
    :histogram - The latency histogram
    :counter   - Count of operations
    :track!    - Function to track a duration (in ms)

  Example:
    (def tracker (latency-tracker ctx \"api-calls\"))
    ((:track! tracker) 42)  ; record 42ms latency"
  [^RuntimeContext ctx ^String name]
  (if (dropwizard-available?)
    (let [hist (histogram ctx name)
          cnt (counter ctx (str name "-count"))]
      {:histogram hist
       :counter cnt
       :track! (fn [ms]
                 (update-histogram! hist (long ms))
                 (inc! cnt))})
    ;; Fallback without histogram
    (let [cnt (counter ctx (str name "-count"))
          total (atom 0)]
      {:counter cnt
       :total total
       :track! (fn [ms]
                 (swap! total + ms)
                 (inc! cnt))})))

(defn throughput-tracker
  "Create a throughput tracking helper with meter and counter.

  Returns a map with:
    :meter   - The throughput meter
    :counter - Total count
    :track!  - Function to record n events (default: 1)

  Example:
    (def tracker (throughput-tracker ctx \"events\"))
    ((:track! tracker))     ; record 1 event
    ((:track! tracker) 10)  ; record 10 events"
  [^RuntimeContext ctx ^String name]
  (if (dropwizard-available?)
    (let [m (meter ctx name)
          cnt (counter ctx (str name "-total"))]
      {:meter m
       :counter cnt
       :track! (fn
                 ([]
                  (mark! m)
                  (inc! cnt))
                 ([n]
                  (mark! m n)
                  (inc! cnt n)))})
    ;; Fallback without meter
    (let [cnt (counter ctx name)]
      {:counter cnt
       :track! (fn
                 ([] (inc! cnt))
                 ([n] (inc! cnt n)))})))

;; =============================================================================
;; Error Rate Tracking
;; =============================================================================

(defn error-rate-tracker
  "Create an error rate tracking helper.

  Returns a map with:
    :success-counter - Count of successes
    :error-counter   - Count of errors
    :record-success! - Record a success
    :record-error!   - Record an error
    :error-rate      - Function to get current error rate (0.0-1.0)

  Example:
    (def tracker (error-rate-tracker ctx \"api\"))
    (try
      (api-call)
      ((:record-success! tracker))
      (catch Exception e
        ((:record-error! tracker))))"
  [^RuntimeContext ctx ^String name]
  (let [success-cnt (counter ctx (str name "-success"))
        error-cnt (counter ctx (str name "-error"))]
    {:success-counter success-cnt
     :error-counter error-cnt
     :record-success! (fn [] (inc! success-cnt))
     :record-error! (fn [] (inc! error-cnt))
     :error-rate (fn []
                   (let [s (counter-value success-cnt)
                         e (counter-value error-cnt)
                         total (+ s e)]
                     (if (zero? total)
                       0.0
                       (double (/ e total)))))}))

;; =============================================================================
;; Metrics Info
;; =============================================================================

(defn metrics-info
  "Get information about metrics capabilities."
  []
  {:dropwizard-available (dropwizard-available?)
   :types {:counter "Counts events (inc/dec)"
           :gauge "Reports value at a point in time"
           :histogram "Distribution of values (requires dropwizard)"
           :meter "Rate of events per second (requires dropwizard)"}
   :helpers [:latency-tracker
             :throughput-tracker
             :error-rate-tracker
             :timed
             :counted
             :metered]})

;; =============================================================================
;; Watermark Metrics
;; =============================================================================
;;
;; Flink provides watermark-related metrics for monitoring event time progress.
;; In Flink 2.1+, split-level watermark metrics are available (FLIP-513).

(defn watermark-gauge
  "Register a gauge that reports the current watermark.

  Useful for monitoring event time progress in process functions.
  The watermark-fn should return the current watermark (Long.MIN_VALUE if none).

  Example:
    (def current-watermark (atom Long/MIN_VALUE))
    (watermark-gauge ctx \"event-time\" \"current-watermark\"
                     #(deref current-watermark))"
  [^RuntimeContext ctx ^String group-name ^String name watermark-fn]
  (gauge ctx group-name name watermark-fn))

(defn watermark-lag-gauge
  "Register a gauge that reports watermark lag (current time - watermark).

  Useful for monitoring how far behind event time processing is.

  watermark-fn should return the current watermark in milliseconds.
  Returns lag in milliseconds (0 if watermark is Long/MIN_VALUE).

  Example:
    (watermark-lag-gauge ctx \"processing\" \"watermark-lag\"
                         #(deref current-watermark-atom))"
  [^RuntimeContext ctx ^String group-name ^String name watermark-fn]
  (gauge ctx group-name name
         (fn []
           (let [wm (watermark-fn)]
             (if (= wm Long/MIN_VALUE)
               0
               (- (System/currentTimeMillis) wm))))))

;; =============================================================================
;; Source Reader Metrics (for Source implementations)
;; =============================================================================
;;
;; These functions help custom Source implementations register metrics
;; using the SourceReaderMetricGroup.

(defn- source-reader-metric-group-available?
  "Check if SourceReaderMetricGroup is available."
  []
  (try
    (Class/forName "org.apache.flink.metrics.groups.SourceReaderMetricGroup")
    true
    (catch ClassNotFoundException _ false)))

(defn source-pending-records-gauge
  "Set the pending records gauge on a SourceReaderMetricGroup.

  pending-fn should return the number of records available but not yet fetched.
  For example, records available beyond the current Kafka offset.

  Example:
    (source-pending-records-gauge source-reader-metric-group
                                   #(count @pending-records))"
  [source-reader-metric-group pending-fn]
  (when-not (source-reader-metric-group-available?)
    (throw (ex-info "SourceReaderMetricGroup not available"
                    {:hint "This feature requires the new Source API"})))
  (let [g (reify Gauge
            (getValue [_] (pending-fn)))]
    (.setPendingRecordsGauge source-reader-metric-group g)))

(defn source-pending-bytes-gauge
  "Set the pending bytes gauge on a SourceReaderMetricGroup.

  pending-fn should return the number of bytes available but not yet fetched.
  For example, bytes remaining in a file beyond the current position.

  Example:
    (source-pending-bytes-gauge source-reader-metric-group
                                 #(- file-size @current-position))"
  [source-reader-metric-group pending-fn]
  (when-not (source-reader-metric-group-available?)
    (throw (ex-info "SourceReaderMetricGroup not available"
                    {:hint "This feature requires the new Source API"})))
  (let [g (reify Gauge
            (getValue [_] (pending-fn)))]
    (.setPendingBytesGauge source-reader-metric-group g)))

(defn source-errors-counter
  "Get the records-in-errors counter from a SourceReaderMetricGroup.

  Use this to track failed record consumption.

  Example:
    (def error-counter (source-errors-counter source-reader-metric-group))
    ;; When an error occurs:
    (inc! error-counter)"
  [source-reader-metric-group]
  (when-not (source-reader-metric-group-available?)
    (throw (ex-info "SourceReaderMetricGroup not available"
                    {:hint "This feature requires the new Source API"})))
  (.getNumRecordsInErrorsCounter source-reader-metric-group))

;; =============================================================================
;; Split-Level Watermark Metrics (Flink 2.1+ / FLIP-513)
;; =============================================================================
;;
;; Flink 2.1 introduced split-level watermark metrics via FLIP-513.
;; These provide per-split visibility into watermark progress.
;;
;; Available metrics at the Split scope:
;;   - watermark.currentWatermark: Last watermark received (ms)
;;   - watermark.activeTimeMsPerSecond: Time active per second (ms)
;;   - watermark.idleTimeMsPerSecond: Time idle per second (ms)
;;   - watermark.pausedTimeMsPerSecond: Time paused due to alignment (ms)
;;   - watermark.accumulatedActiveTimeMs: Total accumulated active time (ms)
;;
;; These metrics are automatically registered by Flink when using FLIP-27 sources
;; with watermark alignment enabled. See flink-clj.watermark for watermark strategies.

(defn- split-watermark-metrics-available?
  "Check if split-level watermark metrics are available (Flink 2.1+)."
  []
  (try
    ;; Check for SourceSplitMetricGroup which was added in FLINK-37410
    (Class/forName "org.apache.flink.runtime.metrics.groups.SourceSplitMetricGroup")
    true
    (catch ClassNotFoundException _ false)))

(defn split-watermark-metrics-info
  "Get information about split-level watermark metrics (Flink 2.1+).

  Returns a map describing available metrics. These are automatically
  registered by Flink for sources using watermark alignment.

  Example:
    (split-watermark-metrics-info)
    ;; => {:available true
    ;;     :metrics {...}}"
  []
  {:available (split-watermark-metrics-available?)
   :metrics (when (split-watermark-metrics-available?)
              {:current-watermark
               {:name "watermark.currentWatermark"
                :description "Last watermark received (ms)"}
               :active-time
               {:name "watermark.activeTimeMsPerSecond"
                :description "Time active (not paused/idle) per second (ms)"}
               :idle-time
               {:name "watermark.idleTimeMsPerSecond"
                :description "Time marked idle by idleness detection per second (ms)"}
               :paused-time
               {:name "watermark.pausedTimeMsPerSecond"
                :description "Time paused due to watermark alignment per second (ms)"}
               :accumulated-active-time
               {:name "watermark.accumulatedActiveTimeMs"
                :description "Total accumulated active time since registered (ms)"}})})

;; =============================================================================
;; Watermark Progress Tracker
;; =============================================================================

(defn watermark-progress-tracker
  "Create a watermark progress tracking helper.

  Tracks watermark advancement with gauges for:
  - Current watermark
  - Watermark lag (current time - watermark)
  - Records processed since last watermark advancement

  Returns a map with:
    :watermark-atom - Atom holding current watermark
    :records-atom - Atom holding records since last advancement
    :update-watermark! - Function to update watermark
    :record-processed! - Function to record a processed element

  Example:
    (def tracker (watermark-progress-tracker ctx \"my-source\"))

    ;; In process function:
    ((:update-watermark! tracker) new-watermark)
    ((:record-processed! tracker))"
  [^RuntimeContext ctx ^String name]
  (let [wm-atom (atom Long/MIN_VALUE)
        records-atom (atom 0)
        last-advancement-atom (atom 0)]
    ;; Register gauges
    (gauge ctx name "currentWatermark" #(deref wm-atom))
    (gauge ctx name "watermarkLag"
           #(let [wm @wm-atom]
              (if (= wm Long/MIN_VALUE)
                0
                (- (System/currentTimeMillis) wm))))
    (gauge ctx name "recordsSinceAdvancement" #(deref records-atom))

    {:watermark-atom wm-atom
     :records-atom records-atom
     :update-watermark! (fn [new-wm]
                          (when (> new-wm @wm-atom)
                            (reset! wm-atom new-wm)
                            (reset! last-advancement-atom @records-atom)
                            (reset! records-atom 0)))
     :record-processed! (fn []
                          (swap! records-atom inc))}))
