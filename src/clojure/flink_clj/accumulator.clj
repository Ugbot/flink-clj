(ns flink-clj.accumulator
  "Accumulators for collecting job-level metrics and statistics.

  Accumulators are used to gather statistics across all parallel instances
  of an operator. Unlike metrics (which are for monitoring), accumulators
  are returned with the job result and can be used for:
  - Counting records processed
  - Computing sums, averages, histograms
  - Collecting sample data
  - Validating job output

  Built-in accumulator types:
  - IntCounter, LongCounter, DoubleCounter
  - IntMinimum, IntMaximum, LongMinimum, LongMaximum
  - DoubleMinimum, DoubleMaximum
  - ListAccumulator (collects samples)
  - AverageAccumulator
  - Histogram (custom)

  Example:
    (require '[flink-clj.accumulator :as acc])

    ;; In your processing function (via RuntimeContext)
    (defn process-with-counting [ctx element]
      (acc/add! ctx \"records-processed\" 1)
      (acc/add! ctx \"total-value\" (:value element))
      element)

    ;; After job execution
    (let [result (flink/execute env \"My Job\")
          count (acc/get-result result \"records-processed\")
          total (acc/get-result result \"total-value\")]
      (println \"Processed\" count \"records with total value\" total))"
  (:import [org.apache.flink.api.common.accumulators
            IntCounter LongCounter DoubleCounter
            IntMinimum IntMaximum LongMinimum LongMaximum
            DoubleMinimum DoubleMaximum
            ListAccumulator AverageAccumulator Histogram Accumulator]
           [org.apache.flink.api.common JobExecutionResult]))

;; =============================================================================
;; Accumulator Creation
;; =============================================================================

(defn int-counter
  "Create an integer counter accumulator.

  Counts integer values. Useful for record counting."
  ([] (IntCounter.))
  ([initial-value] (IntCounter. (int initial-value))))

(defn long-counter
  "Create a long counter accumulator.

  Counts long values. Use for large counts that may exceed Integer.MAX_VALUE."
  ([] (LongCounter.))
  ([initial-value] (LongCounter. (long initial-value))))

(defn double-counter
  "Create a double counter accumulator.

  Sums double values. Use for floating-point aggregations."
  ([] (DoubleCounter.))
  ([initial-value] (DoubleCounter. (double initial-value))))

(defn int-minimum
  "Create an integer minimum accumulator.

  Tracks the minimum integer value seen."
  ([] (IntMinimum.))
  ([initial-value] (IntMinimum. (int initial-value))))

(defn int-maximum
  "Create an integer maximum accumulator.

  Tracks the maximum integer value seen."
  ([] (IntMaximum.))
  ([initial-value] (IntMaximum. (int initial-value))))

(defn long-minimum
  "Create a long minimum accumulator.

  Tracks the minimum long value seen."
  ([] (LongMinimum.))
  ([initial-value] (LongMinimum. (long initial-value))))

(defn long-maximum
  "Create a long maximum accumulator.

  Tracks the maximum long value seen."
  ([] (LongMaximum.))
  ([initial-value] (LongMaximum. (long initial-value))))

(defn double-minimum
  "Create a double minimum accumulator.

  Tracks the minimum double value seen."
  ([] (DoubleMinimum.))
  ([initial-value] (DoubleMinimum. (double initial-value))))

(defn double-maximum
  "Create a double maximum accumulator.

  Tracks the maximum double value seen."
  ([] (DoubleMaximum.))
  ([initial-value] (DoubleMaximum. (double initial-value))))

(defn list-accumulator
  "Create a list accumulator.

  Collects items into a list. Use sparingly - all items are
  collected to the JobManager!

  Example:
    ;; Collect sample errors
    (add! ctx \"errors\" error-message)"
  []
  (ListAccumulator.))

(defn average-accumulator
  "Create an average accumulator.

  Computes the average of all added values."
  []
  (AverageAccumulator.))

(defn histogram
  "Create a histogram accumulator.

  Tracks frequency distribution of integer values.
  The histogram counts occurrences of each unique integer value.

  Note: Flink's Histogram is a simple frequency counter, not a bucket-based
  histogram. For bucket-based distributions, use custom logic on top of
  a ListAccumulator.

  Example:
    (histogram)  ; Create empty histogram
    ;; Later: (.add histogram 42) increments count for value 42"
  []
  (Histogram.))

;; =============================================================================
;; Registration (in RichFunction.open or similar)
;; =============================================================================

(defn register!
  "Register an accumulator with RuntimeContext.

  Call this in a RichFunction's open() method.

  Arguments:
    ctx   - RuntimeContext
    name  - Name for the accumulator
    acc   - Accumulator instance

  Example:
    (defn open-fn [ctx]
      (acc/register! ctx \"record-count\" (acc/long-counter))
      (acc/register! ctx \"max-value\" (acc/int-maximum)))"
  [ctx name ^Accumulator acc]
  (.addAccumulator ctx name acc)
  acc)

(defn get-accumulator
  "Get a previously registered accumulator.

  Call this after register! to get the accumulator for updates.

  Arguments:
    ctx  - RuntimeContext
    name - Name the accumulator was registered with"
  [ctx name]
  (.getAccumulator ctx name))

;; =============================================================================
;; Updates (during processing)
;; =============================================================================

(defn add!
  "Add a value to a counter/sum accumulator.

  If accumulator doesn't exist, registers a new one automatically.

  Arguments:
    ctx   - RuntimeContext
    name  - Accumulator name
    value - Value to add

  Example:
    (acc/add! ctx \"records\" 1)
    (acc/add! ctx \"total-amount\" 99.95)"
  [ctx name value]
  (let [acc (get-accumulator ctx name)]
    (cond
      (nil? acc)
      (throw (ex-info (str "Accumulator not registered: " name)
                      {:name name
                       :hint "Register accumulator in open() method"}))

      (instance? IntCounter acc)
      (.add ^IntCounter acc (int value))

      (instance? LongCounter acc)
      (.add ^LongCounter acc (long value))

      (instance? DoubleCounter acc)
      (.add ^DoubleCounter acc (double value))

      (instance? AverageAccumulator acc)
      (.add ^AverageAccumulator acc (double value))

      (instance? ListAccumulator acc)
      (.add ^ListAccumulator acc value)

      :else
      (.add acc value))))

(defn track-min!
  "Track minimum value.

  Arguments:
    ctx   - RuntimeContext
    name  - Accumulator name
    value - Value to compare"
  [ctx name value]
  (let [acc (get-accumulator ctx name)]
    (cond
      (instance? IntMinimum acc)
      (.add ^IntMinimum acc (int value))

      (instance? LongMinimum acc)
      (.add ^LongMinimum acc (long value))

      (instance? DoubleMinimum acc)
      (.add ^DoubleMinimum acc (double value))

      :else
      (throw (ex-info "Not a minimum accumulator" {:name name})))))

(defn track-max!
  "Track maximum value.

  Arguments:
    ctx   - RuntimeContext
    name  - Accumulator name
    value - Value to compare"
  [ctx name value]
  (let [acc (get-accumulator ctx name)]
    (cond
      (instance? IntMaximum acc)
      (.add ^IntMaximum acc (int value))

      (instance? LongMaximum acc)
      (.add ^LongMaximum acc (long value))

      (instance? DoubleMaximum acc)
      (.add ^DoubleMaximum acc (double value))

      :else
      (throw (ex-info "Not a maximum accumulator" {:name name})))))

(defn collect!
  "Add an item to a list accumulator.

  Use sparingly - all items are sent to JobManager!

  Arguments:
    ctx   - RuntimeContext
    name  - Accumulator name
    item  - Item to collect"
  [ctx name item]
  (let [acc (get-accumulator ctx name)]
    (if (instance? ListAccumulator acc)
      (.add ^ListAccumulator acc item)
      (throw (ex-info "Not a list accumulator" {:name name})))))

(defn histogram-add!
  "Add a value to a histogram accumulator.

  Arguments:
    ctx   - RuntimeContext
    name  - Accumulator name
    value - Value to bucket"
  [ctx name value]
  (let [acc (get-accumulator ctx name)]
    (if (instance? Histogram acc)
      (.add ^Histogram acc (int value))
      (throw (ex-info "Not a histogram accumulator" {:name name})))))

;; =============================================================================
;; Results (after job execution)
;; =============================================================================

(defn get-result
  "Get an accumulator result from a completed job.

  Arguments:
    job-result - JobExecutionResult from execute()
    name       - Accumulator name

  Example:
    (let [result (flink/execute env \"My Job\")]
      (println \"Count:\" (acc/get-result result \"record-count\")))"
  [^JobExecutionResult job-result name]
  (.getAccumulatorResult job-result name))

(defn get-all-results
  "Get all accumulator results from a completed job.

  Returns a map of accumulator-name -> value."
  [^JobExecutionResult job-result]
  (into {} (.getAllAccumulatorResults job-result)))

;; =============================================================================
;; Convenience Macros
;; =============================================================================

(defmacro with-accumulators
  "Register multiple accumulators and provide them in scope.

  Arguments:
    ctx      - RuntimeContext
    bindings - Vector of [name accumulator] pairs
    body     - Code to execute

  Example:
    (with-accumulators ctx
      [[counter (long-counter)]
       [max-val (long-maximum)]]
      ;; counter and max-val are now registered and available
      (add! ctx \"counter\" 1))"
  [ctx bindings & body]
  (let [pairs (partition 2 bindings)
        registrations (for [[sym acc-expr] pairs]
                        `(register! ~ctx ~(name sym) ~acc-expr))]
    `(do ~@registrations ~@body)))

;; =============================================================================
;; Info
;; =============================================================================

(defn accumulator-info
  "Get information about accumulator types."
  []
  {:description "Job-level metrics and statistics collection"
   :types {:int-counter "Sum of integer values"
           :long-counter "Sum of long values"
           :double-counter "Sum of double values"
           :int-minimum "Minimum integer seen"
           :int-maximum "Maximum integer seen"
           :long-minimum "Minimum long seen"
           :long-maximum "Maximum long seen"
           :double-minimum "Minimum double seen"
           :double-maximum "Maximum double seen"
           :list "Collection of items (use sparingly)"
           :average "Running average"
           :histogram "Value distribution"}
   :usage-notes ["Register in RichFunction.open()"
                 "Update during processing"
                 "Read from JobExecutionResult after job completes"
                 "List accumulators send all data to JobManager - use sparingly"]})
