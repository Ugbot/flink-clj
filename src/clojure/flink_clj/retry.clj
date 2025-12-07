(ns flink-clj.retry
  "Retry strategies for fault-tolerant stream processing.

  This namespace provides retry strategy configurations for:
  - Sink connectors (e.g., JDBC, Elasticsearch)
  - Lookup joins
  - Async I/O operations
  - Custom operators

  Retry strategies help handle transient failures like network timeouts,
  temporary unavailability, and rate limiting.

  Example:
    (require '[flink-clj.retry :as retry])

    ;; Fixed delay retry
    (def my-retry (retry/fixed-delay {:max-attempts 3 :delay-ms 1000}))

    ;; Exponential backoff
    (def exp-retry (retry/exponential-backoff {:max-attempts 5
                                                :initial-delay-ms 100
                                                :max-delay-ms 10000
                                                :multiplier 2.0}))

    ;; Use with sink configuration
    (sink/jdbc-sink {:retry my-retry ...})"
  (:require [clojure.string :as str]))

;; =============================================================================
;; Retry Strategy Builders
;; =============================================================================

(defn no-retry
  "Create a no-retry strategy (fail immediately on error).

  Use this when:
  - Errors are not recoverable
  - You want fast failure detection
  - You have external retry mechanisms

  Example:
    (retry/no-retry)"
  []
  {:strategy :no-retry
   :max-attempts 1})

(defn fixed-delay
  "Create a fixed delay retry strategy.

  Arguments:
    opts - Options map:
      :max-attempts - Maximum number of attempts (default: 3)
      :delay-ms     - Delay between retries in milliseconds (default: 1000)

  Use this for:
  - Simple retry scenarios
  - When consistent delay is preferred
  - Rate-limited APIs

  Example:
    (retry/fixed-delay {:max-attempts 5 :delay-ms 2000})"
  ([]
   (fixed-delay {}))
  ([{:keys [max-attempts delay-ms]
     :or {max-attempts 3 delay-ms 1000}}]
   {:strategy :fixed-delay
    :max-attempts max-attempts
    :delay-ms delay-ms}))

(defn exponential-backoff
  "Create an exponential backoff retry strategy.

  Arguments:
    opts - Options map:
      :max-attempts     - Maximum number of attempts (default: 5)
      :initial-delay-ms - Initial delay in milliseconds (default: 100)
      :max-delay-ms     - Maximum delay cap in milliseconds (default: 60000)
      :multiplier       - Delay multiplier per attempt (default: 2.0)
      :jitter?          - Add random jitter to delays (default: false)
      :jitter-factor    - Jitter factor 0.0-1.0 (default: 0.1)

  Use this for:
  - Rate-limited services
  - Overloaded backends
  - Network connectivity issues

  The delay formula is: min(initial * multiplier^attempt, max-delay)

  Example:
    (retry/exponential-backoff {:max-attempts 10
                                 :initial-delay-ms 100
                                 :max-delay-ms 30000
                                 :multiplier 2.0
                                 :jitter? true})"
  ([]
   (exponential-backoff {}))
  ([{:keys [max-attempts initial-delay-ms max-delay-ms multiplier jitter? jitter-factor]
     :or {max-attempts 5
          initial-delay-ms 100
          max-delay-ms 60000
          multiplier 2.0
          jitter? false
          jitter-factor 0.1}}]
   {:strategy :exponential-backoff
    :max-attempts max-attempts
    :initial-delay-ms initial-delay-ms
    :max-delay-ms max-delay-ms
    :multiplier multiplier
    :jitter? jitter?
    :jitter-factor jitter-factor}))

(defn linear-backoff
  "Create a linear backoff retry strategy.

  Arguments:
    opts - Options map:
      :max-attempts - Maximum number of attempts (default: 5)
      :delay-ms     - Initial and incremental delay in milliseconds (default: 1000)
      :max-delay-ms - Maximum delay cap in milliseconds (default: 60000)

  The delay formula is: min(delay * attempt, max-delay)

  Example:
    (retry/linear-backoff {:max-attempts 5 :delay-ms 2000 :max-delay-ms 20000})"
  ([]
   (linear-backoff {}))
  ([{:keys [max-attempts delay-ms max-delay-ms]
     :or {max-attempts 5 delay-ms 1000 max-delay-ms 60000}}]
   {:strategy :linear-backoff
    :max-attempts max-attempts
    :delay-ms delay-ms
    :max-delay-ms max-delay-ms}))

(defn fibonacci-backoff
  "Create a Fibonacci backoff retry strategy.

  Arguments:
    opts - Options map:
      :max-attempts - Maximum number of attempts (default: 5)
      :base-delay-ms - Base delay unit in milliseconds (default: 1000)
      :max-delay-ms  - Maximum delay cap in milliseconds (default: 60000)

  The delay follows Fibonacci sequence: 1, 1, 2, 3, 5, 8, 13, ... * base-delay

  Example:
    (retry/fibonacci-backoff {:max-attempts 8 :base-delay-ms 500})"
  ([]
   (fibonacci-backoff {}))
  ([{:keys [max-attempts base-delay-ms max-delay-ms]
     :or {max-attempts 5 base-delay-ms 1000 max-delay-ms 60000}}]
   {:strategy :fibonacci-backoff
    :max-attempts max-attempts
    :base-delay-ms base-delay-ms
    :max-delay-ms max-delay-ms}))

;; =============================================================================
;; Retry Predicates
;; =============================================================================

(defn retry-on
  "Create a retry predicate that retries on specific exception types.

  Arguments:
    exception-classes - Var-args of exception classes to retry on

  Example:
    (retry/retry-on java.net.SocketTimeoutException
                    java.io.IOException)"
  [& exception-classes]
  {:predicate :exception-classes
   :classes (vec exception-classes)})

(defn retry-on-result
  "Create a retry predicate that retries based on result value.

  Arguments:
    pred-fn - Function that takes result and returns true to retry

  Example:
    (retry/retry-on-result #(nil? %))"
  [pred-fn]
  {:predicate :result
   :fn pred-fn})

(defn retry-on-status
  "Create a retry predicate for HTTP status codes.

  Arguments:
    status-codes - Set of HTTP status codes that trigger retry

  Example:
    (retry/retry-on-status #{429 500 502 503 504})"
  [status-codes]
  {:predicate :http-status
   :codes (set status-codes)})

;; =============================================================================
;; Connector-Specific Retry Configurations
;; =============================================================================

(defn jdbc-retry-options
  "Generate JDBC connector retry options.

  Arguments:
    retry-strategy - Retry strategy from above functions

  Returns a map of JDBC connector options for retry behavior.

  Example:
    (merge (sink/jdbc-options {...})
           (retry/jdbc-retry-options
             (retry/fixed-delay {:max-attempts 3 :delay-ms 1000})))"
  [retry-strategy]
  (case (:strategy retry-strategy)
    :no-retry
    {:sink.max-retries "0"}

    :fixed-delay
    {:sink.max-retries (str (:max-attempts retry-strategy))
     :sink.retry-interval (str (:delay-ms retry-strategy) "ms")}

    ;; JDBC doesn't support exponential backoff natively,
    ;; so we use max-delay as a fixed interval
    (:exponential-backoff :linear-backoff :fibonacci-backoff)
    {:sink.max-retries (str (:max-attempts retry-strategy))
     :sink.retry-interval (str (:initial-delay-ms retry-strategy 1000) "ms")}))

(defn elasticsearch-retry-options
  "Generate Elasticsearch connector retry options.

  Arguments:
    retry-strategy - Retry strategy from above functions

  Returns a map of Elasticsearch connector options.

  Example:
    (merge (sink/elasticsearch-options {...})
           (retry/elasticsearch-retry-options
             (retry/exponential-backoff {:max-attempts 5})))"
  [retry-strategy]
  (case (:strategy retry-strategy)
    :no-retry
    {:sink.bulk-flush.backoff.type "DISABLED"}

    :fixed-delay
    {:sink.bulk-flush.backoff.type "CONSTANT"
     :sink.bulk-flush.backoff.max-retries (str (:max-attempts retry-strategy))
     :sink.bulk-flush.backoff.delay (str (:delay-ms retry-strategy) "ms")}

    :exponential-backoff
    {:sink.bulk-flush.backoff.type "EXPONENTIAL"
     :sink.bulk-flush.backoff.max-retries (str (:max-attempts retry-strategy))
     :sink.bulk-flush.backoff.delay (str (:initial-delay-ms retry-strategy) "ms")}

    (:linear-backoff :fibonacci-backoff)
    {:sink.bulk-flush.backoff.type "CONSTANT"
     :sink.bulk-flush.backoff.max-retries (str (:max-attempts retry-strategy))
     :sink.bulk-flush.backoff.delay (str (:delay-ms retry-strategy 1000) "ms")}))

(defn kafka-retry-options
  "Generate Kafka producer retry options.

  Arguments:
    retry-strategy - Retry strategy from above functions

  Returns a map of Kafka producer properties.

  Example:
    (merge (sink/kafka-options {...})
           (retry/kafka-retry-options
             (retry/exponential-backoff {:max-attempts 10})))"
  [retry-strategy]
  (case (:strategy retry-strategy)
    :no-retry
    {:properties.retries "0"}

    :fixed-delay
    {:properties.retries (str (:max-attempts retry-strategy))
     :properties.retry.backoff.ms (str (:delay-ms retry-strategy))}

    :exponential-backoff
    {:properties.retries (str (:max-attempts retry-strategy))
     :properties.retry.backoff.ms (str (:initial-delay-ms retry-strategy))
     :properties.retry.backoff.max.ms (str (:max-delay-ms retry-strategy))}

    (:linear-backoff :fibonacci-backoff)
    {:properties.retries (str (:max-attempts retry-strategy))
     :properties.retry.backoff.ms (str (:delay-ms retry-strategy 1000))}))

(defn lookup-retry-options
  "Generate lookup join retry options for Table API hints.

  Arguments:
    retry-strategy - Retry strategy from above functions

  Returns a map suitable for lookup hints.

  Example:
    (t/lookup-hint \"dim_table\"
      (merge {:async true}
             (retry/lookup-retry-options
               (retry/fixed-delay {:max-attempts 3}))))"
  [retry-strategy]
  (case (:strategy retry-strategy)
    :no-retry
    {:retry-strategy "no_retry"}

    :fixed-delay
    {:retry-strategy "fixed_delay"
     :fixed-delay (str (:delay-ms retry-strategy) "ms")
     :max-attempts (str (:max-attempts retry-strategy))}

    :exponential-backoff
    ;; Lookup joins use fixed-delay, so we use initial delay
    {:retry-strategy "fixed_delay"
     :fixed-delay (str (:initial-delay-ms retry-strategy) "ms")
     :max-attempts (str (:max-attempts retry-strategy))}

    (:linear-backoff :fibonacci-backoff)
    {:retry-strategy "fixed_delay"
     :fixed-delay (str (:delay-ms retry-strategy 1000) "ms")
     :max-attempts (str (:max-attempts retry-strategy))}))

;; =============================================================================
;; Retry Execution Helpers
;; =============================================================================

(defn- calculate-delay
  "Calculate delay for a given attempt based on strategy."
  [strategy attempt]
  (case (:strategy strategy)
    :no-retry 0
    :fixed-delay (:delay-ms strategy)
    :exponential-backoff
    (let [base-delay (* (:initial-delay-ms strategy)
                        (Math/pow (:multiplier strategy) (dec attempt)))
          capped-delay (min base-delay (:max-delay-ms strategy))
          jitter (if (:jitter? strategy)
                   (* capped-delay (:jitter-factor strategy) (rand))
                   0)]
      (long (+ capped-delay jitter)))
    :linear-backoff
    (min (* (:delay-ms strategy) attempt)
         (:max-delay-ms strategy))
    :fibonacci-backoff
    (let [fib-seq (iterate (fn [[a b]] [b (+ a b)]) [1 1])
          fib-n (first (nth fib-seq (dec attempt)))]
      (min (* (:base-delay-ms strategy) fib-n)
           (:max-delay-ms strategy)))))

(defn with-retry
  "Execute a function with retry logic.

  Arguments:
    strategy - Retry strategy from above functions
    f        - Function to execute (takes no args)
    opts     - Options map:
      :on-retry - Function called on retry (fn [attempt delay exception])
      :on-failure - Function called on final failure (fn [attempts exception])

  Returns the result of f or throws the last exception.

  Example:
    (retry/with-retry
      (retry/exponential-backoff {:max-attempts 5})
      #(http/get \"http://example.com/api\")
      {:on-retry (fn [a d e] (println \"Retry\" a \"after\" d \"ms\"))}))"
  ([strategy f]
   (with-retry strategy f {}))
  ([strategy f {:keys [on-retry on-failure]}]
   (let [max-attempts (:max-attempts strategy)]
     (loop [attempt 1]
       (let [result (try
                      {:success true :value (f)}
                      (catch Exception e
                        {:success false :exception e}))]
         (if (:success result)
           (:value result)
           (if (>= attempt max-attempts)
             (do
               (when on-failure
                 (on-failure attempt (:exception result)))
               (throw (:exception result)))
             (let [delay-ms (calculate-delay strategy attempt)]
               (when on-retry
                 (on-retry attempt delay-ms (:exception result)))
               (Thread/sleep delay-ms)
               (recur (inc attempt))))))))))

(defmacro retrying
  "Execute body with retry logic (macro version).

  Arguments:
    strategy - Retry strategy
    body     - Code to execute

  Example:
    (retry/retrying (retry/fixed-delay {:max-attempts 3})
      (do-something-that-might-fail))"
  [strategy & body]
  `(with-retry ~strategy (fn [] ~@body)))

;; =============================================================================
;; Flink Restart Strategies
;; =============================================================================
;;
;; Flink also has job-level restart strategies for handling task failures.
;; These are configured differently from connector-level retries.

(defn restart-strategy-config
  "Generate Flink configuration for restart strategies.

  This is for job-level restarts after task failures, not for
  individual operation retries.

  Arguments:
    strategy - One of:
      :no-restart - Fail the job on any task failure
      :fixed-delay - Fixed delay between restart attempts
      :exponential-delay - Exponential backoff between restarts
      :failure-rate - Limit restart rate over time

    opts - Strategy-specific options

  Returns a map of configuration key-value pairs.

  Examples:
    ;; No restart
    (restart-strategy-config :no-restart)

    ;; Fixed delay: 3 attempts with 10 second delay
    (restart-strategy-config :fixed-delay
      {:attempts 3 :delay-s 10})

    ;; Exponential delay
    (restart-strategy-config :exponential-delay
      {:initial-delay-s 1
       :max-delay-s 60
       :multiplier 2.0
       :reset-interval-s 3600
       :jitter 0.1})

    ;; Failure rate: max 3 failures per 5 minutes
    (restart-strategy-config :failure-rate
      {:max-failures-per-interval 3
       :failure-interval-s 300
       :delay-s 10})"
  ([strategy]
   (restart-strategy-config strategy {}))
  ([strategy opts]
   (case strategy
     :no-restart
     {:restart-strategy "none"}

     :fixed-delay
     (let [{:keys [attempts delay-s]
            :or {attempts 3 delay-s 10}} opts]
       {:restart-strategy "fixed-delay"
        :restart-strategy.fixed-delay.attempts (str attempts)
        :restart-strategy.fixed-delay.delay (str delay-s "s")})

     :exponential-delay
     (let [{:keys [initial-delay-s max-delay-s multiplier reset-interval-s jitter]
            :or {initial-delay-s 1
                 max-delay-s 60
                 multiplier 2.0
                 reset-interval-s 3600
                 jitter 0.1}} opts]
       {:restart-strategy "exponential-delay"
        :restart-strategy.exponential-delay.initial-backoff (str initial-delay-s "s")
        :restart-strategy.exponential-delay.max-backoff (str max-delay-s "s")
        :restart-strategy.exponential-delay.backoff-multiplier (str multiplier)
        :restart-strategy.exponential-delay.reset-backoff-threshold (str reset-interval-s "s")
        :restart-strategy.exponential-delay.jitter-factor (str jitter)})

     :failure-rate
     (let [{:keys [max-failures-per-interval failure-interval-s delay-s]
            :or {max-failures-per-interval 3
                 failure-interval-s 300
                 delay-s 10}} opts]
       {:restart-strategy "failure-rate"
        :restart-strategy.failure-rate.max-failures-per-interval (str max-failures-per-interval)
        :restart-strategy.failure-rate.failure-rate-interval (str failure-interval-s "s")
        :restart-strategy.failure-rate.delay (str delay-s "s")}))))
