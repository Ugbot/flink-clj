(ns flink-clj.retry-test
  "Tests for retry strategies."
  (:require [clojure.test :refer :all]
            [flink-clj.retry :as retry]))

;; =============================================================================
;; Strategy Builder Tests
;; =============================================================================

(deftest no-retry-test
  (testing "No retry strategy"
    (let [s (retry/no-retry)]
      (is (= :no-retry (:strategy s)))
      (is (= 1 (:max-attempts s))))))

(deftest fixed-delay-test
  (testing "Fixed delay with defaults"
    (let [s (retry/fixed-delay)]
      (is (= :fixed-delay (:strategy s)))
      (is (= 3 (:max-attempts s)))
      (is (= 1000 (:delay-ms s)))))

  (testing "Fixed delay with custom values"
    (let [s (retry/fixed-delay {:max-attempts 5 :delay-ms 2000})]
      (is (= 5 (:max-attempts s)))
      (is (= 2000 (:delay-ms s))))))

(deftest exponential-backoff-test
  (testing "Exponential backoff with defaults"
    (let [s (retry/exponential-backoff)]
      (is (= :exponential-backoff (:strategy s)))
      (is (= 5 (:max-attempts s)))
      (is (= 100 (:initial-delay-ms s)))
      (is (= 60000 (:max-delay-ms s)))
      (is (= 2.0 (:multiplier s)))
      (is (false? (:jitter? s)))))

  (testing "Exponential backoff with custom values"
    (let [s (retry/exponential-backoff {:max-attempts 10
                                         :initial-delay-ms 50
                                         :max-delay-ms 30000
                                         :multiplier 1.5
                                         :jitter? true
                                         :jitter-factor 0.2})]
      (is (= 10 (:max-attempts s)))
      (is (= 50 (:initial-delay-ms s)))
      (is (= 30000 (:max-delay-ms s)))
      (is (= 1.5 (:multiplier s)))
      (is (true? (:jitter? s)))
      (is (= 0.2 (:jitter-factor s))))))

(deftest linear-backoff-test
  (testing "Linear backoff"
    (let [s (retry/linear-backoff {:max-attempts 5 :delay-ms 2000})]
      (is (= :linear-backoff (:strategy s)))
      (is (= 5 (:max-attempts s)))
      (is (= 2000 (:delay-ms s))))))

(deftest fibonacci-backoff-test
  (testing "Fibonacci backoff"
    (let [s (retry/fibonacci-backoff {:max-attempts 8 :base-delay-ms 500})]
      (is (= :fibonacci-backoff (:strategy s)))
      (is (= 8 (:max-attempts s)))
      (is (= 500 (:base-delay-ms s))))))

;; =============================================================================
;; Retry Predicate Tests
;; =============================================================================

(deftest retry-on-test
  (testing "Retry on exception classes"
    (let [pred (retry/retry-on java.net.SocketTimeoutException
                               java.io.IOException)]
      (is (= :exception-classes (:predicate pred)))
      (is (= 2 (count (:classes pred)))))))

(deftest retry-on-result-test
  (testing "Retry on result"
    (let [pred (retry/retry-on-result nil?)]
      (is (= :result (:predicate pred)))
      (is (fn? (:fn pred))))))

(deftest retry-on-status-test
  (testing "Retry on HTTP status"
    (let [pred (retry/retry-on-status #{429 500 502 503})]
      (is (= :http-status (:predicate pred)))
      (is (contains? (:codes pred) 429))
      (is (contains? (:codes pred) 500)))))

;; =============================================================================
;; Connector-Specific Options Tests
;; =============================================================================

(deftest jdbc-retry-options-test
  (testing "JDBC retry options - no retry"
    (let [opts (retry/jdbc-retry-options (retry/no-retry))]
      (is (= "0" (:sink.max-retries opts)))))

  (testing "JDBC retry options - fixed delay"
    (let [opts (retry/jdbc-retry-options
                 (retry/fixed-delay {:max-attempts 3 :delay-ms 1000}))]
      (is (= "3" (:sink.max-retries opts)))
      (is (= "1000ms" (:sink.retry-interval opts))))))

(deftest elasticsearch-retry-options-test
  (testing "Elasticsearch retry options - no retry"
    (let [opts (retry/elasticsearch-retry-options (retry/no-retry))]
      (is (= "DISABLED" (:sink.bulk-flush.backoff.type opts)))))

  (testing "Elasticsearch retry options - exponential"
    (let [opts (retry/elasticsearch-retry-options
                 (retry/exponential-backoff {:max-attempts 5 :initial-delay-ms 100}))]
      (is (= "EXPONENTIAL" (:sink.bulk-flush.backoff.type opts)))
      (is (= "5" (:sink.bulk-flush.backoff.max-retries opts)))
      (is (= "100ms" (:sink.bulk-flush.backoff.delay opts))))))

(deftest kafka-retry-options-test
  (testing "Kafka retry options - no retry"
    (let [opts (retry/kafka-retry-options (retry/no-retry))]
      (is (= "0" (:properties.retries opts)))))

  (testing "Kafka retry options - exponential"
    (let [opts (retry/kafka-retry-options
                 (retry/exponential-backoff {:max-attempts 10
                                              :initial-delay-ms 100
                                              :max-delay-ms 5000}))]
      (is (= "10" (:properties.retries opts)))
      (is (= "100" (:properties.retry.backoff.ms opts)))
      (is (= "5000" (:properties.retry.backoff.max.ms opts))))))

(deftest lookup-retry-options-test
  (testing "Lookup retry options - no retry"
    (let [opts (retry/lookup-retry-options (retry/no-retry))]
      (is (= "no_retry" (:retry-strategy opts)))))

  (testing "Lookup retry options - fixed delay"
    (let [opts (retry/lookup-retry-options
                 (retry/fixed-delay {:max-attempts 3 :delay-ms 1000}))]
      (is (= "fixed_delay" (:retry-strategy opts)))
      (is (= "1000ms" (:fixed-delay opts)))
      (is (= "3" (:max-attempts opts))))))

;; =============================================================================
;; Retry Execution Tests
;; =============================================================================

(deftest with-retry-success-test
  (testing "Successful execution without retry"
    (let [call-count (atom 0)
          result (retry/with-retry
                   (retry/fixed-delay {:max-attempts 3})
                   (fn []
                     (swap! call-count inc)
                     "success"))]
      (is (= "success" result))
      (is (= 1 @call-count)))))

(deftest with-retry-eventual-success-test
  (testing "Success after retries"
    (let [call-count (atom 0)
          result (retry/with-retry
                   (retry/fixed-delay {:max-attempts 3 :delay-ms 10})
                   (fn []
                     (swap! call-count inc)
                     (if (< @call-count 3)
                       (throw (Exception. "temporary failure"))
                       "success")))]
      (is (= "success" result))
      (is (= 3 @call-count)))))

(deftest with-retry-exhausted-test
  (testing "Retry exhausted throws"
    (let [call-count (atom 0)]
      (is (thrown? Exception
            (retry/with-retry
              (retry/fixed-delay {:max-attempts 3 :delay-ms 10})
              (fn []
                (swap! call-count inc)
                (throw (Exception. "always fails"))))))
      (is (= 3 @call-count)))))

(deftest with-retry-callbacks-test
  (testing "Retry callbacks are called"
    (let [retries (atom [])
          failures (atom nil)]
      (try
        (retry/with-retry
          (retry/fixed-delay {:max-attempts 3 :delay-ms 10})
          (fn [] (throw (Exception. "fail")))
          {:on-retry (fn [attempt delay ex]
                       (swap! retries conj {:attempt attempt :delay delay}))
           :on-failure (fn [attempts ex]
                         (reset! failures attempts))})
        (catch Exception _))
      (is (= 2 (count @retries)))
      (is (= 1 (:attempt (first @retries))))
      (is (= 3 @failures)))))

;; =============================================================================
;; Restart Strategy Config Tests
;; =============================================================================

(deftest restart-strategy-config-test
  (testing "No restart"
    (let [config (retry/restart-strategy-config :no-restart)]
      (is (= "none" (:restart-strategy config)))))

  (testing "Fixed delay restart"
    (let [config (retry/restart-strategy-config :fixed-delay
                   {:attempts 5 :delay-s 30})]
      (is (= "fixed-delay" (:restart-strategy config)))
      (is (= "5" (:restart-strategy.fixed-delay.attempts config)))
      (is (= "30s" (:restart-strategy.fixed-delay.delay config)))))

  (testing "Exponential delay restart"
    (let [config (retry/restart-strategy-config :exponential-delay
                   {:initial-delay-s 1
                    :max-delay-s 60
                    :multiplier 2.0})]
      (is (= "exponential-delay" (:restart-strategy config)))
      (is (= "1s" (:restart-strategy.exponential-delay.initial-backoff config)))
      (is (= "60s" (:restart-strategy.exponential-delay.max-backoff config)))))

  (testing "Failure rate restart"
    (let [config (retry/restart-strategy-config :failure-rate
                   {:max-failures-per-interval 3
                    :failure-interval-s 300
                    :delay-s 10})]
      (is (= "failure-rate" (:restart-strategy config)))
      (is (= "3" (:restart-strategy.failure-rate.max-failures-per-interval config)))
      (is (= "300s" (:restart-strategy.failure-rate.failure-rate-interval config))))))
