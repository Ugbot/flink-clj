(ns flink-clj.async-sink-test
  "Tests for async sink configuration."
  (:require [clojure.test :refer :all]
            [flink-clj.async-sink :as async-sink]
            [flink-clj.version :as v]))

;; =============================================================================
;; API Existence Tests
;; =============================================================================

(deftest config-functions-exist-test
  (testing "Configuration builder functions exist"
    (is (fn? async-sink/async-sink-config))
    (is (fn? async-sink/with-max-batch-size))
    (is (fn? async-sink/with-max-in-flight-requests))
    (is (fn? async-sink/with-max-buffered-requests))
    (is (fn? async-sink/with-max-batch-size-in-bytes))
    (is (fn? async-sink/with-max-time-in-buffer-ms))
    (is (fn? async-sink/with-max-record-size-in-bytes))
    (is (fn? async-sink/with-request-timeout-ms))
    (is (fn? async-sink/with-fail-on-timeout)))

  (testing "Extraction functions exist"
    (is (fn? async-sink/config->java-params))
    (is (fn? async-sink/config->properties)))

  (testing "Rate limiting functions exist"
    (is (fn? async-sink/no-rate-limiting))
    (is (fn? async-sink/aimd-rate-limiting))
    (is (fn? async-sink/token-bucket-rate-limiting)))

  (testing "Batching functions exist"
    (is (fn? async-sink/batch-by-partition))
    (is (fn? async-sink/default-batching)))

  (testing "Validation function exists"
    (is (fn? async-sink/validate-config))))

;; =============================================================================
;; Configuration Builder Tests
;; =============================================================================

(deftest async-sink-config-defaults-test
  (testing "Default configuration values"
    (let [config (async-sink/async-sink-config)]
      (is (= 500 (:max-batch-size config)))
      (is (= 50 (:max-in-flight-requests config)))
      (is (= 10000 (:max-buffered-requests config)))
      (is (= (* 5 1024 1024) (:max-batch-size-in-bytes config)))
      (is (= 5000 (:max-time-in-buffer-ms config)))
      (is (= (* 1024 1024) (:max-record-size-in-bytes config)))
      (is (nil? (:request-timeout-ms config)))
      (is (false? (:fail-on-timeout config))))))

(deftest config-builder-chaining-test
  (testing "Builder functions chain correctly"
    (let [config (-> (async-sink/async-sink-config)
                     (async-sink/with-max-batch-size 1000)
                     (async-sink/with-max-in-flight-requests 100)
                     (async-sink/with-max-buffered-requests 50000)
                     (async-sink/with-max-batch-size-in-bytes (* 10 1024 1024))
                     (async-sink/with-max-time-in-buffer-ms 1000)
                     (async-sink/with-max-record-size-in-bytes (* 2 1024 1024))
                     (async-sink/with-request-timeout-ms 30000)
                     (async-sink/with-fail-on-timeout true))]
      (is (= 1000 (:max-batch-size config)))
      (is (= 100 (:max-in-flight-requests config)))
      (is (= 50000 (:max-buffered-requests config)))
      (is (= (* 10 1024 1024) (:max-batch-size-in-bytes config)))
      (is (= 1000 (:max-time-in-buffer-ms config)))
      (is (= (* 2 1024 1024) (:max-record-size-in-bytes config)))
      (is (= 30000 (:request-timeout-ms config)))
      (is (true? (:fail-on-timeout config))))))

;; =============================================================================
;; Configuration Extraction Tests
;; =============================================================================

(deftest config-to-java-params-test
  (testing "Java params extraction"
    (let [config (async-sink/async-sink-config)
          params (async-sink/config->java-params config)]
      (is (vector? params))
      (is (= 8 (count params)))
      (is (= 500 (nth params 0)))  ; max-batch-size
      (is (= 50 (nth params 1)))   ; max-in-flight-requests
      (is (= 10000 (nth params 2))) ; max-buffered-requests
      (is (= (* 5 1024 1024) (nth params 3))) ; max-batch-size-in-bytes
      (is (= 5000 (nth params 4))) ; max-time-in-buffer-ms
      (is (= (* 1024 1024) (nth params 5))) ; max-record-size-in-bytes
      (is (nil? (nth params 6)))   ; request-timeout-ms
      (is (false? (nth params 7)))))) ; fail-on-timeout

(deftest config-to-properties-test
  (testing "Properties conversion"
    (let [config (-> (async-sink/async-sink-config)
                     (async-sink/with-request-timeout-ms 30000))
          props (async-sink/config->properties config)]
      (is (instance? java.util.Properties props))
      (is (= "500" (.getProperty props "max-batch-size")))
      (is (= "50" (.getProperty props "max-in-flight-requests")))
      (is (= "30000" (.getProperty props "request-timeout-ms"))))))

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest validate-config-positive-values-test
  (testing "Validation accepts valid config"
    (let [config (async-sink/async-sink-config)]
      (is (= config (async-sink/validate-config config)))))

  (testing "Validation rejects non-positive max-batch-size"
    (let [config (async-sink/with-max-batch-size {} 0)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"max-batch-size must be positive"
            (async-sink/validate-config config)))))

  (testing "Validation rejects negative max-in-flight-requests"
    (let [config (async-sink/with-max-in-flight-requests {} -1)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"max-in-flight-requests must be positive"
            (async-sink/validate-config config))))))

(deftest validate-config-record-vs-batch-size-test
  (testing "Validation rejects record size > batch size"
    (let [config (-> (async-sink/async-sink-config)
                     (async-sink/with-max-batch-size-in-bytes 1000)
                     (async-sink/with-max-record-size-in-bytes 2000))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"max-record-size-in-bytes cannot exceed max-batch-size-in-bytes"
            (async-sink/validate-config config))))))

;; =============================================================================
;; Default Batching Test
;; =============================================================================

(deftest default-batching-test
  (testing "Default batching config"
    (let [batch-config (async-sink/default-batching)]
      (is (= :fifo (:batch-strategy batch-config))))))

;; =============================================================================
;; Version-Specific Tests
;; =============================================================================

(deftest batch-by-partition-version-test
  (testing "batch-by-partition requires Flink 2.1+"
    (if (v/flink-2?)
      ;; On Flink 2.x, test depends on whether FLIP-509 is available
      (try
        ;; This will either succeed (FLIP-509 available) and require partition-key-fn
        ;; or throw version error (FLIP-509 not available)
        (async-sink/batch-by-partition {})
        (is false "Expected an exception")
        (catch clojure.lang.ExceptionInfo e
          (is (or (re-find #"partition-key-fn is required" (ex-message e))
                  (re-find #"requires Flink 2.1" (ex-message e))))))
      ;; On Flink 1.x, should throw version error
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"requires Flink 2.1"
            (async-sink/batch-by-partition {:partition-key-fn identity}))))))
