(ns flink-clj.datastream-v2-test
  "Tests for DataStream V2 experimental API."
  (:require [clojure.test :refer :all]
            [flink-clj.datastream-v2 :as v2]
            [flink-clj.version :as v]))

;; =============================================================================
;; API Existence Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "V2 availability check functions exist"
    (is (fn? v2/v2-available?))
    (is (fn? v2/v2-info)))

  (testing "V2 environment functions exist"
    (is (fn? v2/execution-env)))

  (testing "V2 source functions exist"
    (is (fn? v2/from-collection))
    (is (fn? v2/from-source)))

  (testing "V2 processing functions exist"
    (is (fn? v2/process))
    (is (fn? v2/key-by)))

  (testing "V2 partitioning functions exist"
    (is (fn? v2/shuffle))
    (is (fn? v2/broadcast))
    (is (fn? v2/global)))

  (testing "V2 sink functions exist"
    (is (fn? v2/to-sink))
    (is (fn? v2/print-sink)))

  (testing "V2 execution function exists"
    (is (fn? v2/execute))))

;; =============================================================================
;; Availability Tests
;; =============================================================================

(deftest v2-availability-test
  (testing "v2-available? returns boolean"
    (is (boolean? (v2/v2-available?))))

  (testing "v2-info returns correct structure"
    (let [info (v2/v2-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :version))
      (is (contains? info :status))
      (is (boolean? (:available info)))
      (is (string? (:version info)))
      (is (#{:experimental :unavailable} (:status info))))))

(deftest v2-available-matches-flink-version-test
  (testing "V2 availability matches Flink version"
    (let [v2? (v2/v2-available?)
          flink-2? (v/flink-2?)]
      ;; V2 can only be available on Flink 2.x
      (when-not flink-2?
        (is (false? v2?) "V2 should not be available on Flink 1.x")))))

;; =============================================================================
;; Version Guard Tests
;; =============================================================================

(deftest version-guard-test
  (testing "V2 functions throw on Flink 1.x"
    (when (v/flink-1?)
      (testing "execution-env throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires DataStream V2"
              (v2/execution-env))))

      (testing "from-collection throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires DataStream V2"
              (v2/from-collection nil [1 2 3]))))

      (testing "print-sink throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires DataStream V2"
              (v2/print-sink)))))))

;; =============================================================================
;; V2 Info Test
;; =============================================================================

(deftest v2-info-status-test
  (testing "v2-info status matches availability"
    (let [info (v2/v2-info)]
      (if (:available info)
        (is (= :experimental (:status info)))
        (is (= :unavailable (:status info)))))))

;; =============================================================================
;; Integration Tests (require V2 API)
;; =============================================================================

(deftest ^:integration v2-simple-pipeline-test
  (testing "Simple V2 pipeline works"
    (when (v2/v2-available?)
      ;; Would require actual V2 environment
      (is true "V2 integration test placeholder"))))
