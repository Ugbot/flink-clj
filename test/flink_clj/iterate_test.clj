(ns flink-clj.iterate-test
  "Tests for iterative stream processing."
  (:require [clojure.test :refer :all]
            [flink-clj.iterate :as iter]
            [flink-clj.version :as v]))

;; =============================================================================
;; Version Detection Tests
;; =============================================================================

(deftest iterations-available-test
  (testing "iterations-available? returns correct value"
    (let [available (iter/iterations-available?)]
      (is (boolean? available))
      ;; Should match the Flink version
      (is (= available (v/flink-1?))))))

(deftest iteration-info-test
  (testing "iteration-info returns proper structure"
    (let [info (iter/iteration-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :flink-version))
      (is (contains? info :notes))
      (is (= (:available info) (v/flink-1?)))
      (is (#{1 2} (:flink-version info))))))

;; =============================================================================
;; Flink 1.x Only Tests
;; =============================================================================

(deftest ^:flink-1.20 iterate-creates-iterative-stream-test
  (when (v/flink-1?)
    (testing "iterate creates proper iterative stream structure"
      ;; Note: Full integration test would require mini cluster
      ;; This test verifies the API structure
      (is (fn? iter/iterate))
      (is (fn? iter/close-with))
      (is (fn? iter/with-iteration)))))

;; =============================================================================
;; Flink 2.x Error Tests
;; =============================================================================

(deftest ^:flink-2.x iterations-throw-on-flink-2-test
  (when (v/flink-2?)
    (testing "iterate throws helpful error on Flink 2.x"
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"not available in Flink 2.x"
            (iter/iterate nil 5000))))))

;; =============================================================================
;; Run tests
;; =============================================================================

(comment
  ;; Run all tests
  (run-tests)

  ;; Check iteration availability
  (iter/iteration-info))
