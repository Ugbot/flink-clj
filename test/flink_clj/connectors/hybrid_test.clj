(ns flink-clj.connectors.hybrid-test
  "Tests for HybridSource connector."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.hybrid :as hybrid]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Core functions exist"
    (is (fn? hybrid/source))
    (is (fn? hybrid/backfill-then-stream))
    (is (fn? hybrid/chain)))

  (testing "Info functions exist"
    (is (fn? hybrid/hybrid-source-info))
    (is (fn? hybrid/switch-context))))

;; =============================================================================
;; Hybrid Source Info Test
;; =============================================================================

(deftest hybrid-source-info-test
  (testing "hybrid-source-info returns proper structure"
    (let [info (hybrid/hybrid-source-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :version))
      (is (contains? info :features))
      (is (boolean? (:available info)))
      (is (= "1.14+" (:version info)))
      (is (sequential? (:features info))))))

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest source-validation-test
  (testing "source requires first source"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"First source is required"
          (hybrid/source {:then ["dummy"]}))))

  (testing "source requires then sources"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"At least one subsequent source required"
          (hybrid/source {:first "dummy" :then []})))))

(deftest backfill-then-stream-validation-test
  (testing "backfill-then-stream requires bounded"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #":bounded source is required"
          (hybrid/backfill-then-stream {:unbounded "dummy"}))))

  (testing "backfill-then-stream requires unbounded"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #":unbounded source is required"
          (hybrid/backfill-then-stream {:bounded "dummy"})))))

(deftest chain-validation-test
  (testing "chain requires at least two sources"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"At least two sources required"
          (hybrid/chain "only-one")))))

;; =============================================================================
;; Switch Context Test
;; =============================================================================

(deftest switch-context-test
  (testing "switch-context creates proper context map"
    (let [prev-state {:offset 100}
          ctx (hybrid/switch-context prev-state)]
      (is (map? ctx))
      (is (= prev-state (:previous-state ctx)))
      (is (number? (:switch-time ctx))))))

;; =============================================================================
;; Integration tests would require actual sources
;; =============================================================================

(deftest ^:integration hybrid-source-creation-test
  (testing "HybridSource integration"
    ;; This test would require actual Source implementations
    ;; Placeholder for integration test
    (is true)))

;; =============================================================================
;; Run tests
;; =============================================================================

(comment
  (run-tests))
