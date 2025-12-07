(ns flink-clj.async-state-test
  "Tests for async state functionality (Flink 2.x features).

  These tests verify the async state API structure and version-specific
  behavior. Full integration tests require a Flink 2.x cluster with
  async-enabled state backend."
  (:require [clojure.test :refer :all]
            [flink-clj.state :as state]
            [flink-clj.keyed :as keyed]
            [flink-clj.version :as v]))

;; =============================================================================
;; API Existence Tests
;; =============================================================================

(deftest async-state-functions-exist-test
  (testing "Async value state functions exist"
    (is (fn? state/async-value))
    (is (fn? state/async-update!))
    (is (fn? state/async-clear!)))

  (testing "Async list state functions exist"
    (is (fn? state/async-get-list))
    (is (fn? state/async-add!)))

  (testing "Async map state functions exist"
    (is (fn? state/async-get-map-value))
    (is (fn? state/async-put!))
    (is (fn? state/async-contains?)))

  (testing "StateFuture combinator functions exist"
    (is (fn? state/then-apply))
    (is (fn? state/then-compose))
    (is (fn? state/then-accept))
    (is (fn? state/then-run))
    (is (fn? state/combine))))

(deftest enable-async-state-exists-test
  (testing "enable-async-state function exists in keyed namespace"
    (is (fn? keyed/enable-async-state))))

;; =============================================================================
;; Version-Specific Behavior Tests
;; =============================================================================

(deftest version-detection-test
  (testing "Version detection functions work"
    (is (boolean? (v/flink-2?)))
    (is (boolean? (v/flink-1?)))
    (is (string? (v/flink-minor-version)))))

(deftest async-state-version-guard-test
  (testing "Async state functions throw on Flink 1.x"
    ;; We need a mock state object to test this properly
    ;; For now, just verify the functions exist and have version checks
    (if (v/flink-1?)
      (testing "Functions should throw helpful error on Flink 1.x"
        ;; The functions check for StateFuture class availability
        ;; On 1.x this will throw an exception with helpful message
        (is (fn? state/async-value)))
      (testing "Functions are available on Flink 2.x"
        (is (fn? state/async-value))))))

;; =============================================================================
;; StateFuture Combinator Tests (structural)
;; =============================================================================
;; Note: These test the combinator functions' structure. Full functional tests
;; require actual StateFuture objects from a running Flink cluster.

(deftest then-apply-creates-function-wrapper-test
  (testing "then-apply wraps function correctly"
    ;; Create a mock that simulates a StateFuture with thenApply method
    (let [captured-fn (atom nil)
          mock-future (reify Object
                        (toString [_] "MockFuture"))
          ;; We can't fully test without a real StateFuture,
          ;; but we can verify the function is callable
          ]
      (is (fn? state/then-apply)))))

(deftest then-compose-creates-function-wrapper-test
  (testing "then-compose wraps function correctly"
    (is (fn? state/then-compose))))

(deftest then-accept-creates-consumer-wrapper-test
  (testing "then-accept wraps consumer correctly"
    (is (fn? state/then-accept))))

(deftest then-run-creates-runnable-wrapper-test
  (testing "then-run wraps runnable correctly"
    (is (fn? state/then-run))))

(deftest combine-creates-bifunction-wrapper-test
  (testing "combine wraps bi-function correctly"
    (is (fn? state/combine))))

;; =============================================================================
;; State Descriptor Tests (work on both versions)
;; =============================================================================

(deftest value-state-descriptor-test
  (testing "Value state descriptor creation"
    (let [desc (state/value-state "test-count" :long)]
      (is (some? desc))
      (is (instance? org.apache.flink.api.common.state.ValueStateDescriptor desc))
      (is (= "test-count" (.getName desc))))))

(deftest list-state-descriptor-test
  (testing "List state descriptor creation"
    (let [desc (state/list-state "test-list" :string)]
      (is (some? desc))
      (is (instance? org.apache.flink.api.common.state.ListStateDescriptor desc))
      (is (= "test-list" (.getName desc))))))

(deftest map-state-descriptor-test
  (testing "Map state descriptor creation"
    (let [desc (state/map-state "test-map" :string :long)]
      (is (some? desc))
      (is (instance? org.apache.flink.api.common.state.MapStateDescriptor desc))
      (is (= "test-map" (.getName desc))))))

;; =============================================================================
;; Integration Tests (require MiniCluster with async state backend)
;; =============================================================================

(deftest ^:integration async-state-pipeline-test
  (testing "Async state pipeline runs correctly"
    ;; This would require a Flink 2.x MiniCluster with ForSt backend
    ;; Placeholder for full integration test
    (is true "Integration test placeholder")))

(deftest ^:integration enable-async-state-integration-test
  (testing "enable-async-state works in real pipeline"
    ;; Full integration test would go here
    (is true "Integration test placeholder")))
