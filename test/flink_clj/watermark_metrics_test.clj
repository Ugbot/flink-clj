(ns flink-clj.watermark-metrics-test
  "Tests for watermark metrics functionality."
  (:require [clojure.test :refer :all]
            [flink-clj.metrics :as m]
            [flink-clj.version :as v]))

;; =============================================================================
;; API Existence Tests
;; =============================================================================

(deftest watermark-metrics-functions-exist-test
  (testing "Watermark gauge functions exist"
    (is (fn? m/watermark-gauge))
    (is (fn? m/watermark-lag-gauge)))

  (testing "Source reader metric functions exist"
    (is (fn? m/source-pending-records-gauge))
    (is (fn? m/source-pending-bytes-gauge))
    (is (fn? m/source-errors-counter)))

  (testing "Split watermark metrics info function exists"
    (is (fn? m/split-watermark-metrics-info)))

  (testing "Watermark progress tracker exists"
    (is (fn? m/watermark-progress-tracker))))

;; =============================================================================
;; Split Watermark Metrics Info Test
;; =============================================================================

(deftest split-watermark-metrics-info-test
  (testing "split-watermark-metrics-info returns correct structure"
    (let [info (m/split-watermark-metrics-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (boolean? (:available info)))
      ;; If available, should have metrics map
      (when (:available info)
        (is (map? (:metrics info)))
        (is (contains? (:metrics info) :current-watermark))
        (is (contains? (:metrics info) :active-time))
        (is (contains? (:metrics info) :idle-time))
        (is (contains? (:metrics info) :paused-time))
        (is (contains? (:metrics info) :accumulated-active-time))))))

;; =============================================================================
;; Metrics Info Test
;; =============================================================================

(deftest metrics-info-test
  (testing "metrics-info returns expected structure"
    (let [info (m/metrics-info)]
      (is (map? info))
      (is (contains? info :dropwizard-available))
      (is (contains? info :types))
      (is (contains? info :helpers))
      (is (contains? (:types info) :counter))
      (is (contains? (:types info) :gauge))
      (is (contains? (:types info) :histogram))
      (is (contains? (:types info) :meter)))))

;; =============================================================================
;; Version-Specific Tests
;; =============================================================================

(deftest split-watermark-version-test
  (testing "Split watermark metrics availability based on Flink version"
    (let [info (m/split-watermark-metrics-info)]
      (if (v/flink-2?)
        ;; On Flink 2.x, may or may not be available depending on exact version
        (is (boolean? (:available info)))
        ;; On Flink 1.x, should not be available
        (is (false? (:available info)))))))

;; =============================================================================
;; Integration Tests (require RuntimeContext)
;; =============================================================================

(deftest ^:integration watermark-progress-tracker-test
  (testing "Watermark progress tracker works with RuntimeContext"
    ;; Would require a real RuntimeContext from MiniCluster
    (is true "Integration test placeholder")))

(deftest ^:integration watermark-gauge-test
  (testing "Watermark gauge registration"
    ;; Would require a real RuntimeContext
    (is true "Integration test placeholder")))
