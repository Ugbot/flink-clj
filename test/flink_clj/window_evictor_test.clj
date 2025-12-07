(ns flink-clj.window-evictor-test
  "Tests for custom evictors."
  (:require [clojure.test :refer :all]
            [flink-clj.window :as w]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Custom evictor function exists"
    (is (fn? w/custom-evictor)))

  (testing "Convenience evictor functions exist"
    (is (fn? w/count-evictor-of))
    (is (fn? w/time-evictor-of))
    (is (fn? w/top-n-evictor))))

;; =============================================================================
;; Custom Evictor Tests
;; =============================================================================

;; Test evictor function - keep only latest N elements
(defn test-keep-latest [{:keys [elements size]}]
  (let [max-size 10]
    (if (> size max-size)
      (- size max-size)  ; Return count to evict
      nil)))

;; Test evictor function - return predicate
(defn test-evict-low-values [{:keys [elements]}]
  (fn [{:keys [value]}]
    (< (:amount value 0) 10)))

;; Test evictor function - return indices
(defn test-evict-indices [{:keys [elements]}]
  ;; Evict every other element
  (filter even? (range (count elements))))

(deftest custom-evictor-test
  (testing "custom-evictor creates CljEvictor"
    (let [evictor (w/custom-evictor {:evict-before #'test-keep-latest})]
      (is (some? evictor))
      (is (instance? flink_clj.CljEvictor evictor))))

  (testing "custom-evictor with evict-after"
    (let [evictor (w/custom-evictor {:evict-after #'test-keep-latest})]
      (is (some? evictor))))

  (testing "custom-evictor with both functions"
    (let [evictor (w/custom-evictor {:evict-before #'test-keep-latest
                                      :evict-after #'test-evict-low-values})]
      (is (some? evictor))))

  (testing "custom-evictor requires at least one function"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"At least one of :evict-before or :evict-after must be provided"
          (w/custom-evictor {})))))

;; =============================================================================
;; Count Evictor Tests
;; =============================================================================

(deftest count-evictor-of-test
  (testing "count-evictor-of creates CountEvictor"
    (let [evictor (w/count-evictor-of 100)]
      (is (some? evictor))
      (is (instance? org.apache.flink.streaming.api.windowing.evictors.CountEvictor
                     evictor))))

  (testing "count-evictor-of with evict-after"
    (let [evictor (w/count-evictor-of 100 true)]
      (is (some? evictor)))))

;; =============================================================================
;; Time Evictor Tests
;; =============================================================================

(deftest time-evictor-of-test
  (testing "time-evictor-of creates TimeEvictor"
    (let [evictor (w/time-evictor-of 30000)]
      (is (some? evictor))
      (is (instance? org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
                     evictor))))

  (testing "time-evictor-of with duration spec"
    (is (some? (w/time-evictor-of [30 :seconds])))
    (is (some? (w/time-evictor-of [5 :minutes])))
    (is (some? (w/time-evictor-of [100 :ms]))))

  (testing "time-evictor-of with evict-after"
    (let [evictor (w/time-evictor-of [30 :seconds] true)]
      (is (some? evictor)))))

;; =============================================================================
;; Top-N Evictor Tests
;; =============================================================================

;; Comparator function for top-n
(defn compare-by-value [a b]
  (compare (:value b) (:value a)))  ; Descending

(deftest top-n-evictor-test
  (testing "top-n-evictor creates evictor"
    (let [evictor (w/top-n-evictor 10 #'compare-by-value)]
      (is (some? evictor))))

  (testing "top-n-evictor with evict-before"
    (let [evictor (w/top-n-evictor 10 #'compare-by-value false)]
      (is (some? evictor)))))

;; =============================================================================
;; Integration Tests (require MiniCluster)
;; =============================================================================

(deftest ^:integration custom-evictor-integration-test
  (testing "Custom evictor works with windowed stream"
    ;; Would require MiniCluster
    (is true "Integration test placeholder")))
