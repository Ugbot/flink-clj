(ns flink-clj.accumulator-test
  "Tests for accumulator functionality."
  (:require [clojure.test :refer :all]
            [flink-clj.accumulator :as acc])
  (:import [org.apache.flink.api.common.accumulators
            IntCounter LongCounter DoubleCounter
            IntMinimum IntMaximum LongMinimum LongMaximum
            DoubleMinimum DoubleMaximum
            ListAccumulator AverageAccumulator Histogram]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Accumulator creation functions exist"
    (is (fn? acc/int-counter))
    (is (fn? acc/long-counter))
    (is (fn? acc/double-counter))
    (is (fn? acc/int-minimum))
    (is (fn? acc/int-maximum))
    (is (fn? acc/long-minimum))
    (is (fn? acc/long-maximum))
    (is (fn? acc/double-minimum))
    (is (fn? acc/double-maximum))
    (is (fn? acc/list-accumulator))
    (is (fn? acc/average-accumulator))
    (is (fn? acc/histogram)))

  (testing "Registration functions exist"
    (is (fn? acc/register!))
    (is (fn? acc/get-accumulator)))

  (testing "Update functions exist"
    (is (fn? acc/add!))
    (is (fn? acc/track-min!))
    (is (fn? acc/track-max!))
    (is (fn? acc/collect!))
    (is (fn? acc/histogram-add!)))

  (testing "Result functions exist"
    (is (fn? acc/get-result))
    (is (fn? acc/get-all-results)))

  (testing "Utility functions exist"
    (is (:macro (meta #'acc/with-accumulators)))
    (is (fn? acc/accumulator-info))))

;; =============================================================================
;; Accumulator Creation Tests
;; =============================================================================

(deftest int-counter-test
  (testing "int-counter creates IntCounter"
    (let [counter (acc/int-counter)]
      (is (instance? IntCounter counter))
      (is (= 0 (.getLocalValue counter)))))

  (testing "int-counter with initial value"
    (let [counter (acc/int-counter 10)]
      (is (= 10 (.getLocalValue counter)))))

  (testing "int-counter add works"
    (let [counter (acc/int-counter)]
      (.add counter (int 5))
      (.add counter (int 3))
      (is (= 8 (.getLocalValue counter))))))

(deftest long-counter-test
  (testing "long-counter creates LongCounter"
    (let [counter (acc/long-counter)]
      (is (instance? LongCounter counter))
      (is (= 0 (.getLocalValue counter)))))

  (testing "long-counter with initial value"
    (let [counter (acc/long-counter 1000000000000)]
      (is (= 1000000000000 (.getLocalValue counter)))))

  (testing "long-counter handles large values"
    (let [counter (acc/long-counter)]
      (.add counter (long Long/MAX_VALUE))
      (is (= Long/MAX_VALUE (.getLocalValue counter))))))

(deftest double-counter-test
  (testing "double-counter creates DoubleCounter"
    (let [counter (acc/double-counter)]
      (is (instance? DoubleCounter counter))
      (is (= 0.0 (.getLocalValue counter)))))

  (testing "double-counter with initial value"
    (let [counter (acc/double-counter 3.14)]
      (is (< (Math/abs (- 3.14 (.getLocalValue counter))) 0.001))))

  (testing "double-counter sums correctly"
    (let [counter (acc/double-counter)]
      (.add counter (double 1.5))
      (.add counter (double 2.5))
      (is (< (Math/abs (- 4.0 (.getLocalValue counter))) 0.001)))))

(deftest int-minimum-test
  (testing "int-minimum creates IntMinimum"
    (let [min-acc (acc/int-minimum)]
      (is (instance? IntMinimum min-acc))))

  (testing "int-minimum tracks minimum"
    (let [min-acc (acc/int-minimum)]
      (.add min-acc (int 10))
      (.add min-acc (int 5))
      (.add min-acc (int 15))
      (is (= 5 (.getLocalValue min-acc))))))

(deftest int-maximum-test
  (testing "int-maximum creates IntMaximum"
    (let [max-acc (acc/int-maximum)]
      (is (instance? IntMaximum max-acc))))

  (testing "int-maximum tracks maximum"
    (let [max-acc (acc/int-maximum)]
      (.add max-acc (int 10))
      (.add max-acc (int 25))
      (.add max-acc (int 15))
      (is (= 25 (.getLocalValue max-acc))))))

(deftest long-minimum-test
  (testing "long-minimum creates LongMinimum"
    (let [min-acc (acc/long-minimum)]
      (is (instance? LongMinimum min-acc))))

  (testing "long-minimum tracks minimum"
    (let [min-acc (acc/long-minimum)]
      (.add min-acc (long 1000000000000))
      (.add min-acc (long 500000000000))
      (.add min-acc (long 1500000000000))
      (is (= 500000000000 (.getLocalValue min-acc))))))

(deftest long-maximum-test
  (testing "long-maximum creates LongMaximum"
    (let [max-acc (acc/long-maximum)]
      (is (instance? LongMaximum max-acc))))

  (testing "long-maximum tracks maximum"
    (let [max-acc (acc/long-maximum)]
      (.add max-acc (long 1000000000000))
      (.add max-acc (long 2500000000000))
      (.add max-acc (long 1500000000000))
      (is (= 2500000000000 (.getLocalValue max-acc))))))

(deftest double-minimum-test
  (testing "double-minimum creates DoubleMinimum"
    (let [min-acc (acc/double-minimum)]
      (is (instance? DoubleMinimum min-acc))))

  (testing "double-minimum tracks minimum"
    (let [min-acc (acc/double-minimum)]
      (.add min-acc (double 10.5))
      (.add min-acc (double 5.5))
      (.add min-acc (double 15.5))
      (is (< (Math/abs (- 5.5 (.getLocalValue min-acc))) 0.001)))))

(deftest double-maximum-test
  (testing "double-maximum creates DoubleMaximum"
    (let [max-acc (acc/double-maximum)]
      (is (instance? DoubleMaximum max-acc))))

  (testing "double-maximum tracks maximum"
    (let [max-acc (acc/double-maximum)]
      (.add max-acc (double 10.5))
      (.add max-acc (double 25.5))
      (.add max-acc (double 15.5))
      (is (< (Math/abs (- 25.5 (.getLocalValue max-acc))) 0.001)))))

(deftest list-accumulator-test
  (testing "list-accumulator creates ListAccumulator"
    (let [list-acc (acc/list-accumulator)]
      (is (instance? ListAccumulator list-acc))))

  (testing "list-accumulator collects items"
    (let [list-acc (acc/list-accumulator)]
      (.add list-acc "item1")
      (.add list-acc "item2")
      (.add list-acc "item3")
      (let [result (.getLocalValue list-acc)]
        (is (= 3 (count result)))
        (is (= ["item1" "item2" "item3"] (vec result)))))))

(deftest average-accumulator-test
  (testing "average-accumulator creates AverageAccumulator"
    (let [avg-acc (acc/average-accumulator)]
      (is (instance? AverageAccumulator avg-acc))))

  (testing "average-accumulator computes average"
    (let [avg-acc (acc/average-accumulator)]
      (.add avg-acc (double 10.0))
      (.add avg-acc (double 20.0))
      (.add avg-acc (double 30.0))
      (is (< (Math/abs (- 20.0 (.getLocalValue avg-acc))) 0.001)))))

(deftest histogram-test
  (testing "histogram creates Histogram"
    (let [hist (acc/histogram)]
      (is (instance? Histogram hist))))

  (testing "histogram tracks frequency"
    (let [hist (acc/histogram)]
      ;; Add values - histogram counts occurrences of each value
      (.add hist (int 5))
      (.add hist (int 5))
      (.add hist (int 10))
      (let [result (.getLocalValue hist)]
        (is (some? result))
        ;; Histogram stores TreeMap<Integer, Integer> of value -> count
        (is (instance? java.util.TreeMap result))
        (is (= 2 (.get result (Integer. 5))))
        (is (= 1 (.get result (Integer. 10))))))))

;; =============================================================================
;; Accumulator Info Test
;; =============================================================================

(deftest accumulator-info-test
  (testing "accumulator-info returns expected structure"
    (let [info (acc/accumulator-info)]
      (is (map? info))
      (is (contains? info :description))
      (is (contains? info :types))
      (is (contains? info :usage-notes))
      (is (map? (:types info)))
      (is (contains? (:types info) :int-counter))
      (is (contains? (:types info) :histogram)))))

;; =============================================================================
;; Integration Tests (require RuntimeContext)
;; =============================================================================

(deftest ^:integration register-and-update-test
  (testing "register! adds accumulator to RuntimeContext"
    ;; Would need RuntimeContext from RichFunction
    (is true "Integration test placeholder")))

(deftest ^:integration add-update-test
  (testing "add! updates accumulator value"
    (is true "Integration test placeholder")))

(deftest ^:integration track-min-max-test
  (testing "track-min! and track-max! work correctly"
    (is true "Integration test placeholder")))

(deftest ^:integration collect-test
  (testing "collect! adds to list accumulator"
    (is true "Integration test placeholder")))

(deftest ^:integration get-result-test
  (testing "get-result retrieves from JobExecutionResult"
    (is true "Integration test placeholder")))
