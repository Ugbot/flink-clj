(ns flink-clj.metrics-test
  "Tests for custom metrics API."
  (:require [clojure.test :refer :all]
            [flink-clj.metrics :as m]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Counter functions exist"
    (is (fn? m/counter))
    (is (fn? m/inc!))
    (is (fn? m/dec!))
    (is (fn? m/add!))
    (is (fn? m/counter-value)))

  (testing "Gauge functions exist"
    (is (fn? m/gauge))
    (is (fn? m/gauge-from-atom)))

  (testing "Histogram functions exist"
    (is (fn? m/histogram))
    (is (fn? m/update-histogram!))
    (is (fn? m/histogram-count)))

  (testing "Meter functions exist"
    (is (fn? m/meter))
    (is (fn? m/mark!))
    (is (fn? m/meter-rate))
    (is (fn? m/meter-count)))

  (testing "Utility functions exist"
    (is (fn? m/metric-group))
    (is (fn? m/add-group))
    (is (fn? m/add-groups))
    (is (fn? m/timed))
    (is (fn? m/counted))
    (is (fn? m/metered))
    (is (fn? m/register-counters!))
    (is (fn? m/register-gauges!))))

;; =============================================================================
;; Wrapper Function Tests
;; =============================================================================

(deftest counted-wrapper-test
  (testing "counted wrapper increments counter on each call"
    ;; Mock counter behavior
    (let [count-atom (atom 0)
          mock-counter (reify org.apache.flink.metrics.Counter
                         (inc [_] (swap! count-atom inc))
                         (inc [_ n] (swap! count-atom + n))
                         (dec [_] (swap! count-atom dec))
                         (dec [_ n] (swap! count-atom - n))
                         (getCount [_] @count-atom))
          wrapped-fn (m/counted mock-counter (fn [x] (* x 2)))]
      (is (= 4 (wrapped-fn 2)))
      (is (= 1 @count-atom))
      (is (= 6 (wrapped-fn 3)))
      (is (= 2 @count-atom)))))

(deftest metered-wrapper-test
  (testing "metered wrapper marks meter on each call"
    (let [count-atom (atom 0)
          mock-meter (reify org.apache.flink.metrics.Meter
                       (markEvent [_] (swap! count-atom inc))
                       (markEvent [_ n] (swap! count-atom + n))
                       (getRate [_] 1.0)
                       (getCount [_] @count-atom))
          wrapped-fn (m/metered mock-meter (fn [x] (+ x 1)))]
      (is (= 3 (wrapped-fn 2)))
      (is (= 1 @count-atom))
      (is (= 5 (wrapped-fn 4)))
      (is (= 2 @count-atom)))))

(deftest timed-test
  (testing "timed function updates histogram and returns result"
    (let [times (atom [])
          mock-histogram (reify org.apache.flink.metrics.Histogram
                           (update [_ value] (swap! times conj value))
                           (getCount [_] (count @times)))
          result (m/timed mock-histogram (fn [] (Thread/sleep 10) :done))]
      (is (= :done result))
      (is (= 1 (count @times)))
      ;; Should have recorded at least 10ms
      (is (>= (first @times) 10)))))

;; =============================================================================
;; Run tests
;; =============================================================================

(comment
  (run-tests))
