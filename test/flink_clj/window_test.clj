(ns flink-clj.window-test
  "Tests for flink-clj.window namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.window :as w])
  (:import [org.apache.flink.streaming.api.windowing.triggers CountTrigger PurgingTrigger]
           [org.apache.flink.streaming.api.windowing.evictors CountEvictor TimeEvictor]
           [java.time Duration]))

;; =============================================================================
;; Time Duration Tests
;; =============================================================================

(defn- to-millis
  "Get milliseconds from a time/duration object, handling both Flink Time and Duration."
  [t]
  (if (instance? Duration t)
    (.toMillis ^Duration t)
    ;; Flink Time has .toMilliseconds method
    (.toMilliseconds t)))

(deftest time-duration-test
  (testing "milliseconds"
    (let [t (w/milliseconds 100)]
      (is (= 100 (to-millis t)))))

  (testing "seconds"
    (let [t (w/seconds 10)]
      (is (= 10000 (to-millis t)))))

  (testing "minutes"
    (let [t (w/minutes 5)]
      (is (= 300000 (to-millis t)))))

  (testing "hours"
    (let [t (w/hours 1)]
      (is (= 3600000 (to-millis t)))))

  (testing "days"
    (let [t (w/days 1)]
      (is (= 86400000 (to-millis t))))))

(deftest duration-spec-test
  (testing "duration from vector spec"
    (is (= 100 (to-millis (w/duration [100 :ms]))))
    (is (= 100 (to-millis (w/duration [100 :milliseconds]))))
    (is (= 10000 (to-millis (w/duration [10 :s]))))
    (is (= 10000 (to-millis (w/duration [10 :seconds]))))
    (is (= 300000 (to-millis (w/duration [5 :m]))))
    (is (= 300000 (to-millis (w/duration [5 :minutes]))))
    (is (= 3600000 (to-millis (w/duration [1 :h]))))
    (is (= 86400000 (to-millis (w/duration [1 :d])))))

  (testing "unknown unit throws"
    (is (thrown? Exception (w/duration [10 :unknown])))))

;; =============================================================================
;; Trigger Tests
;; =============================================================================

(deftest trigger-creation-test
  (testing "count trigger"
    (let [t (w/count-trigger 100)]
      (is (instance? CountTrigger t))))

  (testing "purging trigger"
    (let [t (w/purging (w/count-trigger 100))]
      (is (instance? PurgingTrigger t)))))

;; =============================================================================
;; Evictor Tests
;; =============================================================================

(deftest evictor-creation-test
  (testing "count evictor"
    (let [e (w/count-evictor 100)]
      (is (instance? CountEvictor e))))

  (testing "time evictor"
    (let [e (w/time-evictor (w/seconds 10))]
      (is (instance? TimeEvictor e)))))

;; =============================================================================
;; Count Window Tests
;; =============================================================================

(deftest count-window-api-test
  (testing "count-window function exists"
    (is (fn? w/count-window)))

  (testing "count-window-sliding function exists"
    (is (fn? w/count-window-sliding))))

;; =============================================================================
;; Dynamic Session Window Tests
;; =============================================================================

(deftest dynamic-session-api-test
  (testing "dynamic-session-event-time function exists"
    (is (fn? w/dynamic-session-event-time)))

  (testing "dynamic-session-processing-time function exists"
    (is (fn? w/dynamic-session-processing-time))))

;; =============================================================================
;; to-duration Tests
;; =============================================================================

(deftest to-duration-test
  (testing "to-duration from vector spec"
    (let [d (w/to-duration [10 :seconds])]
      (is (instance? Duration d))
      (is (= 10000 (.toMillis d)))))

  (testing "to-duration from raw number"
    (let [d (w/to-duration 5000)]
      (is (instance? Duration d))
      (is (= 5000 (.toMillis d)))))

  (testing "to-duration from Duration passthrough"
    (let [orig (Duration/ofMinutes 5)
          d (w/to-duration orig)]
      (is (= orig d)))))

;; Note: Full integration tests for window operations require a MiniCluster
;; and are marked as :integration tests. They test the actual window behavior
;; with data flowing through the pipeline.
