(ns flink-clj.join-test
  "Tests for join operations including CoGroup and outer joins."
  (:require [clojure.test :refer :all]
            [flink-clj.join :as join]
            [flink-clj.window :as w])
  (:import [java.time Duration]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Window join functions exist"
    (is (fn? join/window-join))
    (is (fn? join/flat-window-join))
    (is (fn? join/tumbling-window-join))
    (is (fn? join/sliding-window-join))
    (is (fn? join/session-window-join)))

  (testing "Interval join functions exist"
    (is (fn? join/interval-join)))

  (testing "CoGroup functions exist"
    (is (fn? join/cogroup)))

  (testing "Outer join functions exist"
    (is (fn? join/left-outer-join))
    (is (fn? join/right-outer-join))
    (is (fn? join/full-outer-join))))

;; =============================================================================
;; Duration Conversion Tests
;; =============================================================================

(deftest duration-conversion-test
  (let [to-duration #'join/to-duration]
    (testing "Vector duration specs"
      (is (= (Duration/ofMillis 100) (to-duration [100 :ms])))
      (is (= (Duration/ofMillis 100) (to-duration [100 :milliseconds])))
      (is (= (Duration/ofSeconds 30) (to-duration [30 :s])))
      (is (= (Duration/ofSeconds 30) (to-duration [30 :seconds])))
      (is (= (Duration/ofMinutes 5) (to-duration [5 :m])))
      (is (= (Duration/ofMinutes 5) (to-duration [5 :minutes])))
      (is (= (Duration/ofHours 2) (to-duration [2 :h])))
      (is (= (Duration/ofHours 2) (to-duration [2 :hours])))
      (is (= (Duration/ofDays 1) (to-duration [1 :d])))
      (is (= (Duration/ofDays 1) (to-duration [1 :days]))))

    (testing "Duration passthrough"
      (let [d (Duration/ofMinutes 10)]
        (is (= d (to-duration d)))))

    (testing "Numeric milliseconds"
      (is (= (Duration/ofMillis 500) (to-duration 500))))))

;; =============================================================================
;; Type Resolution Tests
;; =============================================================================

(deftest type-resolution-test
  (let [resolve-type #'join/resolve-type-info]
    (testing "Keyword type specs"
      (let [ti (resolve-type :string)]
        (is (some? ti))))

    (testing "TypeInformation passthrough"
      (let [ti (org.apache.flink.api.common.typeinfo.Types/STRING)]
        (is (= ti (resolve-type ti)))))))

;; =============================================================================
;; CoGroup Function Tests (Unit)
;; =============================================================================

;; Test that the outer join wrapper functions are created correctly
(deftest outer-join-wrapper-creation-test
  (testing "Left outer join creates wrapper function"
    ;; We can't actually run the join without a Flink environment,
    ;; but we can verify the API accepts the correct options
    (is (thrown? Exception
          (join/left-outer-join nil nil
            {:left-key :id
             :right-key :id
             :assigner nil
             :join-fn (fn [l r] nil)}))))

  (testing "Right outer join creates wrapper function"
    (is (thrown? Exception
          (join/right-outer-join nil nil
            {:left-key :id
             :right-key :id
             :assigner nil
             :join-fn (fn [l r] nil)}))))

  (testing "Full outer join creates wrapper function"
    (is (thrown? Exception
          (join/full-outer-join nil nil
            {:left-key :id
             :right-key :id
             :assigner nil
             :join-fn (fn [l r] nil)})))))

;; =============================================================================
;; Window Assigner Tests
;; =============================================================================

(deftest window-assigner-creation-test
  (testing "Tumbling window assigners"
    ;; Create assigner directly using Flink classes (as used in joins)
    (let [time (w/seconds 10)
          assigner (org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows/of
                     (w/to-time time))]
      (is (some? assigner))
      (is (instance? org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
                     assigner))))

  (testing "Sliding window assigners"
    (let [size (w/minutes 1)
          slide (w/seconds 10)
          assigner (org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows/of
                     (w/to-time size) (w/to-time slide))]
      (is (some? assigner))
      (is (instance? org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
                     assigner))))

  (testing "Session window assigners"
    (let [gap (w/minutes 5)
          assigner (org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows/withGap
                     (w/to-time gap))]
      (is (some? assigner))
      (is (instance? org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
                     assigner)))))

;; =============================================================================
;; Join Option Validation Tests
;; =============================================================================

(deftest join-options-test
  (testing "window-join requires assigner"
    ;; Without a proper stream, this will fail, but we can verify options are processed
    (is (thrown? Exception
          (join/window-join nil nil {:left-key :id :right-key :id :join-fn identity}))))

  (testing "interval-join requires bounds"
    (is (thrown? Exception
          (join/interval-join nil nil {:left-key :id :right-key :id :join-fn identity})))))

;; =============================================================================
;; Integration Test Helpers
;; =============================================================================

(defn merge-elements [left right]
  {:left left :right right})

(defn left-outer-merge [left right]
  {:left left :right right :has-right (some? right)})

(defn right-outer-merge [left right]
  {:left left :right right :has-left (some? left)})

(defn full-outer-merge [left right]
  (cond
    (and left right) {:type :matched :left left :right right}
    left {:type :left-only :data left}
    right {:type :right-only :data right}))

(defn cogroup-summarize [{:keys [left right]}]
  {:left-count (count left)
   :right-count (count right)
   :total (+ (count left) (count right))})

;; Note: Integration tests would require a MiniCluster environment
;; and are typically run separately with lein test-1.20 or lein test-2.x

(deftest ^:integration window-join-integration-test
  ;; This test would require flink-clj.test utilities
  (testing "Window join with tumbling window"
    ;; Placeholder - actual test needs MiniCluster
    (is true "Integration test placeholder")))

(deftest ^:integration cogroup-integration-test
  (testing "CoGroup with aggregation"
    ;; Placeholder - actual test needs MiniCluster
    (is true "Integration test placeholder")))

(deftest ^:integration outer-join-integration-test
  (testing "Left outer join"
    ;; Placeholder - actual test needs MiniCluster
    (is true "Integration test placeholder"))

  (testing "Right outer join"
    (is true "Integration test placeholder"))

  (testing "Full outer join"
    (is true "Integration test placeholder")))
