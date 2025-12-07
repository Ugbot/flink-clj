(ns flink-clj.window-custom-test
  "Tests for custom window assigners and triggers."
  (:require [clojure.test :refer :all]
            [flink-clj.window :as w]
            [flink-clj.version :as v])
  (:import [org.apache.flink.streaming.api.windowing.windows TimeWindow]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Custom window assigner function exists"
    (is (fn? w/custom-window-assigner)))

  (testing "Custom trigger functions exist"
    (is (fn? w/custom-trigger)))

  (testing "Trigger composition functions exist"
    (is (fn? w/trigger-and))
    (is (fn? w/trigger-or)))

  (testing "Convenience trigger functions exist"
    (is (fn? w/count-and-time-trigger))
    (is (fn? w/delta-trigger))))

;; =============================================================================
;; Custom Window Assigner Tests
;; =============================================================================

;; Define a test window assigner function
(defn test-window-assigner
  "A simple test window assigner that creates fixed windows."
  [{:keys [element timestamp context]}]
  (let [window-size 10000 ; 10 seconds
        start (- timestamp (mod timestamp window-size))]
    {:start start :end (+ start window-size)}))

(defn multi-window-assigner
  "Assigner that creates multiple overlapping windows."
  [{:keys [element timestamp]}]
  ;; Create 3 overlapping windows
  [{:start (- timestamp 5000) :end (+ timestamp 5000)}
   {:start timestamp :end (+ timestamp 10000)}
   {:start (+ timestamp 5000) :end (+ timestamp 15000)}])

(defn conditional-window-assigner
  "Assigner that conditionally assigns windows."
  [{:keys [element timestamp]}]
  ;; Only assign window if element meets criteria
  (when (> (:value element 0) 0)
    {:start timestamp :end (+ timestamp 10000)}))

(deftest custom-window-assigner-test
  (testing "custom-window-assigner creates CljWindowAssigner"
    (let [assigner (w/custom-window-assigner #'test-window-assigner)]
      (is (some? assigner))
      (is (instance? flink_clj.CljWindowAssigner assigner))))

  (testing "custom-window-assigner with event-time flag"
    (let [event-assigner (w/custom-window-assigner #'test-window-assigner true)
          proc-assigner (w/custom-window-assigner #'test-window-assigner false)]
      (is (true? (.isEventTime event-assigner)))
      (is (false? (.isEventTime proc-assigner)))))

  (testing "custom-window-assigner has default trigger"
    (let [assigner (w/custom-window-assigner #'test-window-assigner)]
      ;; Should have a default trigger
      ;; API changed in Flink 2.1: getDefaultTrigger() takes no args
      (is (some? (if (v/flink-2?)
                   (.getDefaultTrigger assigner)
                   (.getDefaultTrigger assigner nil))))))

  (testing "custom-window-assigner has window serializer"
    (let [assigner (w/custom-window-assigner #'test-window-assigner)]
      (is (some? (.getWindowSerializer assigner nil))))))

;; =============================================================================
;; Custom Trigger Tests
;; =============================================================================

;; Test trigger functions
(defn test-on-element
  "Simple element trigger that fires immediately."
  [ctx]
  :fire)

(defn test-on-processing-time
  "Processing time trigger."
  [ctx]
  :fire-and-purge)

(defn test-on-event-time
  "Event time trigger."
  [ctx]
  :continue)

(defn test-clear
  "Clear function."
  [ctx]
  nil)

(deftest custom-trigger-test
  (testing "custom-trigger creates CljTrigger"
    (let [trigger (w/custom-trigger {:on-element #'test-on-element})]
      (is (some? trigger))
      (is (instance? flink_clj.CljTrigger trigger))))

  (testing "custom-trigger with all functions"
    (let [trigger (w/custom-trigger {:on-element #'test-on-element
                                      :on-processing-time #'test-on-processing-time
                                      :on-event-time #'test-on-event-time
                                      :clear #'test-clear})]
      (is (some? trigger))))

  (testing "custom-trigger requires at least one function"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
          #"At least one trigger function must be provided"
          (w/custom-trigger {})))))

;; =============================================================================
;; Trigger Composition Tests
;; =============================================================================

(deftest trigger-and-test
  (testing "trigger-and creates composite trigger"
    (let [t1 (w/custom-trigger {:on-element #'test-on-element})
          t2 (w/custom-trigger {:on-element #'test-on-element})
          composite (w/trigger-and t1 t2)]
      (is (some? composite)))))

(deftest trigger-or-test
  (testing "trigger-or creates composite trigger"
    (let [t1 (w/custom-trigger {:on-element #'test-on-element})
          t2 (w/custom-trigger {:on-element #'test-on-element})
          composite (w/trigger-or t1 t2)]
      (is (some? composite)))))

;; =============================================================================
;; Convenience Trigger Tests
;; =============================================================================

(deftest count-and-time-trigger-test
  (testing "count-and-time-trigger creates trigger"
    (let [trigger (w/count-and-time-trigger 100 [30 :seconds])]
      (is (some? trigger))))

  (testing "count-and-time-trigger with various duration specs"
    (is (some? (w/count-and-time-trigger 100 [100 :ms])))
    (is (some? (w/count-and-time-trigger 100 [1 :minutes])))
    (is (some? (w/count-and-time-trigger 100 [1 :hours])))
    (is (some? (w/count-and-time-trigger 100 30000)))))  ; raw millis

;; Delta function for trigger
(defn price-delta [old new]
  (Math/abs (double (- (:price new 0) (:price old 0)))))

(deftest delta-trigger-test
  (testing "delta-trigger creates trigger"
    (let [trigger (w/delta-trigger 10.0 #'price-delta {:price 0})]
      (is (some? trigger)))))

;; =============================================================================
;; Integration Tests (require MiniCluster)
;; =============================================================================

(deftest ^:integration custom-window-assigner-integration-test
  (testing "Custom window assigner works with stream"
    ;; Would require MiniCluster
    (is true "Integration test placeholder")))

(deftest ^:integration custom-trigger-integration-test
  (testing "Custom trigger works with windowed stream"
    ;; Would require MiniCluster
    (is true "Integration test placeholder")))
