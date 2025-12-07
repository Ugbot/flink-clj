(ns flink-clj.watermark-test
  "Tests for flink-clj.watermark namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.watermark :as wm])
  (:import [org.apache.flink.api.common.eventtime WatermarkStrategy Watermark]
           [java.time Duration]))

;; =============================================================================
;; Built-in Strategy Tests
;; =============================================================================

(deftest no-watermarks-test
  (testing "Create no-watermarks strategy"
    (let [strategy (wm/no-watermarks)]
      (is (instance? WatermarkStrategy strategy)))))

(deftest for-monotonous-timestamps-test
  (testing "Create monotonous timestamps strategy"
    (let [strategy (wm/for-monotonous-timestamps)]
      (is (instance? WatermarkStrategy strategy)))))

(deftest for-bounded-out-of-orderness-test
  (testing "Create strategy with milliseconds"
    (let [strategy (wm/for-bounded-out-of-orderness 5000)]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with Duration"
    (let [strategy (wm/for-bounded-out-of-orderness (Duration/ofSeconds 5))]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with vector spec - seconds"
    (let [strategy (wm/for-bounded-out-of-orderness [5 :seconds])]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with vector spec - milliseconds"
    (let [strategy (wm/for-bounded-out-of-orderness [500 :ms])]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with vector spec - minutes"
    (let [strategy (wm/for-bounded-out-of-orderness [1 :minutes])]
      (is (instance? WatermarkStrategy strategy)))))

;; =============================================================================
;; Timestamp Assigner Tests
;; =============================================================================

(deftest with-timestamp-assigner-test
  (testing "Add keyword timestamp assigner"
    (let [strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                       (wm/with-timestamp-assigner :event-time))]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Add function timestamp assigner"
    (let [extract-fn (fn [event _] (:ts event))
          strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                       (wm/with-timestamp-assigner extract-fn))]
      (is (instance? WatermarkStrategy strategy)))))

;; =============================================================================
;; Idleness Tests
;; =============================================================================

(deftest with-idleness-test
  (testing "Add idleness timeout to strategy"
    (let [strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                       (wm/with-idleness [1 :minutes]))]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Idleness with milliseconds"
    (let [strategy (-> (wm/for-monotonous-timestamps)
                       (wm/with-idleness 60000))]
      (is (instance? WatermarkStrategy strategy)))))

;; =============================================================================
;; Watermark Utility Tests
;; =============================================================================

(deftest watermark-test
  (testing "Create watermark with timestamp"
    (let [w (wm/watermark 1640000000000)]
      (is (instance? Watermark w))
      (is (= 1640000000000 (.getTimestamp w)))))

  (testing "Max watermark constant"
    (is (instance? Watermark wm/max-watermark))
    (is (= Long/MAX_VALUE (.getTimestamp wm/max-watermark)))))

;; =============================================================================
;; Composition Tests
;; =============================================================================

(deftest strategy-composition-test
  (testing "Full composition with all options"
    (let [strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                       (wm/with-timestamp-assigner :event-time)
                       (wm/with-idleness [1 :minutes]))]
      (is (instance? WatermarkStrategy strategy)))))

;; =============================================================================
;; Watermark Alignment Tests
;; =============================================================================

(deftest with-watermark-alignment-test
  (testing "Add watermark alignment to strategy - 2 args"
    (let [strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                       (wm/with-watermark-alignment "my-group" [30 :seconds]))]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Add watermark alignment with update interval - 3 args"
    (let [strategy (-> (wm/for-bounded-out-of-orderness [5 :seconds])
                       (wm/with-watermark-alignment "kafka-partitions" [30 :seconds] [1 :seconds]))]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Alignment with milliseconds duration"
    (let [strategy (-> (wm/for-monotonous-timestamps)
                       (wm/with-watermark-alignment "group" 30000))]
      (is (instance? WatermarkStrategy strategy)))))

;; =============================================================================
;; Custom Generator Tests
;; =============================================================================

;; Define test handlers at top level for serialization
(defn test-on-event
  "Test on-event handler for watermark generator."
  [{:keys [timestamp current-watermark]}]
  (max current-watermark (- timestamp 1000)))

(defn test-on-periodic
  "Test on-periodic handler for watermark generator."
  [{:keys [current-watermark]}]
  current-watermark)

(defn test-marker-check
  "Check if element is a watermark marker."
  [{:keys [element timestamp]}]
  (when (:is-marker element)
    timestamp))

(deftest custom-generator-test
  (testing "Create custom generator with both handlers"
    (let [strategy (wm/custom-generator {:on-event #'test-on-event
                                         :on-periodic #'test-on-periodic})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create custom generator with only on-event"
    (let [strategy (wm/custom-generator {:on-event #'test-on-event})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create custom generator with only on-periodic"
    (let [strategy (wm/custom-generator {:on-periodic #'test-on-periodic})]
      (is (instance? WatermarkStrategy strategy)))))

(deftest periodic-generator-test
  (testing "Create periodic generator"
    (let [strategy (wm/periodic-generator #'test-on-event)]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Periodic generator with timestamp assigner"
    (let [strategy (-> (wm/periodic-generator #'test-on-event)
                       (wm/with-timestamp-assigner :event-time))]
      (is (instance? WatermarkStrategy strategy)))))

(deftest punctuated-generator-test
  (testing "Create punctuated generator"
    (let [strategy (wm/punctuated-generator #'test-marker-check)]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Punctuated generator with timestamp assigner"
    (let [strategy (-> (wm/punctuated-generator #'test-marker-check)
                       (wm/with-timestamp-assigner :ts))]
      (is (instance? WatermarkStrategy strategy)))))

;; =============================================================================
;; Convenience Builder Tests
;; =============================================================================

(defn extract-timestamp
  "Extract timestamp for testing."
  [event _prev]
  (:timestamp event))

(deftest create-strategy-test
  (testing "Create strategy with out-of-orderness and keyword field"
    (let [strategy (wm/create-strategy {:max-out-of-orderness [5 :seconds]
                                        :timestamp-field :event-time})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with monotonous timestamps"
    (let [strategy (wm/create-strategy {:timestamp-field :ts})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with custom function"
    (let [strategy (wm/create-strategy {:max-out-of-orderness [5 :seconds]
                                        :timestamp-fn extract-timestamp})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with idleness"
    (let [strategy (wm/create-strategy {:max-out-of-orderness [5 :seconds]
                                        :timestamp-field :event-time
                                        :idleness-timeout [1 :minutes]})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create strategy with alignment"
    (let [strategy (wm/create-strategy {:max-out-of-orderness [5 :seconds]
                                        :timestamp-field :event-time
                                        :alignment-group "my-sources"
                                        :max-drift [30 :seconds]})]
      (is (instance? WatermarkStrategy strategy))))

  (testing "Create full strategy with all options"
    (let [strategy (wm/create-strategy {:max-out-of-orderness [5 :seconds]
                                        :timestamp-field :event-time
                                        :idleness-timeout [1 :minutes]
                                        :alignment-group "kafka"
                                        :max-drift [30 :seconds]})]
      (is (instance? WatermarkStrategy strategy)))))
