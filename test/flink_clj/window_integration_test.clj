(ns flink-clj.window-integration-test
  "Unit tests for window operations.

  These tests verify that window configurations are constructed correctly
  and can be applied to keyed streams."
  (:require [clojure.test :refer :all]
            [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.keyed :as keyed]
            [flink-clj.window :as w])
  (:import [org.apache.flink.streaming.api.datastream WindowedStream]
           [java.time Duration]))

;; =============================================================================
;; Test Functions (must be top-level for serialization)
;; =============================================================================

(defn get-key [[k _v]] k)
(defn sum-pairs [[k1 v1] [_k2 v2]] [k1 (+ v1 v2)])

;; =============================================================================
;; Version Detection Helper
;; =============================================================================

(defn- time-or-duration?
  "Check if value is a Flink Time (1.x) or Java Duration (2.x)."
  [t]
  (or (instance? Duration t)
      (try
        (let [time-class (Class/forName "org.apache.flink.streaming.api.windowing.time.Time")]
          (instance? time-class t))
        (catch ClassNotFoundException _ false))))

;; =============================================================================
;; Time Duration Helper Tests
;; =============================================================================

(deftest time-duration-helpers-test
  (testing "Time duration helpers create correct Time/Duration objects"
    (is (time-or-duration? (w/milliseconds 100)))
    (is (time-or-duration? (w/seconds 10)))
    (is (time-or-duration? (w/minutes 5)))
    (is (time-or-duration? (w/hours 1)))
    (is (time-or-duration? (w/days 1)))))

(deftest duration-vector-spec-test
  (testing "Duration vector spec converts to Time/Duration"
    (is (time-or-duration? (w/duration [100 :milliseconds])))
    (is (time-or-duration? (w/duration [10 :seconds])))
    (is (time-or-duration? (w/duration [5 :minutes])))
    (is (time-or-duration? (w/duration [1 :hours])))
    (is (time-or-duration? (w/duration [1 :days])))
    ;; Short forms
    (is (time-or-duration? (w/duration [100 :ms])))
    (is (time-or-duration? (w/duration [10 :s])))
    (is (time-or-duration? (w/duration [5 :m])))
    (is (time-or-duration? (w/duration [1 :h])))
    (is (time-or-duration? (w/duration [1 :d])))))

;; =============================================================================
;; Tumbling Window Construction Tests
;; =============================================================================

(deftest tumbling-event-time-construction-test
  (testing "Tumbling event time window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1] ["b" 2]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/tumbling-event-time keyed-stream (w/seconds 10))]
      (is (instance? WindowedStream windowed)))))

(deftest tumbling-event-time-with-offset-test
  (testing "Tumbling event time window with offset"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/tumbling-event-time keyed-stream (w/seconds 10) (w/seconds 5))]
      (is (instance? WindowedStream windowed)))))

(deftest tumbling-processing-time-construction-test
  (testing "Tumbling processing time window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1] ["b" 2]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/tumbling-processing-time keyed-stream (w/seconds 10))]
      (is (instance? WindowedStream windowed)))))

;; =============================================================================
;; Sliding Window Construction Tests
;; =============================================================================

(deftest sliding-event-time-construction-test
  (testing "Sliding event time window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/sliding-event-time keyed-stream (w/minutes 1) (w/seconds 10))]
      (is (instance? WindowedStream windowed)))))

(deftest sliding-processing-time-construction-test
  (testing "Sliding processing time window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/sliding-processing-time keyed-stream (w/minutes 1) (w/seconds 10))]
      (is (instance? WindowedStream windowed)))))

;; =============================================================================
;; Session Window Construction Tests
;; =============================================================================

(deftest session-event-time-construction-test
  (testing "Session event time window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/session-event-time keyed-stream (w/minutes 5))]
      (is (instance? WindowedStream windowed)))))

(deftest session-processing-time-construction-test
  (testing "Session processing time window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/session-processing-time keyed-stream (w/minutes 5))]
      (is (instance? WindowedStream windowed)))))

;; =============================================================================
;; Global Window Construction Tests
;; =============================================================================

(deftest global-window-construction-test
  (testing "Global window creates WindowedStream"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/global-window keyed-stream)]
      (is (instance? WindowedStream windowed)))))

;; =============================================================================
;; Trigger Tests
;; =============================================================================

(deftest count-trigger-test
  (testing "Count trigger creates valid Trigger"
    (let [trigger (w/count-trigger 100)]
      (is (some? trigger)))))

(deftest event-time-trigger-test
  (testing "Event time trigger creates valid Trigger"
    (let [trigger (w/event-time-trigger)]
      (is (some? trigger)))))

(deftest processing-time-trigger-test
  (testing "Processing time trigger creates valid Trigger"
    (let [trigger (w/processing-time-trigger)]
      (is (some? trigger)))))

(deftest purging-trigger-test
  (testing "Purging wrapper creates valid Trigger"
    (let [inner-trigger (w/count-trigger 100)
          trigger (w/purging inner-trigger)]
      (is (some? trigger)))))

;; =============================================================================
;; Evictor Tests
;; =============================================================================

(deftest count-evictor-test
  (testing "Count evictor creates valid Evictor"
    (let [evictor (w/count-evictor 100)]
      (is (some? evictor)))))

(deftest time-evictor-test
  (testing "Time evictor creates valid Evictor"
    (let [evictor (w/time-evictor (w/seconds 10))]
      (is (some? evictor)))))

;; =============================================================================
;; Window with Trigger/Evictor Tests
;; =============================================================================

(deftest window-with-trigger-test
  (testing "Window with custom trigger"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (-> keyed-stream
                       (w/global-window)
                       (w/trigger (w/count-trigger 10)))]
      (is (instance? WindowedStream windowed)))))

(deftest window-with-evictor-test
  (testing "Window with evictor"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (-> keyed-stream
                       (w/tumbling-event-time (w/seconds 10))
                       (w/evictor (w/count-evictor 5)))]
      (is (instance? WindowedStream windowed)))))

;; =============================================================================
;; Allowed Lateness Tests
;; =============================================================================

(deftest allowed-lateness-test
  (testing "Window with allowed lateness"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (-> keyed-stream
                       (w/tumbling-event-time (w/seconds 10))
                       (w/allowed-lateness (w/seconds 5)))]
      (is (instance? WindowedStream windowed)))))

;; =============================================================================
;; Duration Spec Variations
;; =============================================================================

(deftest window-with-vector-duration-test
  (testing "Window accepts vector duration spec"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/tumbling-event-time keyed-stream [10 :seconds])]
      (is (instance? WindowedStream windowed)))))

(deftest window-with-numeric-duration-test
  (testing "Window accepts numeric duration (milliseconds)"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1]])
                           (keyed/key-by #'get-key {:key-type :string}))
          windowed (w/tumbling-event-time keyed-stream 10000)]
      (is (instance? WindowedStream windowed)))))
