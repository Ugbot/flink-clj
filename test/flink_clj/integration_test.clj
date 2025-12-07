(ns flink-clj.integration-test
  "Integration tests that run actual Flink jobs with MiniCluster.

  These tests verify that operators work end-to-end, not just that
  they construct correctly."
  (:require [clojure.test :refer :all]
            [flink-clj.test-utils :as tu]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]))

;; =============================================================================
;; Test Functions (must be top-level for serialization)
;; =============================================================================

(defn double-value [x] (* x 2))
(defn triple-value [x] (* x 3))
(defn inc-value [x] (inc x))
(defn positive? [x] (pos? x))
(defn even-value? [x] (even? x))
(defn tokenize [s] (clojure.string/split s #"\s+"))
(defn to-pair [word] [word 1])
(defn sum-counts [[w1 c1] [w2 c2]] [w1 (+ c1 c2)])
(defn get-word [pair] (first pair))

;; =============================================================================
;; Basic Map Tests
;; =============================================================================

(deftest ^:integration map-integration-test
  (testing "map doubles values"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3 4 5])
                        (stream/flink-map #'double-value)
                        (tu/collect-results))]
        (is (tu/same-elements? [2 4 6 8 10] results)))))

  (testing "map with type hint"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3] {:type :long})
                        (stream/flink-map #'triple-value {:returns :long})
                        (tu/collect-results))]
        (is (tu/same-elements? [3 6 9] results))))))

;; =============================================================================
;; Filter Tests
;; =============================================================================

(deftest ^:integration filter-integration-test
  (testing "filter keeps matching elements"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3 4 5 6])
                        (stream/flink-filter #'even-value?)
                        (tu/collect-results))]
        (is (tu/same-elements? [2 4 6] results)))))

  (testing "filter removes all non-matching"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [-1 -2 0])
                        (stream/flink-filter #'positive?)
                        (tu/collect-results))]
        (is (empty? results))))))

;; =============================================================================
;; FlatMap Tests
;; =============================================================================

(deftest ^:integration flat-map-integration-test
  (testing "flat-map expands elements"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env ["hello world" "foo bar"])
                        (stream/flat-map #'tokenize)
                        (tu/collect-results))]
        (is (tu/same-elements? ["hello" "world" "foo" "bar"] results))))))

;; =============================================================================
;; Chained Operations Tests
;; =============================================================================

(deftest ^:integration chained-operations-test
  (testing "map + filter chain"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3 4 5])
                        (stream/flink-map #'double-value)
                        (stream/flink-filter #'even-value?)
                        (tu/collect-results))]
        ;; All doubled values are even
        (is (tu/same-elements? [2 4 6 8 10] results)))))

  (testing "filter + map + filter chain"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3 4 5 6])
                        (stream/flink-filter #'positive?)
                        (stream/flink-map #'inc-value)
                        (stream/flink-filter #'even-value?)
                        (tu/collect-results))]
        ;; 1->2(even), 2->3, 3->4(even), 4->5, 5->6(even), 6->7
        (is (tu/same-elements? [2 4 6] results))))))

;; =============================================================================
;; Keyed Stream Tests
;; =============================================================================

(deftest ^:integration keyed-reduce-test
  (testing "word count pattern"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env ["hello" "world" "hello" "flink"])
                        (stream/flink-map #'to-pair)
                        (keyed/key-by #'get-word {:key-type :string})
                        (keyed/flink-reduce #'sum-counts)
                        (tu/collect-results))
            ;; Convert results to a map for easier assertion
            result-map (into {} results)]
        ;; hello appears twice, world and flink once
        (is (= 2 (get result-map "hello")))
        (is (= 1 (get result-map "world")))
        (is (= 1 (get result-map "flink")))))))

;; =============================================================================
;; Union Tests
;; =============================================================================

(deftest ^:integration union-test
  (testing "union combines two streams"
    (tu/with-mini-cluster [env]
      (let [stream1 (flink/from-collection env [1 2 3])
            stream2 (flink/from-collection env [4 5 6])
            results (-> (stream/union stream1 stream2)
                        (tu/collect-results))]
        (is (tu/same-elements? [1 2 3 4 5 6] results))))))

;; =============================================================================
;; Multiple Streams Union Tests
;; =============================================================================

(deftest ^:integration multi-union-test
  (testing "Union combines three streams"
    (tu/with-mini-cluster [env]
      (let [stream1 (flink/from-collection env [1 2])
            stream2 (flink/from-collection env [3 4])
            stream3 (flink/from-collection env [5 6])
            results (-> (stream/union stream1 stream2 stream3)
                        (tu/collect-results))]
        (is (tu/same-elements? [1 2 3 4 5 6] results))))))

;; =============================================================================
;; Type-Hinted Operations Tests
;; =============================================================================

(defn square-long ^long [^long x] (* x x))

(deftest ^:integration type-hinted-operations-test
  (testing "Operations with explicit type hints"
    (tu/with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3 4 5] {:type :long})
                        (stream/flink-map #'square-long {:returns :long})
                        (stream/flink-filter #'positive?)
                        (tu/collect-results))]
        (is (tu/same-elements? [1 4 9 16 25] results))))))
