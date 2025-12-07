(ns flink-clj.connected-integration-test
  "Integration tests for connected streams with MiniCluster."
  (:require [clojure.test :refer :all]
            [flink-clj.test-utils :as tu]
            [flink-clj.core :as flink]
            [flink-clj.connected :as co]))

;; =============================================================================
;; Test Functions (must be top-level for serialization)
;; =============================================================================

;; CoMap functions - process each stream type differently
(defn process-int [x]
  {:type :int :value x :doubled (* x 2)})

(defn process-string [s]
  {:type :string :value s :length (count s)})

;; Alternative CoMap that produces same output type
(defn int-to-string [x]
  (str "int:" x))

(defn string-to-upper [s]
  (clojure.string/upper-case s))

;; CoFlatMap functions - expand each element
(defn expand-int [x]
  [x (- x) (* x 2)])

(defn expand-string [s]
  [s (str s "!") (clojure.string/upper-case s)])

;; For testing with different expansion
(defn int-range [x]
  (range 1 (inc x)))

(defn string-chars [s]
  (mapv str (seq s)))

;; =============================================================================
;; CoMap Integration Tests
;; =============================================================================

(deftest ^:integration co-map-basic-test
  (testing "co-map processes both streams"
    (tu/with-mini-cluster [env]
      (let [ints (flink/from-collection env [1 2 3])
            strings (flink/from-collection env ["a" "bb" "ccc"])
            results (-> (co/connect ints strings)
                        (co/co-map #'process-int #'process-string {:returns :clojure})
                        (tu/collect-results))
            int-results (filter #(= :int (:type %)) results)
            string-results (filter #(= :string (:type %)) results)]
        ;; Should have processed all integers
        (is (= 3 (count int-results)))
        (is (= #{2 4 6} (set (map :doubled int-results))))
        ;; Should have processed all strings
        (is (= 3 (count string-results)))
        (is (= #{1 2 3} (set (map :length string-results))))))))

(deftest ^:integration co-map-uniform-output-test
  (testing "co-map can produce uniform output type"
    (tu/with-mini-cluster [env]
      (let [ints (flink/from-collection env [1 2 3])
            strings (flink/from-collection env ["hello" "world"])
            results (-> (co/connect ints strings)
                        (co/co-map #'int-to-string #'string-to-upper {:returns :string})
                        (tu/collect-results))]
        ;; All results should be strings
        (is (every? string? results))
        (is (= 5 (count results)))
        ;; Check specific values exist
        (is (some #{"int:1"} results))
        (is (some #{"int:2"} results))
        (is (some #{"int:3"} results))
        (is (some #{"HELLO"} results))
        (is (some #{"WORLD"} results))))))

;; =============================================================================
;; CoFlatMap Integration Tests
;; =============================================================================

(deftest ^:integration co-flat-map-basic-test
  (testing "co-flat-map expands both streams"
    (tu/with-mini-cluster [env]
      (let [ints (flink/from-collection env [5 -3])
            strings (flink/from-collection env ["hi"])
            results (-> (co/connect ints strings)
                        (co/co-flat-map #'expand-int #'expand-string {:returns :clojure})
                        (tu/collect-results))]
        ;; expand-int [5] -> [5 -5 10]
        ;; expand-int [-3] -> [-3 3 -6]
        ;; expand-string ["hi"] -> ["hi" "hi!" "HI"]
        (is (= 9 (count results)))
        ;; Verify some specific values
        (is (some #{5} results))
        (is (some #{-5} results))
        (is (some #{10} results))
        (is (some #{"hi"} results))
        (is (some #{"hi!"} results))
        (is (some #{"HI"} results))))))

(deftest ^:integration co-flat-map-varied-expansion-test
  (testing "co-flat-map with variable expansion sizes"
    (tu/with-mini-cluster [env]
      (let [ints (flink/from-collection env [2 3]) ; range 1..2, 1..3
            strings (flink/from-collection env ["ab"]) ; chars ["a" "b"]
            results (-> (co/connect ints strings)
                        (co/co-flat-map #'int-range #'string-chars {:returns :clojure})
                        (tu/collect-results))]
        ;; int-range 2 -> [1 2]
        ;; int-range 3 -> [1 2 3]
        ;; string-chars "ab" -> ["a" "b"]
        ;; Total: 2 + 3 + 2 = 7 elements
        (is (= 7 (count results)))
        ;; Should have integers from range
        (is (= #{1 2 3} (set (filter number? results))))
        ;; Should have string chars
        (is (= #{"a" "b"} (set (filter string? results))))))))

;; =============================================================================
;; Connected Streams Edge Cases
;; =============================================================================

;; Note: Flink doesn't allow empty collections, so we test with at least one element
(deftest ^:integration co-map-single-element-test
  (testing "co-map handles streams with single elements"
    (tu/with-mini-cluster [env]
      (let [ints (flink/from-collection env [42])
            strings (flink/from-collection env ["only"])
            results (-> (co/connect ints strings)
                        (co/co-map #'int-to-string #'string-to-upper {:returns :string})
                        (tu/collect-results))]
        (is (= 2 (count results)))
        (is (some #{"int:42"} results))
        (is (some #{"ONLY"} results))))))
