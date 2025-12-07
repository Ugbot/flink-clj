(ns flink-clj.connected-test
  "Tests for flink-clj.connected namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.connected :as co]
            [flink-clj.env :as env]
            [flink-clj.core :as flink])
  (:import [org.apache.flink.streaming.api.datastream ConnectedStreams]))

;; =============================================================================
;; Test functions (must be top-level for serialization)
;; =============================================================================

(defn process-ints [x] {:type :int :value x})
(defn process-strings [s] {:type :string :value s})

(defn expand-ints [x] [x (* x 2)])
(defn expand-strings [s] [s (str s "!")])

;; =============================================================================
;; Connect Tests
;; =============================================================================

(deftest connect-test
  (testing "Connect two streams"
    (let [env (env/create-env)
          stream1 (flink/from-collection env [1 2 3])
          stream2 (flink/from-collection env ["a" "b" "c"])
          connected (co/connect stream1 stream2)]
      (is (instance? ConnectedStreams connected)))))

;; =============================================================================
;; CoMap Tests
;; =============================================================================

(deftest co-map-test
  (testing "Create co-map operation"
    (let [env (env/create-env)
          stream1 (flink/from-collection env [1 2 3])
          stream2 (flink/from-collection env ["a" "b" "c"])
          connected (co/connect stream1 stream2)
          result (co/co-map connected #'process-ints #'process-strings)]
      (is (some? result))))

  (testing "Create co-map with returns option"
    (let [env (env/create-env)
          stream1 (flink/from-collection env [1 2 3])
          stream2 (flink/from-collection env ["a" "b" "c"])
          connected (co/connect stream1 stream2)
          result (co/co-map connected
                            #'process-ints
                            #'process-strings
                            {:returns :clojure})]
      (is (some? result)))))

;; =============================================================================
;; CoFlatMap Tests
;; =============================================================================

(deftest co-flat-map-test
  (testing "Create co-flat-map operation"
    (let [env (env/create-env)
          stream1 (flink/from-collection env [1 2 3])
          stream2 (flink/from-collection env ["a" "b" "c"])
          connected (co/connect stream1 stream2)
          result (co/co-flat-map connected #'expand-ints #'expand-strings)]
      (is (some? result))))

  (testing "Create co-flat-map with returns option"
    (let [env (env/create-env)
          stream1 (flink/from-collection env [1 2 3])
          stream2 (flink/from-collection env ["a" "b" "c"])
          connected (co/connect stream1 stream2)
          result (co/co-flat-map connected
                                 #'expand-ints
                                 #'expand-strings
                                 {:returns :clojure})]
      (is (some? result)))))

;; =============================================================================
;; KeyBy Tests
;; =============================================================================

(deftest key-by-test
  (testing "Key connected streams with type hint"
    ;; Note: key-type is required for connected streams to avoid type erasure issues
    (let [env (env/create-env)
          stream1 (flink/from-collection env [{:id 1 :value "a"} {:id 2 :value "b"}])
          stream2 (flink/from-collection env [{:id 1 :data 100} {:id 2 :data 200}])
          connected (co/connect stream1 stream2)
          keyed (co/key-by connected :id :id {:key-type :long})]
      (is (some? keyed)))))
