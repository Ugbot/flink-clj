(ns flink-clj.state-integration-test
  "Unit tests for state management.

  These tests verify state descriptor construction and configuration,
  as well as state access patterns."
  (:require [clojure.test :refer :all]
            [flink-clj.state :as state])
  (:import [org.apache.flink.api.common.state
            ValueStateDescriptor ListStateDescriptor MapStateDescriptor
            ReducingStateDescriptor]))

;; =============================================================================
;; ValueState Descriptor Tests
;; =============================================================================

(deftest value-state-descriptor-test
  (testing "Create basic ValueState descriptor"
    (let [desc (state/value-state "count" :long)]
      (is (instance? ValueStateDescriptor desc))
      (is (= "count" (.getName desc)))))

  (testing "Create ValueState descriptor with default"
    (let [desc (state/value-state "count" :long {:default 0})]
      (is (instance? ValueStateDescriptor desc))))

  (testing "Create ValueState descriptor with various types"
    (is (instance? ValueStateDescriptor (state/value-state "str-state" :string)))
    (is (instance? ValueStateDescriptor (state/value-state "int-state" :int)))
    (is (instance? ValueStateDescriptor (state/value-state "double-state" :double)))
    (is (instance? ValueStateDescriptor (state/value-state "bool-state" :boolean)))))

;; =============================================================================
;; ListState Descriptor Tests
;; =============================================================================

(deftest list-state-descriptor-test
  (testing "Create ListState descriptor"
    (let [desc (state/list-state "items" :string)]
      (is (instance? ListStateDescriptor desc))
      (is (= "items" (.getName desc)))))

  (testing "Create ListState descriptor with various element types"
    (is (instance? ListStateDescriptor (state/list-state "longs" :long)))
    (is (instance? ListStateDescriptor (state/list-state "strings" :string)))))

;; =============================================================================
;; MapState Descriptor Tests
;; =============================================================================

(deftest map-state-descriptor-test
  (testing "Create MapState descriptor"
    (let [desc (state/map-state "lookup" :string :long)]
      (is (instance? MapStateDescriptor desc))
      (is (= "lookup" (.getName desc)))))

  (testing "Create MapState descriptor with various types"
    (is (instance? MapStateDescriptor (state/map-state "str-to-int" :string :int)))
    (is (instance? MapStateDescriptor (state/map-state "long-to-str" :long :string)))))

;; =============================================================================
;; ReducingState Descriptor Tests
;; =============================================================================

(defn sum-reducer [a b] (+ a b))

(deftest reducing-state-descriptor-test
  (testing "Create ReducingState descriptor"
    (let [desc (state/reducing-state "total" :long sum-reducer)]
      (is (instance? ReducingStateDescriptor desc))
      (is (= "total" (.getName desc))))))

;; =============================================================================
;; TTL Configuration Tests
;; =============================================================================

(deftest ttl-config-test
  (testing "Create TTL config with time-to-live"
    (let [config (state/ttl-config {:ttl [1 :hours]})]
      (is (some? config))))

  (testing "Create TTL config with update type"
    (let [config (state/ttl-config {:ttl [30 :minutes]
                                    :update-type :on-read-and-write})]
      (is (some? config))))

  (testing "Create TTL config with visibility setting"
    (let [config (state/ttl-config {:ttl [1 :hours]
                                    :visibility :return-expired-if-not-cleaned})]
      (is (some? config))))

  (testing "TTL with various time units"
    (is (some? (state/ttl-config {:ttl [100 :ms]})))
    (is (some? (state/ttl-config {:ttl [10 :seconds]})))
    (is (some? (state/ttl-config {:ttl [5 :minutes]})))
    (is (some? (state/ttl-config {:ttl [2 :hours]})))
    (is (some? (state/ttl-config {:ttl [1 :days]})))))

;; =============================================================================
;; State with TTL Tests
;; =============================================================================

(deftest value-state-with-ttl-test
  (testing "Create ValueState with TTL"
    (let [desc (state/value-state "expiring" :string
                                  {:ttl {:ttl [1 :hours]}})]
      (is (instance? ValueStateDescriptor desc)))))

(deftest list-state-with-ttl-test
  (testing "Create ListState with TTL"
    (let [desc (state/list-state "expiring-list" :long
                                 {:ttl {:ttl [30 :minutes]}})]
      (is (instance? ListStateDescriptor desc)))))

(deftest map-state-with-ttl-test
  (testing "Create MapState with TTL"
    (let [desc (state/map-state "expiring-map" :string :long
                                {:ttl {:ttl [15 :minutes]}})]
      (is (instance? MapStateDescriptor desc)))))

;; =============================================================================
;; State Descriptor Naming Tests
;; =============================================================================

(deftest state-descriptor-names-test
  (testing "State descriptors use provided names"
    (let [value-desc (state/value-state "my-value" :long)
          list-desc (state/list-state "my-list" :string)
          map-desc (state/map-state "my-map" :string :long)]
      (is (= "my-value" (.getName value-desc)))
      (is (= "my-list" (.getName list-desc)))
      (is (= "my-map" (.getName map-desc))))))

;; =============================================================================
;; Complex Type State Tests
;; =============================================================================

(deftest clojure-type-state-test
  (testing "State descriptor with Clojure type"
    (let [desc (state/value-state "clj-data" :clojure)]
      (is (instance? ValueStateDescriptor desc)))))
