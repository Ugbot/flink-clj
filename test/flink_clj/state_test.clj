(ns flink-clj.state-test
  "Tests for flink-clj.state namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.state :as state]
            [flink-clj.types :as t])
  (:import [org.apache.flink.api.common.state
            ValueStateDescriptor ListStateDescriptor MapStateDescriptor
            ReducingStateDescriptor StateTtlConfig]))

;; =============================================================================
;; State Descriptor Tests
;; =============================================================================

(deftest value-state-test
  (testing "Basic value state descriptor"
    (let [desc (state/value-state "count" :long)]
      (is (instance? ValueStateDescriptor desc))
      (is (= "count" (.getName desc)))))

  (testing "Value state with default"
    (let [desc (state/value-state "count" :long {:default 0})]
      (is (instance? ValueStateDescriptor desc))))

  (testing "Value state with type spec"
    (let [desc (state/value-state "data" [:row :id :int :name :string])]
      (is (instance? ValueStateDescriptor desc)))))

(deftest list-state-test
  (testing "Basic list state descriptor"
    (let [desc (state/list-state "items" :string)]
      (is (instance? ListStateDescriptor desc))
      (is (= "items" (.getName desc)))))

  (testing "List state with complex type"
    (let [desc (state/list-state "events" [:row :ts :long :data :string])]
      (is (instance? ListStateDescriptor desc)))))

(deftest map-state-test
  (testing "Basic map state descriptor"
    (let [desc (state/map-state "lookup" :string :long)]
      (is (instance? MapStateDescriptor desc))
      (is (= "lookup" (.getName desc)))))

  (testing "Map state with complex value type"
    (let [desc (state/map-state "cache" :string [:row :value :double :ts :long])]
      (is (instance? MapStateDescriptor desc)))))

(deftest reducing-state-test
  (testing "Reducing state descriptor"
    (let [desc (state/reducing-state "total" :long +)]
      (is (instance? ReducingStateDescriptor desc))
      (is (= "total" (.getName desc))))))

;; =============================================================================
;; TTL Configuration Tests
;; =============================================================================

(deftest ttl-config-test
  (testing "Basic TTL config"
    (let [config (state/ttl-config {:ttl [1 :hours]})]
      (is (instance? StateTtlConfig config))))

  (testing "TTL config with all options"
    (let [config (state/ttl-config {:ttl [30 :minutes]
                                    :update-type :on-read-and-write
                                    :visibility :return-expired-if-not-cleaned})]
      (is (instance? StateTtlConfig config))))

  (testing "TTL time units"
    (is (instance? StateTtlConfig (state/ttl-config {:ttl [100 :ms]})))
    (is (instance? StateTtlConfig (state/ttl-config {:ttl [10 :seconds]})))
    (is (instance? StateTtlConfig (state/ttl-config {:ttl [5 :minutes]})))
    (is (instance? StateTtlConfig (state/ttl-config {:ttl [1 :hours]})))
    (is (instance? StateTtlConfig (state/ttl-config {:ttl [1 :days]})))))

(deftest state-with-ttl-test
  (testing "Value state with TTL"
    (let [desc (state/value-state "expiring" :string
                                  {:ttl {:ttl [1 :hours]}})]
      (is (instance? ValueStateDescriptor desc))))

  (testing "List state with TTL"
    (let [desc (state/list-state "expiring-list" :string
                                 {:ttl {:ttl [30 :minutes]}})]
      (is (instance? ListStateDescriptor desc))))

  (testing "Map state with TTL"
    (let [desc (state/map-state "expiring-map" :string :long
                                {:ttl {:ttl [1 :days]}})]
      (is (instance? MapStateDescriptor desc)))))

;; Note: Full integration tests for state operations require a running
;; Flink job with state backend. These would be marked as :integration tests.
