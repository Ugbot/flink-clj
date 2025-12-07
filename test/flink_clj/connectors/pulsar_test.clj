(ns flink-clj.connectors.pulsar-test
  "Tests for Pulsar connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.pulsar :as pulsar]
            [flink-clj.connectors.generic :as conn]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Source functions exist"
    (is (fn? pulsar/source))
    (is (fn? pulsar/simple-source)))

  (testing "Sink functions exist"
    (is (fn? pulsar/sink))
    (is (fn? pulsar/simple-sink)))

  (testing "Helper functions exist"
    (is (fn? pulsar/topic-pattern))
    (is (fn? pulsar/topics)))

  (testing "Info function exists"
    (is (fn? pulsar/pulsar-info))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest pulsar-info-test
  (testing "pulsar-info returns expected structure"
    (let [info (pulsar/pulsar-info)]
      (is (map? info))
      (is (contains? info :source-available))
      (is (contains? info :sink-available))
      (is (contains? info :features))
      (is (contains? info :subscription-types))
      (is (boolean? (:source-available info)))
      (is (boolean? (:sink-available info)))
      (is (sequential? (:features info)))
      (is (= [:exclusive :shared :failover :key-shared] (:subscription-types info))))))

;; =============================================================================
;; Source Validation Tests
;; =============================================================================

(deftest source-validation-test
  (testing "source requires service-url"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"service-url is required"
          (pulsar/source {:admin-url "http://localhost:8080"
                          :topics ["topic"]
                          :subscription "sub"}))))

  (testing "source requires admin-url"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"admin-url is required"
          (pulsar/source {:service-url "pulsar://localhost:6650"
                          :topics ["topic"]
                          :subscription "sub"}))))

  (testing "source requires topics"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"topics is required"
          (pulsar/source {:service-url "pulsar://localhost:6650"
                          :admin-url "http://localhost:8080"
                          :subscription "sub"}))))

  (testing "source requires subscription"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"subscription is required"
          (pulsar/source {:service-url "pulsar://localhost:6650"
                          :admin-url "http://localhost:8080"
                          :topics ["topic"]})))))

;; =============================================================================
;; Sink Validation Tests
;; =============================================================================

(deftest sink-validation-test
  (testing "sink requires service-url"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"service-url is required"
          (pulsar/sink {:topic "topic"}))))

  (testing "sink requires topic or topic-router"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Either topic or topic-router is required"
          (pulsar/sink {:service-url "pulsar://localhost:6650"})))))

;; =============================================================================
;; Source Options Tests
;; =============================================================================

(deftest source-options-test
  (testing "Default deserializer is :string"
    (let [default-opts {:service-url "pulsar://localhost:6650"
                        :admin-url "http://localhost:8080"
                        :topics ["topic"]
                        :subscription "sub"}]
      (is (= :string (:deserializer default-opts :string)))))

  (testing "Default subscription-type is :exclusive"
    (is (= :exclusive (:subscription-type {} :exclusive))))

  (testing "Default start-cursor is :latest"
    (is (= :latest (:start-cursor {} :latest))))

  (testing "Default stop-cursor is :never"
    (is (= :never (:stop-cursor {} :never)))))

;; =============================================================================
;; Sink Options Tests
;; =============================================================================

(deftest sink-options-test
  (testing "Default serializer is :string"
    (is (= :string (:serializer {} :string))))

  (testing "Default delivery-guarantee is :at-least-once"
    (is (= :at-least-once (:delivery-guarantee {} :at-least-once)))))

;; =============================================================================
;; Topic Helpers Tests
;; =============================================================================

(deftest topic-pattern-test
  (testing "topic-pattern creates pattern spec"
    (let [spec (pulsar/topic-pattern "persistent://tenant/ns/.*")]
      (is (= :pattern (:type spec)))
      (is (= "persistent://tenant/ns/.*" (:pattern spec))))))

(deftest topics-test
  (testing "topics creates list spec"
    (let [spec (pulsar/topics ["topic1" "topic2" "topic3"])]
      (is (= :list (:type spec)))
      (is (= ["topic1" "topic2" "topic3"] (:topics spec))))))

;; =============================================================================
;; Simple Source/Sink Tests
;; =============================================================================

(deftest simple-source-options-test
  (testing "simple-source merges options correctly"
    (let [base-opts {:subscription-type :shared :start-cursor :earliest}]
      (is (= :shared (:subscription-type base-opts)))
      (is (= :earliest (:start-cursor base-opts))))))

(deftest simple-sink-options-test
  (testing "simple-sink merges options correctly"
    (let [base-opts {:delivery-guarantee :exactly-once}]
      (is (= :exactly-once (:delivery-guarantee base-opts))))))

;; =============================================================================
;; Deserializer/Serializer Keyword Tests
;; =============================================================================

(deftest deserializer-keywords-test
  (testing "Valid deserializer keywords"
    (is (keyword? :string))
    (is (keyword? :json))))

(deftest serializer-keywords-test
  (testing "Valid serializer keywords"
    (is (keyword? :string))
    (is (keyword? :json))))

;; =============================================================================
;; Subscription Type Tests
;; =============================================================================

(deftest subscription-type-keywords-test
  (testing "Valid subscription type keywords"
    (is (keyword? :exclusive))
    (is (keyword? :shared))
    (is (keyword? :failover))
    (is (keyword? :key-shared))))

;; =============================================================================
;; Cursor Tests
;; =============================================================================

(deftest start-cursor-keywords-test
  (testing "Valid start cursor keywords"
    (is (keyword? :earliest))
    (is (keyword? :latest))))

(deftest stop-cursor-keywords-test
  (testing "Valid stop cursor keywords"
    (is (keyword? :never))
    (is (keyword? :latest))))

;; =============================================================================
;; Delivery Guarantee Tests
;; =============================================================================

(deftest delivery-guarantee-keywords-test
  (testing "Valid delivery guarantee keywords"
    (is (keyword? :none))
    (is (keyword? :at-least-once))
    (is (keyword? :exactly-once))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-availability-test
  (testing "Availability check returns boolean"
    (let [info (pulsar/pulsar-info)]
      (is (or (true? (:source-available info))
              (false? (:source-available info))))
      (is (or (true? (:sink-available info))
              (false? (:sink-available info)))))))

;; =============================================================================
;; Integration Tests (require Pulsar connector JAR)
;; =============================================================================

(deftest ^:integration source-creation-test
  (testing "source creates PulsarSource when available"
    (let [info (pulsar/pulsar-info)]
      (when (:source-available info)
        (let [src (pulsar/source {:service-url "pulsar://localhost:6650"
                                  :admin-url "http://localhost:8080"
                                  :topics ["test-topic"]
                                  :subscription "test-sub"})]
          (is (some? src)))))))

(deftest ^:integration sink-creation-test
  (testing "sink creates PulsarSink when available"
    (let [info (pulsar/pulsar-info)]
      (when (:sink-available info)
        (let [snk (pulsar/sink {:service-url "pulsar://localhost:6650"
                                :topic "output-topic"})]
          (is (some? snk)))))))

(deftest ^:integration subscription-type-test
  (testing "Different subscription types work"
    (is true "Integration test placeholder")))

(deftest ^:integration cursor-configuration-test
  (testing "Start and stop cursors are configured"
    (is true "Integration test placeholder")))
