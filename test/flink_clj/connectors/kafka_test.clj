(ns flink-clj.connectors.kafka-test
  "Tests for flink-clj.connectors.kafka namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.kafka :as kafka]))

;; Helper to detect if Kafka connector is available
(defn- kafka-connector-available?
  "Check if Kafka connector classes are available."
  []
  (try
    (Class/forName "org.apache.flink.connector.kafka.source.KafkaSource")
    true
    (catch ClassNotFoundException _ false)))

;; =============================================================================
;; Kafka Source Tests
;; =============================================================================

(deftest kafka-source-test
  (if (kafka-connector-available?)
    (do
      (testing "Create basic Kafka source with string topic"
        (let [source (kafka/source {:bootstrap-servers "localhost:9092"
                                    :topics "test-topic"
                                    :value-format :string})]
          (is (some? source))))

      (testing "Create Kafka source with multiple topics"
        (let [source (kafka/source {:bootstrap-servers "localhost:9092"
                                    :topics ["topic1" "topic2" "topic3"]
                                    :value-format :string})]
          (is (some? source))))

      (testing "Create Kafka source with regex pattern"
        (let [source (kafka/source {:bootstrap-servers "localhost:9092"
                                    :topics #"events-.*"
                                    :value-format :string})]
          (is (some? source))))

      (testing "Create Kafka source with group ID"
        (let [source (kafka/source {:bootstrap-servers "localhost:9092"
                                    :topics "test-topic"
                                    :group-id "my-consumer-group"
                                    :value-format :string})]
          (is (some? source))))

      (testing "Create Kafka source with different offset modes"
        (is (some? (kafka/source {:bootstrap-servers "localhost:9092"
                                  :topics "test"
                                  :starting-offsets :earliest
                                  :value-format :string})))
        (is (some? (kafka/source {:bootstrap-servers "localhost:9092"
                                  :topics "test"
                                  :starting-offsets :latest
                                  :value-format :string})))
        (is (some? (kafka/source {:bootstrap-servers "localhost:9092"
                                  :topics "test"
                                  :starting-offsets :committed
                                  :group-id "test-group"
                                  :value-format :string}))))

      (testing "Create Kafka source with timestamp offset"
        (let [source (kafka/source {:bootstrap-servers "localhost:9092"
                                    :topics "test-topic"
                                    :starting-offsets 1640000000000
                                    :value-format :string})]
          (is (some? source))))

      (testing "Create Kafka source with additional properties"
        (let [source (kafka/source {:bootstrap-servers "localhost:9092"
                                    :topics "test-topic"
                                    :value-format :string
                                    :properties {"auto.offset.reset" "earliest"
                                                 "enable.auto.commit" "false"}})]
          (is (some? source)))))
    (testing "Kafka connector not available"
      (is true))))

;; =============================================================================
;; Kafka Sink Tests
;; =============================================================================

(deftest kafka-sink-test
  (if (kafka-connector-available?)
    (do
      (testing "Create basic Kafka sink"
        (let [sink (kafka/sink {:bootstrap-servers "localhost:9092"
                                :topic "output-topic"
                                :value-format :string})]
          (is (some? sink))))

      (testing "Create Kafka sink with at-least-once delivery"
        (let [sink (kafka/sink {:bootstrap-servers "localhost:9092"
                                :topic "output-topic"
                                :value-format :string
                                :delivery-guarantee :at-least-once})]
          (is (some? sink))))

      (testing "Create Kafka sink with exactly-once delivery"
        (let [sink (kafka/sink {:bootstrap-servers "localhost:9092"
                                :topic "output-topic"
                                :value-format :string
                                :delivery-guarantee :exactly-once
                                :transactional-id-prefix "my-app"})]
          (is (some? sink))))

      (testing "Create Kafka sink with no delivery guarantee"
        (let [sink (kafka/sink {:bootstrap-servers "localhost:9092"
                                :topic "output-topic"
                                :value-format :string
                                :delivery-guarantee :none})]
          (is (some? sink))))

      (testing "Create Kafka sink with additional properties"
        (let [sink (kafka/sink {:bootstrap-servers "localhost:9092"
                                :topic "output-topic"
                                :value-format :string
                                :properties {"acks" "all"
                                             "retries" "3"}})]
          (is (some? sink)))))
    (testing "Kafka connector not available"
      (is true))))

;; =============================================================================
;; Convenience Function Tests
;; =============================================================================

(deftest convenience-functions-test
  (if (kafka-connector-available?)
    (do
      (testing "string-source creates valid source"
        (is (some? (kafka/string-source "localhost:9092" "test-topic")))
        (is (some? (kafka/string-source "localhost:9092" "test-topic" "my-group"))))

      (testing "string-sink creates valid sink"
        (is (some? (kafka/string-sink "localhost:9092" "output-topic")))
        (is (some? (kafka/string-sink "localhost:9092" "output-topic" :exactly-once)))))
    (testing "Kafka connector not available"
      (is true))))

;; Note: Full integration tests with actual Kafka cluster would be marked as
;; :integration tests and require a running Kafka broker.
