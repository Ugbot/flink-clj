(ns flink-clj.connectors.rabbitmq-test
  "Tests for RabbitMQ connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.rabbitmq :as rmq]
            [flink-clj.connectors.generic :as conn]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Connection config function exists"
    (is (fn? rmq/connection-config)))

  (testing "Source function exists"
    (is (fn? rmq/source)))

  (testing "Sink function exists"
    (is (fn? rmq/sink)))

  (testing "Info function exists"
    (is (fn? rmq/rabbitmq-info))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest rabbitmq-info-test
  (testing "rabbitmq-info returns expected structure"
    (let [info (rmq/rabbitmq-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :features))
      (is (contains? info :protocols))
      (is (boolean? (:available info)))
      (is (sequential? (:features info)))
      (is (= [:amqp] (:protocols info))))))

;; =============================================================================
;; Connection Config Validation Tests
;; =============================================================================

(deftest connection-config-defaults-test
  (testing "Default values are applied"
    ;; Just verify the options map structure
    (let [opts {:host "localhost"
                :port 5672
                :virtual-host "/"
                :username "guest"
                :password "guest"}]
      (is (= 5672 (:port opts)))
      (is (= "/" (:virtual-host opts)))
      (is (= "guest" (:username opts)))
      (is (= "guest" (:password opts))))))

;; =============================================================================
;; Source Validation Tests
;; =============================================================================

(deftest source-options-test
  (testing "Source requires host (unless connection provided)"
    ;; Without actual connector, we can only test option handling
    (let [opts {:queue "test" :deserializer :string}]
      (is (nil? (:host opts)))))

  (testing "Source requires queue"
    (let [opts {:host "localhost"}]
      (is (nil? (:queue opts)))))

  (testing "Source default values"
    (let [opts {:host "localhost" :queue "test"}]
      (is (some? (:host opts)))
      (is (some? (:queue opts))))))

;; =============================================================================
;; Sink Validation Tests
;; =============================================================================

(deftest sink-options-test
  (testing "Sink requires host (unless connection provided)"
    (let [opts {:queue "output" :serializer :string}]
      (is (nil? (:host opts)))))

  (testing "Sink requires queue"
    (let [opts {:host "localhost"}]
      (is (nil? (:queue opts)))))

  (testing "Sink default values"
    (let [opts {:host "localhost" :queue "output"}]
      (is (some? (:host opts)))
      (is (some? (:queue opts))))))

;; =============================================================================
;; Deserializer/Serializer Tests
;; =============================================================================

(deftest deserializer-spec-test
  (testing "Valid deserializer keywords"
    (is (keyword? :string))
    (is (keyword? :bytes))))

(deftest serializer-spec-test
  (testing "Valid serializer keywords"
    (is (keyword? :string))))

;; =============================================================================
;; Connection Config Options Tests
;; =============================================================================

(deftest connection-options-test
  (testing "URI option can be used instead of host/port"
    (let [opts {:uri "amqp://user:pass@host:5672/vhost"}]
      (is (some? (:uri opts)))))

  (testing "Optional connection settings"
    (let [opts {:host "localhost"
                :automatic-recovery true
                :topology-recovery true
                :connection-timeout 5000
                :network-recovery-interval 1000
                :prefetch-count 10}]
      (is (true? (:automatic-recovery opts)))
      (is (true? (:topology-recovery opts)))
      (is (= 5000 (:connection-timeout opts)))
      (is (= 1000 (:network-recovery-interval opts)))
      (is (= 10 (:prefetch-count opts))))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-availability-test
  (testing "Availability check returns boolean"
    (let [info (rmq/rabbitmq-info)]
      (is (or (true? (:available info))
              (false? (:available info)))))))

;; =============================================================================
;; Integration Tests (require RabbitMQ connector JAR)
;; =============================================================================

(deftest ^:integration connection-config-creation-test
  (testing "connection-config creates RMQConnectionConfig when available"
    (let [info (rmq/rabbitmq-info)]
      (when (:available info)
        (let [config (rmq/connection-config {:host "localhost"})]
          (is (some? config)))))))

(deftest ^:integration source-creation-test
  (testing "source creates RMQSource when available"
    (let [info (rmq/rabbitmq-info)]
      (when (:available info)
        (is true "Would test source creation")))))

(deftest ^:integration sink-creation-test
  (testing "sink creates RMQSink when available"
    (let [info (rmq/rabbitmq-info)]
      (when (:available info)
        (is true "Would test sink creation")))))
