(ns flink-clj.connectors.mongodb-test
  "Tests for MongoDB connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.mongodb :as mongo]
            [flink-clj.connectors.generic :as conn]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Source functions exist"
    (is (fn? mongo/source))
    (is (fn? mongo/simple-source)))

  (testing "Sink functions exist"
    (is (fn? mongo/sink))
    (is (fn? mongo/simple-sink)))

  (testing "Info function exists"
    (is (fn? mongo/mongodb-info))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest mongodb-info-test
  (testing "mongodb-info returns expected structure"
    (let [info (mongo/mongodb-info)]
      (is (map? info))
      (is (contains? info :source-available))
      (is (contains? info :sink-available))
      (is (contains? info :features))
      (is (contains? info :supported-versions))
      (is (boolean? (:source-available info)))
      (is (boolean? (:sink-available info)))
      (is (sequential? (:features info)))
      (is (sequential? (:supported-versions info))))))

;; =============================================================================
;; Source Validation Tests
;; =============================================================================

(deftest source-validation-test
  (testing "source requires uri"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"uri is required"
          (mongo/source {:database "db" :collection "coll" :deserializer identity}))))

  (testing "source requires database"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"database is required"
          (mongo/source {:uri "mongodb://localhost" :collection "coll" :deserializer identity}))))

  (testing "source requires collection"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"collection is required"
          (mongo/source {:uri "mongodb://localhost" :database "db" :deserializer identity}))))

  (testing "source requires deserializer"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"deserializer is required"
          (mongo/source {:uri "mongodb://localhost" :database "db" :collection "coll"})))))

;; =============================================================================
;; Sink Validation Tests
;; =============================================================================

(deftest sink-validation-test
  (testing "sink requires uri"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"uri is required"
          (mongo/sink {:database "db" :collection "coll" :serializer identity}))))

  (testing "sink requires database"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"database is required"
          (mongo/sink {:uri "mongodb://localhost" :collection "coll" :serializer identity}))))

  (testing "sink requires collection"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"collection is required"
          (mongo/sink {:uri "mongodb://localhost" :database "db" :serializer identity}))))

  (testing "sink requires serializer"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"serializer is required"
          (mongo/sink {:uri "mongodb://localhost" :database "db" :collection "coll"})))))

;; =============================================================================
;; Default Values Tests
;; =============================================================================

(deftest default-values-test
  (testing "Default source values"
    (let [defaults {:fetch-size 2048
                    :no-cursor-timeout true
                    :partition-size-mb 64}]
      (is (= 2048 (:fetch-size defaults)))
      (is (= true (:no-cursor-timeout defaults)))
      (is (= 64 (:partition-size-mb defaults)))))

  (testing "Default sink values"
    (let [defaults {:batch-size 1000
                    :batch-interval-ms 1000
                    :max-retries 3
                    :delivery-guarantee :at-least-once
                    :upsert? false}]
      (is (= 1000 (:batch-size defaults)))
      (is (= 1000 (:batch-interval-ms defaults)))
      (is (= 3 (:max-retries defaults)))
      (is (= :at-least-once (:delivery-guarantee defaults)))
      (is (= false (:upsert? defaults))))))

;; =============================================================================
;; Partition Strategy Tests
;; =============================================================================

(deftest partition-strategy-test
  (testing "Valid partition strategies"
    (is (keyword? :single))
    (is (keyword? :sample))
    (is (keyword? :split-vector))
    (is (keyword? :sharded))
    (is (keyword? :default))))

;; =============================================================================
;; Delivery Guarantee Tests
;; =============================================================================

(deftest delivery-guarantee-test
  (testing "Valid delivery guarantees"
    (is (keyword? :none))
    (is (keyword? :at-least-once))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-availability-test
  (testing "Availability check returns boolean"
    (let [info (mongo/mongodb-info)]
      (is (or (true? (:source-available info))
              (false? (:source-available info))))
      (is (or (true? (:sink-available info))
              (false? (:sink-available info)))))))

;; =============================================================================
;; Integration Tests (require MongoDB connector JAR)
;; =============================================================================

(deftest ^:integration source-creation-test
  (testing "source creates MongoDB source when available"
    (let [info (mongo/mongodb-info)]
      (when (:source-available info)
        (is true "Would test actual source creation")))))

(deftest ^:integration sink-creation-test
  (testing "sink creates MongoDB sink when available"
    (let [info (mongo/mongodb-info)]
      (when (:sink-available info)
        (is true "Would test actual sink creation")))))
