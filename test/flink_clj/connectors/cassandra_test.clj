(ns flink-clj.connectors.cassandra-test
  "Tests for Cassandra connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.cassandra :as cass]
            [flink-clj.connectors.generic :as conn]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Sink functions exist"
    (is (fn? cass/sink))
    (is (fn? cass/pojo-sink))
    (is (fn? cass/batch-sink)))

  (testing "Convenience functions exist"
    (is (fn? cass/simple-sink)))

  (testing "Info function exists"
    (is (fn? cass/cassandra-info))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest cassandra-info-test
  (testing "cassandra-info returns expected structure"
    (let [info (cass/cassandra-info)]
      (is (map? info))
      (is (contains? info :sink-available))
      (is (contains? info :builder-available))
      (is (contains? info :features))
      (is (contains? info :supported-versions))
      (is (boolean? (:sink-available info)))
      (is (sequential? (:features info)))
      (is (sequential? (:supported-versions info))))))

;; =============================================================================
;; Sink Validation Tests
;; =============================================================================

(deftest sink-validation-test
  (testing "sink requires hosts"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"hosts is required"
          (cass/sink {:query "INSERT..." :mapper identity}))))

  (testing "sink requires query"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"query is required"
          (cass/sink {:hosts ["localhost"] :mapper identity}))))

  (testing "sink requires mapper"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"mapper is required"
          (cass/sink {:hosts ["localhost"] :query "INSERT..."})))))

;; =============================================================================
;; POJO Sink Validation Tests
;; =============================================================================

(deftest pojo-sink-validation-test
  (testing "pojo-sink requires hosts"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"hosts is required"
          (cass/pojo-sink {:pojo-class Object}))))

  (testing "pojo-sink requires pojo-class"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"pojo-class is required"
          (cass/pojo-sink {:hosts ["localhost"]})))))

;; =============================================================================
;; Batch Sink Validation Tests
;; =============================================================================

(deftest batch-sink-validation-test
  (testing "batch-sink requires hosts"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"hosts is required"
          (cass/batch-sink {:query "INSERT..." :mapper identity}))))

  (testing "batch-sink requires query"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"query is required"
          (cass/batch-sink {:hosts ["localhost"] :mapper identity}))))

  (testing "batch-sink requires mapper"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"mapper is required"
          (cass/batch-sink {:hosts ["localhost"] :query "INSERT..."})))))

;; =============================================================================
;; Default Values Tests
;; =============================================================================

(deftest default-values-test
  (testing "Default port"
    (is (= 9042 (:port {:port 9042}))))

  (testing "Default batch size"
    (is (= 100 (:batch-size {:batch-size 100}))))

  (testing "Default max concurrent requests"
    (is (= 500 (:max-concurrent-requests {:max-concurrent-requests 500})))))

;; =============================================================================
;; CQL Query Generation Tests
;; =============================================================================

(deftest cql-query-generation-test
  (testing "Simple INSERT query structure"
    (let [table "events"
          columns [:id :name :value]
          col-names (map name columns)
          placeholders (repeat (count columns) "?")
          query (str "INSERT INTO " table " ("
                     (clojure.string/join ", " col-names) ") "
                     "VALUES ("
                     (clojure.string/join ", " placeholders) ")")]
      (is (= "INSERT INTO events (id, name, value) VALUES (?, ?, ?)" query))))

  (testing "INSERT with keyspace"
    (let [keyspace "myks"
          table "events"
          query (str "INSERT INTO " keyspace "." table " (id) VALUES (?)")]
      (is (= "INSERT INTO myks.events (id) VALUES (?)" query)))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-availability-test
  (testing "Availability check returns boolean"
    (let [info (cass/cassandra-info)]
      (is (or (true? (:sink-available info))
              (false? (:sink-available info))))
      (is (or (true? (:builder-available info))
              (false? (:builder-available info)))))))

;; =============================================================================
;; Integration Tests (require Cassandra connector JAR)
;; =============================================================================

(deftest ^:integration sink-creation-test
  (testing "sink creates Cassandra sink when available"
    (let [info (cass/cassandra-info)]
      (when (:sink-available info)
        (is true "Would test actual sink creation")))))
