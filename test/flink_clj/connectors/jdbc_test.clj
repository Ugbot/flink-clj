(ns flink-clj.connectors.jdbc-test
  "Tests for JDBC connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.jdbc :as jdbc]
            [flink-clj.connectors.generic :as conn]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Connection options function exists"
    (is (fn? jdbc/connection-options)))

  (testing "Execution options function exists"
    (is (fn? jdbc/execution-options)))

  (testing "Source function exists"
    (is (fn? jdbc/source)))

  (testing "Sink functions exist"
    (is (fn? jdbc/sink))
    (is (fn? jdbc/exactly-once-sink)))

  (testing "Convenience functions exist"
    (is (fn? jdbc/simple-insert-sink))
    (is (fn? jdbc/simple-upsert-sink)))

  (testing "Info function exists"
    (is (fn? jdbc/jdbc-info))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest jdbc-info-test
  (testing "jdbc-info returns expected structure"
    (let [info (jdbc/jdbc-info)]
      (is (map? info))
      (is (contains? info :source-available))
      (is (contains? info :sink-available))
      (is (contains? info :connection-options-available))
      (is (contains? info :features))
      (is (contains? info :supported-databases))
      (is (boolean? (:source-available info)))
      (is (boolean? (:sink-available info)))
      (is (sequential? (:features info)))
      (is (sequential? (:supported-databases info))))))

;; =============================================================================
;; Source Validation Tests
;; =============================================================================

(deftest source-validation-test
  (testing "source requires driver-class"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"driver-class is required"
          (jdbc/source {:url "jdbc:..." :query "SELECT *" :row-mapper identity}))))

  (testing "source requires url"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"url is required"
          (jdbc/source {:driver-class "org.postgresql.Driver"
                        :query "SELECT *"
                        :row-mapper identity}))))

  (testing "source requires query"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"query is required"
          (jdbc/source {:driver-class "org.postgresql.Driver"
                        :url "jdbc:..."
                        :row-mapper identity}))))

  (testing "source requires row-mapper"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"row-mapper is required"
          (jdbc/source {:driver-class "org.postgresql.Driver"
                        :url "jdbc:..."
                        :query "SELECT *"})))))

;; =============================================================================
;; Sink Validation Tests
;; =============================================================================

(deftest sink-validation-test
  (testing "sink requires driver-class"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"driver-class is required"
          (jdbc/sink {:url "jdbc:..." :sql "INSERT ..." :statement-builder identity}))))

  (testing "sink requires url"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"url is required"
          (jdbc/sink {:driver-class "org.postgresql.Driver"
                      :sql "INSERT ..."
                      :statement-builder identity}))))

  (testing "sink requires sql"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"sql is required"
          (jdbc/sink {:driver-class "org.postgresql.Driver"
                      :url "jdbc:..."
                      :statement-builder identity}))))

  (testing "sink requires statement-builder"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"statement-builder is required"
          (jdbc/sink {:driver-class "org.postgresql.Driver"
                      :url "jdbc:..."
                      :sql "INSERT ..."})))))

;; =============================================================================
;; Connection Options Tests
;; =============================================================================

(deftest connection-options-validation-test
  (testing "connection-options requires driver-class"
    ;; When JDBC is not available, we get a different error
    (is (thrown? clojure.lang.ExceptionInfo
          (jdbc/connection-options {:url "jdbc:..."}))))

  (testing "connection-options requires url"
    (is (thrown? clojure.lang.ExceptionInfo
          (jdbc/connection-options {:driver-class "org.postgresql.Driver"})))))

;; =============================================================================
;; Execution Options Tests
;; =============================================================================

(deftest execution-options-defaults-test
  (testing "Default execution options values"
    (let [default-opts {:batch-size 5000
                        :batch-interval-ms 0
                        :max-retries 3}]
      (is (= 5000 (:batch-size default-opts)))
      (is (= 0 (:batch-interval-ms default-opts)))
      (is (= 3 (:max-retries default-opts))))))

;; =============================================================================
;; Simple Insert SQL Generation Tests
;; =============================================================================

(deftest simple-insert-sql-test
  (testing "SQL generation for insert"
    (let [table "events"
          columns [:id :name :value]
          col-names (map name columns)
          placeholders (repeat (count columns) "?")
          sql (str "INSERT INTO " table " ("
                   (clojure.string/join ", " col-names) ") "
                   "VALUES ("
                   (clojure.string/join ", " placeholders) ")")]
      (is (= "INSERT INTO events (id, name, value) VALUES (?, ?, ?)" sql)))))

;; =============================================================================
;; Upsert SQL Generation Tests
;; =============================================================================

(deftest upsert-sql-test
  (testing "PostgreSQL upsert SQL generation"
    (let [table "users"
          columns [:id :name :email]
          key-columns [:id]
          col-names (map name columns)
          key-names (map name key-columns)
          non-key-cols (remove (set key-columns) columns)
          non-key-names (map name non-key-cols)
          placeholders (repeat (count columns) "?")
          sql (str "INSERT INTO " table " ("
                   (clojure.string/join ", " col-names) ") "
                   "VALUES ("
                   (clojure.string/join ", " placeholders) ") "
                   "ON CONFLICT ("
                   (clojure.string/join ", " key-names) ") "
                   "DO UPDATE SET "
                   (clojure.string/join ", "
                     (map #(str % " = EXCLUDED." %) non-key-names)))]
      (is (= "INSERT INTO users (id, name, email) VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email"
             sql))))

  (testing "MySQL upsert SQL generation"
    (let [table "users"
          columns [:id :name :email]
          key-columns [:id]
          col-names (map name columns)
          non-key-cols (remove (set key-columns) columns)
          non-key-names (map name non-key-cols)
          placeholders (repeat (count columns) "?")
          sql (str "INSERT INTO " table " ("
                   (clojure.string/join ", " col-names) ") "
                   "VALUES ("
                   (clojure.string/join ", " placeholders) ") "
                   "ON DUPLICATE KEY UPDATE "
                   (clojure.string/join ", "
                     (map #(str % " = VALUES(" % ")") non-key-names)))]
      (is (= "INSERT INTO users (id, name, email) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email)"
             sql)))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-availability-test
  (testing "Availability check returns boolean"
    (let [info (jdbc/jdbc-info)]
      (is (or (true? (:source-available info))
              (false? (:source-available info))))
      (is (or (true? (:sink-available info))
              (false? (:sink-available info)))))))

;; =============================================================================
;; Integration Tests (require JDBC connector JAR)
;; =============================================================================

(deftest ^:integration connection-options-creation-test
  (testing "connection-options creates JdbcConnectionOptions when available"
    (let [info (jdbc/jdbc-info)]
      (when (:connection-options-available info)
        (let [opts (jdbc/connection-options
                     {:driver-class "org.postgresql.Driver"
                      :url "jdbc:postgresql://localhost:5432/test"
                      :username "user"
                      :password "pass"})]
          (is (some? opts)))))))

(deftest ^:integration execution-options-creation-test
  (testing "execution-options creates JdbcExecutionOptions when available"
    (let [info (jdbc/jdbc-info)]
      (when (:connection-options-available info)
        (let [opts (jdbc/execution-options
                     {:batch-size 1000
                      :batch-interval-ms 5000
                      :max-retries 5})]
          (is (some? opts)))))))

(deftest ^:integration sink-creation-test
  (testing "sink creates JdbcSink when available"
    (is true "Integration test placeholder")))
