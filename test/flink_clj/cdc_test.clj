(ns flink-clj.cdc-test
  "Tests for CDC connectors."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.cdc :as cdc]))

;; =============================================================================
;; MySQL CDC Tests
;; =============================================================================

(deftest mysql-cdc-options-test
  (testing "MySQL CDC options with required fields"
    (let [opts (cdc/mysql-cdc-options {:hostname "localhost"
                                        :username "root"
                                        :password "secret"
                                        :database-name "mydb"
                                        :table-name "users"})]
      (is (= "mysql-cdc" (:connector opts)))
      (is (= "localhost" (:hostname opts)))
      (is (= "3306" (:port opts)))
      (is (= "root" (:username opts)))
      (is (= "mydb" (:database-name opts)))
      (is (= "users" (:table-name opts)))))

  (testing "MySQL CDC options with optional fields"
    (let [opts (cdc/mysql-cdc-options {:hostname "localhost"
                                        :port 3307
                                        :username "root"
                                        :password "secret"
                                        :database-name "mydb"
                                        :table-name "users"
                                        :server-id 12345
                                        :server-time-zone "UTC"
                                        :scan-startup-mode :initial})]
      (is (= "3307" (:port opts)))
      (is (= "12345" (:server-id opts)))
      (is (= "UTC" (:server-time-zone opts)))
      (is (= "initial" (get opts :scan.startup.mode))))))

(deftest mysql-cdc-source-test
  (testing "MySQL CDC source SQL generation"
    (let [sql (cdc/mysql-cdc-source "orders_cdc"
                {:hostname "localhost"
                 :username "root"
                 :password "secret"
                 :database "shop"
                 :table "orders"
                 :columns [{:name "id" :type "BIGINT" :primary-key? true}
                           {:name "product" :type "STRING"}
                           {:name "amount" :type "DECIMAL(10,2)"}]})]
      (is (string? sql))
      (is (.contains sql "CREATE TABLE orders_cdc"))
      (is (.contains sql "id BIGINT"))
      (is (.contains sql "product STRING"))
      (is (.contains sql "PRIMARY KEY (id)"))
      (is (.contains sql "'connector' = 'mysql-cdc'"))
      (is (.contains sql "'hostname' = 'localhost'"))
      (is (.contains sql "'database-name' = 'shop'")))))

;; =============================================================================
;; PostgreSQL CDC Tests
;; =============================================================================

(deftest postgres-cdc-options-test
  (testing "PostgreSQL CDC options"
    (let [opts (cdc/postgres-cdc-options {:hostname "localhost"
                                           :username "postgres"
                                           :password "secret"
                                           :database-name "mydb"
                                           :table-name "users"})]
      (is (= "postgres-cdc" (:connector opts)))
      (is (= "5432" (:port opts)))
      (is (= "public" (:schema-name opts))))))

(deftest postgres-cdc-source-test
  (testing "PostgreSQL CDC source SQL generation"
    (let [sql (cdc/postgres-cdc-source "users_cdc"
                {:hostname "localhost"
                 :username "postgres"
                 :password "secret"
                 :database "mydb"
                 :table "users"
                 :columns [{:name "id" :type "BIGINT" :primary-key? true}
                           {:name "name" :type "STRING"}]})]
      (is (.contains sql "CREATE TABLE users_cdc"))
      (is (.contains sql "'connector' = 'postgres-cdc'"))
      (is (.contains sql "'schema-name' = 'public'")))))

;; =============================================================================
;; MongoDB CDC Tests
;; =============================================================================

(deftest mongodb-cdc-options-test
  (testing "MongoDB CDC options"
    (let [opts (cdc/mongodb-cdc-options {:hosts "localhost:27017"
                                          :database "mydb"
                                          :collection "users"})]
      (is (= "mongodb-cdc" (:connector opts)))
      (is (= "localhost:27017" (:hosts opts)))
      (is (= "mydb" (:database opts)))
      (is (= "users" (:collection opts))))))

(deftest mongodb-cdc-source-test
  (testing "MongoDB CDC source SQL generation"
    (let [sql (cdc/mongodb-cdc-source "users_cdc"
                {:hosts "localhost:27017"
                 :database "mydb"
                 :collection "users"
                 :columns [{:name "_id" :type "STRING" :primary-key? true}
                           {:name "name" :type "STRING"}]})]
      (is (.contains sql "'connector' = 'mongodb-cdc'"))
      (is (.contains sql "'hosts' = 'localhost:27017'")))))

;; =============================================================================
;; Kafka CDC Format Tests
;; =============================================================================

(deftest kafka-debezium-source-test
  (testing "Kafka Debezium source SQL generation"
    (let [sql (cdc/kafka-debezium-source "orders"
                {:bootstrap-servers "localhost:9092"
                 :topic "dbserver1.inventory.orders"
                 :group-id "flink-consumer"
                 :columns [{:name "id" :type "BIGINT" :primary-key? true}
                           {:name "product" :type "STRING"}]})]
      (is (.contains sql "'connector' = 'kafka'"))
      (is (.contains sql "'format' = 'debezium-json'"))
      (is (.contains sql "'topic' = 'dbserver1.inventory.orders'")))))

(deftest kafka-canal-source-test
  (testing "Kafka Canal source SQL generation"
    (let [sql (cdc/kafka-canal-source "orders"
                {:bootstrap-servers "localhost:9092"
                 :topic "canal-orders"
                 :group-id "flink-consumer"
                 :columns [{:name "id" :type "BIGINT" :primary-key? true}]})]
      (is (.contains sql "'format' = 'canal-json'")))))

(deftest kafka-maxwell-source-test
  (testing "Kafka Maxwell source SQL generation"
    (let [sql (cdc/kafka-maxwell-source "orders"
                {:bootstrap-servers "localhost:9092"
                 :topic "maxwell-orders"
                 :group-id "flink-consumer"
                 :columns [{:name "id" :type "BIGINT" :primary-key? true}]})]
      (is (.contains sql "'format' = 'maxwell-json'")))))

;; =============================================================================
;; CDC Metadata Tests
;; =============================================================================

(deftest with-cdc-metadata-test
  (testing "Adding CDC metadata columns"
    (let [columns [{:name "id" :type "BIGINT" :primary-key? true}
                   {:name "name" :type "STRING"}]
          with-meta (cdc/with-cdc-metadata columns [:op-ts :table])]
      (is (= 4 (count with-meta)))
      (is (some #(= "op_ts" (:name %)) with-meta))
      (is (some #(= "source_table" (:name %)) with-meta)))))

;; =============================================================================
;; Generic CDC Source Tests
;; =============================================================================

(deftest cdc-source-test
  (testing "Generic CDC source SQL generation"
    (let [sql (cdc/cdc-source "my_table"
                {:connector "mysql-cdc"
                 :columns [{:name "id" :type "BIGINT" :primary-key? true}]
                 :options {:hostname "localhost"
                           :port "3306"
                           :username "root"
                           :password "secret"
                           :database-name "mydb"
                           :table-name "mytable"}})]
      (is (.contains sql "CREATE TABLE my_table"))
      (is (.contains sql "'connector' = 'mysql-cdc'"))
      (is (.contains sql "'hostname' = 'localhost'")))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest cdc-info-test
  (testing "CDC info returns expected connectors"
    (let [info (cdc/cdc-info)]
      (is (map? info))
      (is (contains? info :mysql-cdc))
      (is (contains? info :postgres-cdc))
      (is (contains? info :mongodb-cdc))
      (is (contains? info :oracle-cdc))
      (is (contains? info :sqlserver-cdc)))))

(deftest startup-modes-test
  (testing "Startup modes info"
    (let [modes (cdc/startup-modes)]
      (is (contains? modes :initial))
      (is (contains? modes :earliest-offset))
      (is (contains? modes :latest-offset)))))
