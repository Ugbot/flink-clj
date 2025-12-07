(ns flink-clj.table-test
  "Tests for Table API functionality."
  (:require [clojure.test :refer :all]
            [flink-clj.table :as t]
            [flink-clj.version :as v]))

;; =============================================================================
;; API Existence Tests
;; =============================================================================

(deftest table-api-functions-exist-test
  (testing "Availability check functions exist"
    (is (fn? t/table-api-available?))
    (is (fn? t/table-api-info)))

  (testing "Environment functions exist"
    (is (fn? t/create-table-env))
    (is (fn? t/get-config)))

  (testing "SQL execution functions exist"
    (is (fn? t/execute-sql))
    (is (fn? t/sql-query)))

  (testing "Stream conversion functions exist"
    (is (fn? t/from-data-stream))
    (is (fn? t/to-data-stream))
    (is (fn? t/to-changelog-stream)))

  (testing "Table registration functions exist"
    (is (fn? t/create-temporary-view!))
    (is (fn? t/drop-temporary-view!))
    (is (fn? t/from-view)))

  (testing "Table operation functions exist"
    (is (fn? t/select))
    (is (fn? t/where))
    (is (fn? t/filter))
    (is (fn? t/group-by))
    (is (fn? t/agg-select))
    (is (fn? t/order-by))
    (is (fn? t/limit))
    (is (fn? t/offset))
    (is (fn? t/fetch))
    (is (fn? t/distinct)))

  (testing "Column manipulation functions exist"
    (is (fn? t/add-columns))
    (is (fn? t/add-or-replace-columns))
    (is (fn? t/drop-columns))
    (is (fn? t/rename-columns))
    (is (fn? t/alias-as)))

  (testing "Join functions exist"
    (is (fn? t/join))
    (is (fn? t/left-outer-join))
    (is (fn? t/right-outer-join))
    (is (fn? t/full-outer-join))
    (is (fn? t/join-lateral))
    (is (fn? t/left-outer-join-lateral)))

  (testing "Set operation functions exist"
    (is (fn? t/union))
    (is (fn? t/union-all))
    (is (fn? t/intersect))
    (is (fn? t/intersect-all))
    (is (fn? t/minus))
    (is (fn? t/minus-all)))

  (testing "Window functions exist"
    (is (fn? t/tumble-window))
    (is (fn? t/slide-window))
    (is (fn? t/session-window))
    (is (fn? t/cumulate-window))
    (is (fn? t/over)))

  (testing "Source creation functions exist"
    (is (fn? t/from-values)))

  (testing "Output functions exist"
    (is (fn? t/insert-into!))
    (is (fn? t/execute-insert))
    (is (fn? t/execute)))

  (testing "DDL helper functions exist"
    (is (fn? t/create-table-sql))
    (is (fn? t/create-table!))
    (is (fn? t/drop-table!)))

  (testing "Connector helper functions exist"
    (is (fn? t/kafka-connector-options))
    (is (fn? t/filesystem-connector-options))
    (is (fn? t/jdbc-connector-options))
    (is (fn? t/print-connector-options))
    (is (fn? t/blackhole-connector-options)))

  (testing "Schema helper functions exist"
    (is (fn? t/column))
    (is (fn? t/watermark-column))
    (is (fn? t/computed-column))
    (is (fn? t/proctime-column))
    (is (fn? t/rowtime-column)))

  (testing "Catalog functions exist"
    (is (fn? t/list-catalogs))
    (is (fn? t/list-databases))
    (is (fn? t/list-tables))
    (is (fn? t/list-views))
    (is (fn? t/use-catalog!))
    (is (fn? t/use-database!)))

  (testing "Statement set functions exist"
    (is (fn? t/create-statement-set))
    (is (fn? t/add-insert-sql))
    (is (fn? t/execute-statement-set)))

  (testing "UDF functions exist"
    (is (fn? t/register-function!))
    (is (fn? t/drop-function!))
    (is (fn? t/list-functions)))

  (testing "Module functions exist"
    (is (fn? t/list-modules))
    (is (fn? t/load-module!))
    (is (fn? t/unload-module!)))

  (testing "Explain functions exist"
    (is (fn? t/explain))
    (is (fn? t/explain-json))
    (is (fn? t/explain-estimated-cost))
    (is (fn? t/print-schema)))

  (testing "Schema inspection functions exist"
    (is (fn? t/get-schema))
    (is (fn? t/get-column-names))
    (is (fn? t/get-column-count)))

  (testing "Advanced query helper functions exist"
    (is (fn? t/top-n-sql))
    (is (fn? t/top-n))
    (is (fn? t/dedup-sql))
    (is (fn? t/deduplicate))
    (is (fn? t/temporal-join-sql))
    (is (fn? t/lookup-join-sql))
    (is (fn? t/interval-join-sql))
    (is (fn? t/match-recognize-sql))
    (is (fn? t/match-recognize))
    (is (fn? t/unnest-sql))
    (is (fn? t/unnest))
    (is (fn? t/with-sql))
    (is (fn? t/with-query)))

  (testing "Query hint functions exist"
    (is (fn? t/hint-sql))
    (is (fn? t/broadcast-hint))
    (is (fn? t/shuffle-hash-hint))
    (is (fn? t/state-ttl-hint))
    (is (fn? t/lookup-hint))))

;; =============================================================================
;; Availability Tests
;; =============================================================================

(deftest table-api-available-test
  (testing "table-api-available? returns boolean"
    (is (boolean? (t/table-api-available?)))))

(deftest table-api-info-test
  (testing "table-api-info returns correct structure"
    (let [info (t/table-api-info)]
      (is (map? info))
      (is (contains? info :available))
      (is (contains? info :version))
      (is (contains? info :features))
      (is (boolean? (:available info)))
      (is (string? (:version info)))
      (is (map? (:features info))))))

;; =============================================================================
;; DDL SQL Generation Tests
;; =============================================================================

(deftest create-table-sql-test
  (testing "Basic CREATE TABLE SQL generation"
    (let [sql (t/create-table-sql "orders"
                [{:name "id" :type "BIGINT"}
                 {:name "product" :type "STRING"}
                 {:name "amount" :type "DECIMAL(10,2)"}]
                {:connector "print"})]
      (is (string? sql))
      (is (.contains sql "CREATE TABLE `orders`"))
      (is (.contains sql "`id` BIGINT"))
      (is (.contains sql "`product` STRING"))
      (is (.contains sql "`amount` DECIMAL(10,2)"))
      (is (.contains sql "'connector' = 'print'"))))

  (testing "CREATE TABLE with primary key"
    (let [sql (t/create-table-sql "users"
                [{:name "id" :type "BIGINT" :primary-key? true}
                 {:name "name" :type "STRING" :not-null? true}]
                {:connector "print"})]
      (is (.contains sql "PRIMARY KEY (`id`) NOT ENFORCED"))
      (is (.contains sql "`name` STRING NOT NULL"))))

  (testing "CREATE TABLE with Kafka connector"
    (let [sql (t/create-table-sql "events"
                [{:name "event_id" :type "STRING"}
                 {:name "payload" :type "STRING"}]
                (t/kafka-connector-options {:servers "localhost:9092"
                                            :topic "events"
                                            :format :json}))]
      (is (.contains sql "'connector' = 'kafka'"))
      (is (.contains sql "'topic' = 'events'"))
      (is (.contains sql "'format' = 'json'")))))

;; =============================================================================
;; Column Helper Tests
;; =============================================================================

(deftest column-helper-test
  (testing "Basic column creation"
    (let [col (t/column "id" "BIGINT")]
      (is (= "id" (:name col)))
      (is (= "BIGINT" (:type col)))
      (is (nil? (:primary-key? col)))
      (is (nil? (:not-null? col)))))

  (testing "Column with options"
    (let [col (t/column "id" "BIGINT" {:primary-key? true :not-null? true})]
      (is (true? (:primary-key? col)))
      (is (true? (:not-null? col))))))

(deftest watermark-column-test
  (testing "Watermark column creation"
    (let [wm (t/watermark-column "event_time" "event_time - INTERVAL '5' SECOND")]
      (is (map? wm))
      (is (= "event_time" (get-in wm [:watermark :column])))
      (is (= "event_time - INTERVAL '5' SECOND" (get-in wm [:watermark :expr]))))))

(deftest computed-column-test
  (testing "Computed column creation"
    (let [col (t/computed-column "total" "quantity * price")]
      (is (= "total" (:name col)))
      (is (= "quantity * price" (:computed col))))))

;; =============================================================================
;; Connector Options Tests
;; =============================================================================

(deftest kafka-connector-options-test
  (testing "Kafka connector options generation"
    (let [opts (t/kafka-connector-options {:servers "localhost:9092"
                                           :topic "events"
                                           :group-id "my-group"
                                           :format :json
                                           :scan-startup :earliest})]
      (is (= "kafka" (:connector opts)))
      (is (= "localhost:9092" (:properties.bootstrap.servers opts)))
      (is (= "events" (:topic opts)))
      (is (= "my-group" (:properties.group.id opts)))
      (is (= "json" (:format opts)))
      (is (= "earliest-offset" (:scan.startup.mode opts))))))

(deftest filesystem-connector-options-test
  (testing "Filesystem connector options generation"
    (let [opts (t/filesystem-connector-options {:path "/data/events"
                                                :format :parquet})]
      (is (= "filesystem" (:connector opts)))
      (is (= "/data/events" (:path opts)))
      (is (= "parquet" (:format opts))))))

(deftest jdbc-connector-options-test
  (testing "JDBC connector options generation"
    (let [opts (t/jdbc-connector-options {:url "jdbc:postgresql://localhost/db"
                                          :table-name "orders"
                                          :username "user"
                                          :password "pass"})]
      (is (= "jdbc" (:connector opts)))
      (is (= "jdbc:postgresql://localhost/db" (:url opts)))
      (is (= "orders" (:table-name opts)))
      (is (= "user" (:username opts)))
      (is (= "pass" (:password opts))))))

(deftest print-connector-options-test
  (testing "Print connector options"
    (let [opts (t/print-connector-options {})]
      (is (= "print" (:connector opts))))
    (let [opts (t/print-connector-options {:print-identifier "DEBUG"})]
      (is (= "DEBUG" (:print-identifier opts))))))

(deftest blackhole-connector-options-test
  (testing "Blackhole connector options"
    (let [opts (t/blackhole-connector-options)]
      (is (= "blackhole" (:connector opts))))))

;; =============================================================================
;; Version Guard Tests
;; =============================================================================

(deftest table-api-version-guard-test
  (testing "Table API functions throw when not available"
    (when-not (t/table-api-available?)
      (testing "create-table-env throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires Flink Table API"
              (t/create-table-env nil))))

      (testing "execute-sql throws"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
              #"requires Flink Table API"
              (t/execute-sql nil "SELECT 1")))))))

;; =============================================================================
;; Window Helper Tests
;; =============================================================================

(deftest tumble-window-test
  (testing "Tumble window SQL generation"
    (let [window (t/tumble-window "event_time" "10 MINUTE")]
      (is (string? window))
      (is (.contains window "TUMBLE"))
      (is (.contains window "event_time"))
      (is (.contains window "10 MINUTE")))))

(deftest slide-window-test
  (testing "Slide window SQL generation"
    (let [window (t/slide-window "event_time" "1 HOUR" "10 MINUTE")]
      (is (string? window))
      (is (.contains window "HOP"))
      (is (.contains window "event_time")))))

(deftest session-window-test
  (testing "Session window SQL generation"
    (let [window (t/session-window "event_time" "30 MINUTE")]
      (is (string? window))
      (is (.contains window "SESSION"))
      (is (.contains window "event_time"))
      (is (.contains window "30 MINUTE")))))

(deftest cumulate-window-test
  (testing "Cumulate window SQL generation"
    (let [window (t/cumulate-window "event_time" "1 HOUR" "1 DAY")]
      (is (string? window))
      (is (.contains window "CUMULATE"))
      (is (.contains window "event_time")))))

;; =============================================================================
;; Time Column Helper Tests
;; =============================================================================

(deftest proctime-column-test
  (testing "Processing time column definition"
    (let [col (t/proctime-column "proc_time")]
      (is (map? col))
      (is (= "proc_time" (:name col)))
      (is (.contains (:type col) "PROCTIME")))))

(deftest rowtime-column-test
  (testing "Row time column definition"
    (let [col (t/rowtime-column "event_time" "ts" "ts - INTERVAL '5' SECOND")]
      (is (map? col))
      (is (= "event_time" (:name col)))
      (is (map? (:watermark col)))
      (is (= "event_time" (get-in col [:watermark :column]))))))

;; =============================================================================
;; Advanced Query SQL Generation Tests
;; =============================================================================

(deftest top-n-sql-test
  (testing "Basic Top-N SQL generation"
    (let [sql (t/top-n-sql "scores" 10 "score DESC")]
      (is (string? sql))
      (is (.contains sql "ROW_NUMBER()"))
      (is (.contains sql "ORDER BY score DESC"))
      (is (.contains sql "row_num <= 10"))))

  (testing "Top-N with partition"
    (let [sql (t/top-n-sql "sales" 3 "amount DESC" "*" {:partition-by "region"})]
      (is (.contains sql "PARTITION BY region")))))

(deftest dedup-sql-test
  (testing "Deduplication SQL generation"
    (let [sql (t/dedup-sql "events" "user_id" "event_time ASC")]
      (is (string? sql))
      (is (.contains sql "ROW_NUMBER()"))
      (is (.contains sql "PARTITION BY user_id"))
      (is (.contains sql "row_num = 1")))))

(deftest interval-join-sql-test
  (testing "Interval join SQL generation"
    (let [sql (t/interval-join-sql "orders o" "shipments s"
                                   "o.id = s.order_id"
                                   "s.ship_time BETWEEN o.order_time AND o.order_time + INTERVAL '4' HOUR")]
      (is (string? sql))
      (is (.contains sql "orders o"))
      (is (.contains sql "shipments s"))
      (is (.contains sql "BETWEEN")))))

(deftest unnest-sql-test
  (testing "UNNEST SQL generation"
    (let [sql (t/unnest-sql "orders o" "o.items" "item")]
      (is (string? sql))
      (is (.contains sql "CROSS JOIN UNNEST"))
      (is (.contains sql "o.items")))))

(deftest with-sql-test
  (testing "WITH clause SQL generation"
    (let [sql (t/with-sql
                [{:name "high_value" :query "SELECT * FROM orders WHERE amount > 1000"}]
                "SELECT * FROM high_value")]
      (is (string? sql))
      (is (.contains sql "WITH"))
      (is (.contains sql "high_value AS")))))

(deftest match-recognize-sql-test
  (testing "MATCH_RECOGNIZE SQL generation"
    (let [sql (t/match-recognize-sql "stock_prices"
                {:partition-by "symbol"
                 :order-by "ts"
                 :pattern "DOWN+ UP"
                 :define {:DOWN "DOWN.price < PREV(DOWN.price)"
                          :UP "UP.price > PREV(UP.price)"}
                 :measures {:start_price "FIRST(DOWN.price)"
                            :end_price "UP.price"}})]
      (is (string? sql))
      (is (.contains sql "MATCH_RECOGNIZE"))
      (is (.contains sql "PARTITION BY symbol"))
      (is (.contains sql "PATTERN (DOWN+ UP)"))
      (is (.contains sql "ONE ROW PER MATCH")))))

;; =============================================================================
;; Query Hint Tests
;; =============================================================================

(deftest hint-sql-test
  (testing "Query hint insertion"
    (let [sql (t/hint-sql "SELECT * FROM orders" ["BROADCAST(dim)"])]
      (is (.contains sql "/*+"))
      (is (.contains sql "BROADCAST(dim)")))))

(deftest broadcast-hint-test
  (testing "Broadcast hint generation"
    (is (= "BROADCAST(dim)" (t/broadcast-hint "dim")))))

(deftest shuffle-hash-hint-test
  (testing "Shuffle hash hint generation"
    (is (= "SHUFFLE_HASH(right)" (t/shuffle-hash-hint "right")))))

(deftest state-ttl-hint-test
  (testing "State TTL hint generation"
    (let [hint (t/state-ttl-hint {"orders" "1d" "customers" "12h"})]
      (is (.contains hint "STATE_TTL"))
      (is (.contains hint "'orders' = '1d'")))))

;; =============================================================================
;; Integration Tests (require Table API)
;; =============================================================================

(deftest ^:integration table-env-creation-test
  (testing "Table environment can be created"
    (when (t/table-api-available?)
      ;; Would require actual StreamExecutionEnvironment
      (is true "Integration test placeholder"))))

(deftest ^:integration simple-sql-execution-test
  (testing "Simple SQL can be executed"
    (when (t/table-api-available?)
      ;; Would require actual table environment
      (is true "Integration test placeholder"))))

(deftest ^:integration from-values-test
  (testing "Table from values can be created"
    (when (t/table-api-available?)
      ;; Would require actual table environment
      (is true "Integration test placeholder"))))

;; =============================================================================
;; TableDescriptor Tests
;; =============================================================================

(deftest table-descriptor-test
  (testing "Table descriptor creation"
    (let [desc (t/table-descriptor "kafka")]
      (is (= "kafka" (:connector desc)))
      (is (= [] (:schema desc)))
      (is (= {} (:options desc))))))

(deftest with-schema-test
  (testing "Adding schema to descriptor"
    (let [desc (-> (t/table-descriptor "kafka")
                   (t/with-schema [{:name "id" :type "BIGINT"}
                                   {:name "name" :type "STRING"}]))]
      (is (= 2 (count (:schema desc))))
      (is (= "id" (:name (first (:schema desc))))))))

(deftest with-format-test
  (testing "Setting format on descriptor"
    (let [desc (-> (t/table-descriptor "kafka")
                   (t/with-format "json"))]
      (is (= "json" (:format desc))))))

(deftest with-option-test
  (testing "Adding single option"
    (let [desc (-> (t/table-descriptor "kafka")
                   (t/with-option "topic" "users"))]
      (is (= "users" (get (:options desc) "topic"))))))

(deftest with-options-test
  (testing "Adding multiple options"
    (let [desc (-> (t/table-descriptor "kafka")
                   (t/with-options {"topic" "users"
                                    "properties.bootstrap.servers" "localhost:9092"}))]
      (is (= "users" (get (:options desc) "topic")))
      (is (= "localhost:9092" (get (:options desc) "properties.bootstrap.servers"))))))

(deftest with-partitioned-by-test
  (testing "Setting partition columns"
    (let [desc (-> (t/table-descriptor "filesystem")
                   (t/with-partitioned-by ["year" "month"]))]
      (is (= ["year" "month"] (:partitioned-by desc))))))

(deftest with-comment-test
  (testing "Setting comment"
    (let [desc (-> (t/table-descriptor "kafka")
                   (t/with-comment "User events from Kafka"))]
      (is (= "User events from Kafka" (:comment desc))))))

(deftest descriptor->sql-test
  (testing "Descriptor to SQL conversion"
    (let [desc (-> (t/table-descriptor "kafka")
                   (t/with-schema [{:name "id" :type "BIGINT" :primary-key? true}
                                   {:name "name" :type "STRING"}])
                   (t/with-format "json")
                   (t/with-option "topic" "users"))
          sql (t/descriptor->sql "my_table" desc)]
      (is (string? sql))
      (is (.contains sql "CREATE TABLE my_table"))
      (is (.contains sql "id BIGINT"))
      (is (.contains sql "name STRING"))
      (is (.contains sql "PRIMARY KEY (id)"))
      (is (.contains sql "'connector' = 'kafka'"))
      (is (.contains sql "'format' = 'json'")))))

;; =============================================================================
;; UDF SQL Tests
;; =============================================================================

(deftest create-function-sql-test
  (testing "Basic CREATE FUNCTION SQL"
    (let [sql (t/create-function-sql "MY_UPPER" "com.example.MyUpperFunction")]
      (is (= "CREATE FUNCTION MY_UPPER AS 'com.example.MyUpperFunction' LANGUAGE JAVA" sql))))

  (testing "Temporary function"
    (let [sql (t/create-function-sql "MY_UPPER" "com.example.MyUpperFunction"
                {:temporary? true})]
      (is (.contains sql "TEMPORARY FUNCTION"))))

  (testing "If not exists"
    (let [sql (t/create-function-sql "MY_UPPER" "com.example.MyUpperFunction"
                {:if-not-exists? true})]
      (is (.contains sql "IF NOT EXISTS")))))

(deftest create-system-function-sql-test
  (testing "System function SQL"
    (let [sql (t/create-system-function-sql "MY_CONCAT" "com.example.MyConcatFunction")]
      (is (.contains sql "TEMPORARY SYSTEM FUNCTION"))
      (is (.contains sql "MY_CONCAT")))))

(deftest drop-function-sql-test
  (testing "Basic DROP FUNCTION SQL"
    (is (= "DROP FUNCTION MY_UPPER" (t/drop-function-sql "MY_UPPER"))))

  (testing "Drop with IF EXISTS"
    (let [sql (t/drop-function-sql "MY_UPPER" {:if-exists? true})]
      (is (.contains sql "IF EXISTS")))))

(deftest alter-function-sql-test
  (testing "ALTER FUNCTION SQL"
    (let [sql (t/alter-function-sql "MY_UPPER" "com.example.NewUpperFunction")]
      (is (.contains sql "ALTER FUNCTION MY_UPPER"))
      (is (.contains sql "AS 'com.example.NewUpperFunction'")))))

;; =============================================================================
;; Catalog SQL Tests
;; =============================================================================

(deftest create-catalog-sql-test
  (testing "CREATE CATALOG SQL generation"
    (let [sql (t/create-catalog-sql "my_hive"
                {:type "hive"
                 :hive-conf-dir "/etc/hive/conf"})]
      (is (.contains sql "CREATE CATALOG my_hive"))
      (is (.contains sql "'type' = 'hive'"))
      (is (.contains sql "'hive-conf-dir' = '/etc/hive/conf'")))))

(deftest drop-catalog-sql-test
  (testing "DROP CATALOG SQL"
    (is (= "DROP CATALOG my_catalog" (t/drop-catalog-sql "my_catalog"))))

  (testing "DROP CATALOG with IF EXISTS"
    (let [sql (t/drop-catalog-sql "my_catalog" {:if-exists? true})]
      (is (.contains sql "IF EXISTS")))))

(deftest create-database-sql-test
  (testing "Basic CREATE DATABASE SQL"
    (is (= "CREATE DATABASE analytics" (t/create-database-sql "analytics"))))

  (testing "CREATE DATABASE with options"
    (let [sql (t/create-database-sql "analytics"
                {:if-not-exists? true
                 :comment "Analytics data warehouse"
                 :properties {"owner" "data-team"}})]
      (is (.contains sql "IF NOT EXISTS"))
      (is (.contains sql "COMMENT 'Analytics data warehouse'"))
      (is (.contains sql "'owner' = 'data-team'")))))

(deftest drop-database-sql-test
  (testing "DROP DATABASE with CASCADE"
    (let [sql (t/drop-database-sql "old_db" {:cascade? true})]
      (is (.contains sql "CASCADE"))))

  (testing "DROP DATABASE with RESTRICT (default)"
    (let [sql (t/drop-database-sql "old_db")]
      (is (.contains sql "RESTRICT")))))

;; =============================================================================
;; Catalog Options Helper Tests
;; =============================================================================

(deftest hive-catalog-options-test
  (testing "Hive catalog options"
    (let [opts (t/hive-catalog-options {:hive-conf-dir "/etc/hive/conf"})]
      (is (= "hive" (:type opts)))
      (is (= "/etc/hive/conf" (:hive-conf-dir opts)))
      (is (= "default" (:default-database opts))))))

(deftest jdbc-catalog-options-test
  (testing "JDBC catalog options"
    (let [opts (t/jdbc-catalog-options {:base-url "jdbc:postgresql://localhost:5432"
                                         :default-database "postgres"
                                         :username "user"
                                         :password "pass"})]
      (is (= "jdbc" (:type opts)))
      (is (= "jdbc:postgresql://localhost:5432" (:base-url opts))))))

(deftest iceberg-catalog-options-test
  (testing "Iceberg catalog options"
    (let [opts (t/iceberg-catalog-options {:catalog-type "hive"
                                            :uri "thrift://localhost:9083"
                                            :warehouse "s3://bucket/warehouse"})]
      (is (= "iceberg" (:type opts)))
      (is (= "hive" (:catalog-type opts)))
      (is (= "thrift://localhost:9083" (:uri opts))))))

(deftest paimon-catalog-options-test
  (testing "Paimon catalog options"
    (let [opts (t/paimon-catalog-options {:warehouse "s3://bucket/paimon"})]
      (is (= "paimon" (:type opts)))
      (is (= "s3://bucket/paimon" (:warehouse opts))))))

(deftest catalog-info-test
  (testing "Catalog info returns expected catalogs"
    (let [info (t/catalog-info)]
      (is (contains? info :hive))
      (is (contains? info :jdbc))
      (is (contains? info :iceberg))
      (is (contains? info :paimon)))))
