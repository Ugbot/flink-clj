(ns flink-clj.connectors.cdc
  "Change Data Capture (CDC) connectors for streaming database changes.

  CDC connectors capture row-level changes from databases and stream them
  as insert/update/delete events. This namespace provides helpers for
  configuring CDC sources via SQL DDL.

  Supported CDC Connectors (via JARs):
  - MySQL CDC (Debezium-based)
  - PostgreSQL CDC (Debezium-based)
  - MongoDB CDC (Debezium-based)
  - Oracle CDC (Debezium-based)
  - SQL Server CDC (Debezium-based)
  - TiDB CDC
  - Db2 CDC
  - Vitess CDC

  Format Support:
  - debezium-json - Debezium JSON format
  - canal-json - Alibaba Canal JSON format
  - maxwell-json - Zendesk Maxwell JSON format
  - debezium-avro-confluent - Debezium Avro with Confluent Schema Registry

  Required JARs (add to classpath):
  - flink-connector-mysql-cdc-X.X.X.jar (for MySQL)
  - flink-connector-postgres-cdc-X.X.X.jar (for PostgreSQL)
  - flink-connector-mongodb-cdc-X.X.X.jar (for MongoDB)
  - flink-sql-connector-kafka-X.X.X.jar (for Kafka CDC sources)

  Example:
    (require '[flink-clj.connectors.cdc :as cdc])
    (require '[flink-clj.table :as t])

    ;; Create a MySQL CDC source
    (t/execute-sql table-env
      (cdc/mysql-cdc-source \"orders\"
        {:hostname \"localhost\"
         :port 3306
         :username \"root\"
         :password \"secret\"
         :database \"shop\"
         :table \"orders\"
         :columns [{:name \"id\" :type \"BIGINT\" :primary-key? true}
                   {:name \"product\" :type \"STRING\"}
                   {:name \"amount\" :type \"DECIMAL(10,2)\"}
                   {:name \"created_at\" :type \"TIMESTAMP(3)\"}]}))"
  (:require [clojure.string :as str]))

;; =============================================================================
;; SQL Type Helpers
;; =============================================================================

(defn- format-column
  "Format a column definition for SQL DDL."
  [{:keys [name type primary-key? not-null? metadata-from]}]
  (str name " " type
       (when not-null? " NOT NULL")
       (when metadata-from (str " METADATA FROM '" metadata-from "'"))))

(defn- format-columns
  "Format column definitions for SQL DDL."
  [columns]
  (str/join ",\n  " (map format-column columns)))

(defn- format-primary-key
  "Format PRIMARY KEY constraint."
  [columns]
  (let [pk-cols (filter :primary-key? columns)]
    (when (seq pk-cols)
      (str ",\n  PRIMARY KEY (" (str/join ", " (map :name pk-cols)) ") NOT ENFORCED"))))

(defn- format-options
  "Format WITH options for SQL DDL."
  [options]
  (str/join ",\n  "
    (for [[k v] options]
      (str "'" (name k) "' = '" v "'"))))

;; =============================================================================
;; MySQL CDC
;; =============================================================================

(defn mysql-cdc-options
  "Generate MySQL CDC connector options.

  Required:
    :hostname - MySQL server hostname
    :port - MySQL server port (default: 3306)
    :username - Database username
    :password - Database password
    :database-name - Database name (supports regex)
    :table-name - Table name (supports regex)

  Optional:
    :server-id - Unique server ID for binlog reading
    :server-time-zone - Server timezone
    :scan-startup-mode - initial, earliest-offset, latest-offset, specific-offset, timestamp
    :scan-startup-timestamp-millis - Timestamp for startup mode
    :debezium.* - Additional Debezium properties
    :jdbc.* - Additional JDBC properties"
  [{:keys [hostname port username password database-name table-name
           server-id server-time-zone scan-startup-mode
           scan-startup-timestamp-millis]
    :or {port 3306}
    :as opts}]
  (cond-> {:connector "mysql-cdc"
           :hostname hostname
           :port (str port)
           :username username
           :password password
           :database-name database-name
           :table-name table-name}
    server-id (assoc :server-id (str server-id))
    server-time-zone (assoc :server-time-zone server-time-zone)
    scan-startup-mode (assoc :scan.startup.mode (name scan-startup-mode))
    scan-startup-timestamp-millis (assoc :scan.startup.timestamp-millis
                                         (str scan-startup-timestamp-millis))
    ;; Pass through any debezium.* or jdbc.* properties
    true (merge (select-keys opts (filter #(or (str/starts-with? (name %) "debezium.")
                                               (str/starts-with? (name %) "jdbc."))
                                          (keys opts))))))

(defn mysql-cdc-source
  "Generate CREATE TABLE SQL for a MySQL CDC source.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :hostname - MySQL server hostname
      :port - MySQL server port (default: 3306)
      :username - Database username
      :password - Database password
      :database - Database name (or :database-name)
      :table - Source table name (or :table-name)
      :columns - Vector of column specs [{:name \"id\" :type \"BIGINT\" :primary-key? true}]

  Example:
    (mysql-cdc-source \"orders_cdc\"
      {:hostname \"localhost\"
       :port 3306
       :username \"cdc_user\"
       :password \"secret\"
       :database \"shop\"
       :table \"orders\"
       :columns [{:name \"id\" :type \"BIGINT\" :primary-key? true}
                 {:name \"product\" :type \"STRING\"}
                 {:name \"amount\" :type \"DECIMAL(10,2)\"}]})"
  [table-name {:keys [hostname port username password database table database-name table-name-opt columns]
               :or {port 3306}
               :as config}]
  (let [db-name (or database database-name)
        tbl-name (or table table-name-opt)
        options (mysql-cdc-options (assoc config
                                          :database-name db-name
                                          :table-name tbl-name))]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

;; =============================================================================
;; PostgreSQL CDC
;; =============================================================================

(defn postgres-cdc-options
  "Generate PostgreSQL CDC connector options.

  Required:
    :hostname - PostgreSQL server hostname
    :port - PostgreSQL server port (default: 5432)
    :username - Database username
    :password - Database password
    :database-name - Database name
    :schema-name - Schema name (default: public)
    :table-name - Table name (supports regex)

  Optional:
    :slot-name - Replication slot name
    :decoding-plugin-name - wal2json, decoderbufs, pgoutput
    :scan-startup-mode - initial, latest-offset"
  [{:keys [hostname port username password database-name schema-name table-name
           slot-name decoding-plugin-name scan-startup-mode]
    :or {port 5432 schema-name "public"}
    :as opts}]
  (cond-> {:connector "postgres-cdc"
           :hostname hostname
           :port (str port)
           :username username
           :password password
           :database-name database-name
           :schema-name schema-name
           :table-name table-name}
    slot-name (assoc :slot.name slot-name)
    decoding-plugin-name (assoc :decoding.plugin.name decoding-plugin-name)
    scan-startup-mode (assoc :scan.startup.mode (name scan-startup-mode))
    true (merge (select-keys opts (filter #(str/starts-with? (name %) "debezium.")
                                          (keys opts))))))

(defn postgres-cdc-source
  "Generate CREATE TABLE SQL for a PostgreSQL CDC source.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :hostname - PostgreSQL server hostname
      :port - PostgreSQL server port (default: 5432)
      :username - Database username
      :password - Database password
      :database - Database name
      :schema - Schema name (default: public)
      :table - Source table name
      :columns - Vector of column specs"
  [table-name {:keys [hostname port username password database schema table columns]
               :or {port 5432 schema "public"}
               :as config}]
  (let [options (postgres-cdc-options {:hostname hostname
                                       :port port
                                       :username username
                                       :password password
                                       :database-name database
                                       :schema-name schema
                                       :table-name table})]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

;; =============================================================================
;; MongoDB CDC
;; =============================================================================

(defn mongodb-cdc-options
  "Generate MongoDB CDC connector options.

  Required:
    :hosts - MongoDB hosts (comma-separated host:port)
    :database - Database name
    :collection - Collection name

  Optional:
    :username - Authentication username
    :password - Authentication password
    :connection-options - Additional connection options
    :scan-startup-mode - initial, latest-offset, timestamp
    :scan-startup-timestamp-millis - Timestamp for startup mode"
  [{:keys [hosts database collection username password
           connection-options scan-startup-mode scan-startup-timestamp-millis]
    :as opts}]
  (cond-> {:connector "mongodb-cdc"
           :hosts hosts
           :database database
           :collection collection}
    username (assoc :username username)
    password (assoc :password password)
    connection-options (assoc :connection.options connection-options)
    scan-startup-mode (assoc :scan.startup.mode (name scan-startup-mode))
    scan-startup-timestamp-millis (assoc :scan.startup.timestamp-millis
                                         (str scan-startup-timestamp-millis))))

(defn mongodb-cdc-source
  "Generate CREATE TABLE SQL for a MongoDB CDC source.

  MongoDB documents are mapped to Flink rows. The _id field is typically
  the primary key.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :hosts - MongoDB hosts (e.g., \"localhost:27017\")
      :database - Database name
      :collection - Collection name
      :columns - Vector of column specs"
  [table-name {:keys [hosts database collection columns] :as config}]
  (let [options (mongodb-cdc-options config)]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

;; =============================================================================
;; Oracle CDC
;; =============================================================================

(defn oracle-cdc-options
  "Generate Oracle CDC connector options.

  Required:
    :hostname - Oracle server hostname
    :port - Oracle server port (default: 1521)
    :username - Database username
    :password - Database password
    :database-name - Database name (SID or service name)
    :schema-name - Schema name
    :table-name - Table name

  Optional:
    :scan-startup-mode - initial, latest-offset"
  [{:keys [hostname port username password database-name schema-name table-name
           scan-startup-mode]
    :or {port 1521}
    :as opts}]
  (cond-> {:connector "oracle-cdc"
           :hostname hostname
           :port (str port)
           :username username
           :password password
           :database-name database-name
           :schema-name schema-name
           :table-name table-name}
    scan-startup-mode (assoc :scan.startup.mode (name scan-startup-mode))
    true (merge (select-keys opts (filter #(str/starts-with? (name %) "debezium.")
                                          (keys opts))))))

(defn oracle-cdc-source
  "Generate CREATE TABLE SQL for an Oracle CDC source."
  [table-name {:keys [hostname port username password database schema table columns]
               :or {port 1521}
               :as config}]
  (let [options (oracle-cdc-options {:hostname hostname
                                     :port port
                                     :username username
                                     :password password
                                     :database-name database
                                     :schema-name schema
                                     :table-name table})]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

;; =============================================================================
;; SQL Server CDC
;; =============================================================================

(defn sqlserver-cdc-options
  "Generate SQL Server CDC connector options.

  Required:
    :hostname - SQL Server hostname
    :port - SQL Server port (default: 1433)
    :username - Database username
    :password - Database password
    :database-name - Database name
    :schema-name - Schema name (default: dbo)
    :table-name - Table name"
  [{:keys [hostname port username password database-name schema-name table-name
           scan-startup-mode]
    :or {port 1433 schema-name "dbo"}
    :as opts}]
  (cond-> {:connector "sqlserver-cdc"
           :hostname hostname
           :port (str port)
           :username username
           :password password
           :database-name database-name
           :schema-name schema-name
           :table-name table-name}
    scan-startup-mode (assoc :scan.startup.mode (name scan-startup-mode))
    true (merge (select-keys opts (filter #(str/starts-with? (name %) "debezium.")
                                          (keys opts))))))

(defn sqlserver-cdc-source
  "Generate CREATE TABLE SQL for a SQL Server CDC source."
  [table-name {:keys [hostname port username password database schema table columns]
               :or {port 1433 schema "dbo"}
               :as config}]
  (let [options (sqlserver-cdc-options {:hostname hostname
                                        :port port
                                        :username username
                                        :password password
                                        :database-name database
                                        :schema-name schema
                                        :table-name table})]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

;; =============================================================================
;; Kafka CDC Formats (for reading CDC data from Kafka)
;; =============================================================================

(defn kafka-debezium-source
  "Generate CREATE TABLE SQL for reading Debezium format from Kafka.

  Use this when you have Debezium Connect writing to Kafka and want
  to consume the CDC events in Flink.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :bootstrap-servers - Kafka bootstrap servers
      :topic - Kafka topic name
      :group-id - Consumer group ID
      :columns - Column definitions
      :schema-registry - (optional) Confluent Schema Registry URL for Avro"
  [table-name {:keys [bootstrap-servers topic group-id columns schema-registry]
               :as config}]
  (let [format-type (if schema-registry "debezium-avro-confluent" "debezium-json")
        options (cond-> {:connector "kafka"
                         :topic topic
                         :properties.bootstrap.servers bootstrap-servers
                         :properties.group.id group-id
                         :scan.startup.mode "earliest-offset"
                         :format format-type}
                  schema-registry (assoc :debezium-avro-confluent.url schema-registry))]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

(defn kafka-canal-source
  "Generate CREATE TABLE SQL for reading Canal JSON format from Kafka.

  Canal is Alibaba's MySQL binlog incremental subscription & consumption
  middleware.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :bootstrap-servers - Kafka bootstrap servers
      :topic - Kafka topic name
      :group-id - Consumer group ID
      :columns - Column definitions"
  [table-name {:keys [bootstrap-servers topic group-id columns] :as config}]
  (let [options {:connector "kafka"
                 :topic topic
                 :properties.bootstrap.servers bootstrap-servers
                 :properties.group.id group-id
                 :scan.startup.mode "earliest-offset"
                 :format "canal-json"}]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

(defn kafka-maxwell-source
  "Generate CREATE TABLE SQL for reading Maxwell JSON format from Kafka.

  Maxwell is Zendesk's daemon that reads MySQL binlogs and writes
  row updates as JSON to Kafka.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :bootstrap-servers - Kafka bootstrap servers
      :topic - Kafka topic name
      :group-id - Consumer group ID
      :columns - Column definitions"
  [table-name {:keys [bootstrap-servers topic group-id columns] :as config}]
  (let [options {:connector "kafka"
                 :topic topic
                 :properties.bootstrap.servers bootstrap-servers
                 :properties.group.id group-id
                 :scan.startup.mode "earliest-offset"
                 :format "maxwell-json"}]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options options)
         "\n)")))

;; =============================================================================
;; CDC Metadata Columns
;; =============================================================================

(defn with-cdc-metadata
  "Add common CDC metadata columns to a column list.

  CDC sources provide metadata about each change event that can be
  exposed as virtual columns.

  Available metadata:
    :op-ts - Operation timestamp
    :database - Source database name
    :schema - Source schema name
    :table - Source table name
    :op-type - Operation type (c=create, u=update, d=delete, r=read)

  Example:
    (with-cdc-metadata
      [{:name \"id\" :type \"BIGINT\" :primary-key? true}
       {:name \"name\" :type \"STRING\"}]
      [:op-ts :table])"
  [columns metadata-cols]
  (let [metadata-defs {:op-ts {:name "op_ts" :type "TIMESTAMP_LTZ(3)" :metadata-from "op_ts"}
                       :database {:name "source_database" :type "STRING" :metadata-from "database_name"}
                       :schema {:name "source_schema" :type "STRING" :metadata-from "schema_name"}
                       :table {:name "source_table" :type "STRING" :metadata-from "table_name"}
                       :op-type {:name "op_type" :type "STRING" :metadata-from "op_type"}}]
    (concat columns (map metadata-defs metadata-cols))))

;; =============================================================================
;; Generic CDC Table Builder
;; =============================================================================

(defn cdc-source
  "Generate CREATE TABLE SQL for any CDC source.

  This is a generic builder that works with any CDC connector.
  Use this when you need more control or for unsupported databases.

  Arguments:
    table-name - Flink table name
    config - Configuration map with:
      :connector - CDC connector type (\"mysql-cdc\", \"postgres-cdc\", etc.)
      :columns - Column definitions
      :options - Map of connector options (keys are option names, values are strings)

  Example:
    (cdc-source \"my_table\"
      {:connector \"mysql-cdc\"
       :columns [{:name \"id\" :type \"BIGINT\" :primary-key? true}]
       :options {:hostname \"localhost\"
                 :port \"3306\"
                 :username \"root\"
                 :password \"secret\"
                 :database-name \"mydb\"
                 :table-name \"mytable\"}})"
  [table-name {:keys [connector columns options]}]
  (let [all-options (assoc options :connector connector)]
    (str "CREATE TABLE " table-name " (\n  "
         (format-columns columns)
         (format-primary-key columns)
         "\n) WITH (\n  "
         (format-options all-options)
         "\n)")))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn cdc-info
  "Get information about available CDC connectors.

  Note: Availability depends on JARs in the classpath."
  []
  {:mysql-cdc {:connector "mysql-cdc"
               :jar "flink-connector-mysql-cdc"
               :description "Debezium-based MySQL CDC connector"}
   :postgres-cdc {:connector "postgres-cdc"
                  :jar "flink-connector-postgres-cdc"
                  :description "Debezium-based PostgreSQL CDC connector"}
   :mongodb-cdc {:connector "mongodb-cdc"
                 :jar "flink-connector-mongodb-cdc"
                 :description "Debezium-based MongoDB CDC connector"}
   :oracle-cdc {:connector "oracle-cdc"
                :jar "flink-connector-oracle-cdc"
                :description "Debezium-based Oracle CDC connector"}
   :sqlserver-cdc {:connector "sqlserver-cdc"
                   :jar "flink-connector-sqlserver-cdc"
                   :description "Debezium-based SQL Server CDC connector"}
   :tidb-cdc {:connector "tidb-cdc"
              :jar "flink-connector-tidb-cdc"
              :description "TiDB CDC connector"}
   :db2-cdc {:connector "db2-cdc"
             :jar "flink-connector-db2-cdc"
             :description "Debezium-based Db2 CDC connector"}
   :vitess-cdc {:connector "vitess-cdc"
                :jar "flink-connector-vitess-cdc"
                :description "Vitess CDC connector"}})

(defn startup-modes
  "Get information about CDC startup modes."
  []
  {:initial {:description "Snapshot existing data, then read binlog/WAL"}
   :earliest-offset {:description "Start from earliest available binlog/WAL position"}
   :latest-offset {:description "Start from latest binlog/WAL position (no snapshot)"}
   :specific-offset {:description "Start from a specific binlog/WAL position"}
   :timestamp {:description "Start from a specific timestamp"}})
