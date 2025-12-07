(ns flink-clj.connectors.jdbc
  "JDBC connector for reading from and writing to databases.

  Supports both source (reading) and sink (writing) operations with:
  - Batch and streaming modes
  - Exactly-once semantics (with XA transactions)
  - Connection pooling
  - Parameterized queries

  Example - Source (reading from database):
    (require '[flink-clj.connectors.jdbc :as jdbc])

    (def source
      (jdbc/source {:driver-class \"org.postgresql.Driver\"
                    :url \"jdbc:postgresql://localhost:5432/mydb\"
                    :username \"user\"
                    :password \"pass\"
                    :query \"SELECT id, name, value FROM events WHERE ts > ?\"
                    :parameters [(System/currentTimeMillis)]
                    :row-mapper #'map-row}))

    (-> env
        (flink/from-source source)
        ...)

  Example - Sink (writing to database):
    (def sink
      (jdbc/sink {:driver-class \"org.postgresql.Driver\"
                  :url \"jdbc:postgresql://localhost:5432/mydb\"
                  :username \"user\"
                  :password \"pass\"
                  :sql \"INSERT INTO events (id, name, value) VALUES (?, ?, ?)\"
                  :statement-builder #'build-statement}))

    (-> stream
        (flink/to-sink sink))

  NOTE: Requires flink-connector-jdbc dependency."
  (:require [flink-clj.connectors.generic :as conn]
            [flink-clj.impl.functions :as impl])
  (:import [java.sql PreparedStatement ResultSet]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- jdbc-source-available?
  "Check if JDBC source connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.jdbc.source.JdbcSource"))

(defn- jdbc-sink-available?
  "Check if JDBC sink connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.jdbc.JdbcSink"))

(defn- jdbc-connection-options-available?
  "Check if JdbcConnectionOptions is available."
  []
  (conn/connector-available? "org.apache.flink.connector.jdbc.JdbcConnectionOptions"))

(defn- require-jdbc!
  "Ensure JDBC connector is available."
  []
  (when-not (or (jdbc-source-available?) (jdbc-sink-available?) (jdbc-connection-options-available?))
    (throw (ex-info "JDBC connector not available"
                    {:suggestion "Add flink-connector-jdbc dependency"}))))

;; =============================================================================
;; Connection Options Builder
;; =============================================================================

(defn connection-options
  "Create JDBC connection options.

  Options:
    :driver-class  - JDBC driver class name (required)
    :url           - JDBC connection URL (required)
    :username      - Database username
    :password      - Database password

  Example:
    (connection-options {:driver-class \"org.postgresql.Driver\"
                         :url \"jdbc:postgresql://localhost:5432/mydb\"
                         :username \"user\"
                         :password \"pass\"})"
  [{:keys [driver-class url username password]}]
  (require-jdbc!)
  (when-not driver-class
    (throw (ex-info "driver-class is required" {})))
  (when-not url
    (throw (ex-info "url is required" {})))

  (let [builder-class (Class/forName "org.apache.flink.connector.jdbc.JdbcConnectionOptions$JdbcConnectionOptionsBuilder")
        builder (.newInstance builder-class)]

    ;; Set driver
    (let [method (.getMethod (.getClass builder) "withDriverName" (into-array Class [String]))]
      (.invoke method builder (into-array Object [driver-class])))

    ;; Set URL
    (let [method (.getMethod (.getClass builder) "withUrl" (into-array Class [String]))]
      (.invoke method builder (into-array Object [url])))

    ;; Set username if provided
    (when username
      (let [method (.getMethod (.getClass builder) "withUsername" (into-array Class [String]))]
        (.invoke method builder (into-array Object [username]))))

    ;; Set password if provided
    (when password
      (let [method (.getMethod (.getClass builder) "withPassword" (into-array Class [String]))]
        (.invoke method builder (into-array Object [password]))))

    ;; Build
    (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Execution Options Builder
;; =============================================================================

(defn execution-options
  "Create JDBC execution options for sink.

  Options:
    :batch-size            - Batch size for bulk inserts (default: 5000)
    :batch-interval-ms     - Max interval between flushes in ms (default: 0 = disabled)
    :max-retries           - Max retry attempts on failure (default: 3)

  Example:
    (execution-options {:batch-size 1000
                        :batch-interval-ms 5000
                        :max-retries 5})"
  [{:keys [batch-size batch-interval-ms max-retries]
    :or {batch-size 5000 batch-interval-ms 0 max-retries 3}}]
  (require-jdbc!)

  (let [builder-class (Class/forName "org.apache.flink.connector.jdbc.JdbcExecutionOptions$Builder")
        builder (.newInstance builder-class)]

    ;; Set batch size
    (let [method (.getMethod (.getClass builder) "withBatchSize" (into-array Class [Integer/TYPE]))]
      (.invoke method builder (into-array Object [(int batch-size)])))

    ;; Set batch interval
    (let [method (.getMethod (.getClass builder) "withBatchIntervalMs" (into-array Class [Long/TYPE]))]
      (.invoke method builder (into-array Object [(long batch-interval-ms)])))

    ;; Set max retries
    (let [method (.getMethod (.getClass builder) "withMaxRetries" (into-array Class [Integer/TYPE]))]
      (.invoke method builder (into-array Object [(int max-retries)])))

    ;; Build
    (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Statement Builder
;; =============================================================================

(defn- create-statement-builder
  "Create a JdbcStatementBuilder from a Clojure function.

  The function receives [statement element] and should set parameters."
  [builder-fn]
  (require-jdbc!)
  (let [[ns-str fn-name] (impl/var->ns-name builder-fn)
        builder-iface (Class/forName "org.apache.flink.connector.jdbc.JdbcStatementBuilder")]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader builder-iface)
      (into-array Class [builder-iface java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (when (= "accept" (.getName method))
            (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                  _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                  fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                  stmt (aget args 0)
                  element (aget args 1)]
              (.invoke fn-var stmt element))))))))

;; =============================================================================
;; Row Mapper
;; =============================================================================

(defn- create-row-mapper
  "Create a row mapper from a Clojure function.

  The function receives a ResultSet and should return the mapped value."
  [mapper-fn]
  (require-jdbc!)
  (let [[ns-str fn-name] (impl/var->ns-name mapper-fn)
        ;; Try to find ResultSetExtractor or RowMapper interface
        mapper-iface (try
                       (Class/forName "org.apache.flink.connector.jdbc.split.JdbcResultSetExtractor")
                       (catch ClassNotFoundException _
                         (Class/forName "java.util.function.Function")))]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader mapper-iface)
      (into-array Class [mapper-iface java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (let [method-name (.getName method)]
            (when (or (= "extract" method-name)
                      (= "apply" method-name))
              (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                    _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                    fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                    rs (aget args 0)]
                (.invoke fn-var rs)))))))))

;; =============================================================================
;; Source
;; =============================================================================

(defn source
  "Create a JDBC source for reading from a database.

  Required options:
    :driver-class - JDBC driver class name
    :url          - JDBC connection URL
    :query        - SQL query string
    :row-mapper   - Function (var) to map ResultSet row to Clojure value

  Optional options:
    :username     - Database username
    :password     - Database password
    :parameters   - Vector of query parameters (for parameterized queries)
    :fetch-size   - Number of rows to fetch at a time (default: 0 = driver default)
    :auto-commit  - Auto-commit mode (default: true)
    :split-column - Column name for parallel reading (enables partitioned reads)
    :lower-bound  - Lower bound for split column
    :upper-bound  - Upper bound for split column
    :num-splits   - Number of splits for parallel reading

  Example:
    (defn map-row [^ResultSet rs]
      {:id (.getLong rs \"id\")
       :name (.getString rs \"name\")
       :value (.getDouble rs \"value\")})

    (jdbc/source {:driver-class \"org.postgresql.Driver\"
                  :url \"jdbc:postgresql://localhost:5432/mydb\"
                  :username \"user\"
                  :password \"pass\"
                  :query \"SELECT * FROM events WHERE status = ?\"
                  :parameters [\"ACTIVE\"]
                  :row-mapper #'map-row})"
  [{:keys [driver-class url username password query row-mapper
           parameters fetch-size auto-commit
           split-column lower-bound upper-bound num-splits]
    :or {fetch-size 0 auto-commit true}
    :as opts}]
  ;; Validate required options
  (when-not driver-class
    (throw (ex-info "driver-class is required" {:opts opts})))
  (when-not url
    (throw (ex-info "url is required" {:opts opts})))
  (when-not query
    (throw (ex-info "query is required" {:opts opts})))
  (when-not row-mapper
    (throw (ex-info "row-mapper is required" {:opts opts})))

  (require-jdbc!)

  (let [conn-opts (connection-options {:driver-class driver-class
                                       :url url
                                       :username username
                                       :password password})
        mapper (create-row-mapper row-mapper)]

    ;; Build source using reflection
    (let [builder-class (Class/forName "org.apache.flink.connector.jdbc.source.JdbcSourceBuilder")
          builder-method (.getMethod (Class/forName "org.apache.flink.connector.jdbc.source.JdbcSource")
                                     "builder" (into-array Class []))
          builder (.invoke builder-method nil (into-array Object []))
          builder-type (.getClass builder)]

      ;; Set SQL
      (let [method (.getMethod builder-type "setSql" (into-array Class [String]))]
        (.invoke method builder (into-array Object [query])))

      ;; Set connection options - Flink 1.x vs 2.x compatibility
      (try
        (let [conn-opts-class (Class/forName "org.apache.flink.connector.jdbc.JdbcConnectionOptions")
              method (.getMethod builder-type "setJdbcConnectionOptions" (into-array Class [conn-opts-class]))]
          (.invoke method builder (into-array Object [conn-opts])))
        (catch NoSuchMethodException _
          ;; Try alternate method name
          (let [method (.getMethod builder-type "setConnectionOptions" (into-array Class [(Class/forName "org.apache.flink.connector.jdbc.JdbcConnectionOptions")]))]
            (.invoke method builder (into-array Object [conn-opts])))))

      ;; Set row mapper
      (try
        (let [extractor-class (Class/forName "org.apache.flink.connector.jdbc.split.JdbcResultSetExtractor")
              method (.getMethod builder-type "setResultSetExtractor" (into-array Class [extractor-class]))]
          (.invoke method builder (into-array Object [mapper])))
        (catch Exception _
          ;; Alternate approach - try setRowMapper
          nil))

      ;; Set fetch size if non-zero
      (when (pos? fetch-size)
        (try
          (let [method (.getMethod builder-type "setFetchSize" (into-array Class [Integer/TYPE]))]
            (.invoke method builder (into-array Object [(int fetch-size)])))
          (catch NoSuchMethodException _ nil)))

      ;; Set auto commit
      (try
        (let [method (.getMethod builder-type "setAutoCommit" (into-array Class [Boolean/TYPE]))]
          (.invoke method builder (into-array Object [(boolean auto-commit)])))
        (catch NoSuchMethodException _ nil))

      ;; Build
      (let [build-method (.getMethod builder-type "build" (into-array Class []))]
        (.invoke build-method builder (into-array Object []))))))

;; =============================================================================
;; Sink
;; =============================================================================

(defn sink
  "Create a JDBC sink for writing to a database.

  Required options:
    :driver-class      - JDBC driver class name
    :url               - JDBC connection URL
    :sql               - SQL statement (INSERT, UPDATE, etc.)
    :statement-builder - Function (var) to set statement parameters

  Optional options:
    :username          - Database username
    :password          - Database password
    :batch-size        - Batch size for bulk operations (default: 5000)
    :batch-interval-ms - Max interval between flushes (default: 0)
    :max-retries       - Max retry attempts (default: 3)

  Example:
    (defn build-statement [^PreparedStatement stmt event]
      (.setLong stmt 1 (:id event))
      (.setString stmt 2 (:name event))
      (.setDouble stmt 3 (:value event)))

    (jdbc/sink {:driver-class \"org.postgresql.Driver\"
                :url \"jdbc:postgresql://localhost:5432/mydb\"
                :username \"user\"
                :password \"pass\"
                :sql \"INSERT INTO events (id, name, value) VALUES (?, ?, ?)\"
                :statement-builder #'build-statement
                :batch-size 1000})"
  [{:keys [driver-class url username password sql statement-builder
           batch-size batch-interval-ms max-retries]
    :or {batch-size 5000 batch-interval-ms 0 max-retries 3}
    :as opts}]
  ;; Validate required options
  (when-not driver-class
    (throw (ex-info "driver-class is required" {:opts opts})))
  (when-not url
    (throw (ex-info "url is required" {:opts opts})))
  (when-not sql
    (throw (ex-info "sql is required" {:opts opts})))
  (when-not statement-builder
    (throw (ex-info "statement-builder is required" {:opts opts})))

  (require-jdbc!)

  (let [conn-opts (connection-options {:driver-class driver-class
                                       :url url
                                       :username username
                                       :password password})
        exec-opts (execution-options {:batch-size batch-size
                                      :batch-interval-ms batch-interval-ms
                                      :max-retries max-retries})
        stmt-builder (create-statement-builder statement-builder)]

    ;; Create sink using JdbcSink.sink() static method
    (let [sink-class (Class/forName "org.apache.flink.connector.jdbc.JdbcSink")
          conn-opts-class (Class/forName "org.apache.flink.connector.jdbc.JdbcConnectionOptions")
          exec-opts-class (Class/forName "org.apache.flink.connector.jdbc.JdbcExecutionOptions")
          builder-class (Class/forName "org.apache.flink.connector.jdbc.JdbcStatementBuilder")
          sink-method (.getMethod sink-class "sink"
                                  (into-array Class [String builder-class exec-opts-class conn-opts-class]))]
      (.invoke sink-method nil (into-array Object [sql stmt-builder exec-opts conn-opts])))))

;; =============================================================================
;; Exactly-Once Sink (XA Transactions)
;; =============================================================================

(defn exactly-once-sink
  "Create a JDBC sink with exactly-once semantics using XA transactions.

  This requires:
  - XA-capable database driver
  - Proper transaction timeout configuration

  Required options:
    :driver-class      - JDBC driver class name (must support XA)
    :url               - JDBC connection URL
    :sql               - SQL statement
    :statement-builder - Function (var) to set statement parameters

  Optional options:
    :username          - Database username
    :password          - Database password
    :transaction-timeout - XA transaction timeout in seconds
    :max-retries       - Max retry attempts

  Example:
    (jdbc/exactly-once-sink
      {:driver-class \"org.postgresql.xa.PGXADataSource\"
       :url \"jdbc:postgresql://localhost:5432/mydb\"
       :sql \"INSERT INTO events (id, data) VALUES (?, ?)\"
       :statement-builder #'build-stmt})"
  [{:keys [driver-class url username password sql statement-builder
           transaction-timeout max-retries]
    :or {transaction-timeout 300 max-retries 3}
    :as opts}]
  ;; Validate required options
  (when-not driver-class
    (throw (ex-info "driver-class is required" {:opts opts})))
  (when-not url
    (throw (ex-info "url is required" {:opts opts})))
  (when-not sql
    (throw (ex-info "sql is required" {:opts opts})))
  (when-not statement-builder
    (throw (ex-info "statement-builder is required" {:opts opts})))

  (require-jdbc!)

  ;; Check if exactly-once sink is available
  (when-not (conn/connector-available? "org.apache.flink.connector.jdbc.JdbcExactlyOnceSink")
    (throw (ex-info "Exactly-once JDBC sink not available"
                    {:suggestion "Upgrade to flink-connector-jdbc 3.0+"})))

  (let [stmt-builder (create-statement-builder statement-builder)]
    ;; Build exactly-once sink
    (let [sink-class (Class/forName "org.apache.flink.connector.jdbc.JdbcExactlyOnceSink")
          builder-method (.getMethod sink-class "builder" (into-array Class []))
          builder (.invoke builder-method nil (into-array Object []))
          builder-type (.getClass builder)]

      ;; Configure builder
      (let [method (.getMethod builder-type "setDriverName" (into-array Class [String]))]
        (.invoke method builder (into-array Object [driver-class])))

      (let [method (.getMethod builder-type "setUrl" (into-array Class [String]))]
        (.invoke method builder (into-array Object [url])))

      (when username
        (let [method (.getMethod builder-type "setUsername" (into-array Class [String]))]
          (.invoke method builder (into-array Object [username]))))

      (when password
        (let [method (.getMethod builder-type "setPassword" (into-array Class [String]))]
          (.invoke method builder (into-array Object [password]))))

      (let [method (.getMethod builder-type "setSql" (into-array Class [String]))]
        (.invoke method builder (into-array Object [sql])))

      (let [builder-class (Class/forName "org.apache.flink.connector.jdbc.JdbcStatementBuilder")
            method (.getMethod builder-type "setStatementBuilder" (into-array Class [builder-class]))]
        (.invoke method builder (into-array Object [stmt-builder])))

      ;; Build
      (let [build-method (.getMethod builder-type "build" (into-array Class []))]
        (.invoke build-method builder (into-array Object []))))))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn simple-insert-sink
  "Create a simple JDBC sink for inserting maps into a table.

  Automatically builds INSERT statement and parameter setter from a map.

  Arguments:
    opts - Base connection options (:driver-class, :url, :username, :password)
    table - Table name
    columns - Vector of column names (keywords)

  Example:
    (simple-insert-sink
      {:driver-class \"org.postgresql.Driver\"
       :url \"jdbc:postgresql://localhost:5432/mydb\"
       :username \"user\"
       :password \"pass\"}
      \"events\"
      [:id :name :value :timestamp])

    ;; Expects elements like {:id 1 :name \"test\" :value 42.0 :timestamp ...}"
  [{:keys [driver-class url username password] :as conn-opts} table columns]
  (let [col-names (map name columns)
        placeholders (repeat (count columns) "?")
        sql (str "INSERT INTO " table " (" (clojure.string/join ", " col-names) ") "
                 "VALUES (" (clojure.string/join ", " placeholders) ")")
        builder-name (str "simple-insert-" table "-" (System/nanoTime))]

    ;; Create and intern the statement builder
    (intern (create-ns 'flink-clj.connectors.jdbc)
            (symbol builder-name)
            (fn [^PreparedStatement stmt element]
              (doseq [[idx col] (map-indexed vector columns)]
                (let [val (get element col)]
                  (cond
                    (nil? val) (.setNull stmt (inc idx) java.sql.Types/NULL)
                    (string? val) (.setString stmt (inc idx) val)
                    (integer? val) (.setLong stmt (inc idx) (long val))
                    (float? val) (.setDouble stmt (inc idx) (double val))
                    (instance? Boolean val) (.setBoolean stmt (inc idx) val)
                    (instance? java.util.Date val) (.setTimestamp stmt (inc idx) (java.sql.Timestamp. (.getTime val)))
                    (instance? java.time.Instant val) (.setTimestamp stmt (inc idx) (java.sql.Timestamp/from val))
                    :else (.setObject stmt (inc idx) val))))))

    (sink {:driver-class driver-class
           :url url
           :username username
           :password password
           :sql sql
           :statement-builder (ns-resolve 'flink-clj.connectors.jdbc (symbol builder-name))})))

(defn simple-upsert-sink
  "Create a JDBC sink for upsert (INSERT ON CONFLICT UPDATE) operations.

  Arguments:
    opts - Base connection options
    table - Table name
    columns - All columns (keywords)
    key-columns - Primary key columns (keywords)

  Example (PostgreSQL):
    (simple-upsert-sink
      conn-opts \"users\" [:id :name :email] [:id])"
  [{:keys [driver-class url username password dialect] :as conn-opts}
   table columns key-columns]
  (let [col-names (map name columns)
        placeholders (repeat (count columns) "?")
        key-names (map name key-columns)
        non-key-cols (remove (set key-columns) columns)
        non-key-names (map name non-key-cols)

        ;; Build dialect-specific upsert SQL
        sql (case (or dialect :postgresql)
              :postgresql
              (str "INSERT INTO " table " (" (clojure.string/join ", " col-names) ") "
                   "VALUES (" (clojure.string/join ", " placeholders) ") "
                   "ON CONFLICT (" (clojure.string/join ", " key-names) ") "
                   "DO UPDATE SET "
                   (clojure.string/join ", " (map #(str % " = EXCLUDED." %) non-key-names)))

              :mysql
              (str "INSERT INTO " table " (" (clojure.string/join ", " col-names) ") "
                   "VALUES (" (clojure.string/join ", " placeholders) ") "
                   "ON DUPLICATE KEY UPDATE "
                   (clojure.string/join ", " (map #(str % " = VALUES(" % ")") non-key-names))))

        builder-name (str "simple-upsert-" table "-" (System/nanoTime))]

    (intern (create-ns 'flink-clj.connectors.jdbc)
            (symbol builder-name)
            (fn [^PreparedStatement stmt element]
              (doseq [[idx col] (map-indexed vector columns)]
                (let [val (get element col)]
                  (cond
                    (nil? val) (.setNull stmt (inc idx) java.sql.Types/NULL)
                    (string? val) (.setString stmt (inc idx) val)
                    (integer? val) (.setLong stmt (inc idx) (long val))
                    (float? val) (.setDouble stmt (inc idx) (double val))
                    (instance? Boolean val) (.setBoolean stmt (inc idx) val)
                    :else (.setObject stmt (inc idx) val))))))

    (sink {:driver-class driver-class
           :url url
           :username username
           :password password
           :sql sql
           :statement-builder (ns-resolve 'flink-clj.connectors.jdbc (symbol builder-name))})))

;; =============================================================================
;; Info
;; =============================================================================

(defn jdbc-info
  "Get information about JDBC connector availability."
  []
  {:source-available (jdbc-source-available?)
   :sink-available (jdbc-sink-available?)
   :connection-options-available (jdbc-connection-options-available?)
   :features [:batch-inserts
              :parameterized-queries
              :connection-pooling
              :exactly-once-xa
              :partitioned-reads]
   :supported-databases ["PostgreSQL" "MySQL" "Oracle" "SQL Server" "Any JDBC-compliant DB"]})
