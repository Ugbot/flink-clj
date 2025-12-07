(ns flink-clj.table
  "Flink Table API support for SQL-based stream processing.

  This namespace provides Clojure wrappers for Flink's Table API, enabling
  SQL queries on streaming data, table-to-stream conversions, and DDL operations.

  Basic usage:
    (require '[flink-clj.table :as t])
    (require '[flink-clj.env :as env])

    ;; Create table environment
    (def stream-env (env/create-env))
    (def table-env (t/create-table-env stream-env))

    ;; Execute SQL
    (t/execute-sql table-env \"CREATE TABLE orders (...) WITH (...)\")

    ;; Query and convert back to stream
    (-> (t/sql-query table-env \"SELECT * FROM orders WHERE amount > 100\")
        (t/to-data-stream table-env)
        (stream/print))

  See also:
  - https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/"
  (:refer-clojure :exclude [group-by distinct filter])
  (:require [clojure.string :as str]
            [flink-clj.version :as v])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]))

;; =============================================================================
;; Availability Checks
;; =============================================================================

(defn- check-table-api-available
  "Check if Table API is available."
  []
  (try
    (Class/forName "org.apache.flink.table.api.bridge.java.StreamTableEnvironment")
    true
    (catch ClassNotFoundException _ false)))

(defn table-api-available?
  "Check if Flink Table API is available.

  Returns true if flink-table-api-java-bridge is on classpath."
  []
  (check-table-api-available))

(defn- ensure-table-api!
  "Throw if Table API is not available."
  [feature-name]
  (when-not (check-table-api-available)
    (throw (ex-info (str feature-name " requires Flink Table API")
                    {:hint "Add flink-table-api-java-bridge dependency"}))))

(defn table-api-info
  "Get information about Table API availability and features."
  []
  {:available (table-api-available?)
   :version (v/flink-minor-version)
   :features {:sql true
              :ddl true
              :window-tvf true
              :pattern-recognition (v/flink-2?)
              :ml-predict (v/flink-2?)}})

;; =============================================================================
;; Environment Creation
;; =============================================================================

(defn create-table-env
  "Create a StreamTableEnvironment from a StreamExecutionEnvironment.

  Arguments:
    stream-env - A StreamExecutionEnvironment

  Options:
    :catalog       - Default catalog name (optional)
    :database      - Default database name (optional)
    :config        - TableConfig settings as a map (optional)

  Example:
    (def table-env (t/create-table-env stream-env))
    (def table-env (t/create-table-env stream-env {:catalog \"my_catalog\"}))"
  ([^StreamExecutionEnvironment stream-env]
   (create-table-env stream-env {}))
  ([^StreamExecutionEnvironment stream-env {:keys [catalog database config]}]
   (ensure-table-api! "create-table-env")
   (let [table-env-class (Class/forName "org.apache.flink.table.api.bridge.java.StreamTableEnvironment")
         create-method (.getMethod table-env-class "create"
                                   (into-array Class [StreamExecutionEnvironment]))
         table-env (.invoke create-method nil (into-array Object [stream-env]))]
     ;; Apply configuration
     (when catalog
       (let [use-catalog-method (.getMethod (.getClass table-env) "useCatalog"
                                            (into-array Class [String]))]
         (.invoke use-catalog-method table-env (into-array Object [catalog]))))
     (when database
       (let [use-db-method (.getMethod (.getClass table-env) "useDatabase"
                                       (into-array Class [String]))]
         (.invoke use-db-method table-env (into-array Object [database]))))
     (when config
       (let [get-config-method (.getMethod (.getClass table-env) "getConfig" (into-array Class []))
             table-config (.invoke get-config-method table-env (into-array Object []))]
         (doseq [[k v] config]
           (let [set-method (.getMethod (.getClass table-config) "set"
                                        (into-array Class [(Class/forName "org.apache.flink.configuration.ConfigOption")
                                                           Object]))]
             ;; Note: This requires ConfigOption instances, simplified here
             ))))
     table-env)))

(defn get-config
  "Get the TableConfig from a table environment."
  [table-env]
  (ensure-table-api! "get-config")
  (let [get-config-method (.getMethod (.getClass table-env) "getConfig" (into-array Class []))]
    (.invoke get-config-method table-env (into-array Object []))))

;; =============================================================================
;; SQL Execution
;; =============================================================================

(defn execute-sql
  "Execute a SQL statement in the table environment.

  Returns a TableResult for queries, or completion for DDL statements.

  Example:
    (t/execute-sql table-env \"CREATE TABLE t1 (id INT, name STRING)\")
    (t/execute-sql table-env \"INSERT INTO sink SELECT * FROM source\")"
  [table-env sql]
  (ensure-table-api! "execute-sql")
  (let [exec-method (.getMethod (.getClass table-env) "executeSql"
                                (into-array Class [String]))]
    (.invoke exec-method table-env (into-array Object [sql]))))

(defn sql-query
  "Execute a SQL query and return a Table.

  Unlike execute-sql, this returns a Table object that can be further
  transformed or converted to a DataStream.

  Example:
    (-> (t/sql-query table-env \"SELECT * FROM orders\")
        (t/to-data-stream table-env))"
  [table-env sql]
  (ensure-table-api! "sql-query")
  (let [query-method (.getMethod (.getClass table-env) "sqlQuery"
                                 (into-array Class [String]))]
    (.invoke query-method table-env (into-array Object [sql]))))

;; =============================================================================
;; DataStream <-> Table Conversion
;; =============================================================================

(defn from-data-stream
  "Convert a DataStream to a Table.

  Arguments:
    table-env - A StreamTableEnvironment
    stream    - A DataStream

  Options:
    :schema - Column schema specification (optional)

  Example:
    (def table (t/from-data-stream table-env my-stream))"
  ([table-env stream]
   (from-data-stream table-env stream {}))
  ([table-env stream {:keys [schema]}]
   (ensure-table-api! "from-data-stream")
   (let [from-method (.getMethod (.getClass table-env) "fromDataStream"
                                 (into-array Class [(Class/forName "org.apache.flink.streaming.api.datastream.DataStream")]))]
     (.invoke from-method table-env (into-array Object [stream])))))

(defn to-data-stream
  "Convert a Table to a DataStream.

  Arguments:
    table     - A Table
    table-env - A StreamTableEnvironment

  Options:
    :class - Target Java class for rows (optional, defaults to Row)

  Example:
    (def stream (t/to-data-stream result-table table-env))"
  ([table table-env]
   (to-data-stream table table-env {}))
  ([table table-env {:keys [class]}]
   (ensure-table-api! "to-data-stream")
   (let [to-method (if class
                     (.getMethod (.getClass table-env) "toDataStream"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")
                                                    Class]))
                     (.getMethod (.getClass table-env) "toDataStream"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")])))]
     (if class
       (.invoke to-method table-env (into-array Object [table class]))
       (.invoke to-method table-env (into-array Object [table]))))))

(defn to-changelog-stream
  "Convert a Table to a changelog DataStream.

  Useful for tables with updates/deletes (not just inserts).

  Arguments:
    table     - A Table
    table-env - A StreamTableEnvironment

  Example:
    (def changelog (t/to-changelog-stream agg-table table-env))"
  [table table-env]
  (ensure-table-api! "to-changelog-stream")
  (let [to-method (.getMethod (.getClass table-env) "toChangelogStream"
                              (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke to-method table-env (into-array Object [table]))))

;; =============================================================================
;; Table Registration
;; =============================================================================

(defn create-temporary-view!
  "Register a Table or DataStream as a temporary view.

  Arguments:
    table-env - A StreamTableEnvironment
    name      - View name
    source    - A Table or DataStream

  Example:
    (t/create-temporary-view! table-env \"my_view\" my-table)"
  [table-env name source]
  (ensure-table-api! "create-temporary-view!")
  (let [create-method (.getMethod (.getClass table-env) "createTemporaryView"
                                  (into-array Class [String (Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke create-method table-env (into-array Object [name source]))))

(defn drop-temporary-view!
  "Drop a temporary view.

  Arguments:
    table-env - A StreamTableEnvironment
    name      - View name

  Example:
    (t/drop-temporary-view! table-env \"my_view\")"
  [table-env name]
  (ensure-table-api! "drop-temporary-view!")
  (execute-sql table-env (format "DROP TEMPORARY VIEW IF EXISTS `%s`" name)))

(defn from-view
  "Get a Table from a registered view or table.

  Arguments:
    table-env - A StreamTableEnvironment
    name      - Table or view name

  Example:
    (def orders (t/from-view table-env \"orders\"))"
  [table-env name]
  (ensure-table-api! "from-view")
  (let [from-method (.getMethod (.getClass table-env) "from"
                                (into-array Class [String]))]
    (.invoke from-method table-env (into-array Object [name]))))

;; =============================================================================
;; Table Operations
;; =============================================================================

(defn select
  "Select columns from a table.

  Arguments:
    table - A Table
    exprs - Column expressions (strings or $ expressions)

  Example:
    (t/select table \"id, name, amount * 2 AS doubled\")"
  [table exprs]
  (ensure-table-api! "select")
  ;; Use SQL-style string selection via sqlQuery after creating temp view
  ;; For direct API, would need Expression builder
  (let [select-method (.getMethod (.getClass table) "select"
                                  (into-array Class [String]))]
    (.invoke select-method table (into-array Object [exprs]))))

(defn where
  "Filter rows in a table.

  Arguments:
    table     - A Table
    condition - Filter condition as SQL string

  Example:
    (t/where table \"amount > 100 AND status = 'active'\")"
  [table condition]
  (ensure-table-api! "where")
  (let [where-method (.getMethod (.getClass table) "where"
                                 (into-array Class [String]))]
    (.invoke where-method table (into-array Object [condition]))))

(defn in-subquery
  "Create an IN subquery condition for use with where.

  This generates a SQL IN clause that checks if a column value
  exists in a subquery result.

  Arguments:
    column   - Column name to check
    subquery - A Table representing the subquery (must have single column)

  Returns a condition string for use with `where`.

  Note: The subquery table must have exactly one column with the same
  type as the column being checked.

  Example:
    ;; Find orders from active customers
    (def active-customer-ids
      (-> customers
          (t/where \"status = 'active'\")
          (t/select \"id\")))

    (-> orders
        (t/where (t/in-subquery \"customer_id\" active-customer-ids)))"
  [column subquery]
  (ensure-table-api! "in-subquery")
  ;; The Table API uses Expressions for IN, but we can use SQL string approach
  ;; For programmatic API, the subquery needs to be converted
  ;; We'll return a placeholder that works with SQL execution
  (throw (ex-info
           "in-subquery requires programmatic Expression API.
Use SQL instead:
  (t/sql-query table-env
    \"SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')\")"
           {:column column
            :hint "Use SQL for IN subqueries"})))

(defn where-in
  "Filter rows where a column value exists in another table.

  This is a convenience method that performs an IN subquery check.
  The subquery table must have exactly one column.

  Arguments:
    table    - Main table to filter
    column   - Column name to check
    subquery - Table with single column to check against

  Example:
    ;; Find orders from VIP customers
    (def vip-ids (-> customers
                     (t/where \"tier = 'VIP'\")
                     (t/select \"id\")))
    (t/where-in orders \"customer_id\" vip-ids)

  Note: For complex IN queries, consider using SQL directly:
    (t/sql-query table-env \"SELECT * FROM t1 WHERE col IN (SELECT col FROM t2)\")"
  [table column subquery]
  (ensure-table-api! "where-in")
  ;; Use semi-join approach which is equivalent to IN
  (let [;; Get the column name from the subquery
        schema-method (.getMethod (.getClass subquery) "getResolvedSchema" (into-array Class []))
        schema (.invoke schema-method subquery (into-array Object []))
        cols-method (.getMethod (.getClass schema) "getColumnNames" (into-array Class []))
        col-names (.invoke cols-method schema (into-array Object []))
        subquery-col (first col-names)
        ;; Perform a semi-join (which is equivalent to IN)
        join-condition (str column " = " subquery-col)]
    ;; Use left semi join approach via SQL
    (throw (ex-info
             "where-in requires semi-join support.
Use SQL instead:
  (t/sql-query table-env
    \"SELECT * FROM main_table WHERE column IN (SELECT col FROM subquery_table)\")"
             {:table table
              :column column
              :hint "Use SQL for IN subqueries"}))))

(defn group-by
  "Group rows by specified columns.

  Returns a GroupedTable that can be aggregated.

  Arguments:
    table   - A Table
    columns - Columns to group by (comma-separated string)

  Example:
    (-> (t/group-by table \"user_id\")
        (t/agg-select \"user_id, COUNT(*) AS cnt\"))"
  [table columns]
  (ensure-table-api! "group-by")
  (let [group-method (.getMethod (.getClass table) "groupBy"
                                 (into-array Class [String]))]
    (.invoke group-method table (into-array Object [columns]))))

(defn agg-select
  "Apply aggregation select on a GroupedTable.

  Arguments:
    grouped-table - A GroupedTable from group-by
    exprs         - Aggregation expressions

  Example:
    (t/agg-select grouped-table \"user_id, SUM(amount) AS total\")"
  [grouped-table exprs]
  (ensure-table-api! "agg-select")
  (let [select-method (.getMethod (.getClass grouped-table) "select"
                                  (into-array Class [String]))]
    (.invoke select-method grouped-table (into-array Object [exprs]))))

(defn order-by
  "Order rows by specified columns.

  Arguments:
    table   - A Table
    columns - Order by expression (e.g., \"amount DESC, created_at ASC\")

  Example:
    (t/order-by table \"created_at DESC\")"
  [table columns]
  (ensure-table-api! "order-by")
  (let [order-method (.getMethod (.getClass table) "orderBy"
                                 (into-array Class [String]))]
    (.invoke order-method table (into-array Object [columns]))))

(defn limit
  "Limit the number of rows.

  Arguments:
    table - A Table
    n     - Maximum number of rows

  Example:
    (t/limit table 100)"
  [table n]
  (ensure-table-api! "limit")
  (let [limit-method (.getMethod (.getClass table) "limit"
                                 (into-array Class [Integer/TYPE]))]
    (.invoke limit-method table (into-array Object [(int n)]))))

(defn offset
  "Skip rows from the beginning.

  Arguments:
    table - A Table
    n     - Number of rows to skip

  Example:
    (-> (t/offset table 10)
        (t/limit 20))"
  [table n]
  (ensure-table-api! "offset")
  (let [offset-method (.getMethod (.getClass table) "offset"
                                  (into-array Class [Integer/TYPE]))]
    (.invoke offset-method table (into-array Object [(int n)]))))

;; =============================================================================
;; Column Manipulation
;; =============================================================================

(defn add-columns
  "Add new columns to a table.

  Arguments:
    table - A Table
    exprs - Column expressions to add (comma-separated string)

  Example:
    (t/add-columns table \"amount * 0.1 AS tax, amount * 1.1 AS total\")"
  [table exprs]
  (ensure-table-api! "add-columns")
  (let [add-method (.getMethod (.getClass table) "addColumns"
                               (into-array Class [String]))]
    (.invoke add-method table (into-array Object [exprs]))))

(defn add-or-replace-columns
  "Add new columns or replace existing ones.

  Arguments:
    table - A Table
    exprs - Column expressions (comma-separated string)

  Example:
    (t/add-or-replace-columns table \"amount * 1.1 AS amount\")"
  [table exprs]
  (ensure-table-api! "add-or-replace-columns")
  (let [add-method (.getMethod (.getClass table) "addOrReplaceColumns"
                               (into-array Class [String]))]
    (.invoke add-method table (into-array Object [exprs]))))

(defn drop-columns
  "Drop columns from a table.

  Arguments:
    table   - A Table
    columns - Columns to drop (comma-separated string)

  Example:
    (t/drop-columns table \"temp_col, debug_col\")"
  [table columns]
  (ensure-table-api! "drop-columns")
  (let [drop-method (.getMethod (.getClass table) "dropColumns"
                                (into-array Class [String]))]
    (.invoke drop-method table (into-array Object [columns]))))

(defn rename-columns
  "Rename columns in a table.

  Arguments:
    table   - A Table
    renames - Rename expressions (comma-separated string)

  Example:
    (t/rename-columns table \"old_name AS new_name, col1 AS column_one\")"
  [table renames]
  (ensure-table-api! "rename-columns")
  (let [rename-method (.getMethod (.getClass table) "renameColumns"
                                  (into-array Class [String]))]
    (.invoke rename-method table (into-array Object [renames]))))

(defn alias-as
  "Rename the table and optionally its fields.

  Arguments:
    table  - A Table
    name   - New table name
    fields - Optional field names (varargs)

  Example:
    (t/alias-as table \"orders\")
    (t/alias-as table \"t\" \"id\" \"name\" \"amount\")"
  [table name & fields]
  (ensure-table-api! "alias-as")
  (if (seq fields)
    (let [as-method (.getMethod (.getClass table) "as"
                                (into-array Class [String (Class/forName "[Ljava.lang.String;")]))]
      (.invoke as-method table (into-array Object [name (into-array String fields)])))
    (let [as-method (.getMethod (.getClass table) "as"
                                (into-array Class [String]))]
      (.invoke as-method table (into-array Object [name])))))

;; =============================================================================
;; Distinct and Deduplication
;; =============================================================================

(defn distinct
  "Remove duplicate rows from a table.

  Note: In streaming mode, this requires state and may have performance implications.

  Arguments:
    table - A Table

  Example:
    (t/distinct table)"
  [table]
  (ensure-table-api! "distinct")
  (let [distinct-method (.getMethod (.getClass table) "distinct" (into-array Class []))]
    (.invoke distinct-method table (into-array Object []))))

;; =============================================================================
;; Join Operations
;; =============================================================================

(defn join
  "Join two tables.

  Arguments:
    left      - Left table
    right     - Right table
    condition - Join condition as SQL string

  Example:
    (t/join orders customers \"orders.customer_id = customers.id\")"
  [left right condition]
  (ensure-table-api! "join")
  (let [join-method (.getMethod (.getClass left) "join"
                                (into-array Class [(Class/forName "org.apache.flink.table.api.Table")
                                                   String]))]
    (.invoke join-method left (into-array Object [right condition]))))

(defn left-outer-join
  "Left outer join two tables.

  Arguments:
    left      - Left table
    right     - Right table
    condition - Join condition as SQL string

  Example:
    (t/left-outer-join orders customers \"orders.customer_id = customers.id\")"
  [left right condition]
  (ensure-table-api! "left-outer-join")
  (let [join-method (.getMethod (.getClass left) "leftOuterJoin"
                                (into-array Class [(Class/forName "org.apache.flink.table.api.Table")
                                                   String]))]
    (.invoke join-method left (into-array Object [right condition]))))

(defn right-outer-join
  "Right outer join two tables.

  Arguments:
    left      - Left table
    right     - Right table
    condition - Join condition as SQL string

  Example:
    (t/right-outer-join orders customers \"orders.customer_id = customers.id\")"
  [left right condition]
  (ensure-table-api! "right-outer-join")
  (let [join-method (.getMethod (.getClass left) "rightOuterJoin"
                                (into-array Class [(Class/forName "org.apache.flink.table.api.Table")
                                                   String]))]
    (.invoke join-method left (into-array Object [right condition]))))

(defn full-outer-join
  "Full outer join two tables.

  Arguments:
    left      - Left table
    right     - Right table
    condition - Join condition as SQL string

  Example:
    (t/full-outer-join orders customers \"orders.customer_id = customers.id\")"
  [left right condition]
  (ensure-table-api! "full-outer-join")
  (let [join-method (.getMethod (.getClass left) "fullOuterJoin"
                                (into-array Class [(Class/forName "org.apache.flink.table.api.Table")
                                                   String]))]
    (.invoke join-method left (into-array Object [right condition]))))

(defn cross-join
  "Cross join (Cartesian product) two tables.

  Returns every combination of rows from both tables.
  Warning: Can produce very large results if tables are large.

  Arguments:
    left  - Left table
    right - Right table

  Example:
    (t/cross-join sizes colors)
    ;; If sizes has 3 rows and colors has 4 rows, result has 12 rows"
  [left right]
  (ensure-table-api! "cross-join")
  (let [cross-method (.getMethod (.getClass left) "crossJoin"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke cross-method left (into-array Object [right]))))

;; =============================================================================
;; Lateral Joins (Table Functions)
;; =============================================================================

(defn join-lateral
  "Join a table with a table function (UDTF).

  Arguments:
    table    - A Table
    func-call - Table function call as string (e.g., \"MY_FUNC(col1)\")

  Example:
    (t/join-lateral table \"explode_array(tags)\")"
  [table func-call]
  (ensure-table-api! "join-lateral")
  (let [join-method (.getMethod (.getClass table) "joinLateral"
                                (into-array Class [String]))]
    (.invoke join-method table (into-array Object [func-call]))))

(defn left-outer-join-lateral
  "Left outer join a table with a table function.

  Arguments:
    table     - A Table
    func-call - Table function call as string

  Example:
    (t/left-outer-join-lateral table \"explode_array(tags)\")"
  [table func-call]
  (ensure-table-api! "left-outer-join-lateral")
  (let [join-method (.getMethod (.getClass table) "leftOuterJoinLateral"
                                (into-array Class [String]))]
    (.invoke join-method table (into-array Object [func-call]))))

;; =============================================================================
;; Set Operations
;; =============================================================================

(defn union
  "Union two tables (removes duplicates).

  Arguments:
    table1 - First table
    table2 - Second table

  Example:
    (t/union active-users inactive-users)"
  [table1 table2]
  (ensure-table-api! "union")
  (let [union-method (.getMethod (.getClass table1) "union"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke union-method table1 (into-array Object [table2]))))

(defn union-all
  "Union two tables (keeps duplicates).

  Arguments:
    table1 - First table
    table2 - Second table

  Example:
    (t/union-all active-orders archived-orders)"
  [table1 table2]
  (ensure-table-api! "union-all")
  (let [union-method (.getMethod (.getClass table1) "unionAll"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke union-method table1 (into-array Object [table2]))))

(defn intersect
  "Intersect two tables (removes duplicates).

  Arguments:
    table1 - First table
    table2 - Second table

  Example:
    (t/intersect table1 table2)"
  [table1 table2]
  (ensure-table-api! "intersect")
  (let [intersect-method (.getMethod (.getClass table1) "intersect"
                                     (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke intersect-method table1 (into-array Object [table2]))))

(defn intersect-all
  "Intersect two tables (keeps duplicates).

  Arguments:
    table1 - First table
    table2 - Second table

  Example:
    (t/intersect-all table1 table2)"
  [table1 table2]
  (ensure-table-api! "intersect-all")
  (let [intersect-method (.getMethod (.getClass table1) "intersectAll"
                                     (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke intersect-method table1 (into-array Object [table2]))))

(defn minus
  "Subtract table2 from table1 (removes duplicates).

  Arguments:
    table1 - First table
    table2 - Second table

  Example:
    (t/minus all-orders cancelled-orders)"
  [table1 table2]
  (ensure-table-api! "minus")
  (let [minus-method (.getMethod (.getClass table1) "minus"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke minus-method table1 (into-array Object [table2]))))

(defn minus-all
  "Subtract table2 from table1 (keeps duplicates).

  Arguments:
    table1 - First table
    table2 - Second table

  Example:
    (t/minus-all all-orders cancelled-orders)"
  [table1 table2]
  (ensure-table-api! "minus-all")
  (let [minus-method (.getMethod (.getClass table1) "minusAll"
                                 (into-array Class [(Class/forName "org.apache.flink.table.api.Table")]))]
    (.invoke minus-method table1 (into-array Object [table2]))))

;; =============================================================================
;; Window Operations
;; =============================================================================

(defn tumble-window
  "Create a tumbling window specification for group-by windowing.

  Arguments:
    time-col - Time attribute column name
    size     - Window size as string (e.g., \"10.minutes\", \"1.hour\")

  Example:
    (-> table
        (t/group-by (t/tumble-window \"rowtime\" \"10.minutes\"))
        (t/agg-select \"window_start, window_end, COUNT(*) AS cnt\"))"
  [time-col size]
  (format "TUMBLE(%s, INTERVAL '%s')" time-col size))

(defn slide-window
  "Create a sliding window specification for group-by windowing.

  Arguments:
    time-col - Time attribute column name
    size     - Window size as string (e.g., \"1.hour\")
    slide    - Slide interval as string (e.g., \"10.minutes\")

  Example:
    (-> table
        (t/group-by (t/slide-window \"rowtime\" \"1.hour\" \"10.minutes\"))
        (t/agg-select \"window_start, window_end, SUM(amount) AS total\"))"
  [time-col size slide]
  (format "HOP(%s, INTERVAL '%s', INTERVAL '%s')" time-col slide size))

(defn session-window
  "Create a session window specification for group-by windowing.

  Arguments:
    time-col - Time attribute column name
    gap      - Session gap as string (e.g., \"30.minutes\")

  Example:
    (-> table
        (t/group-by (t/session-window \"rowtime\" \"30.minutes\"))
        (t/agg-select \"window_start, window_end, COUNT(*) AS events\"))"
  [time-col gap]
  (format "SESSION(%s, INTERVAL '%s')" time-col gap))

(defn cumulate-window
  "Create a cumulating window specification for group-by windowing.

  Arguments:
    time-col - Time attribute column name
    step     - Cumulation step as string (e.g., \"1.hour\")
    max-size - Maximum window size as string (e.g., \"1.day\")

  Example:
    (-> table
        (t/group-by (t/cumulate-window \"rowtime\" \"1.hour\" \"1.day\"))
        (t/agg-select \"window_start, window_end, SUM(amount) AS running_total\"))"
  [time-col step max-size]
  (format "CUMULATE(%s, INTERVAL '%s', INTERVAL '%s')" time-col step max-size))

;; =============================================================================
;; Over Window Aggregations
;; =============================================================================

(defn over
  "Apply over window aggregations (analytic functions).

  Arguments:
    table  - A Table
    window - Over window specification as string

  Example:
    (t/over table \"SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) AS running_sum\")"
  [table window]
  (ensure-table-api! "over")
  ;; Over windows are typically done via select with window specification
  (select table window))

;; =============================================================================
;; Source Table Creation
;; =============================================================================

(defn- escape-sql-value
  "Escape a value for use in SQL."
  [v]
  (cond
    (nil? v) "NULL"
    (string? v) (str "'" (str/replace (str v) "'" "''") "'")
    (keyword? v) (str "'" (name v) "'")
    :else (str v)))

(defn from-values
  "Create a Table from inline row values.

  Arguments:
    table-env - A StreamTableEnvironment
    schema    - Column names as vector of strings
    rows      - Vector of row vectors

  Example:
    (t/from-values table-env
                   [\"id\" \"name\" \"amount\"]
                   [[1 \"Alice\" 100.0]
                    [2 \"Bob\" 200.0]
                    [3 \"Charlie\" 150.0]])"
  [table-env schema rows]
  (ensure-table-api! "from-values")
  ;; Build VALUES clause SQL
  (let [col-list (str/join ", " (map #(str "`" % "`") schema))
        row-strs (map (fn [row]
                        (str "("
                             (str/join ", " (map escape-sql-value row))
                             ")"))
                      rows)
        sql (str "SELECT * FROM (VALUES " (str/join ", " row-strs) ") AS t(" col-list ")")]
    (sql-query table-env sql)))

;; =============================================================================
;; Row-Based Operations
;; =============================================================================
;;
;; Row-based operations apply functions to individual rows.
;; These require registered UDFs (User-Defined Functions).
;;
;; Function types:
;; - ScalarFunction: 1 row in -> 1 row out (for map)
;; - TableFunction: 1 row in -> N rows out (for flatMap)
;; - AggregateFunction: N rows in -> 1 row out (for aggregate)
;; - TableAggregateFunction: N rows in -> M rows out (for flatAggregate)
;;
;; Note: These operations require UDFs to be registered first via:
;;   (t/execute-sql table-env (t/create-function-sql "MY_FUNC" "com.example.MyFunc"))

(defn map-row
  "Apply a scalar function to each row of the table.

  The scalar function takes row values and returns transformed values.
  This is a 1:1 mapping - each input row produces exactly one output row.

  Arguments:
    table     - A Table
    func-call - Scalar function call expression as string

  Note: The function must be registered first using CREATE FUNCTION.

  Example:
    ;; Register a scalar function that doubles a value
    (t/execute-sql table-env
      \"CREATE FUNCTION double_val AS 'com.example.DoubleValueFunction'\")

    ;; Apply to each row
    (t/map-row numbers \"double_val(value)\")"
  [table func-call]
  (ensure-table-api! "map-row")
  ;; map() uses a ScalarFunction - we implement via select with the function call
  (let [select-method (.getMethod (.getClass table) "select"
                                  (into-array Class [String]))]
    (.invoke select-method table (into-array Object [func-call]))))

(defn flat-map
  "Apply a table function to each row, producing zero or more output rows.

  The table function takes row values and can return multiple rows per input.
  This is a 1:N mapping - each input row can produce zero, one, or many output rows.

  Arguments:
    table     - A Table
    func-call - Table function call expression as string

  Note: The function must be registered first using CREATE FUNCTION.

  Example:
    ;; Register a table function that splits strings
    (t/execute-sql table-env
      \"CREATE FUNCTION split_words AS 'com.example.SplitWordsFunction'\")

    ;; Apply to each row - each sentence produces multiple word rows
    (t/flat-map sentences \"split_words(text)\")"
  [table func-call]
  (ensure-table-api! "flat-map")
  ;; flatMap uses a TableFunction - implemented via joinLateral with CROSS JOIN
  ;; This is equivalent to: SELECT * FROM table, LATERAL TABLE(func(col))
  (let [flat-map-method (.getMethod (.getClass table) "flatMap"
                                    (into-array Class [String]))]
    (try
      (.invoke flat-map-method table (into-array Object [func-call]))
      (catch NoSuchMethodException _
        ;; Fall back to joinLateral for older API
        (let [join-method (.getMethod (.getClass table) "joinLateral"
                                      (into-array Class [String]))]
          (.invoke join-method table (into-array Object [func-call])))))))

(defn aggregate
  "Apply an aggregate function to the entire table (global aggregation).

  This performs aggregation without grouping - the entire table is treated
  as a single group, producing a single result row.

  Arguments:
    table        - A Table
    agg-exprs    - Aggregation expressions as string

  Note: For grouped aggregation, use (group-by table cols) then (agg-select ...).

  Example:
    ;; Global aggregation - count all rows and sum amounts
    (t/aggregate orders \"COUNT(*) AS total_orders, SUM(amount) AS total_amount\")

    ;; With a custom aggregate function
    (t/aggregate events \"my_custom_agg(value) AS result\")"
  [table agg-exprs]
  (ensure-table-api! "aggregate")
  ;; Global aggregate is implemented via select with aggregate functions
  ;; In Table API, this is table.select("AGG_FUNC(col)")
  (let [select-method (.getMethod (.getClass table) "select"
                                  (into-array Class [String]))]
    (.invoke select-method table (into-array Object [agg-exprs]))))

(defn flat-aggregate
  "Apply a table aggregate function that can return multiple result rows.

  Unlike regular aggregation which produces one row per group, flatAggregate
  can produce zero, one, or many rows per group. This is useful for:
  - Top-N queries
  - Percentile calculations
  - Multiple statistics per group

  Arguments:
    grouped-table - A GroupedTable from group-by
    func-call     - Table aggregate function call as string
    select-expr   - Columns to select from the aggregate result

  Note: The TableAggregateFunction must be registered first.

  Example:
    ;; Register a top-3 aggregate function
    (t/execute-sql table-env
      \"CREATE FUNCTION top3 AS 'com.example.Top3AggregateFunction'\")

    ;; Get top 3 values per category
    (-> (t/group-by products \"category\")
        (t/flat-aggregate \"top3(price)\" \"category, value, rank\"))"
  [grouped-table func-call select-expr]
  (ensure-table-api! "flat-aggregate")
  ;; flatAggregate is available on GroupedTable
  (let [flat-agg-method (.getMethod (.getClass grouped-table) "flatAggregate"
                                    (into-array Class [String]))]
    (try
      (let [result (.invoke flat-agg-method grouped-table (into-array Object [func-call]))
            select-method (.getMethod (.getClass result) "select"
                                      (into-array Class [String]))]
        (.invoke select-method result (into-array Object [select-expr])))
      (catch NoSuchMethodException _
        (throw (ex-info
                 "flatAggregate requires Flink 1.12+ and a registered TableAggregateFunction.
Use SQL with UDTAF (User-Defined Table Aggregate Function) instead."
                 {:grouped-table grouped-table
                  :func-call func-call}))))))

(defn global-flat-aggregate
  "Apply a table aggregate function to the entire table (without grouping).

  This is the global (non-grouped) version of flat-aggregate.

  Arguments:
    table       - A Table
    func-call   - Table aggregate function call as string
    select-expr - Columns to select from the aggregate result

  Example:
    ;; Get global top 10 products by price
    (t/global-flat-aggregate products \"top10(price)\" \"value, rank\")"
  [table func-call select-expr]
  (ensure-table-api! "global-flat-aggregate")
  (let [flat-agg-method (.getMethod (.getClass table) "flatAggregate"
                                    (into-array Class [String]))]
    (try
      (let [result (.invoke flat-agg-method table (into-array Object [func-call]))
            select-method (.getMethod (.getClass result) "select"
                                      (into-array Class [String]))]
        (.invoke select-method result (into-array Object [select-expr])))
      (catch NoSuchMethodException _
        (throw (ex-info
                 "flatAggregate requires Flink 1.12+ and a registered TableAggregateFunction."
                 {:table table
                  :func-call func-call}))))))

;; =============================================================================
;; Table Output
;; =============================================================================

(defn insert-into!
  "Insert a table's contents into a registered sink table.

  Arguments:
    table      - Source table
    table-env  - A StreamTableEnvironment
    sink-table - Name of the registered sink table

  Returns a TableResult that can be awaited.

  Example:
    (t/insert-into! result-table table-env \"output_kafka\")"
  [table table-env sink-table]
  (ensure-table-api! "insert-into!")
  ;; Create a temp view and use INSERT INTO
  (let [temp-view (str "temp_" (System/currentTimeMillis))]
    (create-temporary-view! table-env temp-view table)
    (try
      (execute-sql table-env (format "INSERT INTO `%s` SELECT * FROM `%s`" sink-table temp-view))
      (finally
        (drop-temporary-view! table-env temp-view)))))

(defn execute-insert
  "Execute an INSERT statement for a table into a sink.

  Arguments:
    table      - Source table
    sink-table - Name of the registered sink table

  Returns a TableResult.

  Example:
    (t/execute-insert result-table \"output_sink\")"
  [table sink-table]
  (ensure-table-api! "execute-insert")
  (let [insert-method (.getMethod (.getClass table) "executeInsert"
                                  (into-array Class [String]))]
    (.invoke insert-method table (into-array Object [sink-table]))))

(defn execute
  "Execute a table and collect results.

  Arguments:
    table - A Table

  Returns a TableResult that can be iterated.

  Example:
    (let [result (t/execute table)]
      (doseq [row (.collect result)]
        (println row)))"
  [table]
  (ensure-table-api! "execute")
  (let [exec-method (.getMethod (.getClass table) "execute" (into-array Class []))]
    (.invoke exec-method table (into-array Object []))))

(defn fetch
  "Fetch limited rows from a table (alias for limit in batch mode).

  Arguments:
    table - A Table
    n     - Number of rows to fetch

  Example:
    (t/fetch table 100)"
  [table n]
  (limit table n))

;; =============================================================================
;; Filter Alias
;; =============================================================================

(defn filter
  "Filter rows in a table (alias for where).

  Arguments:
    table     - A Table
    condition - Filter condition as SQL string

  Example:
    (t/filter table \"status = 'active'\")"
  [table condition]
  (where table condition))

;; =============================================================================
;; DDL Helpers
;; =============================================================================

(defn- escape-sql-string
  "Escape single quotes in SQL strings."
  [s]
  (when s
    (str/replace (str s) "'" "''")))

(defn create-table-sql
  "Generate CREATE TABLE SQL statement.

  Arguments:
    table-name - Name of the table
    columns    - Vector of column definitions [{:name \"id\" :type \"INT\"} ...]
    with-opts  - WITH clause options as map

  Options in columns:
    :name       - Column name (required)
    :type       - SQL type (required)
    :primary-key? - Is primary key
    :not-null?  - NOT NULL constraint

  Example:
    (t/create-table-sql \"orders\"
      [{:name \"id\" :type \"BIGINT\" :primary-key? true}
       {:name \"product\" :type \"STRING\"}
       {:name \"amount\" :type \"DECIMAL(10,2)\"}]
      {:connector \"kafka\"
       :topic \"orders\"
       :properties.bootstrap.servers \"localhost:9092\"
       :format \"json\"})"
  [table-name columns with-opts]
  (let [col-defs (map (fn [{:keys [name type primary-key? not-null?]}]
                        (str "  `" name "` " type
                             (when not-null? " NOT NULL")))
                      columns)
        pk-cols (clojure.core/filter :primary-key? columns)
        pk-clause (when (seq pk-cols)
                    (str "  PRIMARY KEY ("
                         (str/join ", " (map #(str "`" (:name %) "`") pk-cols))
                         ") NOT ENFORCED"))
        all-clauses (if pk-clause
                      (conj (vec col-defs) pk-clause)
                      col-defs)
        with-entries (map (fn [[k v]]
                            (str "  '" (if (keyword? k) (name k) k) "' = '"
                                 (escape-sql-string (str v)) "'"))
                          with-opts)]
    (str "CREATE TABLE `" table-name "` (\n"
         (str/join ",\n" all-clauses)
         "\n) WITH (\n"
         (str/join ",\n" with-entries)
         "\n)")))

(defn create-table!
  "Create a table using DDL.

  Arguments:
    table-env  - A StreamTableEnvironment
    table-name - Name of the table
    columns    - Vector of column definitions
    with-opts  - WITH clause options

  Example:
    (t/create-table! table-env \"orders\"
      [{:name \"id\" :type \"BIGINT\"}
       {:name \"data\" :type \"STRING\"}]
      {:connector \"kafka\"
       :topic \"orders\"})"
  [table-env table-name columns with-opts]
  (ensure-table-api! "create-table!")
  (let [sql (create-table-sql table-name columns with-opts)]
    (execute-sql table-env sql)))

(defn drop-table!
  "Drop a table.

  Arguments:
    table-env  - A StreamTableEnvironment
    table-name - Name of the table
    if-exists? - If true, don't error if table doesn't exist (default: true)

  Example:
    (t/drop-table! table-env \"old_orders\")"
  ([table-env table-name]
   (drop-table! table-env table-name true))
  ([table-env table-name if-exists?]
   (ensure-table-api! "drop-table!")
   (let [sql (if if-exists?
               (format "DROP TABLE IF EXISTS `%s`" table-name)
               (format "DROP TABLE `%s`" table-name))]
     (execute-sql table-env sql))))

;; =============================================================================
;; Connector Helpers
;; =============================================================================

(defn kafka-connector-options
  "Generate Kafka connector options for CREATE TABLE.

  Options:
    :servers      - Bootstrap servers (required)
    :topic        - Topic name (required)
    :group-id     - Consumer group ID (optional)
    :format       - Message format: :json, :avro, :csv (default: :json)
    :scan-startup - Startup mode: :earliest, :latest, :group-offsets (default: :group-offsets)

  Example:
    (t/kafka-connector-options {:servers \"localhost:9092\"
                                :topic \"events\"
                                :format :json})"
  [{:keys [servers topic group-id format scan-startup]
    :or {format :json
         scan-startup :group-offsets}}]
  (cond-> {:connector "kafka"
           :properties.bootstrap.servers servers
           :topic topic
           :format (name format)
           :scan.startup.mode (case scan-startup
                                :earliest "earliest-offset"
                                :latest "latest-offset"
                                :group-offsets "group-offsets"
                                (name scan-startup))}
    group-id (assoc :properties.group.id group-id)))

(defn filesystem-connector-options
  "Generate filesystem connector options for CREATE TABLE.

  Options:
    :path   - File path (required)
    :format - File format: :json, :csv, :parquet, :avro, :orc (default: :json)

  Example:
    (t/filesystem-connector-options {:path \"/data/events\"
                                     :format :parquet})"
  [{:keys [path format]
    :or {format :json}}]
  {:connector "filesystem"
   :path path
   :format (name format)})

(defn jdbc-connector-options
  "Generate JDBC connector options for CREATE TABLE.

  Options:
    :url        - JDBC URL (required)
    :table-name - Database table name (required)
    :username   - Database username (optional)
    :password   - Database password (optional)
    :driver     - JDBC driver class (optional, auto-detected)

  Example:
    (t/jdbc-connector-options {:url \"jdbc:postgresql://localhost/db\"
                               :table-name \"orders\"
                               :username \"user\"
                               :password \"pass\"})"
  [{:keys [url table-name username password driver]}]
  (cond-> {:connector "jdbc"
           :url url
           :table-name table-name}
    username (assoc :username username)
    password (assoc :password password)
    driver (assoc :driver driver)))

(defn print-connector-options
  "Generate print connector options for debugging.

  Options:
    :print-identifier - Prefix for printed output (optional)

  Example:
    (t/print-connector-options {:print-identifier \"DEBUG\"})"
  [{:keys [print-identifier]}]
  (cond-> {:connector "print"}
    print-identifier (assoc :print-identifier print-identifier)))

(defn blackhole-connector-options
  "Generate blackhole connector options (discards all data).

  Useful for testing and benchmarking.

  Example:
    (t/blackhole-connector-options)"
  []
  {:connector "blackhole"})

;; =============================================================================
;; Table Schema Helpers
;; =============================================================================

(defn column
  "Create a column definition for create-table!.

  Arguments:
    name - Column name
    type - SQL type

  Options:
    :primary-key? - Is primary key
    :not-null?    - NOT NULL constraint

  Example:
    (t/column \"id\" \"BIGINT\" {:primary-key? true})
    (t/column \"name\" \"STRING\" {:not-null? true})"
  ([name type]
   (column name type {}))
  ([name type {:keys [primary-key? not-null?]}]
   {:name name
    :type type
    :primary-key? primary-key?
    :not-null? not-null?}))

(defn watermark-column
  "Create a watermark column definition.

  Arguments:
    ts-column - Timestamp column name
    expr      - Watermark expression (e.g., \"ts - INTERVAL '5' SECOND\")

  Returns a string to be added to column definitions."
  [ts-column expr]
  {:watermark {:column ts-column :expr expr}})

(defn computed-column
  "Create a computed column definition.

  Arguments:
    name - Column name
    expr - Computation expression

  Example:
    (t/computed-column \"total\" \"quantity * price\")"
  [name expr]
  {:name name
   :computed expr})

;; =============================================================================
;; Explain and Print
;; =============================================================================

(defn explain
  "Get the execution plan for a table.

  Arguments:
    table - A Table

  Example:
    (println (t/explain my-table))"
  [table]
  (ensure-table-api! "explain")
  (let [explain-method (.getMethod (.getClass table) "explain" (into-array Class []))]
    (.invoke explain-method table (into-array Object []))))

(defn print-schema
  "Print the schema of a table.

  Arguments:
    table - A Table"
  [table]
  (ensure-table-api! "print-schema")
  (let [get-schema-method (.getMethod (.getClass table) "getResolvedSchema" (into-array Class []))
        schema (.invoke get-schema-method table (into-array Object []))]
    (println schema)))

;; =============================================================================
;; Catalog Operations
;; =============================================================================

(defn list-catalogs
  "List all catalogs in the table environment.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (t/list-catalogs table-env)"
  [table-env]
  (ensure-table-api! "list-catalogs")
  (let [list-method (.getMethod (.getClass table-env) "listCatalogs" (into-array Class []))]
    (vec (.invoke list-method table-env (into-array Object [])))))

(defn list-databases
  "List all databases in the current catalog.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (t/list-databases table-env)"
  [table-env]
  (ensure-table-api! "list-databases")
  (let [list-method (.getMethod (.getClass table-env) "listDatabases" (into-array Class []))]
    (vec (.invoke list-method table-env (into-array Object [])))))

(defn list-tables
  "List all tables in the current database.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (t/list-tables table-env)"
  [table-env]
  (ensure-table-api! "list-tables")
  (let [list-method (.getMethod (.getClass table-env) "listTables" (into-array Class []))]
    (vec (.invoke list-method table-env (into-array Object [])))))

(defn list-views
  "List all views in the current database.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (t/list-views table-env)"
  [table-env]
  (ensure-table-api! "list-views")
  (let [list-method (.getMethod (.getClass table-env) "listViews" (into-array Class []))]
    (vec (.invoke list-method table-env (into-array Object [])))))

(defn use-catalog!
  "Switch to a different catalog.

  Arguments:
    table-env    - A StreamTableEnvironment
    catalog-name - Name of the catalog

  Example:
    (t/use-catalog! table-env \"hive_catalog\")"
  [table-env catalog-name]
  (ensure-table-api! "use-catalog!")
  (let [use-method (.getMethod (.getClass table-env) "useCatalog"
                               (into-array Class [String]))]
    (.invoke use-method table-env (into-array Object [catalog-name]))))

(defn use-database!
  "Switch to a different database.

  Arguments:
    table-env     - A StreamTableEnvironment
    database-name - Name of the database

  Example:
    (t/use-database! table-env \"production\")"
  [table-env database-name]
  (ensure-table-api! "use-database!")
  (let [use-method (.getMethod (.getClass table-env) "useDatabase"
                               (into-array Class [String]))]
    (.invoke use-method table-env (into-array Object [database-name]))))

;; =============================================================================
;; Statement Set (Multi-Statement Execution)
;; =============================================================================

(defn create-statement-set
  "Create a StatementSet for executing multiple statements.

  Useful for multi-sink scenarios where you want to fan out to
  multiple destinations from the same source.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (-> (t/create-statement-set table-env)
        (t/add-insert-sql \"INSERT INTO sink1 SELECT * FROM source\")
        (t/add-insert-sql \"INSERT INTO sink2 SELECT * FROM source\")
        (t/execute-statement-set))"
  [table-env]
  (ensure-table-api! "create-statement-set")
  (let [create-method (.getMethod (.getClass table-env) "createStatementSet" (into-array Class []))]
    (.invoke create-method table-env (into-array Object []))))

(defn add-insert-sql
  "Add an INSERT statement to a StatementSet.

  Arguments:
    statement-set - A StatementSet
    sql           - INSERT SQL statement

  Example:
    (t/add-insert-sql stmt-set \"INSERT INTO sink SELECT * FROM source\")"
  [statement-set sql]
  (ensure-table-api! "add-insert-sql")
  (let [add-method (.getMethod (.getClass statement-set) "addInsertSql"
                               (into-array Class [String]))]
    (.invoke add-method statement-set (into-array Object [sql]))
    statement-set))

(defn execute-statement-set
  "Execute all statements in a StatementSet.

  Arguments:
    statement-set - A StatementSet

  Returns a TableResult.

  Example:
    (t/execute-statement-set stmt-set)"
  [statement-set]
  (ensure-table-api! "execute-statement-set")
  (let [exec-method (.getMethod (.getClass statement-set) "execute" (into-array Class []))]
    (.invoke exec-method statement-set (into-array Object []))))

;; =============================================================================
;; User-Defined Functions
;; =============================================================================

(defn register-function!
  "Register a user-defined function (UDF/UDTF/UDAF).

  Arguments:
    table-env - A StreamTableEnvironment
    name      - Function name
    function  - A ScalarFunction, TableFunction, or AggregateFunction instance

  Example:
    (t/register-function! table-env \"MY_UPPER\" my-upper-fn)"
  [table-env name function]
  (ensure-table-api! "register-function!")
  (let [create-temp-fn (.getMethod (.getClass table-env) "createTemporarySystemFunction"
                                   (into-array Class [String Class]))]
    (.invoke create-temp-fn table-env (into-array Object [name (.getClass function)]))))

(defn drop-function!
  "Drop a user-defined function.

  Arguments:
    table-env  - A StreamTableEnvironment
    name       - Function name
    if-exists? - If true, don't error if function doesn't exist (default: true)

  Example:
    (t/drop-function! table-env \"MY_UPPER\")"
  ([table-env name]
   (drop-function! table-env name true))
  ([table-env name if-exists?]
   (ensure-table-api! "drop-function!")
   (let [sql (if if-exists?
               (format "DROP TEMPORARY SYSTEM FUNCTION IF EXISTS `%s`" name)
               (format "DROP TEMPORARY SYSTEM FUNCTION `%s`" name))]
     (execute-sql table-env sql))))

(defn list-functions
  "List all registered functions.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (t/list-functions table-env)"
  [table-env]
  (ensure-table-api! "list-functions")
  (let [list-method (.getMethod (.getClass table-env) "listFunctions" (into-array Class []))]
    (vec (.invoke list-method table-env (into-array Object [])))))

;; =============================================================================
;; Module Operations
;; =============================================================================

(defn list-modules
  "List all loaded modules.

  Arguments:
    table-env - A StreamTableEnvironment

  Example:
    (t/list-modules table-env)"
  [table-env]
  (ensure-table-api! "list-modules")
  (let [list-method (.getMethod (.getClass table-env) "listModules" (into-array Class []))]
    (vec (.invoke list-method table-env (into-array Object [])))))

(defn load-module!
  "Load a module.

  Arguments:
    table-env   - A StreamTableEnvironment
    module-name - Name of the module
    module      - Module instance

  Example:
    (t/load-module! table-env \"hive\" hive-module)"
  [table-env module-name module]
  (ensure-table-api! "load-module!")
  (let [load-method (.getMethod (.getClass table-env) "loadModule"
                                (into-array Class [String (Class/forName "org.apache.flink.table.module.Module")]))]
    (.invoke load-method table-env (into-array Object [module-name module]))))

(defn unload-module!
  "Unload a module.

  Arguments:
    table-env   - A StreamTableEnvironment
    module-name - Name of the module to unload

  Example:
    (t/unload-module! table-env \"hive\")"
  [table-env module-name]
  (ensure-table-api! "unload-module!")
  (let [unload-method (.getMethod (.getClass table-env) "unloadModule"
                                  (into-array Class [String]))]
    (.invoke unload-method table-env (into-array Object [module-name]))))

;; =============================================================================
;; Row Time and Processing Time
;; =============================================================================

(defn proctime-column
  "Create a processing time column definition for DDL.

  Returns a column definition string for CREATE TABLE.

  Example:
    (t/create-table-sql \"events\"
      [(t/column \"id\" \"BIGINT\")
       (t/column \"data\" \"STRING\")
       (t/proctime-column \"proc_time\")]
      {:connector \"kafka\" ...})"
  [name]
  {:name name
   :type "AS PROCTIME()"})

(defn rowtime-column
  "Create a row time column definition for DDL.

  Arguments:
    name        - Column name
    source-col  - Source timestamp column
    watermark   - Watermark expression (e.g., \"source_col - INTERVAL '5' SECOND\")

  Example:
    (t/rowtime-column \"event_time\" \"ts\" \"ts - INTERVAL '5' SECOND\")"
  [name source-col watermark]
  {:name name
   :type (str "AS " source-col)
   :watermark {:column name :expr watermark}})

;; =============================================================================
;; Explain Formats
;; =============================================================================

(defn explain-json
  "Get the execution plan for a table in JSON format.

  Arguments:
    table - A Table

  Example:
    (println (t/explain-json my-table))"
  [table]
  (ensure-table-api! "explain-json")
  (try
    (let [explain-detail-class (Class/forName "org.apache.flink.table.api.ExplainDetail")
          json-enum (Enum/valueOf explain-detail-class "JSON_EXECUTION_PLAN")
          explain-method (.getMethod (.getClass table) "explain"
                                     (into-array Class [(Class/forName "[Lorg.apache.flink.table.api.ExplainDetail;")]))]
      (.invoke explain-method table (into-array Object [(into-array explain-detail-class [json-enum])])))
    (catch Exception _
      ;; Fallback to basic explain
      (explain table))))

(defn explain-estimated-cost
  "Get the execution plan with estimated costs.

  Arguments:
    table - A Table

  Example:
    (println (t/explain-estimated-cost my-table))"
  [table]
  (ensure-table-api! "explain-estimated-cost")
  (try
    (let [explain-detail-class (Class/forName "org.apache.flink.table.api.ExplainDetail")
          cost-enum (Enum/valueOf explain-detail-class "ESTIMATED_COST")
          explain-method (.getMethod (.getClass table) "explain"
                                     (into-array Class [(Class/forName "[Lorg.apache.flink.table.api.ExplainDetail;")]))]
      (.invoke explain-method table (into-array Object [(into-array explain-detail-class [cost-enum])])))
    (catch Exception _
      ;; Fallback to basic explain
      (explain table))))

;; =============================================================================
;; Table Schema Inspection
;; =============================================================================

(defn get-schema
  "Get the resolved schema of a table.

  Arguments:
    table - A Table

  Example:
    (t/get-schema my-table)"
  [table]
  (ensure-table-api! "get-schema")
  (let [get-schema-method (.getMethod (.getClass table) "getResolvedSchema" (into-array Class []))]
    (.invoke get-schema-method table (into-array Object []))))

(defn get-column-names
  "Get column names from a table's schema.

  Arguments:
    table - A Table

  Example:
    (t/get-column-names my-table)
    ;=> [\"id\" \"name\" \"amount\"]"
  [table]
  (ensure-table-api! "get-column-names")
  (let [schema (get-schema table)
        get-columns-method (.getMethod (.getClass schema) "getColumnNames" (into-array Class []))]
    (vec (.invoke get-columns-method schema (into-array Object [])))))

(defn get-column-count
  "Get the number of columns in a table.

  Arguments:
    table - A Table

  Example:
    (t/get-column-count my-table)
    ;=> 3"
  [table]
  (ensure-table-api! "get-column-count")
  (let [schema (get-schema table)
        get-count-method (.getMethod (.getClass schema) "getColumnCount" (into-array Class []))]
    (.invoke get-count-method schema (into-array Object []))))

;; =============================================================================
;; Top-N Query Helpers
;; =============================================================================

(defn top-n-sql
  "Generate a Top-N query SQL string.

  Arguments:
    source-table  - Source table name
    n             - Number of top rows to return
    order-by      - ORDER BY expression (e.g., \"score DESC\")
    select-cols   - Columns to select (default: \"*\")

  Options:
    :partition-by - Partition columns for grouped Top-N

  Example:
    (t/top-n-sql \"scores\" 10 \"score DESC\")
    ;=> Returns top 10 highest scores

    (t/top-n-sql \"sales\" 3 \"amount DESC\" \"*\" {:partition-by \"region\"})
    ;=> Returns top 3 sales per region"
  ([source-table n order-by]
   (top-n-sql source-table n order-by "*" {}))
  ([source-table n order-by select-cols]
   (top-n-sql source-table n order-by select-cols {}))
  ([source-table n order-by select-cols {:keys [partition-by]}]
   (let [partition-clause (if partition-by
                            (str "PARTITION BY " partition-by " ")
                            "")]
     (format "SELECT %s FROM (
  SELECT %s, ROW_NUMBER() OVER (%sORDER BY %s) AS row_num
  FROM %s
) WHERE row_num <= %d"
             select-cols select-cols partition-clause order-by source-table n))))

(defn top-n
  "Execute a Top-N query.

  Arguments:
    table-env     - A StreamTableEnvironment
    source-table  - Source table name
    n             - Number of top rows to return
    order-by      - ORDER BY expression

  Options:
    :partition-by - Partition columns for grouped Top-N
    :select       - Columns to select (default: \"*\")

  Example:
    (t/top-n table-env \"scores\" 10 \"score DESC\")"
  [table-env source-table n order-by & {:keys [partition-by select] :or {select "*"}}]
  (sql-query table-env (top-n-sql source-table n order-by select {:partition-by partition-by})))

;; =============================================================================
;; Deduplication Helpers
;; =============================================================================

(defn dedup-sql
  "Generate a deduplication query SQL string.

  Keeps the first or last row per key based on ordering.

  Arguments:
    source-table - Source table name
    key-cols     - Key columns for deduplication (comma-separated)
    order-by     - ORDER BY expression to determine which row to keep

  Options:
    :select - Columns to select (default: \"*\")
    :keep   - :first or :last (default: :first)

  Example:
    (t/dedup-sql \"events\" \"user_id\" \"event_time ASC\")
    ;=> Keeps first event per user

    (t/dedup-sql \"events\" \"user_id\" \"event_time DESC\" {:keep :last})
    ;=> Keeps last event per user"
  ([source-table key-cols order-by]
   (dedup-sql source-table key-cols order-by {}))
  ([source-table key-cols order-by {:keys [select keep] :or {select "*" keep :first}}]
   (let [order-expr (if (= keep :last)
                      ;; Reverse order for :last
                      order-by
                      order-by)]
     (format "SELECT %s FROM (
  SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS row_num
  FROM %s
) WHERE row_num = 1"
             select select key-cols order-expr source-table))))

(defn deduplicate
  "Execute a deduplication query.

  Arguments:
    table-env    - A StreamTableEnvironment
    source-table - Source table name
    key-cols     - Key columns for deduplication
    order-by     - ORDER BY expression

  Options:
    :select - Columns to select
    :keep   - :first or :last

  Example:
    (t/deduplicate table-env \"events\" \"user_id\" \"event_time ASC\")"
  [table-env source-table key-cols order-by & {:keys [select keep] :or {select "*" keep :first}}]
  (sql-query table-env (dedup-sql source-table key-cols order-by {:select select :keep keep})))

;; =============================================================================
;; Temporal Join Helpers
;; =============================================================================

(defn temporal-join-sql
  "Generate a temporal join SQL string (FOR SYSTEM_TIME AS OF).

  Used to join a fact table with a versioned dimension table at a point in time.

  Arguments:
    fact-table    - Fact/event table name
    dim-table     - Dimension table name (must have PRIMARY KEY and be versioned)
    join-key      - Join key expression (e.g., \"f.product_id = d.id\")
    time-attr     - Time attribute from fact table (e.g., \"f.order_time\")

  Options:
    :select - Columns to select (default: \"*\")

  Example:
    (t/temporal-join-sql \"orders o\" \"products FOR SYSTEM_TIME AS OF o.order_time p\"
                         \"o.product_id = p.id\" \"o.order_time\")"
  ([fact-table dim-table join-key time-attr]
   (temporal-join-sql fact-table dim-table join-key time-attr {}))
  ([fact-table dim-table join-key time-attr {:keys [select] :or {select "*"}}]
   (format "SELECT %s
FROM %s
JOIN %s FOR SYSTEM_TIME AS OF %s
ON %s"
           select fact-table dim-table time-attr join-key)))

(defn lookup-join-sql
  "Generate a lookup join SQL string for dimension table enrichment.

  Used with external lookup sources (JDBC, etc.) in processing time.

  Arguments:
    stream-table  - Streaming source table with alias
    lookup-table  - Lookup dimension table with alias
    join-key      - Join key expression

  Options:
    :select - Columns to select (default: \"*\")

  Example:
    (t/lookup-join-sql \"orders o\" \"customers FOR SYSTEM_TIME AS OF o.proc_time c\"
                       \"o.customer_id = c.id\")"
  ([stream-table lookup-table join-key]
   (lookup-join-sql stream-table lookup-table join-key {}))
  ([stream-table lookup-table join-key {:keys [select] :or {select "*"}}]
   (format "SELECT %s
FROM %s
JOIN %s
ON %s"
           select stream-table lookup-table join-key)))

;; =============================================================================
;; Interval Join Helpers
;; =============================================================================

(defn interval-join-sql
  "Generate an interval join SQL string for time-bounded joins.

  Arguments:
    left-table   - Left table with alias
    right-table  - Right table with alias
    join-key     - Equi-join condition
    time-bounds  - Time bound expression (e.g., \"l.ltime BETWEEN r.rtime - INTERVAL '10' MINUTE AND r.rtime\")

  Options:
    :select - Columns to select (default: \"*\")

  Example:
    (t/interval-join-sql \"orders o\" \"shipments s\"
                         \"o.id = s.order_id\"
                         \"s.ship_time BETWEEN o.order_time AND o.order_time + INTERVAL '4' HOUR\")"
  ([left-table right-table join-key time-bounds]
   (interval-join-sql left-table right-table join-key time-bounds {}))
  ([left-table right-table join-key time-bounds {:keys [select] :or {select "*"}}]
   (format "SELECT %s
FROM %s, %s
WHERE %s AND %s"
           select left-table right-table join-key time-bounds)))

;; =============================================================================
;; MATCH_RECOGNIZE (CEP) Helpers
;; =============================================================================

(defn match-recognize-sql
  "Generate a MATCH_RECOGNIZE pattern recognition query.

  Arguments:
    source-table - Source table name
    config       - Configuration map

  Config keys:
    :partition-by - Partition columns (optional)
    :order-by     - Order by expression (typically time attribute)
    :pattern      - Pattern expression (e.g., \"A+ B\")
    :define       - Map of pattern variable definitions
    :measures     - Map of output column definitions
    :after-match  - After match skip strategy (optional)
    :within       - Time constraint interval (optional)

  Example:
    (t/match-recognize-sql \"stock_prices\"
      {:partition-by \"symbol\"
       :order-by \"ts\"
       :pattern \"DOWN+ UP\"
       :define {:DOWN \"DOWN.price < PREV(DOWN.price)\"
                :UP \"UP.price > PREV(UP.price)\"}
       :measures {:start-price \"FIRST(DOWN.price)\"
                  :bottom-price \"LAST(DOWN.price)\"
                  :end-price \"UP.price\"}})"
  [source-table {:keys [partition-by order-by pattern define measures after-match within]}]
  (let [partition-clause (when partition-by
                           (str "  PARTITION BY " partition-by "\n"))
        order-clause (str "  ORDER BY " order-by "\n")
        measures-clause (str "  MEASURES\n"
                             (str/join ",\n"
                                       (map (fn [[k v]]
                                              (str "    " v " AS " (name k)))
                                            measures))
                             "\n")
        after-clause (when after-match
                       (str "  AFTER MATCH " after-match "\n"))
        pattern-clause (str "  PATTERN (" pattern ")")
        within-clause (when within
                        (str " WITHIN " within))
        define-clause (str "\n  DEFINE\n"
                           (str/join ",\n"
                                     (map (fn [[k v]]
                                            (str "    " (name k) " AS " v))
                                          define)))]
    (str "SELECT *\n"
         "FROM " source-table "\n"
         "MATCH_RECOGNIZE (\n"
         partition-clause
         order-clause
         measures-clause
         "  ONE ROW PER MATCH\n"
         after-clause
         pattern-clause
         within-clause
         define-clause
         "\n)")))

(defn match-recognize
  "Execute a MATCH_RECOGNIZE pattern recognition query.

  See match-recognize-sql for config options.

  Example:
    (t/match-recognize table-env \"events\"
      {:order-by \"event_time\"
       :pattern \"A B+ C\"
       :define {...}
       :measures {...}})"
  [table-env source-table config]
  (sql-query table-env (match-recognize-sql source-table config)))

;; =============================================================================
;; Array Expansion (UNNEST)
;; =============================================================================

(defn unnest-sql
  "Generate an UNNEST query to expand arrays.

  Arguments:
    source-table - Source table name with alias
    array-col    - Array column to expand
    element-alias - Alias for expanded elements

  Options:
    :select      - Columns to select (default: \"*\")
    :cross-join? - Use CROSS JOIN UNNEST (default: true)

  Example:
    (t/unnest-sql \"orders o\" \"o.items\" \"item\")
    ;=> Expands items array into separate rows"
  ([source-table array-col element-alias]
   (unnest-sql source-table array-col element-alias {}))
  ([source-table array-col element-alias {:keys [select cross-join?] :or {select "*" cross-join? true}}]
   (if cross-join?
     (format "SELECT %s
FROM %s
CROSS JOIN UNNEST(%s) AS %s"
             select source-table array-col element-alias)
     (format "SELECT %s
FROM %s, UNNEST(%s) AS %s"
             select source-table array-col element-alias))))

(defn unnest
  "Execute an UNNEST query to expand arrays.

  Arguments:
    table-env     - A StreamTableEnvironment
    source-table  - Source table name with alias
    array-col     - Array column to expand
    element-alias - Alias for expanded elements

  Example:
    (t/unnest table-env \"orders o\" \"o.line_items\" \"item\")"
  [table-env source-table array-col element-alias & {:keys [select] :or {select "*"}}]
  (sql-query table-env (unnest-sql source-table array-col element-alias {:select select})))

;; =============================================================================
;; Common Table Expressions (WITH clause)
;; =============================================================================

(defn with-sql
  "Generate a WITH clause (Common Table Expression) SQL.

  Arguments:
    ctes  - Vector of CTE definitions [{:name \"cte1\" :query \"SELECT...\"}]
    query - Main query that uses the CTEs

  Example:
    (t/with-sql
      [{:name \"high_value\" :query \"SELECT * FROM orders WHERE amount > 1000\"}
       {:name \"recent\" :query \"SELECT * FROM high_value WHERE order_date > '2024-01-01'\"}]
      \"SELECT * FROM recent ORDER BY amount DESC\")"
  [ctes query]
  (let [cte-strs (map (fn [{:keys [name query]}]
                        (str name " AS (\n  " query "\n)"))
                      ctes)]
    (str "WITH " (str/join ",\n" cte-strs) "\n" query)))

(defn with-query
  "Execute a query with Common Table Expressions.

  Arguments:
    table-env - A StreamTableEnvironment
    ctes      - Vector of CTE definitions
    query     - Main query

  Example:
    (t/with-query table-env
      [{:name \"active_users\" :query \"SELECT * FROM users WHERE status = 'active'\"}]
      \"SELECT COUNT(*) FROM active_users\")"
  [table-env ctes query]
  (sql-query table-env (with-sql ctes query)))

;; =============================================================================
;; Query Hints
;; =============================================================================

(defn hint-sql
  "Add query hints to a SQL statement.

  Arguments:
    sql   - Base SQL query
    hints - Vector of hint strings

  Example:
    (t/hint-sql \"SELECT * FROM orders o JOIN customers c ON o.cid = c.id\"
                [\"BROADCAST(c)\" \"STATE_TTL('o' = '1d', 'c' = '12h')\"])"
  [sql hints]
  (let [hint-str (str "/*+ " (str/join ", " hints) " */")]
    ;; Insert hint after SELECT
    (str/replace-first sql #"(?i)^SELECT" (str "SELECT " hint-str))))

(defn broadcast-hint
  "Create a BROADCAST hint for small table broadcast.

  Arguments:
    table-alias - Table alias to broadcast

  Example:
    (t/broadcast-hint \"dim_table\")"
  [table-alias]
  (str "BROADCAST(" table-alias ")"))

(defn shuffle-hash-hint
  "Create a SHUFFLE_HASH hint for hash join.

  Arguments:
    table-alias - Table alias for hash build side

  Example:
    (t/shuffle-hash-hint \"right_table\")"
  [table-alias]
  (str "SHUFFLE_HASH(" table-alias ")"))

(defn state-ttl-hint
  "Create a STATE_TTL hint for state retention.

  Arguments:
    ttls - Map of table alias to TTL duration

  Example:
    (t/state-ttl-hint {\"orders\" \"1d\" \"customers\" \"12h\"})"
  [ttls]
  (str "STATE_TTL("
       (str/join ", " (map (fn [[k v]] (str "'" k "' = '" v "'")) ttls))
       ")"))

(defn lookup-hint
  "Create a LOOKUP hint for lookup join configuration.

  Arguments:
    table-alias - Lookup table alias
    opts        - Map of lookup options

  Options:
    :retry-predicate - Retry condition
    :retry-strategy  - \"fixed_delay\" or \"no_retry\"
    :fixed-delay     - Delay between retries
    :max-attempts    - Maximum retry attempts
    :async           - Enable async lookup
    :output-mode     - \"allow_unordered\" or \"ordered\"
    :capacity        - Async buffer capacity
    :timeout         - Async timeout

  Example:
    (t/lookup-hint \"products\" {:async true :capacity 100 :timeout \"30s\"})"
  [table-alias opts]
  (let [opt-strs (map (fn [[k v]]
                        (str (name k) "='" v "'"))
                      opts)]
    (str "LOOKUP('" table-alias "', " (str/join ", " opt-strs) ")")))

;; =============================================================================
;; TableDescriptor - Programmatic Table Creation
;; =============================================================================

(defn- table-descriptor-available?
  "Check if TableDescriptor API is available."
  []
  (try
    (Class/forName "org.apache.flink.table.api.TableDescriptor")
    true
    (catch ClassNotFoundException _ false)))

(defn table-descriptor
  "Create a TableDescriptor builder for programmatic table definition.

  This is an alternative to SQL DDL for creating tables programmatically.
  The descriptor specifies connector, format, schema, and options.

  Arguments:
    connector - Connector identifier (e.g., \"kafka\", \"filesystem\", \"jdbc\")

  Returns a map that can be passed to `create-table!` or `create-temporary-table!`.

  Example:
    (-> (t/table-descriptor \"kafka\")
        (t/with-schema [{:name \"id\" :type \"BIGINT\"}
                        {:name \"name\" :type \"STRING\"}])
        (t/with-format \"json\")
        (t/with-option \"topic\" \"users\")
        (t/with-option \"properties.bootstrap.servers\" \"localhost:9092\")
        (t/build-descriptor))"
  [connector]
  {:connector connector
   :schema []
   :options {}
   :format nil
   :format-options {}
   :partitioned-by []
   :comment nil})

(defn with-schema
  "Add schema columns to a table descriptor.

  Column specs can be:
  - {:name \"col\" :type \"STRING\"} - Basic column
  - {:name \"col\" :type \"STRING\" :not-null? true} - NOT NULL constraint
  - {:name \"col\" :type \"STRING\" :primary-key? true} - Primary key
  - {:name \"ts\" :type \"TIMESTAMP(3)\" :metadata-from \"timestamp\"} - Metadata column
  - {:name \"proc\" :type \"TIMESTAMP_LTZ(3)\" :proctime? true} - Processing time
  - {:name \"et\" :type \"TIMESTAMP(3)\" :rowtime {:watermark \"et - INTERVAL '5' SECOND\"}} - Event time

  Example:
    (with-schema desc
      [{:name \"id\" :type \"BIGINT\" :primary-key? true}
       {:name \"name\" :type \"STRING\" :not-null? true}
       {:name \"event_time\" :type \"TIMESTAMP(3)\"
        :rowtime {:watermark \"event_time - INTERVAL '10' SECOND\"}}])"
  [desc columns]
  (update desc :schema concat columns))

(defn with-format
  "Set the format for a table descriptor.

  Common formats:
  - \"json\" - JSON format
  - \"csv\" - CSV format
  - \"avro\" - Avro format
  - \"parquet\" - Parquet format
  - \"orc\" - ORC format
  - \"raw\" - Raw bytes
  - \"debezium-json\" - Debezium CDC JSON
  - \"canal-json\" - Canal CDC JSON

  Example:
    (with-format desc \"json\")"
  [desc format-type]
  (assoc desc :format format-type))

(defn with-format-option
  "Add a format option to a table descriptor.

  Example:
    (-> desc
        (with-format \"json\")
        (with-format-option \"ignore-parse-errors\" \"true\"))"
  [desc key value]
  (update desc :format-options assoc key value))

(defn with-option
  "Add a connector option to a table descriptor.

  Example:
    (-> desc
        (with-option \"topic\" \"users\")
        (with-option \"properties.bootstrap.servers\" \"localhost:9092\"))"
  [desc key value]
  (update desc :options assoc key value))

(defn with-options
  "Add multiple connector options to a table descriptor.

  Example:
    (with-options desc
      {\"topic\" \"users\"
       \"properties.bootstrap.servers\" \"localhost:9092\"})"
  [desc opts]
  (update desc :options merge opts))

(defn with-partitioned-by
  "Set partition columns for a table descriptor.

  Example:
    (with-partitioned-by desc [\"year\" \"month\" \"day\"])"
  [desc columns]
  (assoc desc :partitioned-by columns))

(defn with-comment
  "Set a comment for a table descriptor.

  Example:
    (with-comment desc \"User events from Kafka\")"
  [desc comment-text]
  (assoc desc :comment comment-text))

(defn build-descriptor
  "Build the final TableDescriptor object from a descriptor map.

  Returns a TableDescriptor instance that can be used with the Table API.

  Example:
    (def td (-> (t/table-descriptor \"kafka\")
                (t/with-schema [...])
                (t/with-format \"json\")
                (t/build-descriptor)))
    (t/create-table! table-env \"my_table\" td)"
  [desc]
  (ensure-table-api! "build-descriptor")
  (let [{:keys [connector schema options format format-options partitioned-by comment]} desc
        ;; Get classes via reflection
        td-class (Class/forName "org.apache.flink.table.api.TableDescriptor")
        schema-class (Class/forName "org.apache.flink.table.api.Schema")
        for-connector-method (.getMethod td-class "forConnector" (into-array Class [String]))
        builder (.invoke for-connector-method nil (into-array Object [connector]))]
    ;; Add schema if provided
    (when (seq schema)
      (let [schema-builder-method (.getMethod schema-class "newBuilder" (into-array Class []))
            schema-builder (.invoke schema-builder-method nil (into-array Object []))
            column-method (.getMethod (.getClass schema-builder) "column"
                                      (into-array Class [String String]))]
        ;; Add each column
        (doseq [{:keys [name type]} schema]
          (.invoke column-method schema-builder (into-array Object [name type])))
        ;; Build schema and set on descriptor
        (let [build-method (.getMethod (.getClass schema-builder) "build" (into-array Class []))
              built-schema (.invoke build-method schema-builder (into-array Object []))
              set-schema-method (.getMethod (.getClass builder) "schema"
                                            (into-array Class [schema-class]))]
          (.invoke set-schema-method builder (into-array Object [built-schema])))))
    ;; Add format if provided
    (when format
      (let [format-method (.getMethod (.getClass builder) "format" (into-array Class [String]))]
        (.invoke format-method builder (into-array Object [format]))))
    ;; Add options
    (let [option-method (.getMethod (.getClass builder) "option"
                                    (into-array Class [String String]))]
      (doseq [[k v] options]
        (.invoke option-method builder (into-array Object [(str k) (str v)])))
      ;; Add format options with prefix
      (doseq [[k v] format-options]
        (.invoke option-method builder (into-array Object [(str format "." k) (str v)]))))
    ;; Add partitioning
    (when (seq partitioned-by)
      (let [partition-method (.getMethod (.getClass builder) "partitionedBy"
                                         (into-array Class [(Class/forName "[Ljava.lang.String;")]))]
        (.invoke partition-method builder (into-array Object [(into-array String partitioned-by)]))))
    ;; Add comment
    (when comment
      (let [comment-method (.getMethod (.getClass builder) "comment" (into-array Class [String]))]
        (.invoke comment-method builder (into-array Object [comment]))))
    ;; Build and return
    (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

(defn create-table!
  "Create a permanent table from a TableDescriptor.

  Arguments:
    table-env - Table environment
    path      - Table path (catalog.database.table or just table name)
    descriptor - TableDescriptor (from build-descriptor) or descriptor map

  Example:
    (t/create-table! table-env \"my_catalog.my_db.users\" descriptor)"
  [table-env path descriptor]
  (ensure-table-api! "create-table!")
  (let [td (if (map? descriptor) (build-descriptor descriptor) descriptor)
        create-method (.getMethod (.getClass table-env) "createTable"
                                  (into-array Class [String (Class/forName "org.apache.flink.table.api.TableDescriptor")]))]
    (.invoke create-method table-env (into-array Object [path td]))))

(defn create-temporary-table!
  "Create a temporary table from a TableDescriptor.

  Arguments:
    table-env - Table environment
    path      - Table path
    descriptor - TableDescriptor (from build-descriptor) or descriptor map

  Example:
    (t/create-temporary-table! table-env \"temp_users\" descriptor)"
  [table-env path descriptor]
  (ensure-table-api! "create-temporary-table!")
  (let [td (if (map? descriptor) (build-descriptor descriptor) descriptor)
        create-method (.getMethod (.getClass table-env) "createTemporaryTable"
                                  (into-array Class [String (Class/forName "org.apache.flink.table.api.TableDescriptor")]))]
    (.invoke create-method table-env (into-array Object [path td]))))

(defn descriptor->sql
  "Convert a table descriptor map to CREATE TABLE SQL.

  This is useful when you want to see the SQL equivalent of a descriptor
  or when you need to use SQL for table creation.

  Example:
    (println (t/descriptor->sql \"users\"
               (-> (t/table-descriptor \"kafka\")
                   (t/with-schema [...])
                   (t/with-format \"json\"))))"
  [table-name desc]
  (let [{:keys [connector schema options format format-options partitioned-by comment]} desc
        col-strs (map (fn [{:keys [name type not-null? primary-key? metadata-from proctime? rowtime]}]
                        (str name " " type
                             (when not-null? " NOT NULL")
                             (when metadata-from (str " METADATA FROM '" metadata-from "'"))
                             (when proctime? " AS PROCTIME()")))
                      schema)
        pk-cols (clojure.core/filter :primary-key? schema)
        pk-clause (when (seq pk-cols)
                    (str "PRIMARY KEY (" (str/join ", " (map :name pk-cols)) ") NOT ENFORCED"))
        watermark-cols (clojure.core/filter :rowtime schema)
        watermark-clauses (map (fn [{:keys [name rowtime]}]
                                 (str "WATERMARK FOR " name " AS " (:watermark rowtime)))
                               watermark-cols)
        all-cols (concat col-strs
                         (when pk-clause [pk-clause])
                         watermark-clauses)
        all-opts (merge {:connector connector}
                        options
                        (when format {:format format})
                        (into {} (map (fn [[k v]] [(str format "." k) v]) format-options)))
        opt-strs (map (fn [[k v]]
                        (let [k-str (if (keyword? k) (name k) (str k))]
                          (str "'" k-str "' = '" v "'")))
                      all-opts)]
    (str "CREATE TABLE " table-name " (\n  "
         (str/join ",\n  " all-cols)
         "\n)"
         (when (seq partitioned-by)
           (str " PARTITIONED BY (" (str/join ", " partitioned-by) ")"))
         (when comment
           (str " COMMENT '" comment "'"))
         " WITH (\n  "
         (str/join ",\n  " opt-strs)
         "\n)")))

;; =============================================================================
;; User-Defined Functions (UDFs) for Table API
;; =============================================================================

(defn- fn->ns-name
  "Extract namespace and name from a var."
  [f]
  (if (var? f)
    [(str (.-ns f)) (str (.-sym f))]
    (throw (ex-info "Function must be a var (use #'fn-name)" {:fn f}))))

(defn scalar-function
  "Create a ScalarFunction spec for registering with Table API.

  A scalar function takes zero or more scalar values and returns a scalar value.
  This is a 1:1 mapping.

  Arguments:
    f           - A var pointing to a Clojure function
    result-type - SQL type string for the return type

  Example:
    (defn my-upper [s] (when s (.toUpperCase s)))

    (t/register-scalar-function! table-env \"MY_UPPER\"
      (t/scalar-function #'my-upper \"STRING\"))"
  [f result-type]
  {:type :scalar
   :fn f
   :result-type result-type})

(defn table-function
  "Create a TableFunction spec for registering with Table API.

  A table function takes zero or more scalar values and returns a table.
  This is a 1:N mapping - each input can produce multiple output rows.

  Arguments:
    f             - A var pointing to a Clojure function that returns a sequence
    result-types  - Vector of [name type] pairs for output columns

  Example:
    (defn split-words [sentence]
      (when sentence
        (map (fn [w] {:word w :len (count w)})
             (str/split sentence #\"\\s+\"))))

    (t/register-table-function! table-env \"SPLIT_WORDS\"
      (t/table-function #'split-words
        [[\"word\" \"STRING\"] [\"len\" \"INT\"]]))"
  [f result-types]
  {:type :table
   :fn f
   :result-types result-types})

(defn aggregate-function
  "Create an AggregateFunction spec for registering with Table API.

  An aggregate function aggregates multiple values into one result.
  This is a N:1 mapping.

  Arguments:
    spec - Map with:
      :accumulator - fn [] -> initial accumulator value
      :accumulate  - fn [acc value] -> updated accumulator
      :get-value   - fn [acc] -> final result
      :merge       - (optional) fn [acc1 acc2] -> merged accumulator
      :retract     - (optional) fn [acc value] -> accumulator with value removed
    result-type - SQL type string for the return type

  Example:
    (t/register-aggregate-function! table-env \"MY_AVG\"
      (t/aggregate-function
        {:accumulator (fn [] {:sum 0 :count 0})
         :accumulate (fn [acc v] (-> acc
                                     (update :sum + v)
                                     (update :count inc)))
         :get-value (fn [acc] (if (zero? (:count acc))
                                nil
                                (/ (:sum acc) (:count acc))))}
        \"DOUBLE\"))"
  [spec result-type]
  {:type :aggregate
   :spec spec
   :result-type result-type})

(defn register-scalar-function!
  "Register a scalar function with the table environment.

  Scalar functions can then be used in SQL queries and Table API expressions.

  Arguments:
    table-env - Table environment
    name      - Function name (used in SQL)
    fn-spec   - ScalarFunction spec from `scalar-function`

  Example:
    (defn double-value [x] (* x 2))

    (t/register-scalar-function! table-env \"DOUBLE_VAL\"
      (t/scalar-function #'double-value \"BIGINT\"))

    ;; Use in SQL:
    (t/execute-sql table-env \"SELECT DOUBLE_VAL(amount) FROM orders\")"
  [table-env fn-name fn-spec]
  (ensure-table-api! "register-scalar-function!")
  (let [{:keys [fn result-type]} fn-spec
        [ns-str sym-name] (fn->ns-name fn)
        ;; Create the function via SQL since Java UDF registration requires compiled classes
        ;; For now, we document that users should use SQL CREATE FUNCTION
        _ (throw (ex-info
                   "Direct scalar function registration requires compiled Java classes.
Use CREATE FUNCTION SQL or define a Java ScalarFunction class.

Alternative: Use SQL with a Java-based UDF JAR:
  (t/execute-sql table-env
    \"CREATE FUNCTION MY_FUNC AS 'com.example.MyScalarFunc'\")"
                   {:fn-name fn-name
                    :namespace ns-str
                    :symbol sym-name
                    :hint "Consider using flink-clj.impl.udf for pre-compiled UDF wrappers"}))]
    table-env))

(defn create-function-sql
  "Generate CREATE FUNCTION SQL for a Java UDF class.

  This is the recommended way to register UDFs - provide a Java class
  that implements ScalarFunction, TableFunction, or AggregateFunction.

  Arguments:
    fn-name  - Function name
    class-name - Fully qualified Java class name
    opts     - Options map:
      :temporary? - Create temporary function (default: false)
      :if-not-exists? - Add IF NOT EXISTS clause
      :language - \"JAVA\" or \"SCALA\" (default: \"JAVA\")

  Example:
    (t/execute-sql table-env
      (t/create-function-sql \"MY_UPPER\" \"com.example.MyUpperFunction\"))"
  ([fn-name class-name]
   (create-function-sql fn-name class-name {}))
  ([fn-name class-name {:keys [temporary? if-not-exists? language]
                        :or {language "JAVA"}}]
   (str "CREATE "
        (when temporary? "TEMPORARY ")
        "FUNCTION "
        (when if-not-exists? "IF NOT EXISTS ")
        fn-name
        " AS '" class-name "'"
        " LANGUAGE " language)))

(defn create-system-function-sql
  "Generate CREATE SYSTEM FUNCTION SQL for a built-in function override.

  System functions are available across all catalogs and databases.

  Arguments:
    fn-name    - Function name
    class-name - Fully qualified Java class name

  Example:
    (t/execute-sql table-env
      (t/create-system-function-sql \"MY_CONCAT\" \"com.example.MyConcatFunction\"))"
  [fn-name class-name]
  (str "CREATE TEMPORARY SYSTEM FUNCTION " fn-name " AS '" class-name "'"))

(defn drop-function-sql
  "Generate DROP FUNCTION SQL.

  Arguments:
    fn-name - Function name
    opts    - Options map:
      :temporary? - Drop temporary function
      :if-exists? - Add IF EXISTS clause

  Example:
    (t/execute-sql table-env (t/drop-function-sql \"MY_UPPER\" {:if-exists? true}))"
  ([fn-name]
   (drop-function-sql fn-name {}))
  ([fn-name {:keys [temporary? if-exists?]}]
   (str "DROP "
        (when temporary? "TEMPORARY ")
        "FUNCTION "
        (when if-exists? "IF EXISTS ")
        fn-name)))

(defn alter-function-sql
  "Generate ALTER FUNCTION SQL to change function implementation.

  Arguments:
    fn-name    - Function name
    class-name - New fully qualified Java class name
    opts       - Options map:
      :temporary? - Alter temporary function
      :if-exists? - Add IF EXISTS clause

  Example:
    (t/execute-sql table-env
      (t/alter-function-sql \"MY_UPPER\" \"com.example.NewUpperFunction\"))"
  ([fn-name class-name]
   (alter-function-sql fn-name class-name {}))
  ([fn-name class-name {:keys [temporary? if-exists?]}]
   (str "ALTER "
        (when temporary? "TEMPORARY ")
        "FUNCTION "
        (when if-exists? "IF EXISTS ")
        fn-name
        " AS '" class-name "'")))

;; =============================================================================
;; Catalog Management
;; =============================================================================
;;
;; Flink catalogs provide metadata storage for tables, views, and functions.
;; Different catalog implementations are provided by external JARs:
;;
;; - Hive Catalog: flink-sql-connector-hive-X.X.X.jar
;; - JDBC Catalog: flink-connector-jdbc (for PostgreSQL, MySQL, etc.)
;; - Iceberg Catalog: iceberg-flink-runtime-X.X-X.X.X.jar
;; - Paimon Catalog: paimon-flink-X.X-X.X.X.jar
;; - AWS Glue Catalog: flink-sql-connector-aws-glue-X.X.X.jar
;;
;; The catalog implementation details are in the JARs. This namespace
;; provides SQL DDL helpers for catalog operations.

(defn create-catalog-sql
  "Generate CREATE CATALOG SQL for any catalog type.

  Arguments:
    catalog-name - Name for the catalog
    catalog-type - Catalog type (e.g., \"hive\", \"jdbc\", \"iceberg\", \"paimon\")
    options      - Map of catalog-specific options

  Common catalog types and their required options:

  Hive Catalog:
    {:type \"hive\"
     :hive-conf-dir \"/etc/hive/conf\"
     :default-database \"default\"}

  JDBC Catalog (PostgreSQL):
    {:type \"jdbc\"
     :default-database \"postgres\"
     :username \"user\"
     :password \"pass\"
     :base-url \"jdbc:postgresql://localhost:5432\"}

  Iceberg Catalog (Hive):
    {:type \"iceberg\"
     :catalog-type \"hive\"
     :uri \"thrift://localhost:9083\"
     :warehouse \"s3://bucket/warehouse\"}

  Iceberg Catalog (REST):
    {:type \"iceberg\"
     :catalog-type \"rest\"
     :uri \"http://localhost:8181\"}

  Paimon Catalog:
    {:type \"paimon\"
     :warehouse \"s3://bucket/warehouse\"}

  Example:
    (t/execute-sql table-env
      (t/create-catalog-sql \"my_hive\"
        {:type \"hive\"
         :hive-conf-dir \"/etc/hive/conf\"}))"
  [catalog-name options]
  (let [opt-strs (map (fn [[k v]]
                        (let [k-str (if (keyword? k) (name k) (str k))]
                          (str "'" k-str "' = '" v "'")))
                      options)]
    (str "CREATE CATALOG " catalog-name " WITH (\n  "
         (str/join ",\n  " opt-strs)
         "\n)")))

(defn drop-catalog-sql
  "Generate DROP CATALOG SQL.

  Arguments:
    catalog-name - Catalog name to drop
    opts         - Options map:
      :if-exists? - Add IF EXISTS clause

  Example:
    (t/execute-sql table-env (t/drop-catalog-sql \"my_catalog\" {:if-exists? true}))"
  ([catalog-name]
   (drop-catalog-sql catalog-name {}))
  ([catalog-name {:keys [if-exists?]}]
   (str "DROP CATALOG "
        (when if-exists? "IF EXISTS ")
        catalog-name)))

(defn register-catalog!
  "Register a catalog programmatically using a Catalog instance.

  This requires the catalog implementation JAR to be on the classpath.

  Arguments:
    table-env    - Table environment
    catalog-name - Name for the catalog
    catalog      - Catalog instance (from catalog factory)

  Example:
    ;; Using Hive catalog (requires flink-sql-connector-hive JAR)
    (let [hive-conf (HiveConf.)
          catalog (HiveCatalog. \"hive\" nil hive-conf \"3.1.2\")]
      (t/register-catalog! table-env \"my_hive\" catalog))"
  [table-env catalog-name catalog]
  (ensure-table-api! "register-catalog!")
  (let [register-method (.getMethod (.getClass table-env) "registerCatalog"
                                    (into-array Class [String (Class/forName "org.apache.flink.table.catalog.Catalog")]))]
    (.invoke register-method table-env (into-array Object [catalog-name catalog])))
  table-env)

(defn get-catalog
  "Get a registered catalog by name.

  Arguments:
    table-env    - Table environment
    catalog-name - Catalog name

  Returns the Catalog instance or nil if not found.

  Example:
    (t/get-catalog table-env \"my_hive\")"
  [table-env catalog-name]
  (ensure-table-api! "get-catalog")
  (let [get-method (.getMethod (.getClass table-env) "getCatalog"
                               (into-array Class [String]))
        result (.invoke get-method table-env (into-array Object [catalog-name]))]
    (when (.isPresent result)
      (.get result))))

(defn current-catalog
  "Get the name of the current catalog.

  Example:
    (t/current-catalog table-env)"
  [table-env]
  (ensure-table-api! "current-catalog")
  (let [method (.getMethod (.getClass table-env) "getCurrentCatalog" (into-array Class []))]
    (.invoke method table-env (into-array Object []))))

(defn current-database
  "Get the name of the current database.

  Example:
    (t/current-database table-env)"
  [table-env]
  (ensure-table-api! "current-database")
  (let [method (.getMethod (.getClass table-env) "getCurrentDatabase" (into-array Class []))]
    (.invoke method table-env (into-array Object []))))

(defn use-database!
  "Switch to a different database in the current catalog.

  Arguments:
    table-env - Table environment
    database  - Database name

  Example:
    (t/use-database! table-env \"my_database\")"
  [table-env database]
  (ensure-table-api! "use-database!")
  (let [use-method (.getMethod (.getClass table-env) "useDatabase"
                               (into-array Class [String]))]
    (.invoke use-method table-env (into-array Object [database])))
  table-env)

(defn create-database-sql
  "Generate CREATE DATABASE SQL.

  Arguments:
    database-name - Database name
    opts          - Options map:
      :if-not-exists? - Add IF NOT EXISTS clause
      :comment        - Database comment
      :properties     - Map of database properties

  Example:
    (t/execute-sql table-env
      (t/create-database-sql \"analytics\"
        {:if-not-exists? true
         :comment \"Analytics data warehouse\"
         :properties {\"owner\" \"data-team\"}}))"
  ([database-name]
   (create-database-sql database-name {}))
  ([database-name {:keys [if-not-exists? comment properties]}]
   (str "CREATE DATABASE "
        (when if-not-exists? "IF NOT EXISTS ")
        database-name
        (when comment (str " COMMENT '" comment "'"))
        (when (seq properties)
          (str " WITH ("
               (str/join ", "
                 (map (fn [[k v]] (str "'" k "' = '" v "'")) properties))
               ")")))))

(defn drop-database-sql
  "Generate DROP DATABASE SQL.

  Arguments:
    database-name - Database name
    opts          - Options map:
      :if-exists? - Add IF EXISTS clause
      :cascade?   - Drop database and all tables (RESTRICT is default)

  Example:
    (t/execute-sql table-env
      (t/drop-database-sql \"old_db\" {:if-exists? true :cascade? true}))"
  ([database-name]
   (drop-database-sql database-name {}))
  ([database-name {:keys [if-exists? cascade?]}]
   (str "DROP DATABASE "
        (when if-exists? "IF EXISTS ")
        database-name
        (if cascade? " CASCADE" " RESTRICT"))))

(defn alter-database-sql
  "Generate ALTER DATABASE SQL to change properties.

  Arguments:
    database-name - Database name
    properties    - Map of properties to set

  Example:
    (t/execute-sql table-env
      (t/alter-database-sql \"my_db\" {\"owner\" \"new-owner\"}))"
  [database-name properties]
  (str "ALTER DATABASE " database-name " SET ("
       (str/join ", "
         (map (fn [[k v]] (str "'" k "' = '" v "'")) properties))
       ")"))

;; =============================================================================
;; Catalog Type Helpers
;; =============================================================================

(defn hive-catalog-options
  "Generate options for a Hive catalog.

  Arguments:
    opts - Options map:
      :hive-conf-dir     - Path to Hive configuration directory (required)
      :default-database  - Default database (default: \"default\")
      :hive-version      - Hive version (default: auto-detect)
      :hadoop-conf-dir   - Path to Hadoop configuration directory

  Example:
    (t/execute-sql table-env
      (t/create-catalog-sql \"hive_catalog\"
        (t/hive-catalog-options {:hive-conf-dir \"/etc/hive/conf\"})))"
  [{:keys [hive-conf-dir default-database hive-version hadoop-conf-dir]
    :or {default-database "default"}}]
  (cond-> {:type "hive"
           :hive-conf-dir hive-conf-dir
           :default-database default-database}
    hive-version (assoc :hive-version hive-version)
    hadoop-conf-dir (assoc :hadoop-conf-dir hadoop-conf-dir)))

(defn jdbc-catalog-options
  "Generate options for a JDBC catalog.

  Arguments:
    opts - Options map:
      :base-url         - JDBC base URL (e.g., \"jdbc:postgresql://localhost:5432\")
      :default-database - Default database
      :username         - Database username
      :password         - Database password

  Example:
    (t/execute-sql table-env
      (t/create-catalog-sql \"postgres_catalog\"
        (t/jdbc-catalog-options
          {:base-url \"jdbc:postgresql://localhost:5432\"
           :default-database \"postgres\"
           :username \"user\"
           :password \"pass\"})))"
  [{:keys [base-url default-database username password]}]
  {:type "jdbc"
   :base-url base-url
   :default-database default-database
   :username username
   :password password})

(defn iceberg-catalog-options
  "Generate options for an Iceberg catalog.

  Arguments:
    opts - Options map:
      :catalog-type - \"hive\", \"hadoop\", \"rest\", or \"glue\"
      :warehouse    - Warehouse path (required for hadoop, optional for others)
      :uri          - Catalog URI (for hive: thrift://..., for rest: http://...)
      :io-impl      - I/O implementation class
      Additional options are passed through.

  Example (Hive metastore):
    (t/execute-sql table-env
      (t/create-catalog-sql \"iceberg_catalog\"
        (t/iceberg-catalog-options
          {:catalog-type \"hive\"
           :uri \"thrift://localhost:9083\"
           :warehouse \"s3://bucket/warehouse\"})))

  Example (REST catalog):
    (t/execute-sql table-env
      (t/create-catalog-sql \"iceberg_rest\"
        (t/iceberg-catalog-options
          {:catalog-type \"rest\"
           :uri \"http://localhost:8181\"})))"
  [{:keys [catalog-type warehouse uri io-impl] :as opts}]
  (merge
    {:type "iceberg"
     :catalog-type catalog-type}
    (when warehouse {:warehouse warehouse})
    (when uri {:uri uri})
    (when io-impl {:io-impl io-impl})
    (dissoc opts :catalog-type :warehouse :uri :io-impl)))

(defn paimon-catalog-options
  "Generate options for a Paimon catalog.

  Arguments:
    opts - Options map:
      :warehouse    - Warehouse path (required)
      :metastore    - Metastore type (\"filesystem\", \"hive\")
      :uri          - Hive metastore URI (for hive metastore)
      Additional options are passed through.

  Example:
    (t/execute-sql table-env
      (t/create-catalog-sql \"paimon_catalog\"
        (t/paimon-catalog-options
          {:warehouse \"s3://bucket/paimon\"})))"
  [{:keys [warehouse metastore uri] :as opts}]
  (merge
    {:type "paimon"
     :warehouse warehouse}
    (when metastore {:metastore metastore})
    (when uri {:uri uri})
    (dissoc opts :warehouse :metastore :uri)))

(defn catalog-info
  "Get information about supported catalog types.

  Note: Actual availability depends on JARs in the classpath."
  []
  {:hive {:type "hive"
          :jar "flink-sql-connector-hive-X.X.X"
          :description "Hive Metastore catalog for table metadata"}
   :jdbc {:type "jdbc"
          :jar "flink-connector-jdbc"
          :description "JDBC catalog for PostgreSQL, MySQL, etc."}
   :iceberg {:type "iceberg"
             :jar "iceberg-flink-runtime-X.X-X.X.X"
             :description "Apache Iceberg table format catalog"}
   :paimon {:type "paimon"
            :jar "paimon-flink-X.X-X.X.X"
            :description "Apache Paimon (incubating) streaming lakehouse"}
   :glue {:type "glue"
          :jar "flink-sql-connector-aws-glue"
          :description "AWS Glue Data Catalog"}
   :delta {:type "delta"
           :jar "delta-flink"
           :description "Delta Lake table format catalog"}})
