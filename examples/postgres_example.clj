(ns postgres-example
  "Example: Using JDBC connector with PostgreSQL and dynamic JAR loading.

  This example demonstrates how to:
  1. Load JDBC connector JARs dynamically
  2. Read from a Kafka topic
  3. Transform the stream
  4. Write results to PostgreSQL using JDBC sink

  Prerequisites:
    docker compose up -d   # Start Kafka, Zookeeper, PostgreSQL

  Usage from REPL:
    (require '[examples.postgres-example :as pg-ex])
    (pg-ex/run-kafka-to-postgres-example)"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str])
  (:import [java.sql DriverManager]
           [java.util Properties]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def postgres-config
  "PostgreSQL connection configuration for local Docker setup."
  {:host "localhost"
   :port 5432
   :database "flink_test"
   :user "flink"
   :password "flink"})

(def jdbc-url
  (format "jdbc:postgresql://%s:%d/%s"
          (:host postgres-config)
          (:port postgres-config)
          (:database postgres-config)))

(def kafka-config
  {:bootstrap-servers "localhost:9092"
   :group-id "flink-postgres-example"})

;; Connector JAR URLs for Flink 1.20
(def jdbc-connector-jars
  "JDBC connector JARs needed for Flink."
  [(env/maven-jar "org.apache.flink" "flink-connector-jdbc" "3.2.0-1.19")
   (env/maven-jar "org.postgresql" "postgresql" "42.7.1")])

;; =============================================================================
;; PostgreSQL Helpers
;; =============================================================================

(defn get-connection
  "Get a JDBC connection to PostgreSQL."
  []
  (DriverManager/getConnection
    jdbc-url
    (:user postgres-config)
    (:password postgres-config)))

(defn query
  "Execute a query and return results as a vector of maps."
  [sql]
  (with-open [conn (get-connection)
              stmt (.createStatement conn)
              rs (.executeQuery stmt sql)]
    (let [meta (.getMetaData rs)
          cols (for [i (range 1 (inc (.getColumnCount meta)))]
                 (keyword (.getColumnName meta i)))]
      (loop [results []]
        (if (.next rs)
          (recur (conj results
                       (into {}
                             (map-indexed
                               (fn [i col]
                                 [col (.getObject rs (inc i))])
                               cols))))
          results)))))

(defn execute!
  "Execute a SQL statement."
  [sql]
  (with-open [conn (get-connection)
              stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn show-word-counts
  "Display current word counts from PostgreSQL."
  []
  (println "\nWord Counts in PostgreSQL:")
  (println "=" 40)
  (doseq [{:keys [word count]} (query "SELECT word, count FROM word_counts ORDER BY count DESC LIMIT 20")]
    (printf "  %-20s %d%n" word count))
  (println))

(defn show-events
  "Display recent events from PostgreSQL."
  []
  (println "\nRecent Events in PostgreSQL:")
  (println "=" 50)
  (doseq [{:keys [id event_type user_id event_time]}
          (query "SELECT id, event_type, user_id, event_time FROM events ORDER BY id DESC LIMIT 10")]
    (printf "  [%d] %-15s user=%-10s at %s%n" id event_type user_id event_time))
  (println))

(defn clear-word-counts!
  "Clear the word_counts table."
  []
  (execute! "TRUNCATE TABLE word_counts")
  (println "Cleared word_counts table"))

;; =============================================================================
;; JDBC Sink Builder (using Flink's JDBC connector)
;; =============================================================================

(defn- jdbc-sink-available?
  "Check if JDBC sink classes are available."
  []
  (try
    (Class/forName "org.apache.flink.connector.jdbc.JdbcSink")
    true
    (catch ClassNotFoundException _ false)))

(defn create-jdbc-sink-for-word-counts
  "Create a JDBC sink for upserting word counts.

  This uses Flink's JDBC connector to upsert (INSERT ON CONFLICT UPDATE)
  word count results into PostgreSQL."
  []
  (if (jdbc-sink-available?)
    (let [jdbc-sink-class (Class/forName "org.apache.flink.connector.jdbc.JdbcSink")
          jdbc-conn-class (Class/forName "org.apache.flink.connector.jdbc.JdbcConnectionOptions$JdbcConnectionOptionsBuilder")

          ;; Create connection options
          conn-builder (.getDeclaredConstructor jdbc-conn-class (into-array Class []))
          conn-opts (-> (.newInstance conn-builder (into-array Object []))
                        (.withUrl jdbc-url)
                        (.withDriverName "org.postgresql.Driver")
                        (.withUsername (:user postgres-config))
                        (.withPassword (:password postgres-config))
                        (.build))

          ;; SQL for upsert
          upsert-sql "INSERT INTO word_counts (word, count, updated_at) VALUES (?, ?, NOW())
                      ON CONFLICT (word) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()"

          ;; Create sink using reflection
          sink-method (.getMethod jdbc-sink-class "sink"
                                  (into-array Class [String
                                                     (Class/forName "org.apache.flink.connector.jdbc.JdbcStatementBuilder")
                                                     (Class/forName "org.apache.flink.connector.jdbc.JdbcConnectionOptions")]))

          ;; Create statement builder (would need Java interop for real implementation)
          ]
      ;; Note: Full JDBC sink creation requires Java lambdas
      ;; This is a placeholder showing the pattern
      (throw (ex-info "JDBC sink requires Java-based statement builder. See examples/JdbcWordCountSink.java"
                      {:suggestion "Use a Java class for the JDBC statement builder"})))
    (throw (ex-info "JDBC connector not available. Add flink-connector-jdbc dependency."
                    {:jars jdbc-connector-jars}))))

;; =============================================================================
;; Stream Processing Functions
;; =============================================================================

(defn parse-event
  "Parse Kafka message into event map."
  [msg]
  {:event-type "word"
   :user-id "system"
   :word (str/lower-case (str/trim (str msg)))})

(defn event-to-word-pair
  "Extract word pair from event."
  [{:keys [word]}]
  [word 1])

(defn sum-pairs
  "Sum two word count pairs."
  [[word c1] [_ c2]]
  [word (+ c1 c2)])

(defn get-word-key
  [[word _]] word)

;; =============================================================================
;; Example: Print Sink (for demonstration without JDBC connector)
;; =============================================================================

(defn format-result
  "Format word count for printing."
  [[word count]]
  (format "Word: %-20s Count: %d" word count))

(defn run-print-word-count-example
  "Run word count that prints results (no JDBC dependency).

  This is useful for testing the pipeline without the JDBC connector."
  []
  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (:bootstrap-servers kafka-config)
                  :topics ["flink-input"]
                  :group-id (:group-id kafka-config)
                  :starting-offsets :earliest
                  :value-format :string})]

    (-> flink-env
        (flink/from-source source "Kafka Source")
        (stream/flink-map #'parse-event)
        (stream/flink-map #'event-to-word-pair)
        (keyed/key-by #'get-word-key {:key-type :string})
        (keyed/flink-reduce #'sum-pairs)
        (stream/flink-map #'format-result)
        (stream/flink-print))

    (.execute flink-env "Word Count Print Example")))

;; =============================================================================
;; Example: Direct JDBC writes via ProcessFunction
;; =============================================================================

;; For production, you would create a custom ProcessFunction that batches
;; and writes to PostgreSQL. Here's a simplified example using direct writes:

(defn write-to-postgres!
  "Write a word count to PostgreSQL (for batch/testing use only).

  In production, use Flink's JDBC connector for proper checkpointing."
  [[word count]]
  (try
    (with-open [conn (get-connection)
                stmt (.prepareStatement conn
                       "INSERT INTO word_counts (word, count, updated_at) VALUES (?, ?, NOW())
                        ON CONFLICT (word) DO UPDATE SET count = EXCLUDED.count, updated_at = NOW()")]
      (.setString stmt 1 word)
      (.setLong stmt 2 count)
      (.executeUpdate stmt))
    (catch Exception e
      (println "Error writing to PostgreSQL:" (.getMessage e)))))

;; =============================================================================
;; REPL Helpers
;; =============================================================================

(defn setup!
  "Set up test data for PostgreSQL examples."
  []
  (println "PostgreSQL Configuration:")
  (println "  URL:" jdbc-url)
  (println "  User:" (:user postgres-config))
  (println "\nTesting connection...")
  (try
    (let [result (query "SELECT version()")]
      (println "Connected!" (first (vals (first result)))))
    (catch Exception e
      (println "Connection failed:" (.getMessage e))
      (println "Make sure Docker is running: docker compose up -d"))))

(comment
  ;; REPL workflow:

  ;; 1. Start Docker services
  ;; $ docker compose up -d

  ;; 2. Test PostgreSQL connection
  (setup!)

  ;; 3. Check existing data
  (show-word-counts)
  (show-events)

  ;; 4. Run the print example (no JDBC connector needed)
  ;; First produce some Kafka messages:
  (require '[examples.kafka-example :as kafka-ex])
  (kafka-ex/setup!)

  ;; Then run the print pipeline
  (run-print-word-count-example)

  ;; 5. Query PostgreSQL
  (query "SELECT * FROM orders LIMIT 5")
  (query "SELECT COUNT(*) FROM events")

  ;; 6. Manual word count write (for testing)
  (write-to-postgres! ["test-word" 42])
  (show-word-counts)

  ;; End
  )
