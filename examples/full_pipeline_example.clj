(ns full-pipeline-example
  "Example: Complete Kafka to PostgreSQL Pipeline with JAR Loading.

  This example demonstrates the full flink-clj workflow:
  1. Loading connector JARs dynamically (like PyFlink)
  2. Reading from Kafka
  3. Processing with windows and state
  4. Writing to PostgreSQL

  Prerequisites:
    docker compose up -d

  Quick Start:
    (require '[examples.full-pipeline-example :as ex])
    (ex/demo!)"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.window :as w]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.admin AdminClient NewTopic]
           [java.sql DriverManager]
           [java.util Properties UUID]
           [java.time Instant]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers "localhost:9092"
           :group-id "flink-clj-demo"
           :input-topic "demo-input"
           :output-topic "demo-output"}
   :postgres {:host "localhost"
              :port 5432
              :database "flink_test"
              :user "flink"
              :password "flink"}})

(def jdbc-url
  (let [{:keys [host port database]} (:postgres config)]
    (format "jdbc:postgresql://%s:%d/%s" host port database)))

;; =============================================================================
;; JAR Loading Configuration
;; =============================================================================

(def connector-jars
  "All connector JARs needed for this example.

  These URLs point to Maven Central. In production, you would:
  1. Download these JARs locally
  2. Use (env/jar-url \"/local/path/to/jar.jar\")

  The maven-jar function generates URLs like:
  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar"
  {:kafka [(env/maven-jar "org.apache.flink" "flink-connector-kafka" "3.3.0-1.20")
           (env/maven-jar "org.apache.kafka" "kafka-clients" "3.6.1")]
   :jdbc [(env/maven-jar "org.apache.flink" "flink-connector-jdbc" "3.2.0-1.19")
          (env/maven-jar "org.postgresql" "postgresql" "42.7.1")]})

(defn download-jars!
  "Download connector JARs to local directory for faster loading."
  [jar-dir]
  (let [dir (io/file jar-dir)]
    (.mkdirs dir)
    (doseq [[connector-type jars] connector-jars
            jar-url jars]
      (let [filename (last (str/split jar-url #"/"))
            dest-file (io/file dir filename)]
        (when-not (.exists dest-file)
          (println "Downloading:" filename)
          (try
            (with-open [in (io/input-stream (java.net.URL. jar-url))
                        out (io/output-stream dest-file)]
              (io/copy in out))
            (println "  Downloaded:" (.getPath dest-file))
            (catch Exception e
              (println "  Failed to download:" (.getMessage e)))))))
    (println "\nJARs in" jar-dir ":")
    (doseq [f (.listFiles dir)]
      (println " " (.getName f)))))

(defn local-jar-paths
  "Get local JAR paths for a given jar directory."
  [jar-dir]
  (let [dir (io/file jar-dir)]
    (when (.exists dir)
      (->> (.listFiles dir)
           (filter #(str/ends-with? (.getName %) ".jar"))
           (map #(env/jar-url (.getAbsolutePath %)))
           vec))))

;; =============================================================================
;; Kafka Helpers
;; =============================================================================

(defn create-kafka-admin []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" (get-in config [:kafka :bootstrap-servers])))]
    (AdminClient/create props)))

(defn create-topics! []
  (let [admin (create-kafka-admin)
        topics [(NewTopic. (get-in config [:kafka :input-topic]) 1 (short 1))
                (NewTopic. (get-in config [:kafka :output-topic]) 1 (short 1))]]
    (try
      (.get (.createTopics admin topics))
      (println "Created Kafka topics")
      (catch Exception e
        (when-not (str/includes? (.getMessage e) "TopicExistsException")
          (throw e))))
    (.close admin)))

(defn create-kafka-producer []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" (get-in config [:kafka :bootstrap-servers]))
                (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
                (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"))]
    (KafkaProducer. props)))

(defn send-event!
  "Send an event to Kafka input topic."
  [producer event-map]
  (let [json-str (pr-str event-map)  ;; Simple EDN serialization
        record (ProducerRecord. (get-in config [:kafka :input-topic])
                                (str (UUID/randomUUID))
                                json-str)]
    (.send producer record)))

(defn generate-events!
  "Generate sample events to Kafka."
  [n]
  (let [producer (create-kafka-producer)
        event-types ["click" "view" "purchase" "signup" "logout"]
        users ["user-001" "user-002" "user-003" "user-004" "user-005"]]
    (try
      (dotimes [i n]
        (send-event! producer
                     {:event-type (rand-nth event-types)
                      :user-id (rand-nth users)
                      :value (rand-int 100)
                      :timestamp (.toString (Instant/now))}))
      (.flush producer)
      (println "Generated" n "events to" (get-in config [:kafka :input-topic]))
      (finally
        (.close producer)))))

;; =============================================================================
;; PostgreSQL Helpers
;; =============================================================================

(defn pg-connection []
  (DriverManager/getConnection jdbc-url
                               (get-in config [:postgres :user])
                               (get-in config [:postgres :password])))

(defn pg-query [sql]
  (with-open [conn (pg-connection)
              stmt (.createStatement conn)
              rs (.executeQuery stmt sql)]
    (let [meta (.getMetaData rs)
          cols (vec (for [i (range 1 (inc (.getColumnCount meta)))]
                      (keyword (.getColumnName meta i))))]
      (loop [results []]
        (if (.next rs)
          (recur (conj results
                       (zipmap cols
                               (for [i (range 1 (inc (count cols)))]
                                 (.getObject rs i)))))
          results)))))

(defn pg-execute! [sql]
  (with-open [conn (pg-connection)
              stmt (.createStatement conn)]
    (.execute stmt sql)))

;; =============================================================================
;; Stream Processing Functions
;; =============================================================================

(defn parse-event
  "Parse EDN event from Kafka message."
  [msg]
  (try
    (read-string msg)
    (catch Exception _
      {:event-type "unknown" :value 0 :user-id "unknown"})))

(defn extract-key
  "Extract grouping key from event."
  [event]
  (str (:event-type event) ":" (:user-id event)))

(defn event-to-aggregation
  "Convert event to aggregation tuple."
  [event]
  [(extract-key event) {:count 1 :sum (:value event 0)}])

(defn merge-aggregations
  "Merge two aggregation values."
  [[key agg1] [_ agg2]]
  [key {:count (+ (:count agg1) (:count agg2))
        :sum (+ (:sum agg1) (:sum agg2))}])

(defn format-output
  "Format aggregation result for output."
  [[key {:keys [count sum]}]]
  (format "%s => count=%d, sum=%d, avg=%.2f" key count sum (/ (double sum) count)))

;; =============================================================================
;; Example Pipelines
;; =============================================================================

(defn run-simple-pipeline
  "Run a simple Kafka to stdout pipeline.

  This example doesn't require additional connector JARs since
  the Kafka connector is included in the dev profile."
  []
  (println "\n=== Running Simple Pipeline ===")
  (println "Reading from:" (get-in config [:kafka :input-topic]))
  (println "Press Ctrl+C to stop\n")

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(get-in config [:kafka :input-topic])]
                  :group-id (get-in config [:kafka :group-id])
                  :starting-offsets :earliest
                  :value-format :string})]

    (-> flink-env
        (flink/from-source source "Kafka Input")
        (stream/flink-map #'parse-event)
        (stream/flink-map #'event-to-aggregation)
        (keyed/key-by #'first {:key-type :string})
        (keyed/flink-reduce #'merge-aggregations)
        (stream/flink-map #'format-output)
        (stream/flink-print))

    (.execute flink-env "Simple Kafka Pipeline")))

(defn run-windowed-pipeline
  "Run a windowed aggregation pipeline."
  []
  (println "\n=== Running Windowed Pipeline ===")
  (println "5-second tumbling windows")
  (println "Press Ctrl+C to stop\n")

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(get-in config [:kafka :input-topic])]
                  :group-id (str (get-in config [:kafka :group-id]) "-windowed")
                  :starting-offsets :latest
                  :value-format :string})]

    (-> flink-env
        (flink/from-source source "Kafka Input")
        (stream/flink-map #'parse-event)
        (stream/flink-map #'event-to-aggregation)
        (keyed/key-by #'first {:key-type :string})
        (w/tumbling-processing-time (w/seconds 5))
        (w/flink-reduce #'merge-aggregations)
        (stream/flink-map #'format-output)
        (stream/flink-print))

    (.execute flink-env "Windowed Kafka Pipeline")))

;; =============================================================================
;; Demo Function
;; =============================================================================

(defn check-services
  "Check if Docker services are running."
  []
  (println "\nChecking services...")

  ;; Check Kafka
  (print "  Kafka: ")
  (try
    (let [admin (create-kafka-admin)]
      (.get (.listTopics admin))
      (.close admin)
      (println "OK"))
    (catch Exception e
      (println "FAILED -" (.getMessage e))))

  ;; Check PostgreSQL
  (print "  PostgreSQL: ")
  (try
    (pg-query "SELECT 1")
    (println "OK")
    (catch Exception e
      (println "FAILED -" (.getMessage e)))))

(defn demo!
  "Run a complete demonstration.

  1. Check Docker services
  2. Create Kafka topics
  3. Generate sample events
  4. Run the streaming pipeline"
  []
  (println "=" 60)
  (println "flink-clj Demo: Kafka + PostgreSQL")
  (println "=" 60)

  (check-services)

  (println "\nCreating Kafka topics...")
  (create-topics!)

  (println "\nGenerating 100 sample events...")
  (generate-events! 100)

  (println "\nStarting streaming pipeline...")
  (println "(Generate more events in another terminal with:")
  (println "  (require '[examples.full-pipeline-example :as ex])")
  (println "  (ex/generate-events! 50))")
  (println)

  (run-simple-pipeline))

;; =============================================================================
;; REPL Helpers
;; =============================================================================

(comment
  ;; === Quick Start ===

  ;; 1. Start Docker services
  ;; $ docker compose up -d

  ;; 2. Run the demo
  (demo!)

  ;; === Individual Operations ===

  ;; Check services
  (check-services)

  ;; Create topics
  (create-topics!)

  ;; Generate events
  (generate-events! 50)

  ;; Run pipelines
  (run-simple-pipeline)
  (run-windowed-pipeline)

  ;; === PostgreSQL Queries ===

  ;; Check tables
  (pg-query "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")

  ;; View orders
  (pg-query "SELECT * FROM orders")

  ;; View aggregations
  (pg-query "SELECT * FROM aggregations ORDER BY window_start DESC LIMIT 10")

  ;; === JAR Management ===

  ;; Show required JAR URLs
  connector-jars

  ;; Download JARs locally (optional - for faster loading)
  (download-jars! "lib/connectors")

  ;; Get local paths
  (local-jar-paths "lib/connectors")

  ;; Create env with local JARs
  (def my-env
    (env/create-env
      {:parallelism 2
       :jars (local-jar-paths "lib/connectors")}))

  ;; End
  )
