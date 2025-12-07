(ns kafka-example
  "Example: Using Kafka connector with dynamic JAR loading.

  This example demonstrates how to:
  1. Load Kafka connector JARs dynamically (like PyFlink's add_jars)
  2. Read from a Kafka topic
  3. Process the stream
  4. Write back to Kafka

  Prerequisites:
    docker compose up -d   # Start Kafka, Zookeeper, PostgreSQL

  Usage from REPL:
    (require '[examples.kafka-example :as kafka-ex])
    (kafka-ex/run-word-count-example)

  Or create test data:
    (kafka-ex/produce-test-messages 100)"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.admin AdminClient NewTopic]
           [java.util Properties]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def kafka-config
  "Kafka connection configuration for local Docker setup."
  {:bootstrap-servers "localhost:9092"
   :group-id "flink-clj-example"})

(def input-topic "flink-input")
(def output-topic "flink-output")

;; Connector JAR URLs - these would be downloaded or already in your classpath
(def kafka-connector-jars
  "Kafka connector JARs needed for Flink 1.20."
  [(env/maven-jar "org.apache.flink" "flink-connector-kafka" "3.3.0-1.20")
   (env/maven-jar "org.apache.kafka" "kafka-clients" "3.6.1")])

;; =============================================================================
;; Kafka Admin Helpers
;; =============================================================================

(defn create-topics!
  "Create Kafka topics if they don't exist."
  []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" (:bootstrap-servers kafka-config)))
        admin (AdminClient/create props)]
    (try
      (let [topics [(NewTopic. input-topic 1 (short 1))
                    (NewTopic. output-topic 1 (short 1))]]
        (.get (.createTopics admin topics)))
      (println "Created topics:" input-topic output-topic)
      (catch Exception e
        (when-not (str/includes? (.getMessage e) "TopicExistsException")
          (throw e))
        (println "Topics already exist"))
      (finally
        (.close admin)))))

(defn produce-test-messages
  "Produce test messages to the input topic for testing."
  [n]
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" (:bootstrap-servers kafka-config))
                (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
                (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"))
        producer (KafkaProducer. props)
        words ["hello" "world" "flink" "clojure" "stream" "processing" "kafka" "data"]]
    (try
      (dotimes [i n]
        (let [word (rand-nth words)
              record (ProducerRecord. input-topic (str i) word)]
          (.send producer record)))
      (.flush producer)
      (println "Produced" n "messages to" input-topic)
      (finally
        (.close producer)))))

;; =============================================================================
;; Stream Processing Functions (must be top-level for serialization)
;; =============================================================================

(defn parse-word
  "Extract word from Kafka record value."
  [value]
  (str/lower-case (str/trim (str value))))

(defn word-to-pair
  "Convert word to [word, 1] pair for counting."
  [word]
  [word 1])

(defn sum-counts
  "Sum two word count pairs."
  [[word count1] [_ count2]]
  [word (+ count1 count2)])

(defn pair-to-string
  "Convert [word, count] pair to string for Kafka output."
  [[word count]]
  (str word ":" count))

(defn get-word
  "Extract word (key) from pair."
  [[word _count]]
  word)

;; =============================================================================
;; Example: Word Count from Kafka
;; =============================================================================

(defn create-kafka-source
  "Create a Kafka source for reading string messages."
  []
  (kafka/source
    {:bootstrap-servers (:bootstrap-servers kafka-config)
     :topics [input-topic]
     :group-id (:group-id kafka-config)
     :starting-offsets :earliest
     :value-format :string}))

(defn create-kafka-sink
  "Create a Kafka sink for writing string messages."
  []
  (kafka/sink
    {:bootstrap-servers (:bootstrap-servers kafka-config)
     :topic output-topic
     :value-format :string}))

(defn run-word-count-example
  "Run a word count example that reads from Kafka and writes results back.

  This example:
  1. Creates an environment with Kafka connector JARs
  2. Reads words from 'flink-input' topic
  3. Counts occurrences of each word
  4. Writes results to 'flink-output' topic

  Usage:
    ;; First, create topics and produce test data
    (create-topics!)
    (produce-test-messages 1000)

    ;; Then run the Flink job
    (run-word-count-example)"
  []
  (println "Starting Kafka word count example...")
  (println "Kafka connector JARs:" kafka-connector-jars)

  ;; Create environment with connector JARs loaded
  ;; Note: In practice, you may want to download these JARs first
  ;; or have them in a local Maven repo
  (let [flink-env (env/create-env
                    {:parallelism 1
                     ;; Load Kafka connector JARs dynamically
                     ;; Uncomment when JARs are available locally:
                     ;; :jars (mapv env/jar-url ["/path/to/flink-connector-kafka.jar"])
                     })

        ;; Build the streaming pipeline
        source (create-kafka-source)
        sink (create-kafka-sink)]

    (-> flink-env
        (flink/from-source source "Kafka Source")
        (stream/flink-map #'parse-word)
        (stream/flink-map #'word-to-pair)
        (keyed/key-by #'get-word {:key-type :string})
        (keyed/flink-reduce #'sum-counts)
        (stream/flink-map #'pair-to-string)
        (flink/to-sink sink "Kafka Sink"))

    ;; Execute the job
    (println "Executing Flink job...")
    (.execute flink-env "Kafka Word Count")))

;; =============================================================================
;; Example: Streaming Aggregation
;; =============================================================================

(defn run-windowed-aggregation-example
  "Run a windowed aggregation example.

  This example:
  1. Reads events from Kafka
  2. Groups by word
  3. Computes 10-second tumbling window aggregations
  4. Writes results to Kafka"
  []
  (require '[flink-clj.window :as w])

  (let [flink-env (env/create-env {:parallelism 1})
        source (create-kafka-source)
        sink (create-kafka-sink)]

    (-> flink-env
        (flink/from-source source "Kafka Source")
        (stream/flink-map #'parse-word)
        (stream/flink-map #'word-to-pair)
        (keyed/key-by #'get-word {:key-type :string})
        ;; 10-second tumbling window
        (w/tumbling-processing-time (w/seconds 10))
        (w/flink-reduce #'sum-counts)
        (stream/flink-map #'pair-to-string)
        (flink/to-sink sink "Kafka Sink"))

    (.execute flink-env "Kafka Windowed Aggregation")))

;; =============================================================================
;; REPL Helpers
;; =============================================================================

(defn setup!
  "Set up the Kafka environment for testing."
  []
  (create-topics!)
  (produce-test-messages 100)
  (println "\nKafka setup complete!")
  (println "View messages at: http://localhost:8080 (Kafka UI)"))

(comment
  ;; REPL workflow:

  ;; 1. Start Docker services
  ;; $ docker compose up -d

  ;; 2. Set up Kafka topics and test data
  (setup!)

  ;; 3. Run the word count example
  (run-word-count-example)

  ;; 4. Check results in Kafka UI at http://localhost:8080

  ;; 5. Produce more messages
  (produce-test-messages 50)

  ;; End
  )
