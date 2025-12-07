# Connectors

Read from and write to external systems.

## Kafka

The most common connector for streaming applications.

### Setup

Add the Kafka connector dependency:

```clojure
;; project.clj
[org.apache.flink/flink-connector-kafka "3.3.0-1.20"]
[org.apache.kafka/kafka-clients "3.6.1"]
```

Or load dynamically:

```clojure
(require '[flink-clj.env :as env])

(env/create-env
  {:parallelism 4
   :jars [(env/maven-jar "org.apache.flink" "flink-connector-kafka" "3.3.0-1.20")
          (env/maven-jar "org.apache.kafka" "kafka-clients" "3.6.1")]})
```

### Kafka Source

Read from Kafka topics:

```clojure
(require '[flink-clj.connectors.kafka :as kafka])

;; Basic string source
(def source
  (kafka/source
    {:bootstrap-servers "localhost:9092"
     :topics ["my-topic"]
     :group-id "my-consumer-group"
     :starting-offsets :earliest
     :value-format :string}))

;; Use the source
(-> (flink/from-source env source "Kafka Source")
    (stream/flink-map #'process-message)
    ...)
```

#### Source Options

| Option | Description | Required |
|--------|-------------|----------|
| `:bootstrap-servers` | Kafka broker addresses | Yes |
| `:topics` | Topic(s) to consume (string, vector, or regex) | Yes |
| `:value-format` | Deserialization format (see below) | Yes |
| `:group-id` | Consumer group ID | No |
| `:starting-offsets` | Where to start (`:earliest`, `:latest`, `:committed`, or timestamp) | No |
| `:bounded-offsets` | Where to stop (for bounded source) | No |
| `:properties` | Additional Kafka consumer properties | No |

#### Value Formats

| Format | Description |
|--------|-------------|
| `:string` | Plain text (SimpleStringSchema) |
| `:json` | JSON with keyword keys (requires cheshire) |
| `:edn` | Clojure EDN format |
| `:nippy` | Fast binary Clojure serialization |
| `:transit` | Transit JSON format |
| `:raw` | Raw bytes pass-through |
| Map | Advanced options, e.g. `{:format :avro :schema-registry "..."}` |

See [Serialization](serialization.md) for details on all formats.

```clojure
;; JSON format
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["events"]
   :group-id "my-group"
   :value-format :json})

;; EDN format
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["events"]
   :group-id "my-group"
   :value-format :edn})

;; Avro with Schema Registry
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["events"]
   :group-id "my-group"
   :value-format {:format :avro
                  :schema-registry "http://localhost:8081"
                  :subject "events-value"}})

;; Multiple topics
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["topic1" "topic2" "topic3"]
   :group-id "my-group"
   :value-format :string})

;; Topic pattern
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics #"events-.*"  ; All topics matching pattern
   :group-id "my-group"
   :value-format :string})

;; Start from timestamp
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["events"]
   :starting-offsets 1699900000000  ; Unix timestamp in ms
   :value-format :string})

;; Bounded source (read to latest and stop)
(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["events"]
   :starting-offsets :earliest
   :bounded-offsets :latest
   :value-format :string})
```

### Kafka Sink

Write to Kafka topics:

```clojure
(def sink
  (kafka/sink
    {:bootstrap-servers "localhost:9092"
     :topic "output-topic"
     :value-format :string}))

;; Use the sink
(-> stream
    (stream/flink-map #'to-string)
    (flink/to-sink sink "Kafka Output"))
```

#### Sink Options

| Option | Description | Required |
|--------|-------------|----------|
| `:bootstrap-servers` | Kafka broker addresses | Yes |
| `:topic` | Topic to write to | Yes |
| `:value-format` | Serialization (`:string` or custom) | Yes |
| `:delivery-guarantee` | `:at-least-once`, `:exactly-once`, `:none` | No |
| `:transactional-id-prefix` | For exactly-once (required with `:exactly-once`) | No |
| `:properties` | Additional Kafka producer properties | No |

```clojure
;; At-least-once delivery (default)
(kafka/sink
  {:bootstrap-servers "localhost:9092"
   :topic "output"
   :value-format :string
   :delivery-guarantee :at-least-once})

;; Exactly-once delivery
(kafka/sink
  {:bootstrap-servers "localhost:9092"
   :topic "output"
   :value-format :string
   :delivery-guarantee :exactly-once
   :transactional-id-prefix "my-app"})
```

### Convenience Functions

```clojure
;; Quick string source
(kafka/string-source "localhost:9092" "my-topic" "my-group")

;; Quick string sink
(kafka/string-sink "localhost:9092" "output-topic")
```

## Files

Read from and write to files.

### Read Text Files

```clojure
(require '[flink-clj.connectors.file :as file])

;; Read lines from file (Flink 1.x only)
(def lines
  (file/read-text-file env {:path "/data/input.txt"}))

;; Read from multiple paths
(def lines
  (file/read-text-file-parallel env {:paths ["/data/part1" "/data/part2"]}))
```

### Write to Files

```clojure
;; StreamingFileSink (Flink 1.x)
(def sink
  (file/streaming-file-sink
    {:path "/output/data"}))

;; With rolling policy
(def sink
  (file/streaming-file-sink
    {:path "/output/data"
     :rolling-policy {:part-size (* 1024 1024 128)     ; 128 MB
                      :rollover-interval [15 :minutes]
                      :inactivity-interval [5 :minutes]}}))

;; Simple line sink
(def sink
  (file/line-sink {:path "/output/lines"}))
```

## Complete Kafka Example

```clojure
(ns my-app.kafka-pipeline
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str]))

(defn parse-event [msg]
  (read-string msg))

(defn valid-event? [event]
  (and (:user-id event) (:event-type event)))

(defn enrich-event [event]
  (assoc event :processed-at (System/currentTimeMillis)))

(defn event->key [event]
  (:user-id event))

(defn event->pair [event]
  [(:user-id event) 1])

(defn sum-counts [[user c1] [_ c2]]
  [user (+ c1 c2)])

(defn format-output [[user count]]
  (str "{\"user\":\"" user "\",\"count\":" count "}"))

(defn -main []
  (let [source (kafka/source
                 {:bootstrap-servers "localhost:9092"
                  :topics ["user-events"]
                  :group-id "event-counter"
                  :starting-offsets :latest
                  :value-format :string})
        sink (kafka/sink
               {:bootstrap-servers "localhost:9092"
                :topic "user-counts"
                :value-format :string
                :delivery-guarantee :at-least-once})]

    (-> (env/create-env {:parallelism 4})
        (flink/register-clojure-types!)
        (flink/from-source source "Kafka Events")

        ;; Parse and validate
        (stream/flink-map #'parse-event)
        (stream/flink-filter #'valid-event?)

        ;; Enrich
        (stream/flink-map #'enrich-event)

        ;; Count per user
        (stream/flink-map #'event->pair)
        (keyed/key-by first {:key-type :string})
        (keyed/flink-reduce #'sum-counts)

        ;; Output
        (stream/flink-map #'format-output)
        (flink/to-sink sink "Kafka Output")

        (flink/execute "Event Counter"))))
```

## Testing Without External Systems

Use collections for testing:

```clojure
(defn test-pipeline []
  (-> (env/create-local-env {:parallelism 1})
      (flink/register-clojure-types!)

      ;; Use collection instead of Kafka
      (flink/from-collection
        [{:user-id "alice" :event-type "click"}
         {:user-id "bob" :event-type "view"}
         {:user-id "alice" :event-type "purchase"}])

      ;; Same processing logic
      (stream/flink-map #'event->pair)
      (keyed/key-by first)
      (keyed/flink-reduce #'sum-counts)

      ;; Print instead of Kafka sink
      (stream/flink-print)

      (flink/execute "Test Pipeline")))
```

## Custom Serialization

For formats other than strings:

```clojure
;; JSON deserialization
(import '[com.fasterxml.jackson.databind ObjectMapper])

(def json-deserializer
  (reify org.apache.flink.api.common.serialization.DeserializationSchema
    (deserialize [_ bytes]
      (let [mapper (ObjectMapper.)]
        (.readValue mapper bytes clojure.lang.PersistentHashMap)))
    (isEndOfStream [_ _] false)
    (getProducedType [_]
      (org.apache.flink.api.common.typeinfo.TypeInformation/of
        clojure.lang.PersistentHashMap))))

(kafka/source
  {:bootstrap-servers "localhost:9092"
   :topics ["events"]
   :value-format json-deserializer})
```
