# Serialization

flink-clj provides built-in support for multiple serialization formats through the `flink-clj.serde` namespace.

## Supported Formats

| Format | Description | Use Case | Dependencies |
|--------|-------------|----------|--------------|
| String | Plain text | Simple text data | None (built-in) |
| JSON | JSON with keyword keys | APIs, web services | cheshire or data.json |
| EDN | Clojure's native format | Clojure-to-Clojure | None (built-in) |
| Nippy | Fast binary Clojure | High-throughput Clojure | taoensso/nippy |
| Transit | Cross-language Clojure-friendly | Polyglot systems | transit-clj |
| Avro | Schema-based binary | Kafka ecosystems | avro, confluent serde |
| Protocol Buffers | Google's binary format | gRPC, high-performance | protobuf-java |

## Quick Start

```clojure
(require '[flink-clj.serde :as serde]
         '[flink-clj.connectors.kafka :as kafka])

;; Use a format keyword
(kafka/source {:bootstrap-servers "localhost:9092"
               :topics ["events"]
               :value-format :json})  ; or :edn, :nippy, :transit

;; Use a schema instance for more control
(kafka/source {:bootstrap-servers "localhost:9092"
               :topics ["events"]
               :value-format (serde/json)})
```

## JSON

JSON serialization using Cheshire (preferred) or clojure.data.json.

```clojure
;; Add dependency
[cheshire "5.12.0"]
;; or
[org.clojure/data.json "2.4.0"]
```

### Usage

```clojure
;; Keyword shortcut
(kafka/source {:value-format :json})

;; Schema instance
(serde/json)

;; With options
(serde/json {:key-fn keyword})  ; Transform JSON keys
```

### Example Pipeline

```clojure
(defn parse-user [event]
  (select-keys event [:user-id :action :timestamp]))

(-> (kafka/source {:bootstrap-servers "localhost:9092"
                   :topics ["user-events"]
                   :value-format :json})
    (flink/from-source env _ "Kafka JSON Input")
    (stream/flink-map #'parse-user)
    (kafka/sink {:bootstrap-servers "localhost:9092"
                 :topic "processed-users"
                 :value-format :json}))
```

## EDN

EDN (Extensible Data Notation) is Clojure's native data format. Perfect for Clojure-to-Clojure communication with full support for keywords, symbols, sets, and other Clojure types.

```clojure
;; No additional dependencies needed

;; Keyword shortcut
(kafka/source {:value-format :edn})

;; Schema instance
(serde/edn)

;; With custom tagged literal readers
(serde/edn {:readers {'my/instant my-instant-reader}
            :default my-default-reader})
```

### Why EDN?

- Full Clojure type support (keywords, symbols, sets, ratios)
- Human-readable
- Extensible with tagged literals
- No external dependencies

## Nippy

Nippy is a fast binary serialization library for Clojure. Ideal for high-throughput systems where both ends are Clojure.

```clojure
;; Add dependency (already included in flink-clj)
[com.taoensso/nippy "3.3.0"]
```

### Usage

```clojure
;; Keyword shortcut
(kafka/source {:value-format :nippy})

;; Schema instance
(serde/nippy)

;; With options
(serde/nippy {:compressor nil})      ; Disable compression
(serde/nippy {:password "secret"})   ; Enable encryption
```

### Performance

Nippy is typically the fastest option for Clojure data:

```
nippy:   52.99 ops/ms
json:    51.36 ops/ms
transit: 17.17 ops/ms
edn:     12.48 ops/ms
```

## Transit

Transit is a format for conveying values between applications written in different programming languages. It's particularly good when you need Clojure-like semantics across different platforms.

```clojure
;; Add dependency
[com.cognitect/transit-clj "1.0.333"]
```

### Usage

```clojure
;; Keyword shortcut
(kafka/source {:value-format :transit})

;; Schema instance (JSON format)
(serde/transit)

;; MessagePack format (more compact)
(serde/transit {:type :msgpack})

;; With custom handlers
(serde/transit {:type :json
                :handlers {...}})
```

## Avro

Apache Avro is a data serialization system with rich schema support. Common in Kafka ecosystems with Schema Registry.

```clojure
;; Add dependencies
[org.apache.avro/avro "1.11.3"]
;; For Schema Registry
[io.confluent/kafka-avro-serializer "7.5.0"]
```

### Usage with Inline Schema

```clojure
(def event-schema
  "{\"type\":\"record\",
    \"name\":\"Event\",
    \"fields\":[
      {\"name\":\"id\",\"type\":\"string\"},
      {\"name\":\"timestamp\",\"type\":\"long\"},
      {\"name\":\"data\",\"type\":\"string\"}
    ]}")

(kafka/source {:value-format {:format :avro
                              :schema event-schema}})
```

### Usage with Schema Registry

```clojure
(kafka/source {:bootstrap-servers "localhost:9092"
               :topics ["events"]
               :value-format {:format :avro
                              :schema-registry "http://localhost:8081"
                              :subject "events-value"}})
```

### Avro + Clojure Maps

The `avro` schema automatically converts Avro GenericRecords to Clojure maps:

```clojure
;; Automatically converts GenericRecord to {:id "123" :timestamp 1234567890}
(serde/avro {:schema-registry "http://localhost:8081"
             :subject "events-value"})

;; For raw GenericRecord access
(serde/avro-generic {:schema-registry "http://localhost:8081"
                     :subject "events-value"})
```

## Protocol Buffers

Google's Protocol Buffers for high-performance binary serialization.

```clojure
;; Add dependency
[com.google.protobuf/protobuf-java "3.25.1"]
```

### Usage

```clojure
;; Requires a generated protobuf Message class
(kafka/source {:value-format {:format :protobuf
                              :message-class MyProto$Event}})
```

### Generating Protobuf Classes

```bash
protoc --java_out=src/java my_events.proto
```

## Raw Bytes

Pass-through schema for when you want to handle serialization yourself.

```clojure
(kafka/source {:value-format :raw})
;; Returns byte arrays directly
```

## Custom Transformations

Wrap any schema with pre/post transformation:

```clojure
;; Add timestamp after deserializing
(serde/transform (serde/json)
                 {:after-deserialize #(assoc % :received-at (System/currentTimeMillis))})

;; Remove sensitive fields before serializing
(serde/transform (serde/json)
                 {:before-serialize #(dissoc % :password :api-key)})
```

## Type Information

Provide explicit Flink TypeInformation for better performance:

```clojure
(serde/typed (serde/json)
             {:type-info (Types/MAP Types/STRING Types/LONG)})
```

## Choosing a Format

### Use JSON when:
- Interoperating with non-Clojure systems
- Human readability is important
- Using web APIs or REST services

### Use EDN when:
- Both producer and consumer are Clojure
- You need full Clojure type support
- No additional dependencies preferred

### Use Nippy when:
- Both producer and consumer are Clojure
- Maximum performance is required
- Data structures are complex

### Use Avro when:
- Schema evolution is important
- Using Confluent Schema Registry
- Kafka ecosystem integration

### Use Protocol Buffers when:
- Interoperating with gRPC services
- Schema is defined in .proto files
- Cross-language compatibility needed

## Dependencies

Add optional dependencies based on formats you need:

```clojure
;; project.clj
:dependencies [;; JSON (pick one)
               [cheshire "5.12.0"]
               [org.clojure/data.json "2.4.0"]

               ;; Transit
               [com.cognitect/transit-clj "1.0.333"]

               ;; Avro
               [org.apache.avro/avro "1.11.3"]
               [io.confluent/kafka-avro-serializer "7.5.0"]

               ;; Protocol Buffers
               [com.google.protobuf/protobuf-java "3.25.1"]]
```

## Example: Multi-Format Pipeline

```clojure
(ns my-app.pipeline
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.connectors.kafka :as kafka]
            [flink-clj.serde :as serde]))

(defn process-event [event]
  (assoc event :processed-at (System/currentTimeMillis)))

(defn -main []
  ;; Read JSON from external system
  (let [input (kafka/source {:bootstrap-servers "localhost:9092"
                             :topics ["external-events"]
                             :value-format :json})

        ;; Write Nippy for internal Clojure service
        internal-sink (kafka/sink {:bootstrap-servers "localhost:9092"
                                   :topic "internal-events"
                                   :value-format :nippy})

        ;; Write JSON for dashboard API
        api-sink (kafka/sink {:bootstrap-servers "localhost:9092"
                              :topic "dashboard-events"
                              :value-format :json})]

    (-> (env/create-env {:parallelism 4})
        (flink/register-clojure-types!)
        (flink/from-source input "JSON Input")
        (stream/flink-map #'process-event)

        ;; Fan out to both sinks
        ((fn [stream]
           (flink/to-sink stream internal-sink "Nippy Output")
           (flink/to-sink stream api-sink "JSON Output")
           stream))

        (flink/execute "Multi-Format Pipeline"))))
```
