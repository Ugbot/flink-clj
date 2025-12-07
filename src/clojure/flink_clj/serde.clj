(ns flink-clj.serde
  "Serialization and deserialization schemas for Flink.

  Provides built-in support for common formats:
  - String (plain text)
  - JSON (via Cheshire or clojure.data.json)
  - EDN (Clojure's native format)
  - Nippy (fast binary Clojure serialization)
  - Avro (with schema registry support)
  - Protocol Buffers

  Usage:
    (require '[flink-clj.serde :as serde])
    (require '[flink-clj.connectors.kafka :as kafka])

    ;; JSON
    (kafka/source {:topics [\"events\"]
                   :value-format (serde/json)})

    ;; EDN
    (kafka/source {:topics [\"events\"]
                   :value-format (serde/edn)})

    ;; Avro with Schema Registry
    (kafka/source {:topics [\"events\"]
                   :value-format (serde/avro {:schema-registry \"http://localhost:8081\"
                                              :subject \"events-value\"})})"
  (:require [clojure.edn :as edn])
  (:import [org.apache.flink.api.common.serialization DeserializationSchema SerializationSchema SimpleStringSchema]
           [org.apache.flink.api.common.typeinfo TypeInformation Types]
           [java.nio.charset StandardCharsets]))

;; =============================================================================
;; String Schema (built-in)
;; =============================================================================

(defn string-schema
  "Create a simple string serialization schema.

  This is Flink's built-in SimpleStringSchema.

  Example:
    (kafka/source {:value-format (serde/string-schema)})"
  []
  (SimpleStringSchema.))

;; =============================================================================
;; JSON Schema
;; =============================================================================

(defn- json-encode
  "Encode Clojure data to JSON bytes."
  [data]
  (try
    ;; Try Cheshire first (faster)
    (let [generate-string (requiring-resolve 'cheshire.core/generate-string)]
      (.getBytes ^String (generate-string data) StandardCharsets/UTF_8))
    (catch Exception _
      ;; Fall back to clojure.data.json
      (try
        (let [write-str (requiring-resolve 'clojure.data.json/write-str)]
          (.getBytes ^String (write-str data) StandardCharsets/UTF_8))
        (catch Exception _
          (throw (ex-info "No JSON library available. Add cheshire or clojure.data.json to dependencies."
                          {:suggestion "[cheshire \"5.12.0\"] or [org.clojure/data.json \"2.4.0\"]"})))))))

(defn- json-decode
  "Decode JSON bytes to Clojure data."
  [^bytes data]
  (let [json-str (String. data StandardCharsets/UTF_8)]
    (try
      ;; Try Cheshire first (faster)
      (let [parse-string (requiring-resolve 'cheshire.core/parse-string)]
        (parse-string json-str true))
      (catch Exception _
        ;; Fall back to clojure.data.json
        (try
          (let [read-str (requiring-resolve 'clojure.data.json/read-str)]
            (read-str json-str :key-fn keyword))
          (catch Exception _
            (throw (ex-info "No JSON library available. Add cheshire or clojure.data.json to dependencies."
                            {:suggestion "[cheshire \"5.12.0\"] or [org.clojure/data.json \"2.4.0\"]"}))))))))

(defn json
  "Create a JSON serialization schema.

  Options:
    :key-fn - Function to transform keys during deserialization (default: keyword)

  Requires cheshire or clojure.data.json on classpath.

  Example:
    (kafka/source {:value-format (serde/json)})
    (kafka/sink {:value-format (serde/json)})"
  ([] (json {}))
  ([{:keys [key-fn] :or {key-fn keyword}}]
   (reify
     DeserializationSchema
     (deserialize [_ message]
       (when message
         (json-decode message)))
     (isEndOfStream [_ _] false)
     (getProducedType [_]
       (TypeInformation/of Object))

     SerializationSchema
     (serialize [_ element]
       (json-encode element)))))

;; =============================================================================
;; EDN Schema
;; =============================================================================

(defn edn
  "Create an EDN serialization schema.

  EDN is Clojure's native data format - perfect for Clojure-to-Clojure
  communication with full support for Clojure data structures.

  Options:
    :readers - Custom tagged literal readers (map)
    :default - Default reader function for unknown tags

  Example:
    (kafka/source {:value-format (serde/edn)})

    ;; With custom readers
    (kafka/source {:value-format (serde/edn {:readers {'my/tag my-reader}})})"
  ([] (edn {}))
  ([{:keys [readers default]}]
   (let [read-opts (cond-> {}
                     readers (assoc :readers readers)
                     default (assoc :default default))]
     (reify
       DeserializationSchema
       (deserialize [_ message]
         (when message
           (let [s (String. ^bytes message StandardCharsets/UTF_8)]
             (if (empty? read-opts)
               (edn/read-string s)
               (edn/read-string read-opts s)))))
       (isEndOfStream [_ _] false)
       (getProducedType [_]
         (TypeInformation/of Object))

       SerializationSchema
       (serialize [_ element]
         (.getBytes (pr-str element) StandardCharsets/UTF_8))))))

;; =============================================================================
;; Nippy Schema (Fast Binary Clojure Serialization)
;; =============================================================================

(defn- nippy-available?
  "Check if Nippy is available."
  []
  (try
    (require 'taoensso.nippy)
    true
    (catch Exception _ false)))

(defn nippy
  "Create a Nippy binary serialization schema.

  Nippy is a fast binary serializer for Clojure data structures.
  Ideal for high-throughput Clojure-to-Clojure communication.

  Options:
    :compressor - Compression algorithm (:lz4, :snappy, :lzma2, or nil for none)
    :encryptor - Encryption configuration
    :password - Password for encryption (simple case)

  Requires [com.taoensso/nippy] on classpath.

  Example:
    (kafka/source {:value-format (serde/nippy)})

    ;; With compression disabled
    (kafka/source {:value-format (serde/nippy {:compressor nil})})"
  ([] (nippy {}))
  ([{:keys [compressor encryptor password]}]
   (when-not (nippy-available?)
     (throw (ex-info "Nippy not available. Add com.taoensso/nippy to dependencies."
                     {:suggestion "[com.taoensso/nippy \"3.3.0\"]"})))
   (let [freeze (requiring-resolve 'taoensso.nippy/freeze)
         thaw (requiring-resolve 'taoensso.nippy/thaw)
         freeze-opts (cond-> {}
                       (some? compressor) (assoc :compressor compressor)
                       encryptor (assoc :encryptor encryptor)
                       password (assoc :password password))
         thaw-opts (cond-> {}
                     encryptor (assoc :encryptor encryptor)
                     password (assoc :password password))]
     (reify
       DeserializationSchema
       (deserialize [_ message]
         (when message
           (if (empty? thaw-opts)
             (thaw message)
             (thaw message thaw-opts))))
       (isEndOfStream [_ _] false)
       (getProducedType [_]
         (TypeInformation/of Object))

       SerializationSchema
       (serialize [_ element]
         (if (empty? freeze-opts)
           (freeze element)
           (freeze element freeze-opts)))))))

;; =============================================================================
;; Avro Schema
;; =============================================================================

(defn- avro-available?
  "Check if Avro is available."
  []
  (try
    (Class/forName "org.apache.avro.Schema")
    true
    (catch ClassNotFoundException _ false)))

(defn- confluent-avro-available?
  "Check if Confluent Avro serializer is available."
  []
  (try
    (Class/forName "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    true
    (catch ClassNotFoundException _ false)))

(defn avro-generic
  "Create an Avro serialization schema using GenericRecord.

  This creates Avro serializers that work with Avro's GenericRecord type,
  suitable for dynamic schema handling.

  Options:
    :schema - Avro schema (string, file path, or Schema object)
    :schema-registry - Schema Registry URL (for Confluent format)
    :subject - Schema subject name (for registry)

  Requires [org.apache.avro/avro] on classpath.
  For Schema Registry: [io.confluent/kafka-avro-serializer]

  Example:
    ;; With inline schema
    (serde/avro-generic {:schema schema-json})

    ;; With Schema Registry
    (serde/avro-generic {:schema-registry \"http://localhost:8081\"
                         :subject \"events-value\"})"
  [{:keys [schema schema-registry subject]}]
  (when-not (avro-available?)
    (throw (ex-info "Avro not available. Add org.apache.avro/avro to dependencies."
                    {:suggestion "[org.apache.avro/avro \"1.11.3\"]"})))

  (if schema-registry
    ;; Use Confluent Schema Registry
    (do
      (when-not (confluent-avro-available?)
        (throw (ex-info "Confluent Avro serializer not available."
                        {:suggestion "[io.confluent/kafka-avro-serializer \"7.5.0\"]"})))
      (let [deser-class (Class/forName "io.confluent.kafka.serializers.KafkaAvroDeserializer")
            ser-class (Class/forName "io.confluent.kafka.serializers.KafkaAvroSerializer")
            deser (.newInstance deser-class)
            ser (.newInstance ser-class)
            config {"schema.registry.url" schema-registry}]
        ;; Configure deserializer
        (let [configure-method (.getMethod deser-class "configure"
                                           (into-array Class [java.util.Map Boolean/TYPE]))]
          (.invoke configure-method deser (into-array Object [config false])))
        ;; Configure serializer
        (let [configure-method (.getMethod ser-class "configure"
                                           (into-array Class [java.util.Map Boolean/TYPE]))]
          (.invoke configure-method ser (into-array Object [config false])))

        (reify
          DeserializationSchema
          (deserialize [_ message]
            (when message
              (let [deserialize-method (.getMethod deser-class "deserialize"
                                                   (into-array Class [String (Class/forName "[B")]))]
                (.invoke deserialize-method deser (into-array Object [subject message])))))
          (isEndOfStream [_ _] false)
          (getProducedType [_]
            (TypeInformation/of Object))

          SerializationSchema
          (serialize [_ element]
            (let [serialize-method (.getMethod ser-class "serialize"
                                               (into-array Class [String Object]))]
              (.invoke serialize-method ser (into-array Object [subject element])))))))

    ;; Use plain Avro (no registry)
    (let [schema-parser-class (Class/forName "org.apache.avro.Schema$Parser")
          schema-obj (cond
                       (string? schema)
                       (let [parser (.newInstance schema-parser-class)
                             parse-method (.getMethod schema-parser-class "parse" (into-array Class [String]))]
                         (.invoke parse-method parser (into-array Object [schema])))

                       (instance? (Class/forName "org.apache.avro.Schema") schema)
                       schema

                       :else
                       (throw (ex-info "Schema must be a string or Avro Schema object" {:schema schema})))

          reader-class (Class/forName "org.apache.avro.generic.GenericDatumReader")
          writer-class (Class/forName "org.apache.avro.generic.GenericDatumWriter")
          schema-class (Class/forName "org.apache.avro.Schema")
          reader-ctor (.getConstructor reader-class (into-array Class [schema-class]))
          writer-ctor (.getConstructor writer-class (into-array Class [schema-class]))
          reader (.newInstance reader-ctor (into-array Object [schema-obj]))
          writer (.newInstance writer-ctor (into-array Object [schema-obj]))]

      (reify
        DeserializationSchema
        (deserialize [_ message]
          (when message
            (let [decoder-factory-class (Class/forName "org.apache.avro.io.DecoderFactory")
                  get-method (.getMethod decoder-factory-class "get" (into-array Class []))
                  factory (.invoke get-method nil (into-array Object []))
                  binary-decoder-method (.getMethod (Class/forName "org.apache.avro.io.DecoderFactory")
                                                    "binaryDecoder"
                                                    (into-array Class [(Class/forName "[B")
                                                                       (Class/forName "org.apache.avro.io.BinaryDecoder")]))
                  decoder (.invoke binary-decoder-method factory (into-array Object [message nil]))
                  read-method (.getMethod reader-class "read"
                                          (into-array Class [Object (Class/forName "org.apache.avro.io.Decoder")]))]
              (.invoke read-method reader (into-array Object [nil decoder])))))
        (isEndOfStream [_ _] false)
        (getProducedType [_]
          (TypeInformation/of Object))

        SerializationSchema
        (serialize [_ element]
          (let [baos (java.io.ByteArrayOutputStream.)
                encoder-factory-class (Class/forName "org.apache.avro.io.EncoderFactory")
                get-method (.getMethod encoder-factory-class "get" (into-array Class []))
                factory (.invoke get-method nil (into-array Object []))
                binary-encoder-method (.getMethod (Class/forName "org.apache.avro.io.EncoderFactory")
                                                  "binaryEncoder"
                                                  (into-array Class [java.io.OutputStream
                                                                     (Class/forName "org.apache.avro.io.BinaryEncoder")]))
                encoder (.invoke binary-encoder-method factory (into-array Object [baos nil]))
                write-method (.getMethod writer-class "write"
                                         (into-array Class [Object (Class/forName "org.apache.avro.io.Encoder")]))
                flush-method (.getMethod (Class/forName "org.apache.avro.io.BinaryEncoder") "flush" (into-array Class []))]
            (.invoke write-method writer (into-array Object [element encoder]))
            (.invoke flush-method encoder (into-array Object []))
            (.toByteArray baos)))))))

(defn avro
  "Create an Avro serialization schema with Clojure map conversion.

  This wraps avro-generic and converts GenericRecords to/from Clojure maps.

  Options:
    :schema - Avro schema (string or Schema object)
    :schema-registry - Schema Registry URL
    :subject - Schema subject name

  Example:
    (kafka/source {:value-format (serde/avro {:schema-registry \"http://localhost:8081\"
                                              :subject \"events-value\"})})"
  [opts]
  (let [inner (avro-generic opts)]
    (reify
      DeserializationSchema
      (deserialize [_ message]
        (when-let [record (.deserialize inner message)]
          ;; Convert GenericRecord to Clojure map
          (try
            (let [record-class (Class/forName "org.apache.avro.generic.GenericRecord")
                  get-schema (.getMethod record-class "getSchema" (into-array Class []))
                  schema (.invoke get-schema record (into-array Object []))
                  fields-class (Class/forName "org.apache.avro.Schema")
                  get-fields (.getMethod fields-class "getFields" (into-array Class []))
                  fields (.invoke get-fields schema (into-array Object []))
                  field-class (Class/forName "org.apache.avro.Schema$Field")
                  name-method (.getMethod field-class "name" (into-array Class []))
                  get-method (.getMethod record-class "get" (into-array Class [String]))]
              (into {}
                    (for [field fields]
                      (let [field-name (.invoke name-method field (into-array Object []))
                            value (.invoke get-method record (into-array Object [field-name]))]
                        [(keyword field-name) value]))))
            (catch Exception e
              ;; If conversion fails, return raw record
              record))))
      (isEndOfStream [_ _] false)
      (getProducedType [_]
        (TypeInformation/of Object))

      SerializationSchema
      (serialize [_ element]
        ;; For serialization, we need the schema to create GenericRecord
        ;; This is more complex - for now, pass through if already a record
        (.serialize inner element)))))

;; =============================================================================
;; Protocol Buffers Schema
;; =============================================================================

(defn- protobuf-available?
  "Check if Protocol Buffers is available."
  []
  (try
    (Class/forName "com.google.protobuf.Message")
    true
    (catch ClassNotFoundException _ false)))

(defn protobuf
  "Create a Protocol Buffers serialization schema.

  Options:
    :message-class - The generated protobuf Message class

  Requires [com.google.protobuf/protobuf-java] on classpath.

  Example:
    (kafka/source {:value-format (serde/protobuf {:message-class MyProto$Event})})"
  [{:keys [message-class]}]
  (when-not (protobuf-available?)
    (throw (ex-info "Protocol Buffers not available."
                    {:suggestion "[com.google.protobuf/protobuf-java \"3.25.1\"]"})))
  (when-not message-class
    (throw (ex-info "message-class is required for protobuf serialization" {})))

  (let [message-base-class (Class/forName "com.google.protobuf.Message")
        parser-method (.getMethod message-class "parser" (into-array Class []))
        parser (.invoke parser-method nil (into-array Object []))
        parser-class (Class/forName "com.google.protobuf.Parser")
        parse-from (.getMethod parser-class "parseFrom" (into-array Class [(Class/forName "[B")]))]

    (reify
      DeserializationSchema
      (deserialize [_ message]
        (when message
          (.invoke parse-from parser (into-array Object [message]))))
      (isEndOfStream [_ _] false)
      (getProducedType [_]
        (TypeInformation/of message-class))

      SerializationSchema
      (serialize [_ element]
        (when element
          (let [to-byte-array (.getMethod message-base-class "toByteArray" (into-array Class []))]
            (.invoke to-byte-array element (into-array Object []))))))))

;; =============================================================================
;; MessagePack Schema
;; =============================================================================

(defn- msgpack-available?
  "Check if MessagePack is available."
  []
  (try
    (require 'msgpack.core)
    true
    (catch Exception _ false)))

(defn msgpack
  "Create a MessagePack serialization schema.

  MessagePack is an efficient binary serialization format - like JSON but faster
  and smaller.

  Requires [clojure-msgpack \"1.2.1\"] on classpath.

  Example:
    (kafka/source {:value-format (serde/msgpack)})"
  []
  (when-not (msgpack-available?)
    (throw (ex-info "MessagePack not available."
                    {:suggestion "[clojure-msgpack \"1.2.1\"]"})))
  (let [pack (requiring-resolve 'msgpack.core/pack)
        unpack (requiring-resolve 'msgpack.core/unpack)]
    (reify
      DeserializationSchema
      (deserialize [_ message]
        (when message
          (unpack message)))
      (isEndOfStream [_ _] false)
      (getProducedType [_]
        (TypeInformation/of Object))

      SerializationSchema
      (serialize [_ element]
        (pack element)))))

;; =============================================================================
;; Transit Schema
;; =============================================================================

(defn- transit-available?
  "Check if Transit is available."
  []
  (try
    (require 'cognitect.transit)
    true
    (catch Exception _ false)))

(defn transit
  "Create a Transit serialization schema.

  Transit is a format for conveying values between applications written
  in different programming languages.

  Options:
    :type - :json (default), :json-verbose, or :msgpack
    :handlers - Custom write/read handlers

  Requires [com.cognitect/transit-clj] on classpath.

  Example:
    (kafka/source {:value-format (serde/transit)})
    (kafka/source {:value-format (serde/transit {:type :msgpack})})"
  ([] (transit {}))
  ([{:keys [type handlers] :or {type :json}}]
   (when-not (transit-available?)
     (throw (ex-info "Transit not available."
                     {:suggestion "[com.cognitect/transit-clj \"1.0.333\"]"})))
   (let [write (requiring-resolve 'cognitect.transit/write)
         writer (requiring-resolve 'cognitect.transit/writer)
         read (requiring-resolve 'cognitect.transit/read)
         reader (requiring-resolve 'cognitect.transit/reader)
         write-opts (when handlers {:handlers handlers})
         read-opts (when handlers {:handlers handlers})]
     (reify
       DeserializationSchema
       (deserialize [_ message]
         (when message
           (let [in (java.io.ByteArrayInputStream. message)
                 r (if read-opts
                     (reader in type read-opts)
                     (reader in type))]
             (read r))))
       (isEndOfStream [_ _] false)
       (getProducedType [_]
         (TypeInformation/of Object))

       SerializationSchema
       (serialize [_ element]
         (let [out (java.io.ByteArrayOutputStream.)
               w (if write-opts
                   (writer out type write-opts)
                   (writer out type))]
           (write w element)
           (.toByteArray out)))))))

;; =============================================================================
;; Raw Bytes Schema
;; =============================================================================

(defn raw-bytes
  "Pass-through schema that returns raw bytes without transformation.

  Useful when you want to handle deserialization yourself.

  Example:
    (kafka/source {:value-format (serde/raw-bytes)})"
  []
  (reify
    DeserializationSchema
    (deserialize [_ message] message)
    (isEndOfStream [_ _] false)
    (getProducedType [_]
      (TypeInformation/of (Class/forName "[B")))

    SerializationSchema
    (serialize [_ element]
      (cond
        (bytes? element) element
        (string? element) (.getBytes ^String element StandardCharsets/UTF_8)
        :else (throw (ex-info "Cannot serialize to bytes" {:element element}))))))

;; =============================================================================
;; Composite / Wrapper Schemas
;; =============================================================================

(defn with-key
  "Wrap a value schema to also include the Kafka message key.

  Returns records as {:key key :value value}.

  Example:
    (kafka/source {:value-format (serde/with-key (serde/json))})"
  [value-schema]
  ;; This requires access to the ConsumerRecord, which isn't available
  ;; through the standard DeserializationSchema interface.
  ;; For full key support, users should use KafkaRecordDeserializationSchema.
  (throw (ex-info "with-key requires KafkaRecordDeserializationSchema - use kafka/source-with-metadata instead"
                  {:suggestion "Use kafka/source-with-metadata for key access"})))

(defn transform
  "Wrap a schema with pre/post transformation functions.

  Options:
    :before-serialize - Function to apply before serialization
    :after-deserialize - Function to apply after deserialization

  Example:
    ;; Add timestamp after deserializing
    (serde/transform (serde/json)
                     {:after-deserialize #(assoc % :received-at (System/currentTimeMillis))})"
  [schema {:keys [before-serialize after-deserialize]}]
  (reify
    DeserializationSchema
    (deserialize [_ message]
      (let [result (.deserialize ^DeserializationSchema schema message)]
        (if after-deserialize
          (after-deserialize result)
          result)))
    (isEndOfStream [_ nextElement]
      (.isEndOfStream ^DeserializationSchema schema nextElement))
    (getProducedType [_]
      (.getProducedType ^DeserializationSchema schema))

    SerializationSchema
    (serialize [_ element]
      (let [to-serialize (if before-serialize
                           (before-serialize element)
                           element)]
        (.serialize ^SerializationSchema schema to-serialize)))))

;; =============================================================================
;; Type-Safe Schemas
;; =============================================================================

(defn typed
  "Create a schema with explicit Flink TypeInformation.

  This helps Flink's type system understand your data better,
  which can improve performance and enable certain optimizations.

  Example:
    (serde/typed (serde/json) {:type-info (Types/MAP (Types/STRING) (Types/LONG))})"
  [schema {:keys [type-info]}]
  (reify
    DeserializationSchema
    (deserialize [_ message]
      (.deserialize ^DeserializationSchema schema message))
    (isEndOfStream [_ nextElement]
      (.isEndOfStream ^DeserializationSchema schema nextElement))
    (getProducedType [_]
      type-info)

    SerializationSchema
    (serialize [_ element]
      (.serialize ^SerializationSchema schema element))))
