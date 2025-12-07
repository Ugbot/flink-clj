(ns flink-clj.connectors.kafka
  "Kafka connector for Flink streams.

  Provides idiomatic Clojure wrappers for Kafka sources and sinks using
  the modern KafkaSource and KafkaSink builders.

  NOTE: Requires flink-connector-kafka dependency for the appropriate Flink version.
        This module uses dynamic class loading and will throw descriptive errors
        if the Kafka connector is not available.

  Example - Kafka Source:
    (require '[flink-clj.connectors.kafka :as kafka])

    (def source
      (kafka/source
        {:bootstrap-servers \"localhost:9092\"
         :topics [\"my-topic\"]
         :group-id \"my-consumer-group\"
         :starting-offsets :earliest
         :value-format :string}))

    (-> env
        (flink/from-source source \"Kafka Source\")
        (stream/flink-map process-fn)
        ...)

  Example - Kafka Sink:
    (def sink
      (kafka/sink
        {:bootstrap-servers \"localhost:9092\"
         :topic \"output-topic\"
         :value-format :string}))

    (-> stream
        (flink/to-sink sink \"Kafka Sink\"))"
  (:require [flink-clj.types :as types])
  (:import [org.apache.flink.api.common.serialization SimpleStringSchema]
           [java.util Properties]
           [java.util.regex Pattern]))

;; =============================================================================
;; Dynamic Class Loading Helpers
;; =============================================================================

(defn- kafka-available?
  "Check if Kafka connector is available."
  []
  (try
    (Class/forName "org.apache.flink.connector.kafka.source.KafkaSource")
    true
    (catch ClassNotFoundException _ false)))

(defn- require-kafka!
  "Throw an error if Kafka connector is not available."
  []
  (when-not (kafka-available?)
    (throw (ex-info "Kafka connector not available. Add flink-connector-kafka dependency."
                    {:suggestion "Add [org.apache.flink/flink-connector-kafka \"VERSION\"] to dependencies"}))))

;; =============================================================================
;; Offset Initializers
;; =============================================================================

(defn- resolve-starting-offsets
  "Convert offset spec to OffsetsInitializer."
  [spec]
  (require-kafka!)
  (let [offsets-class (Class/forName "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer")]
    (cond
      (.isInstance offsets-class spec)
      spec

      (keyword? spec)
      (let [method-name (case spec
                          :earliest "earliest"
                          :latest "latest"
                          :committed "committedOffsets"
                          :committed-with-earliest "committedOffsets"
                          :committed-with-latest "committedOffsets"
                          (throw (ex-info "Unknown offset spec" {:spec spec})))
            method (.getMethod offsets-class method-name (into-array Class []))]
        (.invoke method nil (into-array Object [])))

      (number? spec)
      (let [method (.getMethod offsets-class "timestamp" (into-array Class [Long/TYPE]))]
        (.invoke method nil (into-array Object [(long spec)])))

      :else
      (throw (ex-info "Invalid starting-offsets" {:value spec})))))

;; =============================================================================
;; Delivery Guarantees
;; =============================================================================

(defn- resolve-delivery-guarantee
  "Convert delivery guarantee spec to DeliveryGuarantee enum."
  [spec]
  (let [guarantee-class (Class/forName "org.apache.flink.connector.base.DeliveryGuarantee")
        enum-name (case spec
                    :at-least-once "AT_LEAST_ONCE"
                    :exactly-once "EXACTLY_ONCE"
                    :none "NONE"
                    (throw (ex-info "Unknown delivery guarantee" {:spec spec})))]
    (Enum/valueOf guarantee-class enum-name)))

;; =============================================================================
;; Serializers/Deserializers
;; =============================================================================

(defn- resolve-deserializer
  "Create a deserializer based on format spec.

  Accepts:
  - :string - Plain string format (SimpleStringSchema)
  - :json - JSON format (requires cheshire or data.json)
  - :edn - EDN format
  - :nippy - Nippy binary format
  - :transit - Transit format
  - :msgpack - MessagePack format
  - DeserializationSchema instance - Used directly
  - Map with :format key - Passed to serde namespace

  Examples:
    :string
    :json
    {:format :json :key-fn keyword}
    {:format :avro :schema-registry \"http://localhost:8081\"}"
  [format-spec]
  (cond
    ;; Direct schema instance
    (instance? org.apache.flink.api.common.serialization.DeserializationSchema format-spec)
    format-spec

    ;; Keyword shortcuts
    (keyword? format-spec)
    (case format-spec
      :string (SimpleStringSchema.)
      :json ((requiring-resolve 'flink-clj.serde/json))
      :edn ((requiring-resolve 'flink-clj.serde/edn))
      :nippy ((requiring-resolve 'flink-clj.serde/nippy))
      :transit ((requiring-resolve 'flink-clj.serde/transit))
      :msgpack ((requiring-resolve 'flink-clj.serde/msgpack))
      :raw ((requiring-resolve 'flink-clj.serde/raw-bytes))
      (throw (ex-info "Unknown value format keyword"
                      {:format format-spec
                       :valid-formats [:string :json :edn :nippy :transit :msgpack :raw]})))

    ;; Map with format options
    (map? format-spec)
    (let [{:keys [format]} format-spec
          opts (dissoc format-spec :format)]
      (case format
        :json ((requiring-resolve 'flink-clj.serde/json) opts)
        :edn ((requiring-resolve 'flink-clj.serde/edn) opts)
        :nippy ((requiring-resolve 'flink-clj.serde/nippy) opts)
        :avro ((requiring-resolve 'flink-clj.serde/avro) opts)
        :avro-generic ((requiring-resolve 'flink-clj.serde/avro-generic) opts)
        :protobuf ((requiring-resolve 'flink-clj.serde/protobuf) opts)
        :transit ((requiring-resolve 'flink-clj.serde/transit) opts)
        (throw (ex-info "Unknown value format in map spec"
                        {:format format
                         :valid-formats [:json :edn :nippy :avro :avro-generic :protobuf :transit]}))))

    :else
    (throw (ex-info "Invalid value-format. Use keyword, map, or DeserializationSchema"
                    {:format format-spec}))))

(defn- resolve-serializer
  "Create a serializer based on format spec.

  Accepts same formats as resolve-deserializer."
  [format-spec]
  (cond
    ;; Direct schema instance
    (instance? org.apache.flink.api.common.serialization.SerializationSchema format-spec)
    format-spec

    ;; Keyword shortcuts
    (keyword? format-spec)
    (case format-spec
      :string (SimpleStringSchema.)
      :json ((requiring-resolve 'flink-clj.serde/json))
      :edn ((requiring-resolve 'flink-clj.serde/edn))
      :nippy ((requiring-resolve 'flink-clj.serde/nippy))
      :transit ((requiring-resolve 'flink-clj.serde/transit))
      :msgpack ((requiring-resolve 'flink-clj.serde/msgpack))
      :raw ((requiring-resolve 'flink-clj.serde/raw-bytes))
      (throw (ex-info "Unknown value format keyword"
                      {:format format-spec
                       :valid-formats [:string :json :edn :nippy :transit :msgpack :raw]})))

    ;; Map with format options
    (map? format-spec)
    (let [{:keys [format]} format-spec
          opts (dissoc format-spec :format)]
      (case format
        :json ((requiring-resolve 'flink-clj.serde/json) opts)
        :edn ((requiring-resolve 'flink-clj.serde/edn) opts)
        :nippy ((requiring-resolve 'flink-clj.serde/nippy) opts)
        :avro ((requiring-resolve 'flink-clj.serde/avro) opts)
        :avro-generic ((requiring-resolve 'flink-clj.serde/avro-generic) opts)
        :protobuf ((requiring-resolve 'flink-clj.serde/protobuf) opts)
        :transit ((requiring-resolve 'flink-clj.serde/transit) opts)
        (throw (ex-info "Unknown value format in map spec"
                        {:format format
                         :valid-formats [:json :edn :nippy :avro :avro-generic :protobuf :transit]}))))

    :else
    (throw (ex-info "Invalid value-format. Use keyword, map, or SerializationSchema"
                    {:format format-spec}))))

;; =============================================================================
;; Properties Builder
;; =============================================================================

(defn- build-properties
  "Build a Properties object from a Clojure map."
  [props-map]
  (let [props (Properties.)]
    (doseq [[k v] props-map]
      (.setProperty props (name k) (str v)))
    props))

;; =============================================================================
;; Kafka Source
;; =============================================================================

(defn source
  "Create a KafkaSource for consuming from Kafka.

  Required options:
    :bootstrap-servers - Kafka broker addresses (string)
    :topics - Topic(s) to consume from (string, vector, or regex pattern)
    :value-format - Deserialization format (:string or DeserializationSchema)

  Optional options:
    :group-id - Consumer group ID (string)
    :starting-offsets - Where to start reading:
                        :earliest, :latest, :committed,
                        :committed-with-earliest, :committed-with-latest,
                        or a timestamp (long)
    :bounded-offsets - Where to stop reading (for bounded source):
                       :latest, :committed, or a timestamp
    :properties - Additional Kafka consumer properties (map)

  Example:
    (source {:bootstrap-servers \"localhost:9092\"
             :topics [\"topic1\" \"topic2\"]
             :group-id \"my-group\"
             :starting-offsets :earliest
             :value-format :string})

    ;; With regex pattern for topics
    (source {:bootstrap-servers \"localhost:9092\"
             :topics #\"events-.*\"
             :value-format :string})"
  [{:keys [bootstrap-servers topics group-id starting-offsets bounded-offsets
           value-format properties]
    :or {starting-offsets :earliest}}]
  (require-kafka!)
  (let [source-class (Class/forName "org.apache.flink.connector.kafka.source.KafkaSource")
        builder-method (.getMethod source-class "builder" (into-array Class []))
        builder (.invoke builder-method nil (into-array Object []))
        builder-class (.getClass builder)]

    ;; Bootstrap servers (required)
    (let [m (.getMethod builder-class "setBootstrapServers" (into-array Class [String]))]
      (.invoke m builder (into-array Object [bootstrap-servers])))

    ;; Topics
    (cond
      (string? topics)
      (let [m (.getMethod builder-class "setTopics" (into-array Class [(Class/forName "[Ljava.lang.String;")]))]
        (.invoke m builder (into-array Object [(into-array String [topics])])))

      (sequential? topics)
      (let [m (.getMethod builder-class "setTopics" (into-array Class [(Class/forName "[Ljava.lang.String;")]))]
        (.invoke m builder (into-array Object [(into-array String topics)])))

      (instance? Pattern topics)
      (let [m (.getMethod builder-class "setTopicPattern" (into-array Class [Pattern]))]
        (.invoke m builder (into-array Object [topics])))

      :else
      (throw (ex-info "Topics must be string, vector of strings, or regex Pattern"
                      {:topics topics})))

    ;; Group ID
    (when group-id
      (let [m (.getMethod builder-class "setGroupId" (into-array Class [String]))]
        (.invoke m builder (into-array Object [group-id]))))

    ;; Starting offsets
    (let [offsets-class (Class/forName "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer")
          m (.getMethod builder-class "setStartingOffsets" (into-array Class [offsets-class]))]
      (.invoke m builder (into-array Object [(resolve-starting-offsets starting-offsets)])))

    ;; Bounded offsets (for bounded sources)
    (when bounded-offsets
      (let [offsets-class (Class/forName "org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer")
            m (.getMethod builder-class "setBounded" (into-array Class [offsets-class]))]
        (.invoke m builder (into-array Object [(resolve-starting-offsets bounded-offsets)]))))

    ;; Deserializer
    (let [deser-class (Class/forName "org.apache.flink.api.common.serialization.DeserializationSchema")
          m (.getMethod builder-class "setValueOnlyDeserializer" (into-array Class [deser-class]))]
      (.invoke m builder (into-array Object [(resolve-deserializer value-format)])))

    ;; Additional properties
    (when properties
      (let [m (.getMethod builder-class "setProperties" (into-array Class [Properties]))]
        (.invoke m builder (into-array Object [(build-properties properties)]))))

    ;; Build
    (let [m (.getMethod builder-class "build" (into-array Class []))]
      (.invoke m builder (into-array Object [])))))

;; =============================================================================
;; Kafka Sink
;; =============================================================================

(defn sink
  "Create a KafkaSink for producing to Kafka.

  Required options:
    :bootstrap-servers - Kafka broker addresses (string)
    :topic - Topic to produce to (string) OR
    :topic-selector - Function to select topic per record (fn [record] -> string)
    :value-format - Serialization format (:string or SerializationSchema)

  Optional options:
    :delivery-guarantee - :at-least-once (default), :exactly-once, or :none
    :transactional-id-prefix - Prefix for transactional IDs (for exactly-once)
    :properties - Additional Kafka producer properties (map)

  Example:
    (sink {:bootstrap-servers \"localhost:9092\"
           :topic \"output-topic\"
           :value-format :string
           :delivery-guarantee :at-least-once})

    ;; With exactly-once semantics
    (sink {:bootstrap-servers \"localhost:9092\"
           :topic \"output-topic\"
           :value-format :string
           :delivery-guarantee :exactly-once
           :transactional-id-prefix \"my-app\"})"
  [{:keys [bootstrap-servers topic topic-selector value-format
           delivery-guarantee transactional-id-prefix properties]
    :or {delivery-guarantee :at-least-once}}]
  (require-kafka!)
  ;; Build record serializer
  (let [schema-class (Class/forName "org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema")
        schema-builder-method (.getMethod schema-class "builder" (into-array Class []))
        schema-builder (.invoke schema-builder-method nil (into-array Object []))
        schema-builder-class (.getClass schema-builder)
        serializer (resolve-serializer value-format)]

    ;; Configure schema builder
    (let [m (.getMethod schema-builder-class "setTopic" (into-array Class [String]))]
      (.invoke m schema-builder (into-array Object [topic])))
    (let [ser-class (Class/forName "org.apache.flink.api.common.serialization.SerializationSchema")
          m (.getMethod schema-builder-class "setValueSerializationSchema" (into-array Class [ser-class]))]
      (.invoke m schema-builder (into-array Object [serializer])))
    (let [build-schema (.getMethod schema-builder-class "build" (into-array Class []))
          record-serializer (.invoke build-schema schema-builder (into-array Object []))]

      ;; Build sink
      (let [sink-class (Class/forName "org.apache.flink.connector.kafka.sink.KafkaSink")
            sink-builder-method (.getMethod sink-class "builder" (into-array Class []))
            builder (.invoke sink-builder-method nil (into-array Object []))
            builder-class (.getClass builder)
            guarantee-class (Class/forName "org.apache.flink.connector.base.DeliveryGuarantee")]

        ;; Bootstrap servers
        (let [m (.getMethod builder-class "setBootstrapServers" (into-array Class [String]))]
          (.invoke m builder (into-array Object [bootstrap-servers])))

        ;; Record serializer
        (let [m (.getMethod builder-class "setRecordSerializer" (into-array Class [schema-class]))]
          (.invoke m builder (into-array Object [record-serializer])))

        ;; Delivery guarantee
        (let [m (.getMethod builder-class "setDeliveryGuarantee" (into-array Class [guarantee-class]))]
          (.invoke m builder (into-array Object [(resolve-delivery-guarantee delivery-guarantee)])))

        ;; Transactional ID prefix (required for exactly-once)
        (when transactional-id-prefix
          (let [m (.getMethod builder-class "setTransactionalIdPrefix" (into-array Class [String]))]
            (.invoke m builder (into-array Object [transactional-id-prefix]))))

        ;; Additional properties
        (when properties
          (let [m (.getMethod builder-class "setKafkaProducerConfig" (into-array Class [Properties]))]
            (.invoke m builder (into-array Object [(build-properties properties)]))))

        ;; Build
        (let [m (.getMethod builder-class "build" (into-array Class []))]
          (.invoke m builder (into-array Object [])))))))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn string-source
  "Convenience function for a simple string-valued Kafka source.

  Example:
    (string-source \"localhost:9092\" \"my-topic\" \"my-group\")"
  ([bootstrap-servers topic]
   (string-source bootstrap-servers topic nil))
  ([bootstrap-servers topic group-id]
   (source {:bootstrap-servers bootstrap-servers
            :topics topic
            :group-id group-id
            :value-format :string
            :starting-offsets :earliest})))

(defn string-sink
  "Convenience function for a simple string-valued Kafka sink.

  Example:
    (string-sink \"localhost:9092\" \"output-topic\")"
  ([bootstrap-servers topic]
   (string-sink bootstrap-servers topic :at-least-once))
  ([bootstrap-servers topic delivery-guarantee]
   (sink {:bootstrap-servers bootstrap-servers
          :topic topic
          :value-format :string
          :delivery-guarantee delivery-guarantee})))
