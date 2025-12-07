(ns flink-clj.connectors.pulsar
  "Apache Pulsar connector for consuming and producing messages.

  Supports both batch and streaming modes with exactly-once semantics.

  Example - Source:
    (require '[flink-clj.connectors.pulsar :as pulsar])

    (def source
      (pulsar/source {:service-url \"pulsar://localhost:6650\"
                      :admin-url \"http://localhost:8080\"
                      :topics [\"persistent://tenant/ns/topic\"]
                      :subscription \"my-sub\"
                      :deserializer :string}))

    (-> env
        (flink/from-source source \"Pulsar Source\")
        ...)

  Example - Sink:
    (def sink
      (pulsar/sink {:service-url \"pulsar://localhost:6650\"
                    :topic \"persistent://tenant/ns/output\"
                    :serializer :string}))

    (-> stream
        (flink/to-sink sink \"Pulsar Sink\"))

  NOTE: Requires flink-connector-pulsar dependency."
  (:require [flink-clj.connectors.generic :as conn])
  (:import [java.time Duration]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- pulsar-source-available?
  "Check if Pulsar source connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.pulsar.source.PulsarSource"))

(defn- pulsar-sink-available?
  "Check if Pulsar sink connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.pulsar.sink.PulsarSink"))

(defn- require-pulsar-source!
  "Ensure Pulsar source connector is available."
  []
  (when-not (pulsar-source-available?)
    (throw (ex-info "Pulsar source connector not available"
                    {:suggestion "Add flink-connector-pulsar dependency"}))))

(defn- require-pulsar-sink!
  "Ensure Pulsar sink connector is available."
  []
  (when-not (pulsar-sink-available?)
    (throw (ex-info "Pulsar sink connector not available"
                    {:suggestion "Add flink-connector-pulsar dependency"}))))

;; =============================================================================
;; Deserialization Schema
;; =============================================================================

(defn- resolve-deserializer
  "Resolve deserializer spec to PulsarDeserializationSchema."
  [spec]
  (require-pulsar-source!)
  (cond
    (keyword? spec)
    (case spec
      :string (let [schema-class (Class/forName "org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema")
                    simple-schema (Class/forName "org.apache.flink.api.common.serialization.SimpleStringSchema")
                    wrap-method (.getMethod schema-class "flinkSchema" (into-array Class [(Class/forName "org.apache.flink.api.common.serialization.DeserializationSchema")]))]
                (.invoke wrap-method nil (into-array Object [(.newInstance simple-schema)])))

      :json (let [schema-class (Class/forName "org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema")
                  simple-schema (Class/forName "org.apache.flink.api.common.serialization.SimpleStringSchema")
                  wrap-method (.getMethod schema-class "flinkSchema" (into-array Class [(Class/forName "org.apache.flink.api.common.serialization.DeserializationSchema")]))]
              (.invoke wrap-method nil (into-array Object [(.newInstance simple-schema)])))

      (throw (ex-info "Unknown deserializer" {:spec spec})))

    :else spec))

;; =============================================================================
;; Serialization Schema
;; =============================================================================

(defn- resolve-serializer
  "Resolve serializer spec to PulsarSerializationSchema."
  [spec topic]
  (require-pulsar-sink!)
  (cond
    (keyword? spec)
    (case spec
      :string (let [schema-class (Class/forName "org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema")
                    simple-schema (Class/forName "org.apache.flink.api.common.serialization.SimpleStringSchema")
                    wrap-method (.getMethod schema-class "flinkSchema" (into-array Class [(Class/forName "org.apache.flink.api.common.serialization.SerializationSchema")]))]
                (.invoke wrap-method nil (into-array Object [(.newInstance simple-schema)])))

      :json (let [schema-class (Class/forName "org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema")
                  simple-schema (Class/forName "org.apache.flink.api.common.serialization.SimpleStringSchema")
                  wrap-method (.getMethod schema-class "flinkSchema" (into-array Class [(Class/forName "org.apache.flink.api.common.serialization.SerializationSchema")]))]
              (.invoke wrap-method nil (into-array Object [(.newInstance simple-schema)])))

      (throw (ex-info "Unknown serializer" {:spec spec})))

    :else spec))

;; =============================================================================
;; Subscription Types
;; =============================================================================

(defn- resolve-subscription-type
  "Convert subscription type keyword to Pulsar SubscriptionType."
  [type-kw]
  (require-pulsar-source!)
  (let [sub-type-class (Class/forName "org.apache.pulsar.client.api.SubscriptionType")]
    (case type-kw
      :exclusive (Enum/valueOf sub-type-class "Exclusive")
      :shared (Enum/valueOf sub-type-class "Shared")
      :failover (Enum/valueOf sub-type-class "Failover")
      :key-shared (Enum/valueOf sub-type-class "Key_Shared")
      (throw (ex-info "Unknown subscription type" {:type type-kw})))))

;; =============================================================================
;; Start Cursor
;; =============================================================================

(defn- resolve-start-cursor
  "Convert start cursor spec to StartCursor."
  [spec]
  (require-pulsar-source!)
  (let [cursor-class (Class/forName "org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor")]
    (cond
      (keyword? spec)
      (case spec
        :earliest (let [method (.getMethod cursor-class "earliest" (into-array Class []))]
                    (.invoke method nil (into-array Object [])))
        :latest (let [method (.getMethod cursor-class "latest" (into-array Class []))]
                  (.invoke method nil (into-array Object [])))
        (throw (ex-info "Unknown start cursor" {:spec spec})))

      (number? spec)
      ;; Timestamp
      (let [method (.getMethod cursor-class "fromMessageTime" (into-array Class [Long/TYPE]))]
        (.invoke method nil (into-array Object [(long spec)])))

      :else spec)))

;; =============================================================================
;; Stop Cursor
;; =============================================================================

(defn- resolve-stop-cursor
  "Convert stop cursor spec to StopCursor."
  [spec]
  (require-pulsar-source!)
  (let [cursor-class (Class/forName "org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor")]
    (cond
      (keyword? spec)
      (case spec
        :never (let [method (.getMethod cursor-class "never" (into-array Class []))]
                 (.invoke method nil (into-array Object [])))
        :latest (let [method (.getMethod cursor-class "latest" (into-array Class []))]
                  (.invoke method nil (into-array Object [])))
        (throw (ex-info "Unknown stop cursor" {:spec spec})))

      (number? spec)
      ;; Timestamp
      (let [method (.getMethod cursor-class "atEventTime" (into-array Class [Long/TYPE]))]
        (.invoke method nil (into-array Object [(long spec)])))

      :else spec)))

;; =============================================================================
;; Source
;; =============================================================================

(defn source
  "Create a Pulsar source.

  Required options:
    :service-url  - Pulsar service URL (e.g., \"pulsar://localhost:6650\")
    :admin-url    - Pulsar admin URL (e.g., \"http://localhost:8080\")
    :topics       - Vector of topic names or single topic string
    :subscription - Subscription name

  Optional options:
    :deserializer       - :string, :json, or PulsarDeserializationSchema (default: :string)
    :subscription-type  - :exclusive, :shared, :failover, :key-shared (default: :exclusive)
    :start-cursor       - :earliest, :latest, or timestamp (default: :latest)
    :stop-cursor        - :never, :latest, or timestamp (default: :never)
    :range-generator    - Custom range generator for splits
    :config             - Map of additional Pulsar client config

  Example:
    (pulsar/source {:service-url \"pulsar://localhost:6650\"
                    :admin-url \"http://localhost:8080\"
                    :topics [\"persistent://public/default/events\"]
                    :subscription \"flink-consumer\"
                    :deserializer :string
                    :subscription-type :shared
                    :start-cursor :earliest})"
  [{:keys [service-url admin-url topics subscription deserializer
           subscription-type start-cursor stop-cursor range-generator config]
    :or {deserializer :string
         subscription-type :exclusive
         start-cursor :latest
         stop-cursor :never}
    :as opts}]
  ;; Validate required options
  (when-not service-url
    (throw (ex-info "service-url is required" {:opts opts})))
  (when-not admin-url
    (throw (ex-info "admin-url is required" {:opts opts})))
  (when-not topics
    (throw (ex-info "topics is required" {:opts opts})))
  (when-not subscription
    (throw (ex-info "subscription is required" {:opts opts})))

  (require-pulsar-source!)

  (let [builder-class (Class/forName "org.apache.flink.connector.pulsar.source.PulsarSourceBuilder")
        builder-method (.getMethod (Class/forName "org.apache.flink.connector.pulsar.source.PulsarSource")
                                   "builder" (into-array Class []))
        builder (.invoke builder-method nil (into-array Object []))
        builder-type (.getClass builder)

        ;; Convert topics to array
        topic-list (if (string? topics) [topics] topics)]

    ;; Set service URL
    (let [method (.getMethod builder-type "setServiceUrl" (into-array Class [String]))]
      (.invoke method builder (into-array Object [service-url])))

    ;; Set admin URL
    (let [method (.getMethod builder-type "setAdminUrl" (into-array Class [String]))]
      (.invoke method builder (into-array Object [admin-url])))

    ;; Set topics
    (let [method (.getMethod builder-type "setTopics" (into-array Class [(Class/forName "[Ljava.lang.String;")]))]
      (.invoke method builder (into-array Object [(into-array String topic-list)])))

    ;; Set subscription name
    (let [method (.getMethod builder-type "setSubscriptionName" (into-array Class [String]))]
      (.invoke method builder (into-array Object [subscription])))

    ;; Set deserializer
    (let [deser (resolve-deserializer deserializer)
          deser-class (Class/forName "org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema")
          method (.getMethod builder-type "setDeserializationSchema" (into-array Class [deser-class]))]
      (.invoke method builder (into-array Object [deser])))

    ;; Set subscription type
    (let [sub-type (resolve-subscription-type subscription-type)
          sub-type-class (Class/forName "org.apache.pulsar.client.api.SubscriptionType")
          method (.getMethod builder-type "setSubscriptionType" (into-array Class [sub-type-class]))]
      (.invoke method builder (into-array Object [sub-type])))

    ;; Set start cursor
    (let [cursor (resolve-start-cursor start-cursor)
          cursor-class (Class/forName "org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor")
          method (.getMethod builder-type "setStartCursor" (into-array Class [cursor-class]))]
      (.invoke method builder (into-array Object [cursor])))

    ;; Set stop cursor
    (let [cursor (resolve-stop-cursor stop-cursor)
          cursor-class (Class/forName "org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor")
          method (.getMethod builder-type "setUnboundedStopCursor" (into-array Class [cursor-class]))]
      (.invoke method builder (into-array Object [cursor])))

    ;; Apply additional config
    (when config
      (doseq [[k v] config]
        (let [config-method (try
                              (.getMethod builder-type (str "set" (name k)) (into-array Class [(.getClass v)]))
                              (catch NoSuchMethodException _ nil))]
          (when config-method
            (.invoke config-method builder (into-array Object [v]))))))

    ;; Build
    (let [build-method (.getMethod builder-type "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Sink
;; =============================================================================

(defn sink
  "Create a Pulsar sink.

  Required options:
    :service-url  - Pulsar service URL
    :topic        - Topic to write to (or use :topic-router)

  Optional options:
    :serializer     - :string, :json, or PulsarSerializationSchema (default: :string)
    :topic-router   - Custom topic router function
    :delivery-guarantee - :at-least-once, :exactly-once, :none (default: :at-least-once)
    :config         - Map of additional Pulsar producer config

  Example:
    (pulsar/sink {:service-url \"pulsar://localhost:6650\"
                  :topic \"persistent://public/default/output\"
                  :serializer :string
                  :delivery-guarantee :exactly-once})"
  [{:keys [service-url topic serializer topic-router delivery-guarantee config]
    :or {serializer :string
         delivery-guarantee :at-least-once}
    :as opts}]
  ;; Validate required options
  (when-not service-url
    (throw (ex-info "service-url is required" {:opts opts})))
  (when (and (nil? topic) (nil? topic-router))
    (throw (ex-info "Either topic or topic-router is required" {:opts opts})))

  (require-pulsar-sink!)

  (let [builder-class (Class/forName "org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder")
        builder-method (.getMethod (Class/forName "org.apache.flink.connector.pulsar.sink.PulsarSink")
                                   "builder" (into-array Class []))
        builder (.invoke builder-method nil (into-array Object []))
        builder-type (.getClass builder)]

    ;; Set service URL
    (let [method (.getMethod builder-type "setServiceUrl" (into-array Class [String]))]
      (.invoke method builder (into-array Object [service-url])))

    ;; Set topic(s)
    (when topic
      (let [topic-list (if (string? topic) [topic] topic)
            method (.getMethod builder-type "setTopics" (into-array Class [(Class/forName "[Ljava.lang.String;")]))]
        (.invoke method builder (into-array Object [(into-array String topic-list)]))))

    ;; Set serializer
    (let [ser (resolve-serializer serializer topic)
          ser-class (Class/forName "org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema")
          method (.getMethod builder-type "setSerializationSchema" (into-array Class [ser-class]))]
      (.invoke method builder (into-array Object [ser])))

    ;; Set delivery guarantee
    (let [guarantee-class (Class/forName "org.apache.flink.connector.base.DeliveryGuarantee")
          guarantee (case delivery-guarantee
                      :none (Enum/valueOf guarantee-class "NONE")
                      :at-least-once (Enum/valueOf guarantee-class "AT_LEAST_ONCE")
                      :exactly-once (Enum/valueOf guarantee-class "EXACTLY_ONCE")
                      (throw (ex-info "Unknown delivery guarantee" {:guarantee delivery-guarantee})))
          method (.getMethod builder-type "setDeliveryGuarantee" (into-array Class [guarantee-class]))]
      (.invoke method builder (into-array Object [guarantee])))

    ;; Apply additional config
    (when config
      (doseq [[k v] config]
        (let [config-method (try
                              (.getMethod builder-type (str "set" (name k)) (into-array Class [(.getClass v)]))
                              (catch NoSuchMethodException _ nil))]
          (when config-method
            (.invoke config-method builder (into-array Object [v]))))))

    ;; Build
    (let [build-method (.getMethod builder-type "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn simple-source
  "Create a simple Pulsar source with common defaults.

  Arguments:
    service-url  - Pulsar service URL
    admin-url    - Pulsar admin URL
    topic        - Topic to consume from
    subscription - Subscription name

  Options:
    :subscription-type - :exclusive, :shared, :failover, :key-shared
    :start-cursor      - :earliest, :latest

  Example:
    (simple-source \"pulsar://localhost:6650\"
                   \"http://localhost:8080\"
                   \"my-topic\"
                   \"my-sub\"
                   {:subscription-type :shared})"
  ([service-url admin-url topic subscription]
   (simple-source service-url admin-url topic subscription {}))
  ([service-url admin-url topic subscription opts]
   (source (merge {:service-url service-url
                   :admin-url admin-url
                   :topics [topic]
                   :subscription subscription
                   :deserializer :string}
                  opts))))

(defn simple-sink
  "Create a simple Pulsar sink with common defaults.

  Arguments:
    service-url - Pulsar service URL
    topic       - Topic to write to

  Options:
    :delivery-guarantee - :at-least-once, :exactly-once, :none

  Example:
    (simple-sink \"pulsar://localhost:6650\" \"my-output-topic\")"
  ([service-url topic]
   (simple-sink service-url topic {}))
  ([service-url topic opts]
   (sink (merge {:service-url service-url
                 :topic topic
                 :serializer :string}
                opts))))

;; =============================================================================
;; Topic Patterns
;; =============================================================================

(defn topic-pattern
  "Create a topic pattern for consuming from multiple topics.

  The pattern follows Java regex syntax.

  Example:
    (topic-pattern \"persistent://tenant/namespace/topic-.*\")"
  [pattern]
  {:type :pattern :pattern pattern})

(defn topics
  "Create a list of explicit topics.

  Example:
    (topics [\"topic1\" \"topic2\" \"topic3\"])"
  [topic-list]
  {:type :list :topics topic-list})

;; =============================================================================
;; Info
;; =============================================================================

(defn pulsar-info
  "Get information about Pulsar connector availability."
  []
  {:source-available (pulsar-source-available?)
   :sink-available (pulsar-sink-available?)
   :features [:exactly-once
              :shared-subscription
              :key-shared-subscription
              :topic-patterns
              :schema-registry
              :dead-letter-queues]
   :subscription-types [:exclusive :shared :failover :key-shared]})
