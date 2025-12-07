(ns flink-clj.connectors.rabbitmq
  "RabbitMQ connector for consuming and producing messages.

  Example - Source:
    (require '[flink-clj.connectors.rabbitmq :as rmq])

    (def source
      (rmq/source {:host \"localhost\"
                   :queue \"events\"
                   :deserializer :string}))

    (-> env
        (flink/add-source source \"RabbitMQ Source\")
        ...)

  Example - Sink:
    (def sink
      (rmq/sink {:host \"localhost\"
                 :queue \"output\"
                 :serializer :string}))

    (-> stream
        (flink/add-sink sink \"RabbitMQ Sink\"))

  NOTE: Requires flink-connector-rabbitmq dependency."
  (:require [flink-clj.connectors.generic :as conn])
  (:import [java.util Properties]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- rmq-available?
  "Check if RabbitMQ connector is available."
  []
  (conn/connector-available? "org.apache.flink.streaming.connectors.rabbitmq.RMQSource"))

(defn- require-rmq!
  "Ensure RabbitMQ connector is available."
  []
  (when-not (rmq-available?)
    (throw (ex-info "RabbitMQ connector not available"
                    {:suggestion "Add flink-connector-rabbitmq dependency"}))))

;; =============================================================================
;; Connection Config
;; =============================================================================

(defn connection-config
  "Create a RabbitMQ connection configuration.

  Options:
    :host           - RabbitMQ host (required)
    :port           - Port (default: 5672)
    :virtual-host   - Virtual host (default: \"/\")
    :username       - Username (default: \"guest\")
    :password       - Password (default: \"guest\")
    :uri            - Full AMQP URI (alternative to host/port/etc)
    :automatic-recovery - Enable auto recovery (default: true)
    :topology-recovery - Enable topology recovery (default: true)
    :connection-timeout - Connection timeout in ms
    :network-recovery-interval - Recovery interval in ms
    :prefetch-count - QoS prefetch count

  Example:
    (connection-config {:host \"localhost\"
                        :username \"myuser\"
                        :password \"mypass\"
                        :virtual-host \"/myvhost\"})"
  [{:keys [host port virtual-host username password uri
           automatic-recovery topology-recovery connection-timeout
           network-recovery-interval prefetch-count]
    :or {port 5672 virtual-host "/" username "guest" password "guest"
         automatic-recovery true topology-recovery true}}]
  (require-rmq!)
  (let [config-class (Class/forName "org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig$Builder")
        builder (.newInstance config-class)]

    (if uri
      ;; Use URI if provided
      (let [uri-method (.getMethod (.getClass builder) "setUri" (into-array Class [String]))]
        (.invoke uri-method builder (into-array Object [uri])))
      ;; Otherwise use individual settings
      (do
        (let [host-method (.getMethod (.getClass builder) "setHost" (into-array Class [String]))]
          (.invoke host-method builder (into-array Object [host])))
        (let [port-method (.getMethod (.getClass builder) "setPort" (into-array Class [Integer/TYPE]))]
          (.invoke port-method builder (into-array Object [(int port)])))
        (let [vhost-method (.getMethod (.getClass builder) "setVirtualHost" (into-array Class [String]))]
          (.invoke vhost-method builder (into-array Object [virtual-host])))
        (let [user-method (.getMethod (.getClass builder) "setUserName" (into-array Class [String]))]
          (.invoke user-method builder (into-array Object [username])))
        (let [pass-method (.getMethod (.getClass builder) "setPassword" (into-array Class [String]))]
          (.invoke pass-method builder (into-array Object [password])))))

    ;; Optional settings
    (let [auto-method (.getMethod (.getClass builder) "setAutomaticRecovery" (into-array Class [Boolean/TYPE]))]
      (.invoke auto-method builder (into-array Object [(boolean automatic-recovery)])))

    (let [topo-method (.getMethod (.getClass builder) "setTopologyRecoveryEnabled" (into-array Class [Boolean/TYPE]))]
      (.invoke topo-method builder (into-array Object [(boolean topology-recovery)])))

    (when connection-timeout
      (let [method (.getMethod (.getClass builder) "setConnectionTimeout" (into-array Class [Integer/TYPE]))]
        (.invoke method builder (into-array Object [(int connection-timeout)]))))

    (when network-recovery-interval
      (let [method (.getMethod (.getClass builder) "setNetworkRecoveryInterval" (into-array Class [Integer/TYPE]))]
        (.invoke method builder (into-array Object [(int network-recovery-interval)]))))

    (when prefetch-count
      (let [method (.getMethod (.getClass builder) "setPrefetchCount" (into-array Class [Integer/TYPE]))]
        (.invoke method builder (into-array Object [(int prefetch-count)]))))

    ;; Build
    (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Deserializers/Serializers
;; =============================================================================

(defn- resolve-deserializer
  "Resolve deserializer spec to RMQDeserializationSchema."
  [spec]
  (require-rmq!)
  (cond
    (keyword? spec)
    (case spec
      :string (let [schema-class (Class/forName "org.apache.flink.api.common.serialization.SimpleStringSchema")]
                (.newInstance schema-class))
      :bytes (let [schema-class (Class/forName "org.apache.flink.api.common.serialization.TypeInformationSerializationSchema")]
               ;; Return raw bytes
               nil)
      (throw (ex-info "Unknown deserializer" {:spec spec})))

    :else spec))

(defn- resolve-serializer
  "Resolve serializer spec to SerializationSchema."
  [spec]
  (cond
    (keyword? spec)
    (case spec
      :string (conn/string-schema)
      (throw (ex-info "Unknown serializer" {:spec spec})))
    :else spec))

;; =============================================================================
;; Source
;; =============================================================================

(defn source
  "Create a RabbitMQ source.

  Options:
    :host         - RabbitMQ host (required unless :connection provided)
    :port         - Port (default: 5672)
    :virtual-host - Virtual host (default: \"/\")
    :username     - Username (default: \"guest\")
    :password     - Password (default: \"guest\")
    :queue        - Queue name to consume from (required)
    :deserializer - Deserialization: :string or DeserializationSchema
    :connection   - Pre-built RMQConnectionConfig (alternative to host/port/etc)
    :use-correlation-id - Use correlation ID for deduplication

  Example:
    (rmq/source {:host \"localhost\"
                 :queue \"events\"
                 :deserializer :string})"
  [{:keys [host port virtual-host username password queue deserializer
           connection use-correlation-id]
    :or {port 5672 virtual-host "/" username "guest" password "guest"
         deserializer :string use-correlation-id false}
    :as opts}]
  (require-rmq!)
  (let [conn-config (or connection
                        (connection-config (select-keys opts [:host :port :virtual-host
                                                              :username :password])))
        deser (resolve-deserializer deserializer)
        source-class (Class/forName "org.apache.flink.streaming.connectors.rabbitmq.RMQSource")
        conn-config-class (Class/forName "org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig")
        deser-class (Class/forName "org.apache.flink.api.common.serialization.DeserializationSchema")
        ctor (.getConstructor source-class
                              (into-array Class [conn-config-class String deser-class]))]
    (.newInstance ctor (into-array Object [conn-config queue deser]))))

;; =============================================================================
;; Sink
;; =============================================================================

(defn sink
  "Create a RabbitMQ sink.

  Options:
    :host         - RabbitMQ host (required unless :connection provided)
    :port         - Port (default: 5672)
    :virtual-host - Virtual host (default: \"/\")
    :username     - Username (default: \"guest\")
    :password     - Password (default: \"guest\")
    :queue        - Queue name to publish to (required)
    :serializer   - Serialization: :string or SerializationSchema
    :connection   - Pre-built RMQConnectionConfig (alternative to host/port/etc)

  Example:
    (rmq/sink {:host \"localhost\"
               :queue \"output\"
               :serializer :string})"
  [{:keys [host port virtual-host username password queue serializer connection]
    :or {port 5672 virtual-host "/" username "guest" password "guest"
         serializer :string}
    :as opts}]
  (require-rmq!)
  (let [conn-config (or connection
                        (connection-config (select-keys opts [:host :port :virtual-host
                                                              :username :password])))
        ser (resolve-serializer serializer)
        sink-class (Class/forName "org.apache.flink.streaming.connectors.rabbitmq.RMQSink")
        conn-config-class (Class/forName "org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig")
        ser-class (Class/forName "org.apache.flink.api.common.serialization.SerializationSchema")
        ctor (.getConstructor sink-class
                              (into-array Class [conn-config-class String ser-class]))]
    (.newInstance ctor (into-array Object [conn-config queue ser]))))

;; =============================================================================
;; Info
;; =============================================================================

(defn rabbitmq-info
  "Get information about RabbitMQ connector availability."
  []
  {:available (rmq-available?)
   :features [:source :sink :automatic-recovery :prefetch-control]
   :protocols [:amqp]})
