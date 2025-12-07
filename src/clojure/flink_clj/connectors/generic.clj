(ns flink-clj.connectors.generic
  "Generic connector wrapper for any Flink source or sink.

  Note: Uses `set-prop` instead of `set` to avoid conflict with clojure.core/set.

  Provides a universal interface to configure and use any Flink connector
  without needing a dedicated wrapper. Uses reflection-based builders that
  work with any connector following Flink's builder patterns.

  Example - JDBC Sink:
    (require '[flink-clj.connectors.generic :as conn])

    (def jdbc-sink
      (-> (conn/sink-builder \"org.apache.flink.connector.jdbc.JdbcSink\")
          (conn/invoke-static \"sink\"
            [String JdbcStatementBuilder JdbcConnectionOptions JdbcExecutionOptions]
            [sql-statement statement-builder connection-opts exec-opts])
          (conn/build)))

  Example - Using builder pattern:
    (def es-sink
      (-> (conn/sink-builder \"org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder\")
          (conn/set \"setHosts\" [\"http://localhost:9200\"])
          (conn/set \"setBulkFlushMaxActions\" 1000)
          (conn/set \"setEmitter\" my-emitter)
          (conn/build)))

  Example - Source with fluent builder:
    (def pulsar-source
      (-> (conn/source-builder \"org.apache.flink.connector.pulsar.source.PulsarSource\")
          (conn/invoke-static \"builder\")
          (conn/set \"setServiceUrl\" \"pulsar://localhost:6650\")
          (conn/set \"setTopics\" [\"my-topic\"])
          (conn/set \"setDeserializationSchema\" (conn/string-schema))
          (conn/build)))

  This namespace handles:
  - Reflection-based builder patterns
  - Common serialization schemas
  - Property/configuration mapping
  - Type coercion for Java interop"
  (:require [clojure.string :as str])
  (:import [org.apache.flink.api.common.serialization SimpleStringSchema
            SerializationSchema DeserializationSchema]
           [java.util Properties]))

;; =============================================================================
;; Reflection Utilities
;; =============================================================================

(defn- class-for-name
  "Load a class by name, throwing helpful error if not found."
  [class-name]
  (try
    (Class/forName class-name)
    (catch ClassNotFoundException _
      (throw (ex-info (str "Connector class not found: " class-name
                           ". Ensure the connector JAR is on the classpath.")
                      {:class class-name
                       :hint "Add the connector dependency to your project"})))))

(defn- find-method
  "Find a method by name and parameter count, handling overloads."
  [^Class clazz method-name param-count]
  (let [methods (->> (.getMethods clazz)
                     (filter #(= method-name (.getName %)))
                     (filter #(= param-count (count (.getParameterTypes %)))))]
    (case (count methods)
      0 (throw (ex-info (str "Method not found: " method-name " with " param-count " parameters")
                        {:class (.getName clazz)
                         :method method-name
                         :param-count param-count}))
      1 (first methods)
      ;; Multiple matches - return first, caller should use typed version
      (first methods))))

(defn- find-method-typed
  "Find a method by name and exact parameter types."
  [^Class clazz method-name param-types]
  (try
    (.getMethod clazz method-name (into-array Class param-types))
    (catch NoSuchMethodException _
      (throw (ex-info (str "Method not found: " method-name)
                      {:class (.getName clazz)
                       :method method-name
                       :param-types (mapv #(.getName %) param-types)})))))

(defn- coerce-arg
  "Coerce Clojure values to Java types for reflection calls."
  [arg expected-type]
  (cond
    ;; Nil
    (nil? arg) nil

    ;; Already the right type
    (instance? expected-type arg) arg

    ;; String conversions
    (and (= String expected-type) (keyword? arg))
    (name arg)

    ;; Array conversions
    (and (.isArray expected-type) (sequential? arg))
    (let [component-type (.getComponentType expected-type)]
      (into-array component-type (map #(coerce-arg % component-type) arg)))

    ;; Primitive boxing
    (and (= Integer/TYPE expected-type) (number? arg))
    (int arg)

    (and (= Long/TYPE expected-type) (number? arg))
    (long arg)

    (and (= Double/TYPE expected-type) (number? arg))
    (double arg)

    (and (= Float/TYPE expected-type) (number? arg))
    (float arg)

    (and (= Boolean/TYPE expected-type) (boolean? arg))
    (boolean arg)

    ;; Boxed primitives
    (and (= Integer expected-type) (number? arg))
    (Integer/valueOf (int arg))

    (and (= Long expected-type) (number? arg))
    (Long/valueOf (long arg))

    (and (= Double expected-type) (number? arg))
    (Double/valueOf (double arg))

    ;; Properties from map
    (and (= Properties expected-type) (map? arg))
    (let [props (Properties.)]
      (doseq [[k v] arg]
        (.setProperty props (name k) (str v)))
      props)

    ;; Duration from vector
    (and (= java.time.Duration expected-type) (vector? arg))
    (let [[n unit] arg]
      (case unit
        (:ms :milliseconds) (java.time.Duration/ofMillis n)
        (:s :seconds) (java.time.Duration/ofSeconds n)
        (:m :minutes) (java.time.Duration/ofMinutes n)
        (:h :hours) (java.time.Duration/ofHours n)
        (:d :days) (java.time.Duration/ofDays n)))

    ;; List from vector
    (and (.isAssignableFrom java.util.List expected-type) (vector? arg))
    (java.util.ArrayList. arg)

    ;; Default - pass through
    :else arg))

(defn- invoke-method
  "Invoke a method with automatic argument coercion."
  [obj method-name args]
  (let [clazz (if (class? obj) obj (class obj))
        method (find-method clazz method-name (count args))
        param-types (.getParameterTypes method)
        coerced-args (map coerce-arg args param-types)
        target (if (class? obj) nil obj)]
    (.invoke method target (into-array Object coerced-args))))

;; =============================================================================
;; Builder State
;; =============================================================================

(defrecord ConnectorBuilder [type         ; :source or :sink
                             class-name   ; String class name
                             builder      ; Current builder instance
                             built?       ; Whether build has been called
                             ])

(defn- ensure-not-built
  "Throw if the builder has already been built."
  [{:keys [built? class-name] :as builder}]
  (when built?
    (throw (ex-info "Builder already built. Create a new builder."
                    {:class class-name})))
  builder)

;; =============================================================================
;; Builder Creation
;; =============================================================================

(defn source-builder
  "Create a builder for a Flink Source connector.

  class-name is the fully qualified class name of the source or its builder.

  Examples:
    ;; Direct source class with static builder method
    (source-builder \"org.apache.flink.connector.kafka.source.KafkaSource\")

    ;; Builder class directly
    (source-builder \"org.apache.flink.connector.pulsar.source.PulsarSourceBuilder\")"
  [class-name]
  (->ConnectorBuilder :source class-name nil false))

(defn sink-builder
  "Create a builder for a Flink Sink connector.

  class-name is the fully qualified class name of the sink or its builder.

  Examples:
    ;; Direct sink class with static builder method
    (sink-builder \"org.apache.flink.connector.kafka.sink.KafkaSink\")

    ;; Builder class directly
    (sink-builder \"org.apache.flink.connector.jdbc.JdbcSink\")"
  [class-name]
  (->ConnectorBuilder :sink class-name nil false))

;; =============================================================================
;; Builder Operations
;; =============================================================================

(defn invoke-static
  "Invoke a static method on the connector class.

  Use this to call factory methods like `builder()` or `sink()`.

  Examples:
    ;; No-arg static method
    (invoke-static builder \"builder\")

    ;; With typed arguments
    (invoke-static builder \"sink\" [String] [\"INSERT INTO ...\"])"
  ([{:keys [class-name] :as builder} method-name]
   (ensure-not-built builder)
   (let [clazz (class-for-name class-name)
         method (find-method clazz method-name 0)
         result (.invoke method nil (into-array Object []))]
     (assoc builder :builder result)))

  ([{:keys [class-name] :as builder} method-name param-types args]
   (ensure-not-built builder)
   (let [clazz (class-for-name class-name)
         types (mapv #(if (class? %) % (class-for-name (str %))) param-types)
         method (find-method-typed clazz method-name types)
         coerced-args (map coerce-arg args (.getParameterTypes method))
         result (.invoke method nil (into-array Object coerced-args))]
     (assoc builder :builder result))))

(defn invoke
  "Invoke an instance method on the current builder.

  Use this for builder methods that return the builder (fluent pattern).

  Examples:
    (invoke builder \"setTopic\" [\"my-topic\"])
    (invoke builder \"setBootstrapServers\" [\"localhost:9092\"])"
  [{:keys [builder] :as bldr} method-name args]
  (ensure-not-built bldr)
  (when-not builder
    (throw (ex-info "No builder instance. Call invoke-static first to create one."
                    {})))
  (let [result (invoke-method builder method-name args)]
    ;; If result is same type or subtype, assume fluent pattern
    (if (and result (instance? (class builder) result))
      (assoc bldr :builder result)
      ;; Otherwise, might be terminal method
      (assoc bldr :builder (or result builder)))))

(defn set-prop
  "Set a property on the builder using a setter method.

  Convenience function that calls the setter with a single argument.
  Automatically prefixes 'set' if method name doesn't start with it.

  Examples:
    (set-prop builder :topic \"my-topic\")         ; calls setTopic
    (set-prop builder :bootstrap-servers \"...\")  ; calls setBootstrapServers
    (set-prop builder \"setTopic\" \"my-topic\")   ; explicit method name"
  [{:keys [builder] :as bldr} property value]
  (ensure-not-built bldr)
  (when-not builder
    (throw (ex-info "No builder instance. Call invoke-static first."
                    {})))
  (let [method-name (cond
                      ;; Already a method name
                      (and (string? property) (str/starts-with? property "set"))
                      property

                      ;; Keyword - convert to setter
                      (keyword? property)
                      (let [parts (str/split (name property) #"-")
                            capitalized (map str/capitalize parts)]
                        (str "set" (str/join capitalized)))

                      ;; String without set prefix
                      (string? property)
                      (str "set" (str/capitalize property))

                      :else
                      (throw (ex-info "Invalid property" {:property property})))]
    (invoke bldr method-name [value])))

(defn configure
  "Apply multiple settings from a map.

  Keys are converted to setter method names.

  Example:
    (configure builder
      {:bootstrap-servers \"localhost:9092\"
       :topic \"events\"
       :group-id \"my-group\"})"
  [builder settings]
  (reduce-kv (fn [b k v] (set-prop b k v)) builder settings))

(defn with-properties
  "Set a Properties object on the builder.

  Converts a Clojure map to Java Properties and calls the specified method.

  Example:
    (with-properties builder \"setProperties\"
      {:auto.offset.reset \"earliest\"
       :enable.auto.commit \"false\"})"
  [builder method-name props-map]
  (let [props (Properties.)]
    (doseq [[k v] props-map]
      (.setProperty props (name k) (str v)))
    (invoke builder method-name [props])))

(defn build
  "Build the final connector (source or sink).

  Calls the build() method on the builder and returns the connector.

  Example:
    (def my-source (build source-builder))"
  [{:keys [builder built?] :as bldr}]
  (when built?
    (throw (ex-info "Builder already built" {})))
  (when-not builder
    (throw (ex-info "No builder instance to build" {})))
  (let [result (invoke-method builder "build" [])]
    result))

;; =============================================================================
;; Common Serialization Schemas
;; =============================================================================

(defn string-schema
  "Create a SimpleStringSchema for string serialization/deserialization."
  []
  (SimpleStringSchema.))

(defn json-schema
  "Create a JSON serialization schema.

  Requires flink-clj.serde namespace.

  Options:
    :key-fn - Function to transform JSON keys (default: keyword)"
  ([]
   ((requiring-resolve 'flink-clj.serde/json)))
  ([opts]
   ((requiring-resolve 'flink-clj.serde/json) opts)))

(defn edn-schema
  "Create an EDN serialization schema."
  []
  ((requiring-resolve 'flink-clj.serde/edn)))

(defn nippy-schema
  "Create a Nippy binary serialization schema."
  []
  ((requiring-resolve 'flink-clj.serde/nippy)))

(defn avro-schema
  "Create an Avro serialization schema.

  Options:
    :schema - Avro schema (string, file, or Schema object)
    :schema-registry - Schema registry URL"
  [opts]
  ((requiring-resolve 'flink-clj.serde/avro) opts))

;; =============================================================================
;; Connector Discovery
;; =============================================================================

(defn connector-available?
  "Check if a connector class is available on the classpath.

  Example:
    (connector-available? \"org.apache.flink.connector.jdbc.JdbcSink\")"
  [class-name]
  (try
    (Class/forName class-name)
    true
    (catch ClassNotFoundException _ false)))

(defn require-connector!
  "Ensure a connector is available, throwing helpful error if not.

  Example:
    (require-connector! \"org.apache.flink.connector.jdbc.JdbcSink\"
                        \"flink-connector-jdbc\")"
  [class-name artifact-name]
  (when-not (connector-available? class-name)
    (throw (ex-info (str "Connector not available: " artifact-name)
                    {:class class-name
                     :artifact artifact-name
                     :suggestion (str "Add [org.apache.flink/" artifact-name " \"VERSION\"] to dependencies")}))))

;; =============================================================================
;; Pre-built Connector Helpers
;; =============================================================================

(defn jdbc-execution-options
  "Create JDBC execution options.

  Options:
    :batch-size - Batch size for bulk inserts (default: 5000)
    :batch-interval - Max interval between batches in ms (default: 200)
    :max-retries - Max retry attempts (default: 3)"
  [{:keys [batch-size batch-interval max-retries]
    :or {batch-size 5000 batch-interval 200 max-retries 3}}]
  (require-connector! "org.apache.flink.connector.jdbc.JdbcExecutionOptions"
                      "flink-connector-jdbc")
  (let [builder-class (class-for-name "org.apache.flink.connector.jdbc.JdbcExecutionOptions$Builder")
        builder (.newInstance builder-class)]
    (invoke-method builder "withBatchSize" [batch-size])
    (invoke-method builder "withBatchIntervalMs" [batch-interval])
    (invoke-method builder "withMaxRetries" [max-retries])
    (invoke-method builder "build" [])))

(defn jdbc-connection-options
  "Create JDBC connection options.

  Options:
    :url - JDBC URL (required)
    :driver-name - JDBC driver class name (required)
    :username - Database username
    :password - Database password"
  [{:keys [url driver-name username password]}]
  (require-connector! "org.apache.flink.connector.jdbc.JdbcConnectionOptions"
                      "flink-connector-jdbc")
  (let [builder-class (class-for-name "org.apache.flink.connector.jdbc.JdbcConnectionOptions$JdbcConnectionOptionsBuilder")
        builder (.newInstance builder-class)]
    (invoke-method builder "withUrl" [url])
    (invoke-method builder "withDriverName" [driver-name])
    (when username (invoke-method builder "withUsername" [username]))
    (when password (invoke-method builder "withPassword" [password]))
    (invoke-method builder "build" [])))

(defn elasticsearch-hosts
  "Create Elasticsearch HttpHost array from URLs.

  Example:
    (elasticsearch-hosts [\"http://localhost:9200\" \"http://localhost:9201\"])"
  [urls]
  (require-connector! "org.apache.http.HttpHost" "flink-connector-elasticsearch7")
  (let [host-class (class-for-name "org.apache.http.HttpHost")
        create-method (.getMethod host-class "create" (into-array Class [String]))]
    (into-array host-class
                (map #(.invoke create-method nil (into-array Object [%])) urls))))

;; =============================================================================
;; Direct Connector Factories
;; =============================================================================

(defn jdbc-sink
  "Create a JDBC sink for database output.

  Options:
    :url - JDBC URL (required)
    :driver - JDBC driver class (required)
    :username - Database username
    :password - Database password
    :sql - SQL statement with ? placeholders (required)
    :statement-builder - Function (fn [ps element] ...) to set PreparedStatement params
    :batch-size - Batch size (default: 5000)
    :batch-interval - Batch interval in ms (default: 200)
    :max-retries - Max retries (default: 3)

  Example:
    (jdbc-sink {:url \"jdbc:postgresql://localhost/mydb\"
                :driver \"org.postgresql.Driver\"
                :username \"user\"
                :password \"pass\"
                :sql \"INSERT INTO events (id, data) VALUES (?, ?)\"
                :statement-builder #'set-event-params})"
  [{:keys [url driver username password sql statement-builder
           batch-size batch-interval max-retries]
    :or {batch-size 5000 batch-interval 200 max-retries 3}}]
  (require-connector! "org.apache.flink.connector.jdbc.JdbcSink" "flink-connector-jdbc")

  (let [conn-opts (jdbc-connection-options {:url url
                                            :driver-name driver
                                            :username username
                                            :password password})
        exec-opts (jdbc-execution-options {:batch-size batch-size
                                           :batch-interval batch-interval
                                           :max-retries max-retries})
        ;; Create statement builder using proxy to avoid compile-time class dependency
        stmt-builder-class (class-for-name "org.apache.flink.connector.jdbc.JdbcStatementBuilder")
        stmt-builder (when statement-builder
                       (let [[ns-str fn-name] (if (var? statement-builder)
                                                [(str (.-ns statement-builder))
                                                 (str (.-sym statement-builder))]
                                                (throw (ex-info "statement-builder must be a var"
                                                                {:hint "Use #'my-fn"})))]
                         ;; Create proxy at runtime using the dynamically loaded interface
                         (java.lang.reflect.Proxy/newProxyInstance
                           (.getClassLoader stmt-builder-class)
                           (into-array Class [stmt-builder-class])
                           (reify java.lang.reflect.InvocationHandler
                             (invoke [_ _ method args]
                               (when (= "accept" (.getName method))
                                 (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                                       _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                                       fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                                       ps (aget args 0)
                                       element (aget args 1)]
                                   (.invoke fn-var ps element))))))))
        sink-class (class-for-name "org.apache.flink.connector.jdbc.JdbcSink")
        sink-method (.getMethod sink-class "sink"
                                (into-array Class
                                            [String
                                             stmt-builder-class
                                             (class-for-name "org.apache.flink.connector.jdbc.JdbcExecutionOptions")
                                             (class-for-name "org.apache.flink.connector.jdbc.JdbcConnectionOptions")]))]
    (.invoke sink-method nil (into-array Object [sql stmt-builder exec-opts conn-opts]))))

(defn- print-sink-class-available?
  "Check which PrintSink class is available."
  []
  (or
    ;; Flink 1.x
    (try (Class/forName "org.apache.flink.streaming.api.functions.sink.PrintSinkFunction") :v1
         (catch ClassNotFoundException _ nil))
    ;; Flink 2.x uses PrintSink from connector-base
    (try (Class/forName "org.apache.flink.connector.print.sink.PrintSink") :v2
         (catch ClassNotFoundException _ nil))))

(defn print-sink
  "Create a simple print sink for debugging.

  Options:
    :prefix - Prefix string for output (Flink 1.x only)
    :stderr? - Print to stderr instead of stdout (Flink 1.x only)

  Note: In Flink 2.x, PrintSink has different configuration options."
  ([]
   (print-sink {}))
  ([{:keys [prefix stderr?]}]
   (case (print-sink-class-available?)
     :v1
     (let [print-sink-class (Class/forName "org.apache.flink.streaming.api.functions.sink.PrintSinkFunction")
           ctor (if prefix
                  (.getConstructor print-sink-class (into-array Class [String Boolean/TYPE]))
                  (.getConstructor print-sink-class (into-array Class [Boolean/TYPE])))]
       (if prefix
         (.newInstance ctor (into-array Object [prefix (boolean stderr?)]))
         (.newInstance ctor (into-array Object [(boolean stderr?)]))))

     :v2
     ;; Flink 2.x PrintSink uses builder pattern
     (let [print-sink-class (Class/forName "org.apache.flink.connector.print.sink.PrintSink")
           builder-method (.getMethod print-sink-class "builder" (into-array Class []))
           builder (.invoke builder-method nil (into-array Object []))
           build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
       (.invoke build-method builder (into-array Object [])))

     ;; Neither available
     (throw (ex-info "PrintSink not available in this Flink version"
                     {:hint "Use stream/flink-print instead"})))))

;; =============================================================================
;; Connector Info
;; =============================================================================

(def common-connectors
  "Map of common connector artifacts to their main classes."
  {"flink-connector-kafka" "org.apache.flink.connector.kafka.source.KafkaSource"
   "flink-connector-jdbc" "org.apache.flink.connector.jdbc.JdbcSink"
   "flink-connector-elasticsearch7" "org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder"
   "flink-connector-cassandra" "org.apache.flink.streaming.connectors.cassandra.CassandraSink"
   "flink-connector-rabbitmq" "org.apache.flink.streaming.connectors.rabbitmq.RMQSource"
   "flink-connector-pulsar" "org.apache.flink.connector.pulsar.source.PulsarSource"
   "flink-connector-mongodb" "org.apache.flink.connector.mongodb.source.MongoSource"
   "flink-connector-opensearch" "org.apache.flink.connector.opensearch.sink.OpensearchSink"
   "flink-connector-aws-kinesis-streams" "org.apache.flink.connector.kinesis.source.KinesisStreamsSource"
   "flink-connector-aws-kinesis-firehose" "org.apache.flink.connector.firehose.sink.KinesisFirehoseSink"
   "flink-connector-gcp-pubsub" "org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource"
   "flink-connector-hbase" "org.apache.flink.connector.hbase2.HBaseSinkFunction"})

(defn list-available-connectors
  "List which common connectors are available on the classpath."
  []
  (into {}
        (for [[artifact class-name] common-connectors]
          [artifact {:available (connector-available? class-name)
                     :class class-name}])))

(defn connector-info
  "Get information about the generic connector wrapper."
  []
  {:description "Generic wrapper for any Flink connector"
   :features [:reflection-based-builders
              :automatic-type-coercion
              :fluent-api
              :common-serializers
              :jdbc-helpers
              :elasticsearch-helpers]
   :available-connectors (list-available-connectors)})
