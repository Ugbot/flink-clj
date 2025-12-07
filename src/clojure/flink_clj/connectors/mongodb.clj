(ns flink-clj.connectors.mongodb
  "MongoDB connector for reading and writing stream data.

  Provides source and sink functionality for MongoDB collections
  with support for both batch and streaming modes.

  Example - Source:
    (require '[flink-clj.connectors.mongodb :as mongo])

    (def source
      (mongo/source {:uri \"mongodb://localhost:27017\"
                     :database \"mydb\"
                     :collection \"events\"
                     :deserializer #'parse-doc}))

    (-> env
        (flink/from-source source \"MongoDB Source\")
        ...)

  Example - Sink:
    (def sink
      (mongo/sink {:uri \"mongodb://localhost:27017\"
                   :database \"mydb\"
                   :collection \"output\"
                   :serializer #'to-document}))

    (-> stream
        (flink/to-sink sink))

  NOTE: Requires flink-connector-mongodb dependency."
  (:require [flink-clj.connectors.generic :as conn]
            [flink-clj.impl.functions :as impl]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- mongodb-source-available?
  "Check if MongoDB source connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.mongodb.source.MongoSource"))

(defn- mongodb-sink-available?
  "Check if MongoDB sink connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.mongodb.sink.MongoSink"))

(defn- require-mongodb-source!
  "Ensure MongoDB source connector is available."
  []
  (when-not (mongodb-source-available?)
    (throw (ex-info "MongoDB source connector not available"
                    {:suggestion "Add flink-connector-mongodb dependency"}))))

(defn- require-mongodb-sink!
  "Ensure MongoDB sink connector is available."
  []
  (when-not (mongodb-sink-available?)
    (throw (ex-info "MongoDB sink connector not available"
                    {:suggestion "Add flink-connector-mongodb dependency"}))))

;; =============================================================================
;; Deserializer
;; =============================================================================

(defn- create-deserializer
  "Create a MongoDeserializationSchema from a Clojure function.

  The function receives a BsonDocument and returns the deserialized value."
  [deser-fn]
  (require-mongodb-source!)
  (let [[ns-str fn-name] (impl/var->ns-name deser-fn)
        deser-iface (Class/forName "org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema")]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader deser-iface)
      (into-array Class [deser-iface java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (let [method-name (.getName method)]
            (case method-name
              "deserialize"
              (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                    _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                    fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                    doc (aget args 0)]
                (.invoke fn-var doc))

              "getProducedType"
              ;; Return Object type info
              (org.apache.flink.api.common.typeinfo.Types/GENERIC Object)

              nil)))))))

;; =============================================================================
;; Serializer
;; =============================================================================

(defn- create-serializer
  "Create a MongoSerializationSchema from a Clojure function.

  The function receives an element and returns a BsonDocument."
  [ser-fn]
  (require-mongodb-sink!)
  (let [[ns-str fn-name] (impl/var->ns-name ser-fn)
        ser-iface (Class/forName "org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema")]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader ser-iface)
      (into-array Class [ser-iface java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (when (= "serialize" (.getName method))
            (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                  _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                  fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                  element (aget args 0)
                  context (aget args 1)]
              (.invoke fn-var element context))))))))

;; =============================================================================
;; Source
;; =============================================================================

(defn source
  "Create a MongoDB source for reading from a collection.

  Required options:
    :uri          - MongoDB connection URI
    :database     - Database name
    :collection   - Collection name
    :deserializer - Function (var) to deserialize BsonDocument to Clojure value

  Optional options:
    :fetch-size       - Number of documents per batch (default: 2048)
    :cursor-batch-size - MongoDB cursor batch size
    :no-cursor-timeout - Disable cursor timeout (default: true)
    :partition-strategy - :single, :sample, :split-vector, :sharded, :default
    :partition-size-mb  - Size of each partition in MB (default: 64)
    :samples-per-partition - Samples per partition for :sample strategy

  Example:
    (defn parse-doc [^BsonDocument doc]
      {:id (.getString doc \"_id\")
       :name (.getString doc \"name\")
       :value (.getInt32 doc \"value\")})

    (mongo/source {:uri \"mongodb://localhost:27017\"
                   :database \"mydb\"
                   :collection \"events\"
                   :deserializer #'parse-doc})"
  [{:keys [uri database collection deserializer
           fetch-size cursor-batch-size no-cursor-timeout
           partition-strategy partition-size-mb samples-per-partition]
    :or {fetch-size 2048 no-cursor-timeout true partition-size-mb 64}
    :as opts}]
  ;; Validate required options
  (when-not uri
    (throw (ex-info "uri is required" {:opts opts})))
  (when-not database
    (throw (ex-info "database is required" {:opts opts})))
  (when-not collection
    (throw (ex-info "collection is required" {:opts opts})))
  (when-not deserializer
    (throw (ex-info "deserializer is required" {:opts opts})))

  (require-mongodb-source!)

  (let [deser (create-deserializer deserializer)
        source-class (Class/forName "org.apache.flink.connector.mongodb.source.MongoSource")
        builder-method (.getMethod source-class "builder" (into-array Class []))
        builder (.invoke builder-method nil (into-array Object []))
        builder-type (.getClass builder)]

    ;; Set URI
    (let [method (.getMethod builder-type "setUri" (into-array Class [String]))]
      (.invoke method builder (into-array Object [uri])))

    ;; Set database
    (let [method (.getMethod builder-type "setDatabase" (into-array Class [String]))]
      (.invoke method builder (into-array Object [database])))

    ;; Set collection
    (let [method (.getMethod builder-type "setCollection" (into-array Class [String]))]
      (.invoke method builder (into-array Object [collection])))

    ;; Set deserializer
    (let [deser-class (Class/forName "org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema")
          method (.getMethod builder-type "setDeserializationSchema" (into-array Class [deser-class]))]
      (.invoke method builder (into-array Object [deser])))

    ;; Set fetch size
    (let [method (.getMethod builder-type "setFetchSize" (into-array Class [Integer/TYPE]))]
      (.invoke method builder (into-array Object [(int fetch-size)])))

    ;; Set cursor batch size if specified
    (when cursor-batch-size
      (try
        (let [method (.getMethod builder-type "setCursorBatchSize" (into-array Class [Integer/TYPE]))]
          (.invoke method builder (into-array Object [(int cursor-batch-size)])))
        (catch NoSuchMethodException _ nil)))

    ;; Set no cursor timeout
    (try
      (let [method (.getMethod builder-type "setNoCursorTimeout" (into-array Class [Boolean/TYPE]))]
        (.invoke method builder (into-array Object [(boolean no-cursor-timeout)])))
      (catch NoSuchMethodException _ nil))

    ;; Set partition strategy if specified
    (when partition-strategy
      (try
        (let [strategy-class (Class/forName "org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy")
              strategy (case partition-strategy
                         :single (Enum/valueOf strategy-class "SINGLE")
                         :sample (Enum/valueOf strategy-class "SAMPLE")
                         :split-vector (Enum/valueOf strategy-class "SPLIT_VECTOR")
                         :sharded (Enum/valueOf strategy-class "SHARDED")
                         :default (Enum/valueOf strategy-class "DEFAULT"))
              method (.getMethod builder-type "setPartitionStrategy" (into-array Class [strategy-class]))]
          (.invoke method builder (into-array Object [strategy])))
        (catch Exception _ nil)))

    ;; Set partition size
    (try
      (let [method (.getMethod builder-type "setPartitionSize"
                               (into-array Class [(Class/forName "org.apache.flink.configuration.MemorySize")]))]
        (let [mem-size-class (Class/forName "org.apache.flink.configuration.MemorySize")
              of-method (.getMethod mem-size-class "ofMebiBytes" (into-array Class [Long/TYPE]))
              mem-size (.invoke of-method nil (into-array Object [(long partition-size-mb)]))]
          (.invoke method builder (into-array Object [mem-size]))))
      (catch Exception _ nil))

    ;; Build
    (let [build-method (.getMethod builder-type "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Sink
;; =============================================================================

(defn sink
  "Create a MongoDB sink for writing to a collection.

  Required options:
    :uri          - MongoDB connection URI
    :database     - Database name
    :collection   - Collection name
    :serializer   - Function (var) to serialize Clojure value to BsonDocument

  Optional options:
    :batch-size           - Max documents per batch (default: 1000)
    :batch-interval-ms    - Max interval between flushes (default: 1000)
    :max-retries          - Max retry attempts (default: 3)
    :delivery-guarantee   - :at-least-once, :none (default: :at-least-once)
    :upsert?              - Use upsert mode (default: false)
    :upsert-key           - Field to use as upsert key (default: \"_id\")

  Example:
    (defn to-document [event ctx]
      (let [doc (org.bson.BsonDocument.)]
        (.put doc \"_id\" (org.bson.BsonString. (:id event)))
        (.put doc \"name\" (org.bson.BsonString. (:name event)))
        (.put doc \"value\" (org.bson.BsonInt32. (:value event)))
        doc))

    (mongo/sink {:uri \"mongodb://localhost:27017\"
                 :database \"mydb\"
                 :collection \"output\"
                 :serializer #'to-document})"
  [{:keys [uri database collection serializer
           batch-size batch-interval-ms max-retries
           delivery-guarantee upsert? upsert-key]
    :or {batch-size 1000 batch-interval-ms 1000 max-retries 3
         delivery-guarantee :at-least-once upsert? false upsert-key "_id"}
    :as opts}]
  ;; Validate required options
  (when-not uri
    (throw (ex-info "uri is required" {:opts opts})))
  (when-not database
    (throw (ex-info "database is required" {:opts opts})))
  (when-not collection
    (throw (ex-info "collection is required" {:opts opts})))
  (when-not serializer
    (throw (ex-info "serializer is required" {:opts opts})))

  (require-mongodb-sink!)

  (let [ser (create-serializer serializer)
        sink-class (Class/forName "org.apache.flink.connector.mongodb.sink.MongoSink")
        builder-method (.getMethod sink-class "builder" (into-array Class []))
        builder (.invoke builder-method nil (into-array Object []))
        builder-type (.getClass builder)]

    ;; Set URI
    (let [method (.getMethod builder-type "setUri" (into-array Class [String]))]
      (.invoke method builder (into-array Object [uri])))

    ;; Set database
    (let [method (.getMethod builder-type "setDatabase" (into-array Class [String]))]
      (.invoke method builder (into-array Object [database])))

    ;; Set collection
    (let [method (.getMethod builder-type "setCollection" (into-array Class [String]))]
      (.invoke method builder (into-array Object [collection])))

    ;; Set serializer
    (let [ser-class (Class/forName "org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema")
          method (.getMethod builder-type "setSerializationSchema" (into-array Class [ser-class]))]
      (.invoke method builder (into-array Object [ser])))

    ;; Set batch size
    (let [method (.getMethod builder-type "setBatchSize" (into-array Class [Integer/TYPE]))]
      (.invoke method builder (into-array Object [(int batch-size)])))

    ;; Set batch interval
    (let [method (.getMethod builder-type "setBatchIntervalMs" (into-array Class [Long/TYPE]))]
      (.invoke method builder (into-array Object [(long batch-interval-ms)])))

    ;; Set max retries
    (try
      (let [method (.getMethod builder-type "setMaxRetries" (into-array Class [Integer/TYPE]))]
        (.invoke method builder (into-array Object [(int max-retries)])))
      (catch NoSuchMethodException _ nil))

    ;; Set delivery guarantee
    (try
      (let [guarantee-class (Class/forName "org.apache.flink.connector.base.DeliveryGuarantee")
            guarantee (case delivery-guarantee
                        :none (Enum/valueOf guarantee-class "NONE")
                        :at-least-once (Enum/valueOf guarantee-class "AT_LEAST_ONCE"))
            method (.getMethod builder-type "setDeliveryGuarantee" (into-array Class [guarantee-class]))]
        (.invoke method builder (into-array Object [guarantee])))
      (catch Exception _ nil))

    ;; Build
    (let [build-method (.getMethod builder-type "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn simple-source
  "Create a simple MongoDB source that reads documents as Clojure maps.

  Arguments:
    uri        - MongoDB connection URI
    database   - Database name
    collection - Collection name

  Options:
    :filter    - MongoDB query filter (BsonDocument)
    :projection - Fields to include (vector of strings)

  Example:
    (simple-source \"mongodb://localhost:27017\" \"mydb\" \"events\")"
  ([uri database collection]
   (simple-source uri database collection {}))
  ([uri database collection opts]
   (let [deser-name (str "simple-deser-" collection "-" (System/nanoTime))]
     ;; Create deserializer that converts BsonDocument to Clojure map
     (intern (create-ns 'flink-clj.connectors.mongodb)
             (symbol deser-name)
             (fn [doc]
               ;; Convert BsonDocument to Clojure map
               (when doc
                 (into {}
                       (for [entry (.entrySet doc)]
                         (let [k (.getKey entry)
                               v (.getValue entry)]
                           [(keyword k)
                            (cond
                              (.isString v) (.getValue (.asString v))
                              (.isInt32 v) (.getValue (.asInt32 v))
                              (.isInt64 v) (.getValue (.asInt64 v))
                              (.isDouble v) (.getValue (.asDouble v))
                              (.isBoolean v) (.getValue (.asBoolean v))
                              (.isNull v) nil
                              :else (str v))]))))))

     (source (merge {:uri uri
                     :database database
                     :collection collection
                     :deserializer (ns-resolve 'flink-clj.connectors.mongodb (symbol deser-name))}
                    opts)))))

(defn simple-sink
  "Create a simple MongoDB sink that writes Clojure maps as documents.

  Arguments:
    uri        - MongoDB connection URI
    database   - Database name
    collection - Collection name

  Options:
    :id-field  - Field to use as _id (default: :_id or :id)
    :upsert?   - Use upsert mode (default: false)

  Example:
    (simple-sink \"mongodb://localhost:27017\" \"mydb\" \"output\")"
  ([uri database collection]
   (simple-sink uri database collection {}))
  ([uri database collection {:keys [id-field upsert?] :or {upsert? false} :as opts}]
   (let [ser-name (str "simple-ser-" collection "-" (System/nanoTime))]
     ;; Create serializer that converts Clojure map to BsonDocument
     (intern (create-ns 'flink-clj.connectors.mongodb)
             (symbol ser-name)
             (fn [element ctx]
               (let [doc-class (Class/forName "org.bson.BsonDocument")
                     doc (.newInstance doc-class)]
                 (doseq [[k v] element]
                   (let [bson-val (cond
                                    (nil? v) (let [c (Class/forName "org.bson.BsonNull")]
                                               (.newInstance c))
                                    (string? v) (let [c (Class/forName "org.bson.BsonString")
                                                      ctor (.getConstructor c (into-array Class [String]))]
                                                  (.newInstance ctor (into-array Object [v])))
                                    (integer? v) (let [c (Class/forName "org.bson.BsonInt64")
                                                       ctor (.getConstructor c (into-array Class [Long/TYPE]))]
                                                   (.newInstance ctor (into-array Object [(long v)])))
                                    (float? v) (let [c (Class/forName "org.bson.BsonDouble")
                                                     ctor (.getConstructor c (into-array Class [Double/TYPE]))]
                                                 (.newInstance ctor (into-array Object [(double v)])))
                                    (instance? Boolean v) (let [c (Class/forName "org.bson.BsonBoolean")
                                                                ctor (.getConstructor c (into-array Class [Boolean/TYPE]))]
                                                            (.newInstance ctor (into-array Object [v])))
                                    :else (let [c (Class/forName "org.bson.BsonString")
                                                ctor (.getConstructor c (into-array Class [String]))]
                                            (.newInstance ctor (into-array Object [(str v)]))))]
                     (.put doc (name k) bson-val)))
                 doc)))

     (sink (merge {:uri uri
                   :database database
                   :collection collection
                   :serializer (ns-resolve 'flink-clj.connectors.mongodb (symbol ser-name))
                   :upsert? upsert?}
                  opts)))))

;; =============================================================================
;; Info
;; =============================================================================

(defn mongodb-info
  "Get information about MongoDB connector availability."
  []
  {:source-available (mongodb-source-available?)
   :sink-available (mongodb-sink-available?)
   :features [:change-streams
              :partitioned-reading
              :batched-writes
              :upsert-mode
              :projection-pushdown
              :filter-pushdown]
   :supported-versions ["4.x" "5.x" "6.x"]})
