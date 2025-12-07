(ns flink-clj.connectors.elasticsearch
  "Elasticsearch sink connector for indexing stream data.

  Supports Elasticsearch 7.x and 8.x (OpenSearch compatible).

  Example:
    (require '[flink-clj.connectors.elasticsearch :as es])

    (def sink
      (es/sink {:hosts [\"http://localhost:9200\"]
                :index \"events\"
                :emitter #'my-emitter-fn}))

    (-> stream
        (flink/to-sink sink \"Elasticsearch Sink\"))

  Emitter function format:
    (defn my-emitter [element ctx indexer]
      ;; element - incoming stream element
      ;; ctx - SinkWriter.Context
      ;; indexer - RequestIndexer to add requests
      (let [doc {:timestamp (:ts element)
                 :data (:payload element)}]
        (.add indexer
          (-> (IndexRequest. \"my-index\")
              (.id (str (:id element)))
              (.source (json/encode doc) XContentType/JSON)))))

  NOTE: Requires flink-connector-elasticsearch7 or flink-connector-elasticsearch-base"
  (:require [flink-clj.connectors.generic :as conn]
            [flink-clj.impl.functions :as impl])
  (:import [java.time Duration]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- es7-available?
  "Check if Elasticsearch 7.x connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder"))

(defn- es-base-available?
  "Check if base Elasticsearch connector is available."
  []
  (conn/connector-available? "org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBuilder"))

(defn- require-es!
  "Ensure Elasticsearch connector is available."
  []
  (when-not (or (es7-available?) (es-base-available?))
    (throw (ex-info "Elasticsearch connector not available"
                    {:suggestion "Add flink-connector-elasticsearch7 or flink-connector-elasticsearch-base"}))))

;; =============================================================================
;; Emitter Creation
;; =============================================================================

(defn- create-emitter
  "Create an ElasticsearchEmitter from a Clojure function.

  The emitter function receives [element context indexer]."
  [emitter-fn]
  (require-es!)
  (let [[ns-str fn-name] (impl/var->ns-name (impl/fn->var emitter-fn))
        ;; Use dynamic proxy to implement the emitter interface
        emitter-class (try
                        (Class/forName "org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter")
                        (catch ClassNotFoundException _
                          (Class/forName "org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction")))]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader emitter-class)
      (into-array Class [emitter-class java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (let [method-name (.getName method)]
            (when (= "emit" method-name)
              (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                    _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                    fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                    element (aget args 0)
                    context (aget args 1)
                    indexer (aget args 2)]
                (.invoke fn-var element context indexer)))))))))

;; =============================================================================
;; HTTP Host Parsing
;; =============================================================================

(defn- parse-hosts
  "Parse host strings into HttpHost objects."
  [hosts]
  (require-es!)
  (let [http-host-class (Class/forName "org.apache.http.HttpHost")
        create-method (.getMethod http-host-class "create" (into-array Class [String]))]
    (into-array http-host-class
                (map #(.invoke create-method nil (into-array Object [%])) hosts))))

;; =============================================================================
;; Sink Builder
;; =============================================================================

(defn sink
  "Create an Elasticsearch sink.

  Required options:
    :hosts   - Vector of Elasticsearch host URLs
    :emitter - Emitter function (var) that creates index requests

  Optional options:
    :bulk-flush-max-actions    - Max actions before flush (default: 1000)
    :bulk-flush-max-size-mb    - Max size in MB before flush (default: 5)
    :bulk-flush-interval       - Flush interval [n :unit] (default: [1 :seconds])
    :bulk-flush-backoff-type   - :constant or :exponential (default: :exponential)
    :bulk-flush-backoff-retries - Number of retries (default: 5)
    :bulk-flush-backoff-delay  - Delay between retries [n :unit]
    :connection-timeout        - Connection timeout [n :unit]
    :socket-timeout            - Socket timeout [n :unit]
    :connection-request-timeout - Request timeout [n :unit]
    :connection-path-prefix    - Path prefix for requests

  Example:
    (defn index-event [event ctx indexer]
      (.add indexer
        (-> (IndexRequest. \"events\")
            (.source (json/encode event) XContentType/JSON))))

    (es/sink {:hosts [\"http://localhost:9200\"]
              :emitter #'index-event
              :bulk-flush-max-actions 500})"
  [{:keys [hosts emitter
           bulk-flush-max-actions bulk-flush-max-size-mb bulk-flush-interval
           bulk-flush-backoff-type bulk-flush-backoff-retries bulk-flush-backoff-delay
           connection-timeout socket-timeout connection-request-timeout
           connection-path-prefix]
    :or {bulk-flush-max-actions 1000
         bulk-flush-max-size-mb 5
         bulk-flush-interval [1 :seconds]
         bulk-flush-backoff-type :exponential
         bulk-flush-backoff-retries 5}}]
  (require-es!)

  (let [builder-class (if (es7-available?)
                        (Class/forName "org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder")
                        (Class/forName "org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBuilder"))
        builder (.newInstance builder-class)
        builder-type (.getClass builder)
        http-hosts (parse-hosts hosts)
        es-emitter (create-emitter emitter)

        ;; Helper to convert duration spec
        to-duration (fn [spec]
                      (if (instance? Duration spec)
                        spec
                        (let [[n unit] spec]
                          (case unit
                            (:ms :milliseconds) (Duration/ofMillis n)
                            (:s :seconds) (Duration/ofSeconds n)
                            (:m :minutes) (Duration/ofMinutes n)))))]

    ;; Set hosts
    (let [hosts-method (.getMethod builder-type "setHosts"
                                   (into-array Class [(Class/forName "[Lorg.apache.http.HttpHost;")]))]
      (.invoke hosts-method builder (into-array Object [http-hosts])))

    ;; Set emitter
    (let [emitter-method (.getMethod builder-type "setEmitter"
                                     (into-array Class [(Class/forName "org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter")]))]
      (.invoke emitter-method builder (into-array Object [es-emitter])))

    ;; Set bulk flush config
    (let [actions-method (.getMethod builder-type "setBulkFlushMaxActions" (into-array Class [Integer/TYPE]))]
      (.invoke actions-method builder (into-array Object [(int bulk-flush-max-actions)])))

    (let [size-method (.getMethod builder-type "setBulkFlushMaxSizeMb" (into-array Class [Integer/TYPE]))]
      (.invoke size-method builder (into-array Object [(int bulk-flush-max-size-mb)])))

    (let [interval-method (.getMethod builder-type "setBulkFlushInterval" (into-array Class [Long/TYPE]))]
      (.invoke interval-method builder (into-array Object [(long (.toMillis (to-duration bulk-flush-interval)))])))

    ;; Optional settings
    (when connection-path-prefix
      (let [method (.getMethod builder-type "setConnectionPathPrefix" (into-array Class [String]))]
        (.invoke method builder (into-array Object [connection-path-prefix]))))

    ;; Build
    (let [build-method (.getMethod builder-type "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn simple-sink
  "Create a simple Elasticsearch sink that indexes JSON documents.

  This convenience function creates an emitter that:
  - Converts elements to JSON
  - Uses a field as document ID (optional)
  - Indexes to a specified index

  Options:
    :hosts    - Vector of ES host URLs (required)
    :index    - Index name (required)
    :id-field - Field to use as document ID (optional)
    :json-fn  - Function to convert element to JSON string (default: cheshire/encode)

  Example:
    (simple-sink {:hosts [\"http://localhost:9200\"]
                  :index \"events\"
                  :id-field :event-id})"
  [{:keys [hosts index id-field json-fn]}]
  (require-es!)
  (let [;; Create a simple emitter function
        emitter-name (str "simple-emitter-" (System/nanoTime))
        json-encode (or json-fn
                        (try (requiring-resolve 'cheshire.core/generate-string)
                             (catch Exception _
                               (fn [x] (pr-str x)))))]
    ;; Intern the emitter function
    (intern (create-ns 'flink-clj.connectors.elasticsearch)
            (symbol emitter-name)
            (fn [element ctx indexer]
              (let [index-req-class (Class/forName "org.elasticsearch.action.index.IndexRequest")
                    xcontent-class (Class/forName "org.elasticsearch.common.xcontent.XContentType")
                    json-type (Enum/valueOf xcontent-class "JSON")
                    req (.newInstance (.getConstructor index-req-class (into-array Class [String]))
                                      (into-array Object [index]))
                    source-method (.getMethod index-req-class "source"
                                              (into-array Class [String xcontent-class]))]
                ;; Set ID if field provided
                (when id-field
                  (let [id-method (.getMethod index-req-class "id" (into-array Class [String]))]
                    (.invoke id-method req (into-array Object [(str (get element id-field))]))))
                ;; Set source
                (.invoke source-method req (into-array Object [(json-encode element) json-type]))
                ;; Add to indexer
                (.add indexer req))))

    (sink {:hosts hosts
           :emitter (ns-resolve 'flink-clj.connectors.elasticsearch (symbol emitter-name))})))

;; =============================================================================
;; Info
;; =============================================================================

(defn elasticsearch-info
  "Get information about Elasticsearch connector availability."
  []
  {:es7-available (es7-available?)
   :es-base-available (es-base-available?)
   :features [:bulk-indexing
              :backoff-retry
              :custom-emitters
              :connection-pooling]
   :supported-versions ["7.x" "8.x" "OpenSearch"]})
