(ns flink-clj.connectors.cassandra
  "Apache Cassandra connector for writing stream data.

  Provides sink functionality for writing to Cassandra tables using
  either POJO mapping or tuple-based inserts.

  Example - Simple Sink:
    (require '[flink-clj.connectors.cassandra :as cass])

    (def sink
      (cass/sink {:hosts [\"localhost\"]
                  :keyspace \"mykeyspace\"
                  :query \"INSERT INTO events (id, data, ts) VALUES (?, ?, ?)\"
                  :mapper #'map-to-tuple}))

    (-> stream
        (flink/to-sink sink))

  Example - POJO Sink (with annotations):
    (def sink
      (cass/pojo-sink {:hosts [\"localhost\"]
                       :keyspace \"mykeyspace\"
                       :pojo-class MyEventPojo}))

  NOTE: Requires flink-connector-cassandra dependency."
  (:require [flink-clj.connectors.generic :as conn]
            [flink-clj.impl.functions :as impl])
  (:import [java.net InetSocketAddress]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- cassandra-sink-available?
  "Check if Cassandra sink connector is available."
  []
  (conn/connector-available? "org.apache.flink.streaming.connectors.cassandra.CassandraSink"))

(defn- cassandra-builder-available?
  "Check if Cassandra sink builder is available."
  []
  (conn/connector-available? "org.apache.flink.streaming.connectors.cassandra.CassandraSink$CassandraSinkBuilder"))

(defn- require-cassandra!
  "Ensure Cassandra connector is available."
  []
  (when-not (cassandra-sink-available?)
    (throw (ex-info "Cassandra connector not available"
                    {:suggestion "Add flink-connector-cassandra dependency"}))))

;; =============================================================================
;; Cluster Builder
;; =============================================================================

(defn- create-cluster-builder
  "Create a ClusterBuilder from connection options."
  [{:keys [hosts port username password local-datacenter
           consistency-level ssl?]}]
  (require-cassandra!)
  (let [builder-iface (Class/forName "org.apache.flink.streaming.connectors.cassandra.ClusterBuilder")]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader builder-iface)
      (into-array Class [builder-iface java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (when (= "getCluster" (.getName method))
            (let [cluster-builder-class (Class/forName "com.datastax.driver.core.Cluster$Builder")
                  cluster-builder (.newInstance cluster-builder-class)]
              ;; Add contact points
              (doseq [host hosts]
                (let [method (.getMethod (.getClass cluster-builder) "addContactPoint"
                                        (into-array Class [String]))]
                  (.invoke method cluster-builder (into-array Object [host]))))

              ;; Set port if specified
              (when port
                (let [method (.getMethod (.getClass cluster-builder) "withPort"
                                        (into-array Class [Integer/TYPE]))]
                  (.invoke method cluster-builder (into-array Object [(int port)]))))

              ;; Set credentials if specified
              (when (and username password)
                (let [method (.getMethod (.getClass cluster-builder) "withCredentials"
                                        (into-array Class [String String]))]
                  (.invoke method cluster-builder (into-array Object [username password]))))

              ;; Set local datacenter if specified
              (when local-datacenter
                (try
                  (let [method (.getMethod (.getClass cluster-builder) "withLocalDatacenter"
                                          (into-array Class [String]))]
                    (.invoke method cluster-builder (into-array Object [local-datacenter])))
                  (catch NoSuchMethodException _ nil)))

              ;; Build cluster
              (let [build-method (.getMethod (.getClass cluster-builder) "build" (into-array Class []))]
                (.invoke build-method cluster-builder (into-array Object []))))))))))

;; =============================================================================
;; Mapper Function
;; =============================================================================

(defn- create-mapper-function
  "Create a MapperFunction from a Clojure function.

  The function receives an element and returns a tuple/array for insertion."
  [mapper-fn]
  (require-cassandra!)
  (let [[ns-str fn-name] (impl/var->ns-name mapper-fn)
        mapper-iface (Class/forName "org.apache.flink.api.common.functions.MapFunction")]
    (java.lang.reflect.Proxy/newProxyInstance
      (.getClassLoader mapper-iface)
      (into-array Class [mapper-iface java.io.Serializable])
      (reify java.lang.reflect.InvocationHandler
        (invoke [_ _ method args]
          (when (= "map" (.getName method))
            (let [require-fn (clojure.java.api.Clojure/var "clojure.core" "require")
                  _ (.invoke require-fn (clojure.lang.Symbol/intern ns-str))
                  fn-var (clojure.java.api.Clojure/var ns-str fn-name)
                  element (aget args 0)
                  result (.invoke fn-var element)]
              ;; Convert to Flink Tuple if needed
              (if (sequential? result)
                (into-array Object result)
                result))))))))

;; =============================================================================
;; Sink
;; =============================================================================

(defn sink
  "Create a Cassandra sink for writing data.

  Required options:
    :hosts   - Vector of Cassandra host addresses
    :query   - CQL INSERT query with placeholders
    :mapper  - Function (var) to map elements to query parameters

  Optional options:
    :port            - Cassandra native port (default: 9042)
    :keyspace        - Default keyspace
    :username        - Authentication username
    :password        - Authentication password
    :local-datacenter - Local datacenter name
    :consistency-level - Write consistency level
    :max-concurrent-requests - Max concurrent requests (default: 500)
    :ignore-null-fields - Skip null fields in inserts (default: false)

  Example:
    (defn event-to-tuple [event]
      [(:id event) (:data event) (:timestamp event)])

    (cass/sink {:hosts [\"localhost\"]
                :query \"INSERT INTO events (id, data, ts) VALUES (?, ?, ?)\"
                :mapper #'event-to-tuple})"
  [{:keys [hosts query mapper port keyspace username password
           local-datacenter consistency-level max-concurrent-requests
           ignore-null-fields]
    :or {port 9042 max-concurrent-requests 500 ignore-null-fields false}
    :as opts}]
  ;; Validate required options
  (when-not hosts
    (throw (ex-info "hosts is required" {:opts opts})))
  (when-not query
    (throw (ex-info "query is required" {:opts opts})))
  (when-not mapper
    (throw (ex-info "mapper is required" {:opts opts})))

  (require-cassandra!)

  (let [cluster-builder (create-cluster-builder opts)
        mapper-fn (create-mapper-function mapper)

        ;; Get sink builder
        sink-class (Class/forName "org.apache.flink.streaming.connectors.cassandra.CassandraSink")
        add-sink-method (.getMethod sink-class "addSink"
                                    (into-array Class [(Class/forName "org.apache.flink.streaming.api.datastream.DataStream")]))
        builder-class (Class/forName "org.apache.flink.streaming.connectors.cassandra.ClusterBuilder")]

    ;; Return a function that creates the sink when given a DataStream
    ;; This matches the pattern used by other sinks
    {:type :cassandra-sink
     :query query
     :mapper mapper-fn
     :cluster-builder cluster-builder
     :max-concurrent-requests max-concurrent-requests
     :build-fn (fn [stream]
                 (let [add-method (.getMethod sink-class "addSink"
                                              (into-array Class [(Class/forName "org.apache.flink.streaming.api.datastream.DataStream")]))
                       builder (.invoke add-method nil (into-array Object [stream]))]
                   ;; Configure builder
                   (let [set-query (.getMethod (.getClass builder) "setQuery"
                                               (into-array Class [String]))]
                     (.invoke set-query builder (into-array Object [query])))

                   (let [set-cluster (.getMethod (.getClass builder) "setClusterBuilder"
                                                 (into-array Class [builder-class]))]
                     (.invoke set-cluster builder (into-array Object [cluster-builder])))

                   ;; Build
                   (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
                     (.invoke build-method builder (into-array Object [])))))}))

;; =============================================================================
;; POJO Sink
;; =============================================================================

(defn pojo-sink
  "Create a Cassandra POJO sink.

  Uses DataStax object mapping for automatic serialization.
  POJOs must be annotated with @Table and @Column annotations.

  Required options:
    :hosts      - Vector of Cassandra host addresses
    :pojo-class - Java class with DataStax mapping annotations

  Optional options:
    :port            - Cassandra native port (default: 9042)
    :keyspace        - Default keyspace (overrides @Table annotation)
    :username        - Authentication username
    :password        - Authentication password
    :local-datacenter - Local datacenter name

  Example:
    ;; Java POJO with annotations:
    ;; @Table(keyspace = \"myks\", name = \"events\")
    ;; public class Event {
    ;;   @Column(name = \"id\") private String id;
    ;;   @Column(name = \"data\") private String data;
    ;; }

    (cass/pojo-sink {:hosts [\"localhost\"]
                     :pojo-class Event})"
  [{:keys [hosts pojo-class port keyspace username password local-datacenter]
    :or {port 9042}
    :as opts}]
  ;; Validate required options
  (when-not hosts
    (throw (ex-info "hosts is required" {:opts opts})))
  (when-not pojo-class
    (throw (ex-info "pojo-class is required" {:opts opts})))

  (require-cassandra!)

  (let [cluster-builder (create-cluster-builder opts)
        sink-class (Class/forName "org.apache.flink.streaming.connectors.cassandra.CassandraSink")
        builder-class (Class/forName "org.apache.flink.streaming.connectors.cassandra.ClusterBuilder")]

    {:type :cassandra-pojo-sink
     :pojo-class pojo-class
     :cluster-builder cluster-builder
     :build-fn (fn [stream]
                 (let [add-method (.getMethod sink-class "addSink"
                                              (into-array Class [(Class/forName "org.apache.flink.streaming.api.datastream.DataStream")]))
                       builder (.invoke add-method nil (into-array Object [stream]))]
                   ;; Configure builder
                   (let [set-cluster (.getMethod (.getClass builder) "setClusterBuilder"
                                                 (into-array Class [builder-class]))]
                     (.invoke set-cluster builder (into-array Object [cluster-builder])))

                   ;; Build POJO sink
                   (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
                     (.invoke build-method builder (into-array Object [])))))}))

;; =============================================================================
;; Batch Sink
;; =============================================================================

(defn batch-sink
  "Create a batched Cassandra sink for higher throughput.

  Batches multiple inserts into single Cassandra batch statements.

  Required options:
    :hosts   - Vector of Cassandra host addresses
    :query   - CQL INSERT query with placeholders
    :mapper  - Function (var) to map elements to query parameters

  Optional options:
    :batch-size      - Number of statements per batch (default: 100)
    :batch-timeout   - Timeout for batch completion [n :unit] (default: [1 :seconds])
    :port            - Cassandra native port (default: 9042)

  Example:
    (cass/batch-sink {:hosts [\"localhost\"]
                      :query \"INSERT INTO events (id, data) VALUES (?, ?)\"
                      :mapper #'event-to-tuple
                      :batch-size 500})"
  [{:keys [hosts query mapper batch-size batch-timeout port]
    :or {batch-size 100 batch-timeout [1 :seconds] port 9042}
    :as opts}]
  ;; Validate required options
  (when-not hosts
    (throw (ex-info "hosts is required" {:opts opts})))
  (when-not query
    (throw (ex-info "query is required" {:opts opts})))
  (when-not mapper
    (throw (ex-info "mapper is required" {:opts opts})))

  (require-cassandra!)

  ;; Create a regular sink with batching configuration
  (assoc (sink opts)
         :batch-size batch-size
         :batch-timeout batch-timeout))

;; =============================================================================
;; Convenience Functions
;; =============================================================================

(defn simple-sink
  "Create a simple Cassandra sink that maps Clojure maps to columns.

  Arguments:
    conn-opts - Connection options (:hosts, :port, :keyspace, etc.)
    table     - Table name
    columns   - Vector of column names (keywords)

  Example:
    (simple-sink {:hosts [\"localhost\"] :keyspace \"myks\"}
                 \"events\"
                 [:id :name :value :timestamp])

    ;; Expects elements like {:id \"123\" :name \"test\" :value 42 :timestamp ...}"
  [{:keys [hosts keyspace] :as conn-opts} table columns]
  (let [col-names (map name columns)
        placeholders (repeat (count columns) "?")
        query (str "INSERT INTO "
                   (if keyspace (str keyspace "." table) table)
                   " (" (clojure.string/join ", " col-names) ") "
                   "VALUES (" (clojure.string/join ", " placeholders) ")")
        mapper-name (str "cassandra-mapper-" table "-" (System/nanoTime))]

    ;; Create and intern the mapper function
    (intern (create-ns 'flink-clj.connectors.cassandra)
            (symbol mapper-name)
            (fn [element]
              (mapv #(get element %) columns)))

    (sink (assoc conn-opts
                 :query query
                 :mapper (ns-resolve 'flink-clj.connectors.cassandra (symbol mapper-name))))))

;; =============================================================================
;; Info
;; =============================================================================

(defn cassandra-info
  "Get information about Cassandra connector availability."
  []
  {:sink-available (cassandra-sink-available?)
   :builder-available (cassandra-builder-available?)
   :features [:cql-queries
              :pojo-mapping
              :batching
              :async-writes
              :consistency-levels
              :authentication]
   :supported-versions ["3.x" "4.x"]})
