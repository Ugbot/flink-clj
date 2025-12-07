(ns flink-clj.env
  "StreamExecutionEnvironment creation and configuration.

  This namespace provides functions for creating and configuring
  Flink execution environments, including dynamic JAR loading
  similar to PyFlink's add_jars() functionality."
  (:require [clojure.string :as str])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api CheckpointingMode]
           [org.apache.flink.api.common RuntimeExecutionMode]
           [org.apache.flink.streaming.api.environment CheckpointConfig]
           [org.apache.flink.configuration Configuration PipelineOptions]))

;; Forward declarations
(declare configure-checkpointing!)
(declare jar-url)

(defn create-env
  "Create a StreamExecutionEnvironment.

  Options:
    :parallelism - Default parallelism for operators (int)
    :max-parallelism - Maximum parallelism (int)
    :buffer-timeout - Buffer timeout in ms (long)
    :runtime-mode - Execution mode: :streaming (default), :batch, or :automatic
    :jars - Vector of JAR file URLs (e.g., [\"file:///path/to/connector.jar\"])
    :classpaths - Vector of classpath URLs (e.g., [\"file:///path/to/lib/\"])
    :checkpoint - Checkpoint configuration map:
      :interval - Checkpoint interval in ms (long)
      :mode - :exactly-once or :at-least-once
      :timeout - Checkpoint timeout in ms (long)
      :min-pause - Min pause between checkpoints in ms (long)
      :max-concurrent - Max concurrent checkpoints (int)

  Example:
    (create-env)
    (create-env {:parallelism 4})
    (create-env {:parallelism 4 :runtime-mode :batch})
    (create-env {:parallelism 4
                 :jars [\"file:///opt/flink/connectors/kafka.jar\"]})
    (create-env {:parallelism 4
                 :checkpoint {:interval 60000
                              :mode :exactly-once}})"
  ([]
   (create-env {}))
  ([{:keys [parallelism max-parallelism buffer-timeout runtime-mode jars classpaths checkpoint] :as opts}]
   (let [config (when (or jars classpaths)
                  (let [c (Configuration.)]
                    (when jars
                      (.set c PipelineOptions/JARS (java.util.ArrayList. jars)))
                    (when classpaths
                      (.set c PipelineOptions/CLASSPATHS (java.util.ArrayList. classpaths)))
                    c))
         env (if config
               (StreamExecutionEnvironment/getExecutionEnvironment config)
               (StreamExecutionEnvironment/getExecutionEnvironment))]
     (when parallelism
       (.setParallelism env parallelism))
     (when max-parallelism
       (.setMaxParallelism env max-parallelism))
     (when buffer-timeout
       (.setBufferTimeout env buffer-timeout))
     (when runtime-mode
       (.setRuntimeMode env (case runtime-mode
                              :streaming RuntimeExecutionMode/STREAMING
                              :batch RuntimeExecutionMode/BATCH
                              :automatic RuntimeExecutionMode/AUTOMATIC)))
     (when checkpoint
       (configure-checkpointing! env checkpoint))
     env)))

(defn create-local-env
  "Create a local StreamExecutionEnvironment for testing.

  Options same as create-env plus:
    :web-ui-port - Port for Flink web UI (int, default: disabled)

  Example:
    (create-local-env)
    (create-local-env {:parallelism 2})"
  ([]
   (create-local-env {}))
  ([{:keys [parallelism web-ui-port] :as opts}]
   (let [env (if web-ui-port
               (let [config (org.apache.flink.configuration.Configuration.)]
                 ;; Use .set() which works in both Flink 1.x and 2.x
                 (.set config org.apache.flink.configuration.RestOptions/PORT
                       (Integer/valueOf (int web-ui-port)))
                 (StreamExecutionEnvironment/createLocalEnvironmentWithWebUI config))
               (StreamExecutionEnvironment/createLocalEnvironment))]
     (when parallelism
       (.setParallelism env parallelism))
     env)))

(defn create-remote-env
  "Create a StreamExecutionEnvironment connected to a remote Flink cluster.

  This enables submitting jobs to a running Flink cluster from the REPL.

  Arguments:
    host - JobManager hostname or IP address
    port - JobManager REST port (typically 8081)

  Options:
    :parallelism - Default parallelism for operators (int)
    :jars        - Vector of JAR file paths to ship to cluster

  Example:
    (create-remote-env \"flink-jobmanager\" 8081)
    (create-remote-env \"10.0.0.50\" 8081 {:parallelism 4})
    (create-remote-env \"cluster.example.com\" 8081
                       {:parallelism 8
                        :jars [\"/path/to/my-job.jar\"]})"
  ([host port]
   (create-remote-env host port {}))
  ([host port {:keys [parallelism jars]}]
   (let [jar-urls (when (seq jars)
                    (into-array String (map jar-url jars)))
         env (if jar-urls
               (StreamExecutionEnvironment/createRemoteEnvironment
                 host (int port) jar-urls)
               (StreamExecutionEnvironment/createRemoteEnvironment
                 host (int port) (into-array String [])))]
     (when parallelism
       (.setParallelism env parallelism))
     env)))

(defn- set-externalized-checkpoint!
  "Set externalized checkpoint configuration, handling version differences.
  Flink 1.x uses ExternalizedCheckpointCleanup, Flink 2.x uses ExternalizedCheckpointRetention."
  [^CheckpointConfig config externalized]
  (let [retain-val (case externalized :retain "RETAIN_ON_CANCELLATION" :delete "DELETE_ON_CANCELLATION")]
    (try
      ;; Try Flink 2.x API first (ExternalizedCheckpointRetention)
      (let [retention-class (Class/forName "org.apache.flink.configuration.ExternalizedCheckpointRetention")
            enum-val (Enum/valueOf retention-class retain-val)
            method (.getMethod CheckpointConfig "setExternalizedCheckpointRetention"
                               (into-array Class [retention-class]))]
        (.invoke method config (into-array Object [enum-val])))
      (catch ClassNotFoundException _
        ;; Fall back to Flink 1.x API (ExternalizedCheckpointCleanup)
        (let [cleanup-class (Class/forName
                              "org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup")
              enum-val (Enum/valueOf cleanup-class retain-val)
              method (.getMethod CheckpointConfig "setExternalizedCheckpointCleanup"
                                 (into-array Class [cleanup-class]))]
          (.invoke method config (into-array Object [enum-val])))))))

(defn configure-checkpointing!
  "Configure checkpointing on the environment.

  Options:
    :interval - Checkpoint interval in ms (required)
    :mode - :exactly-once or :at-least-once (default: :exactly-once)
    :timeout - Checkpoint timeout in ms
    :min-pause - Min pause between checkpoints in ms
    :max-concurrent - Max concurrent checkpoints (default: 1)
    :externalized - How to handle checkpoints on cancellation:
      :retain - Keep checkpoints on cancel
      :delete - Delete checkpoints on cancel

  Example:
    (configure-checkpointing! env {:interval 60000
                                   :mode :exactly-once
                                   :timeout 600000})"
  [^StreamExecutionEnvironment env
   {:keys [interval mode timeout min-pause max-concurrent externalized]}]
  (when interval
    (.enableCheckpointing env interval))
  (when mode
    (.enableCheckpointing env interval
                          (case mode
                            :exactly-once CheckpointingMode/EXACTLY_ONCE
                            :at-least-once CheckpointingMode/AT_LEAST_ONCE)))
  (let [^CheckpointConfig config (.getCheckpointConfig env)]
    (when timeout
      (.setCheckpointTimeout config timeout))
    (when min-pause
      (.setMinPauseBetweenCheckpoints config min-pause))
    (when max-concurrent
      (.setMaxConcurrentCheckpoints config max-concurrent))
    (when externalized
      (set-externalized-checkpoint! config externalized)))
  env)

(defn set-parallelism!
  "Set the default parallelism for the environment."
  [^StreamExecutionEnvironment env parallelism]
  (.setParallelism env parallelism)
  env)

(defn set-runtime-mode!
  "Set the runtime execution mode.

  mode can be:
  - :streaming - Unbounded stream processing (default)
  - :batch - Bounded batch processing with optimizations
  - :automatic - Automatically detect based on sources

  Batch mode enables:
  - Sort-based shuffles instead of pipelined shuffles
  - Blocking exchanges for better resource utilization
  - Optimized scheduling for bounded data
  - Final results only (no intermediate outputs)

  Example:
    (set-runtime-mode! env :batch)
    (set-runtime-mode! env :streaming)"
  [^StreamExecutionEnvironment env mode]
  (.setRuntimeMode env (case mode
                         :streaming RuntimeExecutionMode/STREAMING
                         :batch RuntimeExecutionMode/BATCH
                         :automatic RuntimeExecutionMode/AUTOMATIC))
  env)

(defn get-runtime-mode
  "Get the current runtime execution mode."
  [^StreamExecutionEnvironment env]
  (let [config (.getConfiguration env)
        mode-opt (try
                   (let [exec-opts-class (Class/forName "org.apache.flink.configuration.ExecutionOptions")
                         runtime-mode-field (.getField exec-opts-class "RUNTIME_MODE")]
                     (.get config (.get runtime-mode-field nil)))
                   (catch Exception _ nil))]
    (when mode-opt
      (condp = (str mode-opt)
        "STREAMING" :streaming
        "BATCH" :batch
        "AUTOMATIC" :automatic
        nil))))

(defn get-config
  "Get the ExecutionConfig from the environment."
  [^StreamExecutionEnvironment env]
  (.getConfig env))

;; =============================================================================
;; Dynamic JAR/Classpath Loading (PyFlink-style)
;; =============================================================================

(defn add-jars!
  "Add JAR files to the execution environment.

  Paths must be fully-qualified URLs: file:///path/to/jar.jar

  Note: For best results, prefer passing :jars to create-env rather than
  adding JARs after environment creation.

  Example:
    (-> (create-env)
        (add-jars! \"file:///opt/flink/connectors/kafka.jar\"
                   \"file:///opt/flink/connectors/avro.jar\"))"
  [^StreamExecutionEnvironment env & jar-paths]
  (let [config (.getConfiguration env)
        existing (or (.get config PipelineOptions/JARS) (java.util.ArrayList.))
        updated (java.util.ArrayList. existing)]
    (doseq [jar jar-paths]
      (.add updated jar))
    (.set config PipelineOptions/JARS updated))
  env)

(defn add-classpaths!
  "Add classpath URLs to the execution environment.

  Paths must be fully-qualified URLs: file:///path/to/lib/

  Note: For best results, prefer passing :classpaths to create-env rather than
  adding classpaths after environment creation.

  Example:
    (-> (create-env)
        (add-classpaths! \"file:///opt/flink/lib/\"))"
  [^StreamExecutionEnvironment env & classpath-urls]
  (let [config (.getConfiguration env)
        existing (or (.get config PipelineOptions/CLASSPATHS) (java.util.ArrayList.))
        updated (java.util.ArrayList. existing)]
    (doseq [cp classpath-urls]
      (.add updated cp))
    (.set config PipelineOptions/CLASSPATHS updated))
  env)

(defn jar-url
  "Convert a local path to a file:// URL for JAR loading.

  Example:
    (jar-url \"/opt/flink/connectors/kafka.jar\")
    ;=> \"file:///opt/flink/connectors/kafka.jar\"

    (jar-url \"file:///already/a/url.jar\")
    ;=> \"file:///already/a/url.jar\""
  [path]
  (cond
    (str/starts-with? path "file://") path
    (str/starts-with? path "http://") path
    (str/starts-with? path "https://") path
    (str/starts-with? path "/") (str "file://" path)
    :else (str "file:///" path)))

(defn maven-jar
  "Generate a Maven Central URL for a JAR.

  Example:
    (maven-jar \"org.apache.flink\" \"flink-connector-kafka\" \"1.20.0\")
    ;=> \"https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.20.0/flink-connector-kafka-1.20.0.jar\""
  [group-id artifact-id version]
  (let [group-path (str/replace group-id "." "/")]
    (format "https://repo1.maven.org/maven2/%s/%s/%s/%s-%s.jar"
            group-path artifact-id version artifact-id version)))

(defn get-jars
  "Get the list of JAR URLs configured on the environment."
  [^StreamExecutionEnvironment env]
  (let [config (.getConfiguration env)]
    (vec (or (.get config PipelineOptions/JARS) []))))

(defn get-classpaths
  "Get the list of classpath URLs configured on the environment."
  [^StreamExecutionEnvironment env]
  (let [config (.getConfiguration env)]
    (vec (or (.get config PipelineOptions/CLASSPATHS) []))))

;; =============================================================================
;; State Backend Configuration
;; =============================================================================

(defn- forst-available?
  "Check if ForSt state backend is available (Flink 2.x)."
  []
  (try
    (Class/forName "org.apache.flink.state.forst.ForStStateBackendFactory")
    true
    (catch ClassNotFoundException _ false)))

(defn- rocksdb-available?
  "Check if RocksDB state backend is available."
  []
  (try
    (Class/forName "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend")
    true
    (catch ClassNotFoundException _ false)))

(defn set-state-backend!
  "Set the state backend for the environment.

  backend can be:
  - :hashmap - In-memory hashmap state backend (default)
  - :rocksdb - RocksDB state backend (requires flink-statebackend-rocksdb)
  - :forst - ForSt state backend (Flink 2.x, requires flink-statebackend-forst)
  - A StateBackend instance directly

  Options for :rocksdb and :forst:
    :incremental? - Enable incremental checkpoints (default: true for forst)
    :checkpoint-path - Base path for checkpoints

  Example:
    (set-state-backend! env :hashmap)
    (set-state-backend! env :rocksdb)
    (set-state-backend! env :forst {:incremental? true})
    (set-state-backend! env :forst {:checkpoint-path \"s3://bucket/checkpoints\"})"
  ([^StreamExecutionEnvironment env backend]
   (set-state-backend! env backend {}))
  ([^StreamExecutionEnvironment env backend {:keys [incremental? checkpoint-path]}]
   (let [state-backend
         (case backend
           :hashmap
           (let [clazz (Class/forName "org.apache.flink.runtime.state.hashmap.HashMapStateBackend")
                 ctor (.getConstructor clazz (into-array Class []))]
             (.newInstance ctor (into-array Object [])))

           :rocksdb
           (if (rocksdb-available?)
             (let [clazz (Class/forName "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend")
                   ctor (.getConstructor clazz (into-array Class [Boolean/TYPE]))
                   ;; Default to incremental checkpoints
                   incr (if (nil? incremental?) true incremental?)]
               (.newInstance ctor (into-array Object [(boolean incr)])))
             (throw (ex-info "RocksDB state backend not available"
                             {:hint "Add flink-statebackend-rocksdb dependency"})))

           :forst
           (if (forst-available?)
             (let [factory-class (Class/forName "org.apache.flink.state.forst.ForStStateBackendFactory")
                   factory (.newInstance factory-class)
                   create-method (.getMethod factory-class "createFromConfig"
                                             (into-array Class [(Class/forName "org.apache.flink.configuration.ReadableConfig")
                                                                ClassLoader]))
                   config (org.apache.flink.configuration.Configuration.)]
               ;; ForSt uses incremental by default
               (.invoke create-method factory
                        (into-array Object [config (.getContextClassLoader (Thread/currentThread))])))
             (throw (ex-info "ForSt state backend not available"
                             {:hint "ForSt requires Flink 2.x with flink-statebackend-forst dependency"})))

           ;; Direct state backend instance
           backend)]
     ;; Set checkpoint storage if path provided
     (when checkpoint-path
       (let [storage-class (Class/forName "org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage")
             ctor (.getConstructor storage-class (into-array Class [String]))
             storage (.newInstance ctor (into-array Object [checkpoint-path]))]
         (.setCheckpointStorage (.getCheckpointConfig env) storage)))
     (.setStateBackend env state-backend)
     env)))

(defn state-backend-info
  "Get information about available state backends.

  Returns a map with availability info for each backend type."
  []
  {:hashmap {:available true
             :description "In-memory hashmap, suitable for small state"}
   :rocksdb {:available (rocksdb-available?)
             :description "RocksDB, suitable for large state with local SSD"}
   :forst {:available (forst-available?)
           :description "ForSt (Flink 2.x), cloud-native disaggregated state"}})
