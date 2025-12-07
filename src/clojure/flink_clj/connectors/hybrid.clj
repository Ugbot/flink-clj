(ns flink-clj.connectors.hybrid
  "Hybrid source for combining bounded and unbounded sources.

  HybridSource enables seamless switching between multiple sources in sequence,
  typically used for:
  - Reading historical data from batch sources, then switching to streaming
  - Combining data from different storage systems
  - Migration scenarios (old storage -> new storage)

  The sources are consumed in order - when the first source completes (bounded),
  the next source starts from where the first left off.

  Example - Backfill from files, then stream from Kafka:
    (require '[flink-clj.connectors.hybrid :as hybrid]
             '[flink-clj.connectors.kafka :as kafka])

    ;; Read historical data from files, then switch to Kafka
    (def source
      (hybrid/source
        {:first (file/bounded-source {:path \"/historical/data\"})
         :then [(kafka/source {:bootstrap-servers \"localhost:9092\"
                               :topics [\"events\"]
                               :value-format :string})]}))

    (-> env
        (flink/from-source source \"Hybrid Source\")
        ...)

  NOTE: Requires Flink 1.14+ which introduced HybridSource.
        This module uses reflection to support both Flink 1.x and 2.x."
  (:require [flink-clj.impl.functions :as impl])
  (:import [java.util.function Function]))

;; =============================================================================
;; Availability Check
;; =============================================================================

(defn- hybrid-source-available?
  "Check if HybridSource is available (Flink 1.14+)."
  []
  (try
    (Class/forName "org.apache.flink.connector.base.source.hybrid.HybridSource")
    true
    (catch ClassNotFoundException _ false)))

(defn- require-hybrid-source!
  "Ensure HybridSource is available."
  []
  (when-not (hybrid-source-available?)
    (throw (ex-info "HybridSource not available. Requires Flink 1.14+"
                    {:suggestion "Upgrade to Flink 1.14 or later"}))))

;; =============================================================================
;; Enumerator State Converter
;; =============================================================================

(defn- create-identity-converter
  "Create an identity SourceSplitConverter for simple chaining."
  []
  (require-hybrid-source!)
  ;; In HybridSource, we don't actually need a converter for simple chaining
  ;; The addSource method handles this internally
  nil)

;; =============================================================================
;; Source Factory
;; =============================================================================

(defn- create-passthrough-factory
  "Create a SourceFactory that ignores previous enumerator state.

  This is useful when the sources are independent and don't need
  to pass state between them."
  [source]
  (require-hybrid-source!)
  (let [factory-class (Class/forName "org.apache.flink.connector.base.source.hybrid.HybridSource$SourceFactory")]
    ;; Create a Function that returns the source regardless of input
    (reify Function
      (apply [_ _previous-enum-state]
        source))))

(defn- create-position-aware-factory
  "Create a SourceFactory that uses previous source's position.

  converter-fn takes the previous enumerator state and returns
  configuration for the next source (e.g., starting offset)."
  [source-builder-fn]
  (require-hybrid-source!)
  (reify Function
    (apply [_ previous-enum-state]
      (source-builder-fn previous-enum-state))))

;; =============================================================================
;; HybridSource Builder
;; =============================================================================

(defn source
  "Create a HybridSource that chains multiple sources in sequence.

  Sources are consumed in order. When a bounded source completes,
  the next source begins. Useful for backfill-then-stream patterns.

  Options:
    :first - The first source to consume (required, typically bounded)
    :then  - Vector of subsequent sources (required, at least one)
    :position-converters - Optional map of index -> converter-fn
                          for passing position between sources

  Simple usage (independent sources):
    (source {:first file-source
             :then [kafka-source]})

  With position conversion (offset handoff):
    (source {:first file-source
             :then [kafka-source]
             :position-converters {0 my-converter-fn}})

  Where my-converter-fn takes the previous source's final position
  and returns starting configuration for the next source.

  Example - File backfill then Kafka streaming:
    (require '[flink-clj.connectors.file :as file]
             '[flink-clj.connectors.kafka :as kafka])

    (def historical-source
      (file/bounded-source {:path \"/data/historical\"}))

    (def streaming-source
      (kafka/source {:bootstrap-servers \"localhost:9092\"
                     :topics [\"events\"]
                     :starting-offsets :latest
                     :value-format :string}))

    (def hybrid
      (hybrid/source {:first historical-source
                      :then [streaming-source]}))

    (-> env
        (flink/from-source hybrid \"Backfill + Stream\")
        ...)"
  [{:keys [first then position-converters]}]
  ;; Validate inputs first
  (when-not first
    (throw (ex-info "First source is required" {})))
  (when (or (nil? then) (empty? then))
    (throw (ex-info "At least one subsequent source required in :then" {})))
  ;; Then check availability
  (require-hybrid-source!)

  (let [hybrid-class (Class/forName "org.apache.flink.connector.base.source.hybrid.HybridSource")
        source-class (Class/forName "org.apache.flink.api.connector.source.Source")
        builder-method (.getMethod hybrid-class "builder"
                                   (into-array Class [source-class]))
        builder (.invoke builder-method nil (into-array Object [first]))
        builder-class (.getClass builder)]

    ;; Add subsequent sources
    (doseq [[idx next-source] (map-indexed vector then)]
      (let [factory (if-let [converter-fn (get position-converters idx)]
                      ;; Position-aware factory
                      (create-position-aware-factory
                        (fn [prev-state]
                          (converter-fn prev-state next-source)))
                      ;; Simple passthrough factory
                      (create-passthrough-factory next-source))
            function-class (Class/forName "java.util.function.Function")
            add-method (.getMethod builder-class "addSource"
                                   (into-array Class [function-class]))]
        (.invoke add-method builder (into-array Object [factory]))))

    ;; Build the HybridSource
    (let [build-method (.getMethod builder-class "build" (into-array Class []))]
      (.invoke build-method builder (into-array Object [])))))

;; =============================================================================
;; Convenience Builders
;; =============================================================================

(defn backfill-then-stream
  "Create a HybridSource for the common backfill-then-stream pattern.

  Takes a bounded source for historical data and an unbounded source
  for real-time streaming.

  Options:
    :bounded   - Bounded source for historical data (required)
    :unbounded - Unbounded source for streaming (required)
    :handoff   - Optional function to configure streaming source
                 based on bounded source's final position

  Example:
    (backfill-then-stream
      {:bounded file-source
       :unbounded kafka-source})"
  [{:keys [bounded unbounded handoff]}]
  (when-not bounded
    (throw (ex-info ":bounded source is required" {})))
  (when-not unbounded
    (throw (ex-info ":unbounded source is required" {})))

  (source {:first bounded
           :then [unbounded]
           :position-converters (when handoff {0 handoff})}))

(defn chain
  "Chain multiple sources in sequence.

  A convenience wrapper over `source` that takes sources as varargs.
  All sources are consumed in order.

  Example:
    (chain source1 source2 source3)"
  [first-source & more-sources]
  (when (empty? more-sources)
    (throw (ex-info "At least two sources required for chain" {})))
  (source {:first first-source
           :then (vec more-sources)}))

;; =============================================================================
;; Source Info
;; =============================================================================

(defn hybrid-source-info
  "Get information about HybridSource availability and features.

  Returns a map with:
    :available - Whether HybridSource is available
    :version   - Minimum Flink version required
    :features  - Supported features"
  []
  {:available (hybrid-source-available?)
   :version "1.14+"
   :features [:source-chaining
              :backfill-then-stream
              :position-handoff
              :bounded-to-unbounded]
   :notes (if (hybrid-source-available?)
            "HybridSource is available"
            "HybridSource requires Flink 1.14+. Consider upgrading.")})

;; =============================================================================
;; Source Switching Strategies
;; =============================================================================

(defn switch-context
  "Create a switch context for position-aware source transitions.

  This is used with :position-converters to create sources that
  can pick up from where the previous source left off.

  Example:
    (source {:first bounded-source
             :then [streaming-source]
             :position-converters
               {0 (fn [prev-state]
                    (let [ctx (hybrid/switch-context prev-state)]
                      ;; Use ctx to configure next source
                      streaming-source))}})"
  [previous-enumerator-state]
  {:previous-state previous-enumerator-state
   :switch-time (System/currentTimeMillis)})

;; =============================================================================
;; Integration with File and Kafka Sources
;; =============================================================================

(defn file-then-kafka
  "Convenience function for the common file-then-Kafka pattern.

  Reads historical data from files, then switches to Kafka streaming.

  Options:
    :file-path        - Path to historical files
    :bootstrap-servers - Kafka broker addresses
    :topics           - Kafka topic(s) to consume
    :group-id         - Kafka consumer group
    :value-format     - Deserialization format (default: :string)

  Example:
    (file-then-kafka {:file-path \"/historical\"
                      :bootstrap-servers \"localhost:9092\"
                      :topics [\"events\"]
                      :group-id \"my-consumer\"})"
  [{:keys [file-path bootstrap-servers topics group-id value-format]
    :or {value-format :string}}]
  (require-hybrid-source!)
  ;; This is a convenience that requires both connectors
  ;; We'll build the sources dynamically
  (let [;; Create file source via reflection (requires flink-connector-files)
        file-source
        (try
          (let [file-source-class (Class/forName "org.apache.flink.connector.file.src.FileSource")
                for-method (.getMethod file-source-class "forRecordStreamFormat"
                                        (into-array Class [(Class/forName "org.apache.flink.connector.file.src.reader.StreamFormat")
                                                           (Class/forName "[Lorg.apache.flink.core.fs.Path;")]))
                path-class (Class/forName "org.apache.flink.core.fs.Path")
                path-ctor (.getConstructor path-class (into-array Class [String]))
                path (.newInstance path-ctor (into-array Object [file-path]))
                text-format-class (Class/forName "org.apache.flink.connector.file.src.reader.TextLineInputFormat")
                text-format (.newInstance text-format-class (into-array Object []))
                builder (.invoke for-method nil (into-array Object [text-format (into-array path-class [path])]))
                build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
            (.invoke build-method builder (into-array Object [])))
          (catch ClassNotFoundException _
            (throw (ex-info "FileSource not available. Add flink-connector-files dependency."
                            {:suggestion "Add [org.apache.flink/flink-connector-files \"VERSION\"] to dependencies"}))))

        ;; Create Kafka source
        kafka-source
        (try
          ((requiring-resolve 'flink-clj.connectors.kafka/source)
           {:bootstrap-servers bootstrap-servers
            :topics topics
            :group-id group-id
            :starting-offsets :latest
            :value-format value-format})
          (catch Exception e
            (throw (ex-info "Could not create Kafka source"
                            {:error (.getMessage e)
                             :suggestion "Ensure flink-connector-kafka is on classpath"}))))]

    (source {:first file-source
             :then [kafka-source]})))
