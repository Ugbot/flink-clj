(ns flink-clj.async-sink
  "Async sink configuration and utilities for Flink 2.x.

  This namespace provides configuration helpers for creating async sinks
  that efficiently write to external systems with batching and retries.

  Async sinks use the Flink Connector Base's AsyncSinkBase which provides:
  - Automatic batching by size, count, and time
  - Configurable in-flight request limits
  - At-least-once semantics via checkpointing
  - Async request handling with retry support

  Usage:
    1. Configure async sink options using the builder functions
    2. Create an async sink implementation (requires Java)
    3. Attach to stream with (core/to-sink stream sink)

  Example configuration:
    (def sink-config
      (-> (async-sink-config)
          (with-max-batch-size 500)
          (with-max-in-flight-requests 50)
          (with-max-buffered-requests 10000)
          (with-max-batch-size-in-bytes (* 5 1024 1024))  ; 5MB
          (with-max-time-in-buffer-ms 5000)
          (with-max-record-size-in-bytes 1048576)  ; 1MB
          (with-request-timeout-ms 30000)
          (with-fail-on-timeout true)))

  For creating custom async sinks, see the Java documentation for
  AsyncSinkBase and AsyncSinkWriter."
  (:require [flink-clj.version :as v])
  (:import [java.util Properties]))

;; =============================================================================
;; Async Sink Configuration Builder
;; =============================================================================

(defn async-sink-config
  "Create a new async sink configuration map with default values.

  Defaults:
    :max-batch-size 500
    :max-in-flight-requests 50
    :max-buffered-requests 10000
    :max-batch-size-in-bytes 5242880 (5MB)
    :max-time-in-buffer-ms 5000
    :max-record-size-in-bytes 1048576 (1MB)
    :request-timeout-ms nil (no timeout)
    :fail-on-timeout false"
  []
  {:max-batch-size 500
   :max-in-flight-requests 50
   :max-buffered-requests 10000
   :max-batch-size-in-bytes (* 5 1024 1024)
   :max-time-in-buffer-ms 5000
   :max-record-size-in-bytes (* 1024 1024)
   :request-timeout-ms nil
   :fail-on-timeout false})

(defn with-max-batch-size
  "Set the maximum number of elements per batch.

  Larger batches improve throughput but increase latency.
  Default: 500"
  [config size]
  (assoc config :max-batch-size size))

(defn with-max-in-flight-requests
  "Set the maximum number of concurrent async requests.

  Higher values increase parallelism but may overwhelm the destination.
  Default: 50"
  [config requests]
  (assoc config :max-in-flight-requests requests))

(defn with-max-buffered-requests
  "Set the maximum number of buffered request entries.

  Controls backpressure - when buffer is full, processing blocks.
  Default: 10000"
  [config requests]
  (assoc config :max-buffered-requests requests))

(defn with-max-batch-size-in-bytes
  "Set the maximum batch size in bytes.

  Batches are flushed when this byte limit is reached.
  Default: 5MB (5242880 bytes)"
  [config bytes]
  (assoc config :max-batch-size-in-bytes bytes))

(defn with-max-time-in-buffer-ms
  "Set the maximum time elements can stay in buffer before flushing.

  Controls latency - lower values reduce latency at cost of smaller batches.
  Default: 5000ms"
  [config ms]
  (assoc config :max-time-in-buffer-ms ms))

(defn with-max-record-size-in-bytes
  "Set the maximum size in bytes for a single record.

  Records exceeding this size will fail.
  Default: 1MB (1048576 bytes)"
  [config bytes]
  (assoc config :max-record-size-in-bytes bytes))

(defn with-request-timeout-ms
  "Set the timeout for async requests.

  Default: nil (no timeout)"
  [config ms]
  (assoc config :request-timeout-ms ms))

(defn with-fail-on-timeout
  "Set whether to fail the job when requests timeout.

  Default: false (log and continue)"
  [config fail?]
  (assoc config :fail-on-timeout fail?))

;; =============================================================================
;; Configuration Extraction (for Java sink implementations)
;; =============================================================================

(defn config->java-params
  "Convert an async sink configuration to Java constructor parameters.

  Returns a vector suitable for passing to AsyncSinkBase constructor:
  [maxBatchSize maxInFlightRequests maxBufferedRequests
   maxBatchSizeInBytes maxTimeInBufferMS maxRecordSizeInBytes
   requestTimeoutMS failOnTimeout]"
  [config]
  [(:max-batch-size config 500)
   (:max-in-flight-requests config 50)
   (:max-buffered-requests config 10000)
   (:max-batch-size-in-bytes config (* 5 1024 1024))
   (:max-time-in-buffer-ms config 5000)
   (:max-record-size-in-bytes config (* 1024 1024))
   (:request-timeout-ms config)
   (:fail-on-timeout config false)])

(defn config->properties
  "Convert an async sink configuration to java.util.Properties.

  Useful for sink implementations that accept Properties."
  [config]
  (let [props (Properties.)]
    (doseq [[k v] config :when (some? v)]
      (.setProperty props (name k) (str v)))
    props))

;; =============================================================================
;; Rate Limiting Configuration (Flink 2.x)
;; =============================================================================

(defn- rate-limiter-available?
  "Check if rate limiter API is available (Flink 1.16+)."
  []
  (try
    (Class/forName "org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy")
    true
    (catch ClassNotFoundException _ false)))

(defn no-rate-limiting
  "Create a configuration with rate limiting disabled.

  Uses CongestionControlRateLimitingStrategy.NoOpRateLimitingStrategy.
  Only available in Flink 1.16+."
  []
  (when-not (rate-limiter-available?)
    (throw (ex-info "Rate limiting requires Flink 1.16+"
                    {:hint "Rate limiting strategies are not available in this Flink version"})))
  {:rate-limiting-strategy :none})

(defn aimd-rate-limiting
  "Create an AIMD (Additive Increase Multiplicative Decrease) rate limiting config.

  AIMD provides adaptive rate limiting that:
  - Gradually increases throughput on success
  - Rapidly decreases on failures

  Options:
    :initial-rate - Initial requests per second (default: 500)
    :max-rate - Maximum requests per second (default: 10000)
    :rate-increase - Additive increase on success (default: 100)
    :rate-decrease-factor - Multiplicative decrease on failure (default: 0.5)

  Only available in Flink 1.16+."
  ([]
   (aimd-rate-limiting {}))
  ([{:keys [initial-rate max-rate rate-increase rate-decrease-factor]
     :or {initial-rate 500
          max-rate 10000
          rate-increase 100
          rate-decrease-factor 0.5}}]
   (when-not (rate-limiter-available?)
     (throw (ex-info "Rate limiting requires Flink 1.16+"
                     {:hint "Rate limiting strategies are not available in this Flink version"})))
   {:rate-limiting-strategy :aimd
    :initial-rate initial-rate
    :max-rate max-rate
    :rate-increase rate-increase
    :rate-decrease-factor rate-decrease-factor}))

(defn token-bucket-rate-limiting
  "Create a token bucket rate limiting configuration.

  Token bucket provides:
  - Fixed maximum rate
  - Allows bursts up to bucket size
  - Smooth token replenishment

  Options:
    :tokens-per-second - Rate at which tokens are added (default: 1000)
    :bucket-size - Maximum tokens (burst capacity) (default: 1000)

  Only available in Flink 1.16+."
  ([]
   (token-bucket-rate-limiting {}))
  ([{:keys [tokens-per-second bucket-size]
     :or {tokens-per-second 1000
          bucket-size 1000}}]
   (when-not (rate-limiter-available?)
     (throw (ex-info "Rate limiting requires Flink 1.16+"
                     {:hint "Rate limiting strategies are not available in this Flink version"})))
   {:rate-limiting-strategy :token-bucket
    :tokens-per-second tokens-per-second
    :bucket-size bucket-size}))

;; =============================================================================
;; Flink 2.1 Enhanced Batching (FLIP-509)
;; =============================================================================

(defn- enhanced-batching-available?
  "Check if enhanced batching (FLIP-509) is available (Flink 2.1+)."
  []
  (and (v/flink-2?)
       (try
         (Class/forName "org.apache.flink.connector.base.sink.writer.BatchCreator")
         true
         (catch ClassNotFoundException _ false))))

(defn batch-by-partition
  "Create partition-aware batching configuration.

  This enables grouping elements by a partition key for more efficient
  batch writes. Particularly useful for sinks like Cassandra where
  batch writes are more efficient within the same partition.

  Options:
    :partition-key-fn - Var that extracts partition key from element (required)
    :max-partitions-per-batch - Maximum partitions in a single batch (default: 1)

  Only available in Flink 2.1+ (FLIP-509).

  Example:
    (defn get-user-partition [{:keys [user-id]}]
      (mod user-id 100))

    (batch-by-partition {:partition-key-fn #'get-user-partition
                         :max-partitions-per-batch 1})"
  [{:keys [partition-key-fn max-partitions-per-batch]
    :or {max-partitions-per-batch 1}}]
  (when-not (enhanced-batching-available?)
    (throw (ex-info "Partition batching requires Flink 2.1+ (FLIP-509)"
                    {:flink-version (v/flink-minor-version)
                     :hint "This feature is part of FLIP-509 enhanced batching"})))
  (when-not partition-key-fn
    (throw (ex-info "partition-key-fn is required for batch-by-partition"
                    {:hint "Provide a var that extracts partition key from element"})))
  {:batch-strategy :partition
   :partition-key-fn partition-key-fn
   :max-partitions-per-batch max-partitions-per-batch})

(defn default-batching
  "Create default FIFO batching configuration.

  Elements are batched in order without partition awareness.
  This is the default behavior for async sinks."
  []
  {:batch-strategy :fifo})

;; =============================================================================
;; Validation
;; =============================================================================

(defn validate-config
  "Validate an async sink configuration.

  Returns the config if valid, throws if invalid."
  [config]
  (let [{:keys [max-batch-size max-in-flight-requests max-buffered-requests
                max-batch-size-in-bytes max-time-in-buffer-ms max-record-size-in-bytes]} config]
    (when (and max-batch-size (<= max-batch-size 0))
      (throw (ex-info "max-batch-size must be positive" {:value max-batch-size})))
    (when (and max-in-flight-requests (<= max-in-flight-requests 0))
      (throw (ex-info "max-in-flight-requests must be positive" {:value max-in-flight-requests})))
    (when (and max-buffered-requests (<= max-buffered-requests 0))
      (throw (ex-info "max-buffered-requests must be positive" {:value max-buffered-requests})))
    (when (and max-batch-size-in-bytes (<= max-batch-size-in-bytes 0))
      (throw (ex-info "max-batch-size-in-bytes must be positive" {:value max-batch-size-in-bytes})))
    (when (and max-time-in-buffer-ms (<= max-time-in-buffer-ms 0))
      (throw (ex-info "max-time-in-buffer-ms must be positive" {:value max-time-in-buffer-ms})))
    (when (and max-record-size-in-bytes (<= max-record-size-in-bytes 0))
      (throw (ex-info "max-record-size-in-bytes must be positive" {:value max-record-size-in-bytes})))
    (when (and max-record-size-in-bytes max-batch-size-in-bytes
               (> max-record-size-in-bytes max-batch-size-in-bytes))
      (throw (ex-info "max-record-size-in-bytes cannot exceed max-batch-size-in-bytes"
                      {:max-record max-record-size-in-bytes
                       :max-batch max-batch-size-in-bytes})))
    config))
