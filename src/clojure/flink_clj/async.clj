(ns flink-clj.async
  "Async I/O operations for non-blocking external system calls.

  Async I/O allows you to perform database lookups, API calls, or other
  external operations without blocking the stream processing. This
  significantly improves throughput when dealing with high-latency operations.

  Your async function should return either:
  - A value directly (for already-available results)
  - A CompletableFuture that completes with the result
  - A collection of values (if :flat? is true)
  - nil (no output)

  Two ordering modes are available:
  - :unordered - Results are emitted as soon as they complete (lowest latency)
  - :ordered   - Results maintain input order (higher latency, more memory)

  Example:
    (require '[flink-clj.async :as async])

    ;; Define an async lookup function
    (defn lookup-user [user-id]
      ;; Returns a CompletableFuture
      (let [future (java.util.concurrent.CompletableFuture.)]
        ;; Simulate async database lookup
        (.submit executor
          (fn []
            (let [user (db/fetch-user user-id)]
              (.complete future user))))
        future))

    ;; Apply async operation
    (-> user-id-stream
        (async/unordered-wait #'lookup-user
          {:timeout [30 :seconds]
           :capacity 100})
        (stream/flink-map #'process-user))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.datastream DataStream AsyncDataStream]
           [org.apache.flink.api.common.typeinfo TypeInformation]
           [java.util.concurrent TimeUnit]))

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

(defn- to-timeout-ms
  "Convert timeout specification to milliseconds."
  [timeout]
  (cond
    (number? timeout) timeout
    (vector? timeout) (let [[n unit] timeout]
                        (case unit
                          (:ms :milliseconds) n
                          (:s :seconds) (* n 1000)
                          (:m :minutes) (* n 60 1000)
                          (:h :hours) (* n 60 60 1000)))
    :else (throw (ex-info "Invalid timeout" {:timeout timeout}))))

(defn unordered-wait
  "Apply async function with unordered output.

  Results are emitted as soon as they complete, providing the
  lowest latency. Order is not preserved.

  async-fn must be a var that takes an input and returns either:
  - A value (emitted directly)
  - A CompletableFuture that resolves to a value
  - A collection of values (if :flat? is true)
  - nil (no output)

  Options:
    :timeout  - Max time to wait for async operation (required)
                Can be milliseconds or [n :seconds/:minutes]
    :capacity - Max number of concurrent async operations (default: 100)
    :flat?    - Treat collection results as multiple outputs (default: false)
    :returns  - TypeInformation or spec for output type

  Example:
    (defn enrich-event [event]
      (let [future (CompletableFuture.)]
        (async-http-get (str \"/api/users/\" (:user-id event))
          (fn [response]
            (.complete future (assoc event :user-data response))))
        future))

    (unordered-wait stream #'enrich-event
      {:timeout [10 :seconds]
       :capacity 50})"
  [^DataStream stream async-fn {:keys [timeout capacity flat? returns]
                                 :or {capacity 100}}]
  (let [timeout-ms (to-timeout-ms timeout)
        async-wrapper (impl/make-async-function async-fn (boolean flat?))
        result (AsyncDataStream/unorderedWait
                 stream
                 async-wrapper
                 timeout-ms
                 TimeUnit/MILLISECONDS
                 capacity)]
    (if returns
      (.returns result ^TypeInformation (resolve-type-info returns))
      result)))

(defn ordered-wait
  "Apply async function with ordered output.

  Results maintain the order of inputs. This requires buffering
  results until earlier operations complete, which increases
  latency and memory usage.

  See `unordered-wait` for argument documentation.

  Example:
    (ordered-wait stream #'lookup-user
      {:timeout [30 :seconds]
       :capacity 100})"
  [^DataStream stream async-fn {:keys [timeout capacity flat? returns]
                                 :or {capacity 100}}]
  (let [timeout-ms (to-timeout-ms timeout)
        async-wrapper (impl/make-async-function async-fn (boolean flat?))
        result (AsyncDataStream/orderedWait
                 stream
                 async-wrapper
                 timeout-ms
                 TimeUnit/MILLISECONDS
                 capacity)]
    (if returns
      (.returns result ^TypeInformation (resolve-type-info returns))
      result)))

;; =============================================================================
;; Helper Utilities
;; =============================================================================

(defn completed-future
  "Create a CompletableFuture that is already completed with a value.

  Useful for returning cached results in async functions.

  Example:
    (defn lookup-with-cache [id]
      (if-let [cached (get @cache id)]
        (completed-future cached)
        (fetch-from-db id)))"
  [value]
  (java.util.concurrent.CompletableFuture/completedFuture value))

(defn failed-future
  "Create a CompletableFuture that has failed with an exception.

  Example:
    (defn validate-and-lookup [id]
      (if (valid-id? id)
        (lookup id)
        (failed-future (ex-info \"Invalid ID\" {:id id}))))"
  [^Throwable exception]
  (let [future (java.util.concurrent.CompletableFuture.)]
    (.completeExceptionally future exception)
    future))

(defmacro async-do
  "Execute body asynchronously and return a CompletableFuture.

  Wraps the body in a try-catch and completes the future
  with the result or exception.

  Example:
    (defn async-fetch [url]
      (async-do
        (let [response (http/get url)]
          (parse-json (:body response)))))"
  [& body]
  `(let [future# (java.util.concurrent.CompletableFuture.)]
     (future
       (try
         (.complete future# (do ~@body))
         (catch Throwable t#
           (.completeExceptionally future# t#))))
     future#))
