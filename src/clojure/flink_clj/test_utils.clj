(ns flink-clj.test-utils
  "Test utilities for flink-clj integration tests.

  Provides a MiniCluster environment for running Flink jobs in tests,
  and utilities for collecting stream results.

  Example:
    (require '[flink-clj.test-utils :as tu])
    (require '[flink-clj.core :as flink])
    (require '[flink-clj.stream :as stream])

    (tu/with-mini-cluster [env]
      (let [result (-> (flink/from-collection env [1 2 3 4 5])
                       (stream/flink-map double-fn)
                       (tu/collect-results))]
        (is (= [2 4 6 8 10] (sort result)))))"
  (:import [org.apache.flink.runtime.testutils MiniClusterResourceConfiguration
                                              MiniClusterResourceConfiguration$Builder]
           [org.apache.flink.test.util MiniClusterWithClientResource]
           [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api.datastream DataStream]
           [java.util Iterator]))

;; =============================================================================
;; MiniCluster Setup
;; =============================================================================

(def ^:private ^MiniClusterWithClientResource mini-cluster-resource
  "Shared MiniCluster resource."
  (MiniClusterWithClientResource.
    (-> (MiniClusterResourceConfiguration$Builder.)
        (.setNumberTaskManagers 1)
        (.setNumberSlotsPerTaskManager 4)
        (.build))))

(defn start-mini-cluster!
  "Start the MiniCluster. Call before running tests."
  []
  (.before mini-cluster-resource))

(defn stop-mini-cluster!
  "Stop the MiniCluster. Call after tests complete."
  []
  (.after mini-cluster-resource))

(defn create-test-env
  "Create a StreamExecutionEnvironment configured for testing."
  []
  (let [env (StreamExecutionEnvironment/getExecutionEnvironment)]
    (.setParallelism env 1)  ; Use 1 for deterministic tests
    env))

(defmacro with-mini-cluster
  "Execute body with a MiniCluster test environment.

  Binds `env` to a StreamExecutionEnvironment configured for testing.

  Example:
    (with-mini-cluster [env]
      (let [results (-> (flink/from-collection env [1 2 3])
                        (stream/flink-map double-fn)
                        (collect-results))]
        (is (= [2 4 6] results))))"
  [[env-sym] & body]
  `(do
     (start-mini-cluster!)
     (try
       (let [~env-sym (create-test-env)]
         ~@body)
       (finally
         (stop-mini-cluster!)))))

;; =============================================================================
;; Result Collection
;; =============================================================================

(defn- get-stream-iterator
  "Get an iterator for the stream, handling Flink version differences.
  Flink 1.x uses DataStreamUtils.collect(), Flink 2.x uses stream.executeAndCollect()."
  [^DataStream stream]
  (try
    ;; Try Flink 2.x API first (executeAndCollect)
    (let [method (.getMethod DataStream "executeAndCollect" (into-array Class []))]
      (try
        (.invoke method stream (into-array Object []))
        (catch java.lang.reflect.InvocationTargetException ite
          (throw (.getCause ite)))))
    (catch NoSuchMethodException _
      ;; Fall back to Flink 1.x API (DataStreamUtils.collect) using reflection
      (let [utils-class (Class/forName "org.apache.flink.streaming.api.datastream.DataStreamUtils")
            method (.getMethod utils-class "collect" (into-array Class [DataStream]))]
        (.invoke method nil (into-array Object [stream]))))))

(defn collect-results
  "Execute the stream and collect all results into a vector.

  Uses DataStreamUtils.collect() for Flink 1.x or executeAndCollect() for Flink 2.x.

  IMPORTANT: This method blocks until the job completes.
  Use only in tests with bounded sources.

  Example:
    (-> (flink/from-collection env [1 2 3])
        (stream/flink-map inc)
        (collect-results))
    ;=> [2 3 4]"
  [^DataStream stream]
  (let [^Iterator iter (get-stream-iterator stream)]
    (loop [results []]
      (if (.hasNext iter)
        (recur (conj results (.next iter)))
        results))))

(defn collect-n
  "Execute the stream and collect up to n results.

  Useful for testing unbounded streams where you want to verify
  a sample of outputs.

  IMPORTANT: May not return exactly n results if the source produces fewer."
  [^DataStream stream n]
  (let [^Iterator iter (get-stream-iterator stream)]
    (loop [results []
           count 0]
      (if (and (.hasNext iter) (< count n))
        (recur (conj results (.next iter)) (inc count))
        results))))

;; =============================================================================
;; Test Helpers
;; =============================================================================

(defn same-elements?
  "Check if two collections contain the same elements (ignoring order)."
  [coll1 coll2]
  (= (sort coll1) (sort coll2)))

(defmacro with-test-env
  "Create a test environment without MiniCluster (for unit tests).

  Use when you just need an environment but won't execute the job.

  Example:
    (with-test-env [env]
      (let [stream (flink/from-collection env [1 2 3])]
        (is (instance? DataStream stream))))"
  [[env-sym] & body]
  `(let [~env-sym (StreamExecutionEnvironment/getExecutionEnvironment)]
     (.setParallelism ~env-sym 1)
     ~@body))
