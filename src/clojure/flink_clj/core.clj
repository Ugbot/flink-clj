(ns flink-clj.core
  "Main entry point for flink-clj.

  This namespace provides the core API for working with Apache Flink's
  DataStream API from Clojure.

  Type hints:
    Use flink-clj.types to specify TypeInformation for efficient serialization:

    (require '[flink-clj.types :as t])
    (from-collection env data {:type t/STRING})
    (from-collection env data {:type [:row :id :int :name :string]})

  Example usage:
    (-> (env/create-env {:parallelism 4})
        (register-clojure-types!)
        (from-collection [\"hello world\" \"flink clojure\"])
        (stream/flat-map tokenize)
        (keyed/key-by first)
        (keyed/flink-reduce sum-counts)
        (sink/print)
        (execute \"Word Count\"))"
  (:require [flink-clj.env :as env]
            [flink-clj.impl.kryo :as kryo]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.streaming.api.datastream DataStream]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

(defn register-clojure-types!
  "Register Clojure persistent data structures with Flink's Kryo serializer.

  This enables efficient serialization of Clojure maps, vectors, sets, keywords,
  and other core data types across the Flink cluster.

  Should be called on the StreamExecutionEnvironment before building the pipeline.

  Example:
    (-> (env/create-env)
        (register-clojure-types!)
        ...)"
  [^StreamExecutionEnvironment env]
  (kryo/register-clojure-types! env)
  env)

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation.
  Accepts TypeInformation directly or a spec for from-spec."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

(defn from-collection
  "Create a DataStream from a Clojure collection.

  Elements are emitted in order from the collection.

  Options:
    :type - TypeInformation or spec for element type
            Without this, Flink may use inefficient generic serialization.

  Example:
    (from-collection env [1 2 3 4 5])
    (from-collection env [1 2 3] {:type :long})
    (from-collection env [{:id 1 :name \"alice\"}
                          {:id 2 :name \"bob\"}])
    (from-collection env rows {:type [:row :id :int :name :string]})"
  ([^StreamExecutionEnvironment env coll]
   (from-collection env coll nil))
  ([^StreamExecutionEnvironment env coll {:keys [type]}]
   (if type
     (.fromCollection env (vec coll) ^TypeInformation (resolve-type-info type))
     (.fromCollection env (vec coll)))))

(defn from-elements
  "Create a DataStream from individual elements.

  Options (as first argument after env):
    :type - TypeInformation or spec for element type

  Example:
    (from-elements env 1 2 3 4 5)
    (from-elements env {:type :long} 1 2 3 4 5)"
  [^StreamExecutionEnvironment env & args]
  (let [[opts elements] (if (map? (first args))
                          [(first args) (rest args)]
                          [nil args])
        type-info (:type opts)]
    (if type-info
      (.fromElements env ^TypeInformation (resolve-type-info type-info)
                     (into-array Object elements))
      (.fromElements env (into-array Object elements)))))

(defn execute
  "Execute the Flink job with the given name.

  This triggers the execution of the pipeline. Returns a JobExecutionResult.

  Example:
    (execute env \"My Flink Job\")"
  ([^StreamExecutionEnvironment env]
   (.execute env))
  ([^StreamExecutionEnvironment env ^String job-name]
   (.execute env job-name)))

(defn execute-async
  "Execute the Flink job asynchronously.

  Returns a JobClient that can be used to monitor the job.

  Example:
    (execute-async env \"My Async Job\")"
  ([^StreamExecutionEnvironment env]
   (.executeAsync env))
  ([^StreamExecutionEnvironment env ^String job-name]
   (.executeAsync env job-name)))

(defn from-source
  "Create a DataStream from a Source connector.

  Used with modern source connectors like KafkaSource, FileSource, etc.

  Options:
    :watermark-strategy - Watermark strategy for event time processing
                          Use (WatermarkStrategy/noWatermarks) for processing time

  Example:
    (require '[flink-clj.connectors.kafka :as kafka])

    (def kafka-source
      (kafka/source {:bootstrap-servers \"localhost:9092\"
                     :topics \"events\"
                     :value-format :string}))

    (from-source env kafka-source \"Kafka Events\")"
  ([^StreamExecutionEnvironment env source source-name]
   (from-source env source source-name nil))
  ([^StreamExecutionEnvironment env source source-name {:keys [watermark-strategy]}]
   (let [strategy (or watermark-strategy
                      (org.apache.flink.api.common.eventtime.WatermarkStrategy/noWatermarks))]
     (.fromSource env source strategy source-name))))

(defn to-sink
  "Send a DataStream to a Sink connector.

  Used with modern sink connectors like KafkaSink, FileSink, etc.

  Example:
    (require '[flink-clj.connectors.kafka :as kafka])

    (def kafka-sink
      (kafka/sink {:bootstrap-servers \"localhost:9092\"
                   :topic \"output\"
                   :value-format :string}))

    (-> stream
        (to-sink kafka-sink \"Kafka Output\"))"
  ([^DataStream stream sink]
   (.sinkTo stream sink))
  ([^DataStream stream sink sink-name]
   (-> stream
       (.sinkTo sink)
       (.name sink-name))))
