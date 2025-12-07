(ns flink-clj.connected
  "Connected stream operations.

  ConnectedStreams represent two streams that share state. Operations on
  connected streams can access both inputs and maintain shared state.

  Use cases:
  - Joining two streams with different types
  - Enriching one stream with data from another
  - Implementing control streams that modify processing behavior

  Example:
    (require '[flink-clj.connected :as co])
    (require '[flink-clj.stream :as stream])

    ;; Connect two streams
    (def connected (co/connect data-stream control-stream))

    ;; Process both streams with different functions
    (def result
      (co/co-map connected
                 process-data-fn
                 process-control-fn))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.datastream
            DataStream ConnectedStreams]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

(defn connect
  "Connect two DataStreams for coordinated processing.

  The two streams can have different types. Use co-map, co-flat-map,
  or process to handle elements from both streams.

  Example:
    (def connected (connect data-stream config-stream))"
  [^DataStream stream1 ^DataStream stream2]
  (.connect stream1 stream2))

(defn co-map
  "Apply different map functions to each side of a connected stream.

  map1-fn processes elements from the first stream.
  map2-fn processes elements from the second stream.
  Both functions must return the same type.

  Functions must be top-level vars (defined with defn) for serialization.

  Options:
    :returns - TypeInformation or spec for output type

  Example:
    (defn process-data [data] {:type :data :value data})
    (defn process-control [ctrl] {:type :control :value ctrl})

    (co-map connected process-data process-control)"
  ([^ConnectedStreams streams map1-fn map2-fn]
   (co-map streams map1-fn map2-fn nil))
  ([^ConnectedStreams streams map1-fn map2-fn {:keys [returns]}]
   (let [output-type (when returns (resolve-type-info returns))
         wrapper (impl/make-co-map-function map1-fn map2-fn output-type)
         result (.map streams wrapper)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

(defn co-flat-map
  "Apply different flat-map functions to each side of a connected stream.

  flat-map1-fn processes elements from the first stream, returns a sequence.
  flat-map2-fn processes elements from the second stream, returns a sequence.
  Both functions must return sequences of the same element type.

  Functions must be top-level vars (defined with defn) for serialization.

  Options:
    :returns - TypeInformation or spec for output element type

  Example:
    (defn expand-data [data] [(:a data) (:b data)])
    (defn expand-control [ctrl] [ctrl])

    (co-flat-map connected expand-data expand-control)"
  ([^ConnectedStreams streams flat-map1-fn flat-map2-fn]
   (co-flat-map streams flat-map1-fn flat-map2-fn nil))
  ([^ConnectedStreams streams flat-map1-fn flat-map2-fn {:keys [returns]}]
   (let [output-type (when returns (resolve-type-info returns))
         wrapper (impl/make-co-flat-map-function flat-map1-fn flat-map2-fn output-type)
         result (.flatMap streams wrapper)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

(defn key-by
  "Partition both streams by key for stateful processing.

  key1-fn extracts key from first stream elements.
  key2-fn extracts key from second stream elements.

  Both key functions should return the same key type.

  Options:
    :key-type - TypeInformation or spec for the key type

  Example:
    (key-by connected :user-id :user-id)"
  ([^ConnectedStreams streams key1-fn key2-fn]
   (key-by streams key1-fn key2-fn nil))
  ([^ConnectedStreams streams key1-fn key2-fn {:keys [key-type]}]
   (let [selector1 (impl/make-key-selector key1-fn)
         selector2 (impl/make-key-selector key2-fn)]
     (if key-type
       (.keyBy streams selector1 selector2 ^TypeInformation (resolve-type-info key-type))
       (.keyBy streams selector1 selector2)))))
