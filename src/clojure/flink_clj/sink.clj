(ns flink-clj.sink
  "Sink operations for writing DataStream output.

  Provides built-in sinks for common destinations.

  Note: Some functions in this namespace use legacy APIs that are deprecated
  in Flink 1.20 and removed in Flink 2.x. See function docstrings for details."
  (:refer-clojure :exclude [print])
  (:require [flink-clj.version :as v])
  (:import [org.apache.flink.streaming.api.datastream DataStream DataStreamSink]))

(defn print
  "Print stream elements to stdout.

  Useful for debugging and examples.

  Example:
    (print stream)
    (print stream \"Output: \")"
  ([^DataStream stream]
   (.print stream))
  ([^DataStream stream ^String sink-identifier]
   (let [^DataStreamSink sink (.print stream)]
     (.name sink sink-identifier))))

(defn print-to-err
  "Print stream elements to stderr.

  Example:
    (print-to-err stream)"
  [^DataStream stream]
  (.printToErr stream))

(defn write-as-text
  "Write stream elements as text to a file.

  DEPRECATED: This function uses the legacy writeAsText API which is deprecated
  in Flink 1.20 and removed in Flink 2.x. Use FileSink with (flink-clj.core/sink-to)
  instead for forward compatibility.

  Each element is converted to string using toString().

  Example:
    (write-as-text stream \"/path/to/output\")"
  {:deprecated "Use FileSink with sink-to for Flink 2.x compatibility"}
  [^DataStream stream ^String path]
  (v/deprecated-in-2x "writeAsText" "FileSink with sink-to")
  (.writeAsText stream path))

(defn add-sink
  "Add a custom SinkFunction to the stream.

  DEPRECATED: This function uses the legacy SinkFunction API which is deprecated
  in Flink 1.20 and removed in Flink 2.x. Use (flink-clj.core/sink-to) with a
  Sink implementation instead for forward compatibility.

  For when you need to implement custom sink logic.

  Example:
    (add-sink stream my-sink-function)"
  {:deprecated "Use sink-to with a Sink implementation for Flink 2.x compatibility"}
  [^DataStream stream sink-fn]
  (v/deprecated-in-2x "SinkFunction/addSink" "Sink with sink-to")
  (.addSink stream sink-fn))

(defn discard
  "Discard all stream elements (no output).

  Useful for benchmarking or when side effects are the goal.

  Example:
    (discard stream)"
  [^DataStream stream]
  ;; Use a no-op print sink with parallelism 1 that we ignore
  (-> stream
      (.print)
      (.setParallelism 1)
      (.name "Discard")))
