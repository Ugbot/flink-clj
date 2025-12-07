(ns flink-clj.connectors.file
  "File connector for reading and writing files.

  Provides idiomatic Clojure wrappers for FileSource and FileSink.

  Example - Reading lines from files:
    (require '[flink-clj.connectors.file :as file])

    (def source
      (file/line-source {:path \"/data/input\"}))

    (-> env
        (flink/from-source source \"File Input\")
        (stream/flink-map process-line)
        ...)

  Example - Writing to files:
    (def sink
      (file/line-sink {:path \"/data/output\"}))

    (-> stream
        (flink/to-sink sink \"File Output\"))

  NOTE: StreamingFileSink is only available in Flink 1.x.
        For Flink 2.x, use flink-connector-files with FileSink."
  (:import [org.apache.flink.api.common.serialization SimpleStringEncoder]
           [org.apache.flink.core.fs Path]
           [java.time Duration]))

;; =============================================================================
;; File Sink (StreamingFileSink - available without extra deps)
;; =============================================================================

(defn- to-duration
  "Convert duration spec to java.time.Duration."
  [spec]
  (cond
    (instance? Duration spec) spec
    (number? spec) (Duration/ofMillis spec)
    (vector? spec)
    (let [[n unit] spec]
      (case unit
        (:ms :milliseconds) (Duration/ofMillis n)
        (:s :seconds) (Duration/ofSeconds n)
        (:m :minutes) (Duration/ofMinutes n)
        (:h :hours) (Duration/ofHours n)
        (:d :days) (Duration/ofDays n)
        (throw (ex-info "Unknown duration unit" {:unit unit}))))
    :else
    (throw (ex-info "Invalid duration spec" {:spec spec}))))

(defn streaming-file-sink
  "Create a StreamingFileSink for writing to files.

  NOTE: StreamingFileSink is only available in Flink 1.x.
        In Flink 2.x, this function will throw an exception.
        Use flink-connector-files with FileSink for Flink 2.x.

  Options:
    :path - Output directory path (required)
    :bucket-assigner - How to partition output files (optional)
    :rolling-policy - When to roll files (optional):
      :part-size - Max part size in bytes before rolling
      :rollover-interval - Max time before rolling
      :inactivity-interval - Roll after inactivity

  Example:
    ;; Simple string output
    (streaming-file-sink {:path \"/output/data\"})

    ;; With rolling policy
    (streaming-file-sink
      {:path \"/output/data\"
       :rolling-policy {:part-size (* 1024 1024 128)      ; 128 MB
                        :rollover-interval [15 :minutes]
                        :inactivity-interval [5 :minutes]}})"
  [{:keys [path rolling-policy]}]
  (try
    (let [sink-class (Class/forName "org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink")
          output-path (Path. path)
          for-row-format (.getMethod sink-class "forRowFormat"
                                     (into-array Class [Path (Class/forName "org.apache.flink.api.common.serialization.Encoder")]))
          builder (.invoke for-row-format nil (into-array Object [output-path (SimpleStringEncoder. "UTF-8")]))]

      ;; Configure rolling policy if specified
      (when rolling-policy
        (let [{:keys [part-size rollover-interval inactivity-interval]} rolling-policy
              policy-class (Class/forName "org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy")
              builder-method (.getMethod policy-class "builder" (into-array Class []))
              policy-builder (.invoke builder-method nil (into-array Object []))]
          (when part-size
            (let [m (.getMethod (.getClass policy-builder) "withMaxPartSize" (into-array Class [Long/TYPE]))]
              (.invoke m policy-builder (into-array Object [(long part-size)]))))
          (when rollover-interval
            (let [m (.getMethod (.getClass policy-builder) "withRolloverInterval" (into-array Class [Duration]))]
              (.invoke m policy-builder (into-array Object [(to-duration rollover-interval)]))))
          (when inactivity-interval
            (let [m (.getMethod (.getClass policy-builder) "withInactivityInterval" (into-array Class [Duration]))]
              (.invoke m policy-builder (into-array Object [(to-duration inactivity-interval)]))))
          (let [build-policy (.getMethod (.getClass policy-builder) "build" (into-array Class []))
                policy (.invoke build-policy policy-builder (into-array Object []))
                with-rolling (.getMethod (.getClass builder) "withRollingPolicy"
                                         (into-array Class [(Class/forName "org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy")]))]
            (.invoke with-rolling builder (into-array Object [policy])))))

      (let [build-method (.getMethod (.getClass builder) "build" (into-array Class []))]
        (.invoke build-method builder (into-array Object []))))
    (catch ClassNotFoundException _
      (throw (ex-info "StreamingFileSink is not available in Flink 2.x. Use flink-connector-files with FileSink."
                      {:flink-version "2.x"
                       :suggestion "Add flink-connector-files dependency and use FileSink"})))))

(defn line-sink
  "Create a simple file sink that writes one line per element.

  Each element is converted to string using str and written as a line.

  Options:
    :path - Output directory path (required)

  Example:
    (line-sink {:path \"/output/lines\"})"
  [{:keys [path]}]
  (streaming-file-sink {:path path}))

;; =============================================================================
;; Note: FileSource requires flink-connector-files dependency
;; The dependency can be added when needed:
;; [org.apache.flink/flink-connector-files \"1.20.0\"]
;; =============================================================================

;; File source utilities will use the basic DataStream.readTextFile for now
;; which is available without extra dependencies

(defn- read-text-file-available?
  "Check if readTextFile method is available (deprecated in Flink 2.x)."
  []
  (try
    (let [env-class (Class/forName "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")]
      (.getMethod env-class "readTextFile" (into-array Class [String String]))
      true)
    (catch NoSuchMethodException _ false)))

(defn read-text-file
  "Read a text file as a DataStream of lines.

  This uses the legacy API which is simpler but less flexible
  than FileSource.

  NOTE: This API is deprecated in Flink 2.x. For Flink 2.x, use FileSource
        from flink-connector-files dependency.

  Options:
    :path - File or directory path (required)
    :charset - Character encoding (default: UTF-8)

  Returns a DataStream<String> of lines.

  Example:
    (read-text-file env {:path \"/data/input.txt\"})"
  [env {:keys [path charset] :or {charset "UTF-8"}}]
  (if (read-text-file-available?)
    (let [env-class (Class/forName "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
          method (.getMethod env-class "readTextFile" (into-array Class [String String]))]
      (.invoke method env (into-array Object [path charset])))
    (throw (ex-info "readTextFile is not available in Flink 2.x. Use FileSource from flink-connector-files."
                    {:flink-version "2.x"
                     :suggestion "Add flink-connector-files dependency and use FileSource"}))))

(defn read-text-file-parallel
  "Read text files in parallel across multiple paths.

  NOTE: This API is deprecated in Flink 2.x. For Flink 2.x, use FileSource
        from flink-connector-files dependency.

  Options:
    :paths - Collection of file/directory paths (required)
    :charset - Character encoding (default: UTF-8)

  Returns a DataStream<String> of lines from all files.

  Example:
    (read-text-file-parallel env {:paths [\"/data/part1\" \"/data/part2\"]})"
  [env {:keys [paths charset] :or {charset "UTF-8"}}]
  (if (read-text-file-available?)
    (let [first-stream (read-text-file env {:path (first paths) :charset charset})]
      (if (= 1 (count paths))
        first-stream
        (let [other-streams (map #(read-text-file env {:path % :charset charset}) (rest paths))]
          (.union first-stream (into-array org.apache.flink.streaming.api.datastream.DataStream other-streams)))))
    (throw (ex-info "readTextFile is not available in Flink 2.x. Use FileSource from flink-connector-files."
                    {:flink-version "2.x"
                     :suggestion "Add flink-connector-files dependency and use FileSource"}))))
