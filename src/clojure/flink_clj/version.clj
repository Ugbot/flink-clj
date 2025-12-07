(ns flink-clj.version
  "Flink version detection and compatibility utilities.

  This namespace provides runtime detection of the Flink version being used,
  allowing code to adapt behavior or provide warnings for deprecated APIs.

  The main mechanism is checking for the presence of OpenContext class which
  exists only in Flink 2.x (replaces Configuration parameter in open() methods).")

(def ^:private flink-version-cache (atom nil))

(defn flink-major-version
  "Returns the major Flink version (1 or 2).

  Detection is based on the presence of OpenContext class:
  - Flink 2.x: Has org.apache.flink.api.common.functions.OpenContext
  - Flink 1.x: Does not have this class

  Result is cached for performance."
  []
  (if-let [cached @flink-version-cache]
    cached
    (let [version (try
                    (Class/forName "org.apache.flink.api.common.functions.OpenContext")
                    2
                    (catch ClassNotFoundException _
                      1))]
      (reset! flink-version-cache version)
      version)))

(defn flink-2?
  "Returns true if running on Flink 2.x."
  []
  (= 2 (flink-major-version)))

(defn flink-1?
  "Returns true if running on Flink 1.x."
  []
  (= 1 (flink-major-version)))

(defmacro when-flink-2
  "Execute body only when running on Flink 2.x."
  [& body]
  `(when (flink-2?) ~@body))

(defmacro when-flink-1
  "Execute body only when running on Flink 1.x."
  [& body]
  `(when (flink-1?) ~@body))

(defn deprecated-in-2x
  "Log a deprecation warning for APIs removed/deprecated in Flink 2.x.

  This should be called at the start of functions that use legacy APIs
  like SourceFunction or SinkFunction."
  [api-name replacement]
  (when (flink-2?)
    (println (str "WARNING: " api-name " is deprecated in Flink 2.x. "
                  "Use " replacement " instead."))))

(defn require-flink-1
  "Throw an exception if not running on Flink 1.x.

  Use this for APIs that are completely removed in Flink 2.x."
  [api-name]
  (when (flink-2?)
    (throw (ex-info (str api-name " is not available in Flink 2.x")
                    {:api api-name
                     :flink-version 2}))))

;; =============================================================================
;; Flink 2.1+ Feature Detection
;; =============================================================================

(def ^:private flink-minor-version-cache (atom nil))

(defn flink-minor-version
  "Returns the minor Flink version as a string (e.g., \"2.1\", \"2.0\", \"1.20\").

  Attempts to get the actual version from Flink's EnvironmentInformation class.
  Falls back to major version detection if unavailable."
  []
  (if-let [cached @flink-minor-version-cache]
    cached
    (let [version (try
                    (let [env-info (Class/forName "org.apache.flink.runtime.util.EnvironmentInformation")
                          get-version (.getMethod env-info "getVersion" (into-array Class []))
                          version-str (str (.invoke get-version nil (into-array Object [])))]
                      ;; Extract major.minor from version string like "2.1.0"
                      (if-let [[_ major minor] (re-matches #"(\d+)\.(\d+).*" version-str)]
                        (str major "." minor)
                        version-str))
                    (catch Exception _
                      (str (flink-major-version) ".x")))]
      (reset! flink-minor-version-cache version)
      version)))

(defn flink-2-1+?
  "Returns true if running on Flink 2.1 or later.

  Detection is based on the presence of AsyncSinkWriter class
  which has enhanced batching support in Flink 2.1+."
  []
  (and (flink-2?)
       (try
         (Class/forName "org.apache.flink.connector.base.sink.writer.AsyncSinkWriter")
         ;; Also check for a 2.1-specific feature
         (let [version (flink-minor-version)]
           (or (= version "2.1")
               (when-let [[_ major minor] (re-matches #"(\d+)\.(\d+)" version)]
                 (and (= major "2")
                      (>= (Integer/parseInt minor) 1)))))
         (catch ClassNotFoundException _ false))))

(defn datastream-v2-available?
  "Returns true if DataStream V2 API is available.

  DataStream V2 is an experimental API introduced in Flink 2.0 with
  different stream types and async-first state access."
  []
  (and (flink-2?)
       (try
         (Class/forName "org.apache.flink.datastream.api.ExecutionEnvironment")
         true
         (catch ClassNotFoundException _ false))))

(defn require-flink-2-1
  "Throw an exception if not running on Flink 2.1+.

  Use this for APIs that require Flink 2.1 features."
  [api-name]
  (when-not (flink-2-1+?)
    (throw (ex-info (str api-name " requires Flink 2.1 or later")
                    {:api api-name
                     :flink-version (flink-minor-version)}))))

(defn require-datastream-v2
  "Throw an exception if DataStream V2 API is not available.

  Use this for experimental V2 API functions."
  [api-name]
  (when-not (datastream-v2-available?)
    (throw (ex-info (str api-name " requires DataStream V2 API (Flink 2.x with experimental features)")
                    {:api api-name
                     :flink-version (flink-minor-version)
                     :hint "Ensure you are using Flink 2.x and the DataStream V2 module is available"}))))

(defmacro when-flink-2-1+
  "Execute body only when running on Flink 2.1+."
  [& body]
  `(when (flink-2-1+?) ~@body))

(defmacro when-datastream-v2
  "Execute body only when DataStream V2 API is available."
  [& body]
  `(when (datastream-v2-available?) ~@body))
