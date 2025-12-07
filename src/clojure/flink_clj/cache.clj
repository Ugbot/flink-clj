(ns flink-clj.cache
  "Distributed cache for sharing read-only data across task instances.

  The distributed cache allows you to register files (local or remote) that
  are then made available to all task instances. This is useful for:
  - Lookup tables / dictionaries
  - Machine learning models
  - Configuration files
  - Reference data

  Files are cached on each TaskManager and accessed via RuntimeContext.

  Example - Register and use a lookup file:
    (require '[flink-clj.cache :as cache])
    (require '[flink-clj.env :as env])

    ;; Register the file with the environment
    (-> (env/create-env)
        (cache/register-file \"hdfs:///data/lookup.csv\" \"lookup\"))

    ;; Access in a RichFunction (via process function)
    (defn enrich-with-lookup [ctx element]
      (let [lookup-file (cache/get-file ctx \"lookup\")
            lookup-data (cache/read-lines lookup-file)]
        ;; Use lookup-data to enrich element
        (assoc element :enriched true)))

  Supported file systems:
  - Local files (file://)
  - HDFS (hdfs://)
  - S3 (s3://)
  - Any Flink-supported filesystem"
  (:import [org.apache.flink.api.common.cache DistributedCache]
           [org.apache.flink.api.common RuntimeExecutionMode]
           [java.io File BufferedReader FileReader]
           [java.nio.file Files Paths]))

;; =============================================================================
;; Registration
;; =============================================================================

(defn register-file
  "Register a file with the distributed cache.

  The file will be downloaded to each TaskManager and made available
  via RuntimeContext.getDistributedCache().

  Arguments:
    env       - StreamExecutionEnvironment
    file-path - Path to the file (local, HDFS, S3, etc.)
    name      - Name to reference the file

  Options:
    :executable - If true, file will be marked as executable (default: false)

  Example:
    (register-file env \"hdfs:///data/model.bin\" \"ml-model\")
    (register-file env \"/local/script.sh\" \"script\" {:executable true})"
  ([env file-path name]
   (register-file env file-path name {}))
  ([env file-path name {:keys [executable] :or {executable false}}]
   (.registerCachedFile env file-path name (boolean executable))
   env))

(defn register-files
  "Register multiple files with the distributed cache.

  Arguments:
    env   - StreamExecutionEnvironment
    files - Map of name -> file-path or name -> {:path ... :executable ...}

  Example:
    (register-files env
      {\"lookup\" \"hdfs:///data/lookup.csv\"
       \"model\" {:path \"s3://bucket/model.bin\" :executable false}
       \"script\" {:path \"/local/run.sh\" :executable true}})"
  [env files]
  (doseq [[name file-spec] files]
    (if (map? file-spec)
      (register-file env (:path file-spec) name file-spec)
      (register-file env file-spec name)))
  env)

;; =============================================================================
;; Access (from RuntimeContext)
;; =============================================================================

(defn get-file
  "Get a cached file from RuntimeContext.

  Use this inside a RichFunction's open() method or process methods
  that have access to RuntimeContext.

  Returns a java.io.File pointing to the local cached copy.

  Arguments:
    ctx  - RuntimeContext (from RichFunction or ProcessFunction)
    name - Name the file was registered with

  Example:
    (defn open-fn [ctx]
      (let [lookup-file (cache/get-file ctx \"lookup\")]
        (load-lookup-table lookup-file)))"
  [ctx name]
  (.getDistributedCache ctx)
  (let [cache (.getDistributedCache ctx)]
    (.getFile cache name)))

(defn get-file-path
  "Get the path to a cached file as a string.

  Arguments:
    ctx  - RuntimeContext
    name - Name the file was registered with

  Returns the absolute path to the local cached file."
  [ctx name]
  (.getAbsolutePath (get-file ctx name)))

;; =============================================================================
;; File Reading Utilities
;; =============================================================================

(defn read-lines
  "Read all lines from a cached file.

  Arguments:
    file - java.io.File from get-file

  Returns a vector of strings, one per line."
  [^File file]
  (vec (Files/readAllLines (.toPath file))))

(defn read-string
  "Read entire file contents as a single string.

  Arguments:
    file - java.io.File from get-file"
  [^File file]
  (String. (Files/readAllBytes (.toPath file))))

(defn read-edn
  "Read file contents as EDN.

  Arguments:
    file - java.io.File from get-file"
  [^File file]
  (clojure.edn/read-string (read-string file)))

(defn read-json
  "Read file contents as JSON.

  Arguments:
    file - java.io.File from get-file

  Requires cheshire or data.json on classpath."
  [^File file]
  (let [parse-fn (or (try (requiring-resolve 'cheshire.core/parse-string)
                          (catch Exception _ nil))
                     (try (requiring-resolve 'clojure.data.json/read-str)
                          (catch Exception _ nil)))]
    (if parse-fn
      (parse-fn (read-string file))
      (throw (ex-info "JSON parsing requires cheshire or clojure.data.json"
                      {:suggestion "Add [cheshire \"5.x\"] or [org.clojure/data.json \"2.x\"]"})))))

(defn read-csv
  "Read file contents as CSV, returning a vector of vectors.

  Arguments:
    file - java.io.File from get-file

  Options:
    :separator - Field separator (default: \\,)
    :header?   - If true, first row is header (default: false)

  Returns vector of row vectors, or if header? is true,
  returns vector of maps with header keys."
  ([^File file]
   (read-csv file {}))
  ([^File file {:keys [separator header?] :or {separator \, header? false}}]
   (let [lines (read-lines file)
         parse-row (fn [line] (vec (.split line (str separator))))]
     (if header?
       (let [header (map keyword (parse-row (first lines)))
             rows (map parse-row (rest lines))]
         (mapv #(zipmap header %) rows))
       (mapv parse-row lines)))))

(defn lines-seq
  "Return a lazy sequence of lines from a cached file.

  Use this for large files to avoid loading everything into memory.
  Remember to close the reader when done.

  Arguments:
    file - java.io.File from get-file

  Returns [reader line-seq] - remember to close reader!"
  [^File file]
  (let [reader (BufferedReader. (FileReader. file))]
    [reader (line-seq reader)]))

;; =============================================================================
;; Lookup Table Builder
;; =============================================================================

(defn build-lookup
  "Build an in-memory lookup map from a cached file.

  Arguments:
    file    - java.io.File from get-file
    key-fn  - Function to extract key from each line/record
    val-fn  - Function to extract value from each line/record

  Options:
    :format    - File format: :lines, :csv, :edn, :json (default: :lines)
    :separator - CSV separator (default: \\,)
    :header?   - CSV has header row (default: false)

  Example - Build lookup from CSV:
    (build-lookup file
      (fn [row] (get row 0))   ; First column as key
      (fn [row] (get row 1))   ; Second column as value
      {:format :csv})

  Example - Build lookup from EDN:
    (build-lookup file :id identity {:format :edn})"
  ([^File file key-fn val-fn]
   (build-lookup file key-fn val-fn {}))
  ([^File file key-fn val-fn {:keys [format separator header?]
                               :or {format :lines separator \, header? false}}]
   (let [data (case format
                :lines (read-lines file)
                :csv (read-csv file {:separator separator :header? header?})
                :edn (let [content (read-edn file)]
                       (if (sequential? content) content [content]))
                :json (let [content (read-json file)]
                        (if (sequential? content) content [content])))]
     (into {} (map (juxt key-fn val-fn) data)))))

;; =============================================================================
;; Cache Info
;; =============================================================================

(defn cache-info
  "Get information about the distributed cache feature."
  []
  {:description "Distributed cache for sharing read-only data across tasks"
   :features [:file-registration
              :multi-filesystem-support
              :executable-files
              :lazy-loading
              :lookup-table-builder]
   :supported-filesystems [:local :hdfs :s3 :gcs :azure]
   :usage-notes ["Register files before job execution"
                 "Access files via RuntimeContext in RichFunctions"
                 "Files are downloaded to TaskManagers on first access"
                 "Use for read-only data only"]})
