(ns word-count-job
  "Demo Job 1: Streaming Word Count

  A classic streaming example that demonstrates:
  - Reading from Kafka
  - FlatMap for tokenization
  - KeyBy for grouping
  - Reduce for aggregation
  - Writing results back to Kafka

  Run:
    1. docker compose up -d
    2. lein with-profile +flink-1.20,+dev repl
    3. (require '[word-count-job :as wc])
    4. (wc/run!)"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str]
            [data-generator :as gen]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers "localhost:9092"
           :group-id "word-count-job"}
   :input-topic "word-stream"
   :output-topic "word-counts"})

;; =============================================================================
;; Processing Functions (must be top-level for serialization)
;; =============================================================================

(defn parse-message
  "Parse EDN message from Kafka."
  [msg]
  (try
    (read-string msg)
    (catch Exception _
      {:text "" :source "unknown"})))

(defn extract-words
  "Extract words from a message, returning a sequence of [word, 1] pairs."
  [{:keys [text]}]
  (when text
    (->> (str/split (str/lower-case text) #"\s+")
         (filter #(> (count %) 2))  ; Filter short words
         (map (fn [word]
                [(str/replace word #"[^a-z]" "") 1]))
         (filter #(> (count (first %)) 0)))))

(defn sum-counts
  "Sum two word count pairs."
  [[word c1] [_ c2]]
  [word (+ c1 c2)])

(defn get-word
  "Extract word from pair for keying."
  [[word _]] word)

(defn format-result
  "Format word count for output."
  [[word count]]
  (format "{\"word\":\"%s\",\"count\":%d}" word count))

;; =============================================================================
;; Job Definition
;; =============================================================================

(defn build-pipeline
  "Build the word count streaming pipeline."
  [flink-env]
  (let [source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(:input-topic config)]
                  :group-id (get-in config [:kafka :group-id])
                  :starting-offsets :latest
                  :value-format :string})
        sink (kafka/sink
               {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                :topic (:output-topic config)
                :value-format :string})]

    (-> flink-env
        (flink/from-source source "Kafka Word Input")
        (stream/flink-map #'parse-message)
        (stream/flat-map #'extract-words)
        (keyed/key-by #'get-word {:key-type :string})
        (keyed/flink-reduce #'sum-counts)
        (stream/flink-map #'format-result)
        (flink/to-sink sink "Kafka Word Count Output"))))

(defn run!
  "Run the word count job.

  Options:
    :generate? - Start data generator (default: true)
    :rate      - Events per second for generator (default: 5)

  Example:
    (run!)
    (run! {:generate? false})  ; Use existing data"
  ([] (run! {}))
  ([{:keys [generate? rate] :or {generate? true rate 5}}]
   (println "\n" (str/join (repeat 60 "=")) "\n")
   (println "  WORD COUNT STREAMING JOB")
   (println "\n" (str/join (repeat 60 "=")) "\n")
   (println "Input topic: " (:input-topic config))
   (println "Output topic:" (:output-topic config))
   (println)

   ;; Start data generator
   (when generate?
     (println "Starting data generator at" rate "events/sec...")
     (gen/start-word-generator! rate))

   ;; Build and run pipeline
   (let [flink-env (env/create-env {:parallelism 1})]
     (build-pipeline flink-env)

     (println "\nStarting Flink job...")
     (println "View Kafka topics at: http://localhost:8080")
     (println "Press Ctrl+C to stop\n")

     (try
       (.execute flink-env "Word Count Job")
       (finally
         (when generate?
           (gen/stop-generator!)))))))

(defn run-print!
  "Run word count with console output (for debugging)."
  []
  (println "\nWord Count (Console Output)")
  (println "=" 40)

  (gen/start-word-generator! 3)

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(:input-topic config)]
                  :group-id (str (get-in config [:kafka :group-id]) "-print")
                  :starting-offsets :latest
                  :value-format :string})]

    (-> flink-env
        (flink/from-source source "Kafka Input")
        (stream/flink-map #'parse-message)
        (stream/flat-map #'extract-words)
        (keyed/key-by #'get-word {:key-type :string})
        (keyed/flink-reduce #'sum-counts)
        (stream/flink-map #'format-result)
        (stream/flink-print))

    (try
      (.execute flink-env "Word Count (Print)")
      (finally
        (gen/stop-generator!)))))

;; =============================================================================
;; REPL
;; =============================================================================

(comment
  ;; Quick start:
  ;; $ docker compose up -d

  ;; Generate test data only
  (gen/generate-batch! :words 100)

  ;; Run with auto-generation
  (run!)

  ;; Run without generator (use existing data)
  (run! {:generate? false})

  ;; Debug with console output
  (run-print!)

  ;; Stop generator manually
  (gen/stop-generator!)
  )
