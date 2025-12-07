(ns word-count
  "Classic word count example using flink-clj.

  Demonstrates:
  - Creating an execution environment
  - Registering Clojure types for serialization
  - Basic transformations (flat-map, map)
  - Keyed aggregations (key-by, reduce)
  - Printing output

  Run with:
    lein with-profile +flink-1.20 run -m examples.word-count"
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.sink :as sink]
            [clojure.string :as str]))

;; Define transformation functions at top level (required for serialization)

(defn tokenize
  "Split a line into [word, 1] tuples."
  [line]
  (for [word (str/split (str/lower-case line) #"\s+")
        :when (not (str/blank? word))]
    [word 1]))

(defn sum-counts
  "Sum two [word, count] tuples."
  [[word count1] [_ count2]]
  [word (+ count1 count2)])

(def sample-text
  "Sample text data for word counting."
  ["Hello world hello Flink"
   "Apache Flink is a stream processing framework"
   "Clojure is a functional programming language"
   "flink-clj brings Clojure and Flink together"
   "Stream processing with Clojure is fun"
   "Hello Clojure hello stream processing"])

(defn get-word
  "Extract the word from a [word, count] tuple."
  [pair]
  (first pair))

(defn word-count-pipeline
  "Build the word count pipeline."
  [env data]
  (-> (flink/from-collection env data)
      (stream/flat-map #'tokenize)
      (keyed/key-by #'get-word {:key-type :string})
      (keyed/flink-reduce #'sum-counts)
      (sink/print)))

(defn run-word-count
  "Run the word count job."
  ([]
   (run-word-count sample-text))
  ([data]
   (let [env (env/create-env {:parallelism 1})]
     (flink/register-clojure-types! env)
     (word-count-pipeline env data)
     (flink/execute env "Word Count Example"))))

(defn -main
  "Entry point for running the word count example."
  [& args]
  (println "Running Word Count Example...")
  (println "Input data:")
  (doseq [line sample-text]
    (println "  " line))
  (println)
  (println "Results:")
  (run-word-count)
  (println)
  (println "Done!"))
