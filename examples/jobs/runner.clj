(ns runner
  "Demo Job Runner

  Unified entry point for running flink-clj demo jobs.

  Available Jobs:
  1. Word Count      - Classic streaming word count
  2. E-Commerce      - Real-time analytics with windowed aggregations
  3. Fraud Detection - Transaction fraud detection with risk scoring

  Quick Start:
    $ docker compose up -d
    $ lein with-profile +flink-1.20,+dev repl

    (require '[runner :as runner])
    (runner/help)
    (runner/run! :word-count)
    (runner/run! :ecommerce)
    (runner/run! :fraud)"
  (:require [word-count-job :as word-count]
            [ecommerce-analytics-job :as ecommerce]
            [fraud-detection-job :as fraud]
            [data-generator :as gen]
            [clojure.string :as str]))

;; =============================================================================
;; Job Registry
;; =============================================================================

(def jobs
  {:word-count
   {:name "Word Count"
    :description "Classic streaming word count from Kafka"
    :input-topic "word-stream"
    :run-fn word-count/run!
    :generator gen/start-word-generator!
    :default-rate 5}

   :ecommerce
   {:name "E-Commerce Analytics"
    :description "Real-time revenue and user activity analytics"
    :input-topic "ecommerce-events"
    :run-fn ecommerce/run!
    :generator gen/start-ecommerce-generator!
    :default-rate 10}

   :fraud
   {:name "Fraud Detection"
    :description "Real-time transaction fraud scoring"
    :input-topic "transactions"
    :run-fn fraud/run!
    :generator gen/start-transaction-generator!
    :default-rate 10}})

;; =============================================================================
;; Help & Information
;; =============================================================================

(defn help
  "Print help information about available demo jobs."
  []
  (println)
  (println (str/join (repeat 70 "=")))
  (println "  flink-clj Demo Jobs")
  (println (str/join (repeat 70 "=")))
  (println)
  (println "Prerequisites:")
  (println "  1. Start Docker services: docker compose up -d")
  (println "  2. Start REPL: lein with-profile +flink-1.20,+dev repl")
  (println "  3. Load runner: (require '[runner :as runner])")
  (println)
  (println "Available Jobs:")
  (println)
  (doseq [[key {:keys [name description input-topic default-rate]}] jobs]
    (println (format "  %-15s %s" (clojure.core/name key) name))
    (println (format "  %-15s %s" "" description))
    (println (format "  %-15s Input: %s | Rate: %d events/sec" "" input-topic default-rate))
    (println))
  (println "Usage:")
  (println "  (runner/run! :word-count)              ; Run with defaults")
  (println "  (runner/run! :fraud {:rate 20})        ; Custom event rate")
  (println "  (runner/run! :ecommerce {:generate? false}) ; No auto-generation")
  (println)
  (println "Other Commands:")
  (println "  (runner/list-jobs)     ; List available jobs")
  (println "  (runner/status)        ; Check Docker services and generator")
  (println "  (runner/stop!)         ; Stop running generator")
  (println "  (runner/generate! :ecommerce 100)  ; Generate batch of events")
  (println)
  (println "Kafka UI: http://localhost:8080")
  (println (str/join (repeat 70 "=")))
  (println))

(defn list-jobs
  "List all available demo jobs."
  []
  (println "\nAvailable Jobs:")
  (doseq [[key {:keys [name description]}] jobs]
    (println (format "  %-15s %s" (clojure.core/name key) name)))
  (println "\nUse (runner/run! <job-key>) to run a job."))

;; =============================================================================
;; Status & Control
;; =============================================================================

(defn status
  "Check the status of Docker services and data generator."
  []
  (println "\n=== Status ===\n")

  ;; Check generator
  (println "Data Generator:" (if (gen/generator-running?) "RUNNING" "stopped"))

  ;; Check Kafka
  (print "Kafka:         ")
  (try
    (let [props (doto (java.util.Properties.)
                  (.put "bootstrap.servers" "localhost:9092"))
          admin (org.apache.kafka.clients.admin.AdminClient/create props)]
      (try
        (.get (.listTopics admin))
        (println "OK (localhost:9092)")
        (finally
          (.close admin))))
    (catch Exception e
      (println "FAILED -" (.getMessage e))))

  ;; Check PostgreSQL
  (print "PostgreSQL:    ")
  (try
    (let [conn (java.sql.DriverManager/getConnection
                 "jdbc:postgresql://localhost:5432/flink_test"
                 "flink" "flink")]
      (try
        (.isValid conn 1)
        (println "OK (localhost:5432)")
        (finally
          (.close conn))))
    (catch Exception e
      (println "FAILED -" (.getMessage e))))

  (println "\nKafka UI: http://localhost:8080")
  (println))

(defn stop!
  "Stop the running data generator."
  []
  (gen/stop-generator!)
  (println "Generator stopped."))

;; =============================================================================
;; Data Generation
;; =============================================================================

(defn generate!
  "Generate a batch of events for a specific job type.

  job-key: :word-count, :ecommerce, or :fraud
  count:   number of events to generate (default: 100)"
  ([job-key] (generate! job-key 100))
  ([job-key count]
   (case job-key
     :word-count (gen/generate-batch! :words count)
     :ecommerce (gen/generate-batch! :ecommerce count)
     :fraud (gen/generate-batch! :transactions count)
     (println "Unknown job type. Use :word-count, :ecommerce, or :fraud"))))

(defn start-generator!
  "Start continuous event generation for a specific job type.

  job-key: :word-count, :ecommerce, or :fraud
  rate:    events per second (optional, uses job default)"
  ([job-key] (start-generator! job-key nil))
  ([job-key rate]
   (if-let [job (get jobs job-key)]
     (let [actual-rate (or rate (:default-rate job))]
       ((:generator job) actual-rate))
     (println "Unknown job type. Use :word-count, :ecommerce, or :fraud"))))

;; =============================================================================
;; Job Runner
;; =============================================================================

(defn run!
  "Run a demo job.

  job-key: :word-count, :ecommerce, or :fraud

  Options:
    :generate? - Auto-start data generator (default: true)
    :rate      - Events per second for generator

  Examples:
    (run! :word-count)
    (run! :ecommerce {:rate 20})
    (run! :fraud {:generate? false})"
  ([job-key] (run! job-key {}))
  ([job-key opts]
   (if-let [job (get jobs job-key)]
     (let [run-fn (:run-fn job)
           opts-with-defaults (merge {:rate (:default-rate job)} opts)]
       (println "\n" (str/join (repeat 60 "=")) "\n")
       (println "  Starting:" (:name job))
       (println "  " (:description job))
       (println "\n" (str/join (repeat 60 "=")) "\n")
       (run-fn opts-with-defaults))
     (do
       (println "Unknown job:" job-key)
       (list-jobs)))))

(defn run-all-generators!
  "Start all generators simultaneously (for multi-job testing)."
  [rate]
  (println "Starting all generators at" rate "events/sec each...")
  (gen/create-topics!)
  (future (gen/start-word-generator! rate))
  (Thread/sleep 100)
  (future (gen/start-ecommerce-generator! rate))
  (Thread/sleep 100)
  (future (gen/start-transaction-generator! rate))
  (println "All generators started. Use (runner/stop!) to stop."))

;; =============================================================================
;; Quick Demo
;; =============================================================================

(defn demo!
  "Run a quick demonstration of all job types.

  This generates sample data for each job type without starting
  the streaming pipelines. Use run! to start actual stream processing."
  []
  (println "\n" (str/join (repeat 70 "=")) "\n")
  (println "  flink-clj Quick Demo")
  (println "\n" (str/join (repeat 70 "=")) "\n")

  (println "Checking services...")
  (status)

  (println "\n--- Generating Sample Data ---\n")

  (println "Generating 50 word stream events...")
  (gen/generate-batch! :words 50)

  (println "\nGenerating 100 e-commerce events...")
  (gen/generate-batch! :ecommerce 100)

  (println "\nGenerating 100 transaction events...")
  (gen/generate-batch! :transactions 100)

  (println "\n" (str/join (repeat 70 "=")) "\n")
  (println "Demo data generated!")
  (println)
  (println "View topics at: http://localhost:8080")
  (println)
  (println "To run streaming jobs:")
  (println "  (runner/run! :word-count)")
  (println "  (runner/run! :ecommerce)")
  (println "  (runner/run! :fraud)")
  (println "\n" (str/join (repeat 70 "=")) "\n"))

;; =============================================================================
;; REPL Auto-Help
;; =============================================================================

(comment
  ;; Quick start
  (help)

  ;; Check status
  (status)

  ;; Generate sample data
  (demo!)

  ;; Run individual jobs
  (run! :word-count)
  (run! :ecommerce)
  (run! :fraud)

  ;; Custom options
  (run! :fraud {:rate 20})
  (run! :ecommerce {:generate? false})

  ;; Manual data generation
  (generate! :ecommerce 200)
  (start-generator! :fraud 15)
  (stop!)
  )
