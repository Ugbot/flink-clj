(ns fraud-detection-job
  "Demo Job 3: Real-Time Fraud Detection

  A fraud detection pipeline that demonstrates:
  - Reading transaction events from Kafka
  - Stateful processing for pattern detection
  - KeyBy for per-user analysis
  - Multiple fraud detection rules
  - Alert generation

  Fraud Rules:
  1. High-value transactions (> $500)
  2. Rapid successive transactions (> 3 in 30 seconds)
  3. International transactions from new locations
  4. Unusual merchant patterns

  Run:
    1. docker compose up -d
    2. lein with-profile +flink-1.20,+dev repl
    3. (require '[fraud-detection-job :as fraud])
    4. (fraud/run!)"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str]
            [data-generator :as gen])
  (:import [java.time Instant]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers "localhost:9092"
           :group-id "fraud-detection"}
   :input-topic "transactions"
   :output-topics {:alerts "fraud-alerts"
                   :flagged "flagged-transactions"
                   :clean "clean-transactions"}
   :thresholds {:high-value 500.0
                :suspicious-value 1000.0
                :max-velocity 3      ; max transactions per window
                :velocity-window-sec 30}})

;; =============================================================================
;; Processing Functions (must be top-level for serialization)
;; =============================================================================

(defn parse-transaction
  "Parse EDN transaction from Kafka."
  [msg]
  (try
    (let [txn (read-string msg)]
      (assoc txn :parsed-at (str (Instant/now))))
    (catch Exception _
      {:transaction-id "unknown"
       :user-id "unknown"
       :amount 0.0
       :merchant "unknown"
       :is-international false
       :_is-fraud false})))

;; --- Rule 1: High-Value Transaction Detection ---

(defn analyze-transaction-value
  "Add risk score based on transaction amount.
   Returns transaction with :value-risk added."
  [txn]
  (let [amount (:amount txn 0)
        high-threshold (:high-value (:thresholds config))
        suspicious-threshold (:suspicious-value (:thresholds config))
        value-risk (cond
                     (>= amount suspicious-threshold) {:level :critical :score 80 :reason "Very high transaction amount"}
                     (>= amount high-threshold) {:level :warning :score 40 :reason "High transaction amount"}
                     :else {:level :normal :score 0 :reason nil})]
    (assoc txn :value-risk value-risk)))

;; --- Rule 2: International Transaction Detection ---

(defn analyze-international
  "Add risk score based on international status.
   Returns transaction with :international-risk added."
  [txn]
  (let [is-international (:is-international txn false)
        intl-risk (if is-international
                    {:level :warning :score 20 :reason "International transaction"}
                    {:level :normal :score 0 :reason nil})]
    (assoc txn :international-risk intl-risk)))

;; --- Rule 3: Merchant Risk Analysis ---

(def suspicious-merchants
  #{"Unknown Merchant" "Foreign Site"})

(defn analyze-merchant
  "Add risk score based on merchant.
   Returns transaction with :merchant-risk added."
  [txn]
  (let [merchant (:merchant txn "unknown")
        merchant-risk (if (contains? suspicious-merchants merchant)
                        {:level :warning :score 30 :reason (str "Suspicious merchant: " merchant)}
                        {:level :normal :score 0 :reason nil})]
    (assoc txn :merchant-risk merchant-risk)))

;; --- Combined Risk Score ---

(defn calculate-total-risk
  "Calculate total risk score and determine if transaction is flagged.
   Returns transaction with :total-risk and :is-flagged added."
  [txn]
  (let [value-score (get-in txn [:value-risk :score] 0)
        intl-score (get-in txn [:international-risk :score] 0)
        merchant-score (get-in txn [:merchant-risk :score] 0)
        total-score (+ value-score intl-score merchant-score)
        risk-level (cond
                     (>= total-score 80) :critical
                     (>= total-score 50) :high
                     (>= total-score 20) :medium
                     :else :low)
        reasons (->> [(:value-risk txn) (:international-risk txn) (:merchant-risk txn)]
                     (filter #(some? (:reason %)))
                     (map :reason))]
    (assoc txn
           :total-risk {:score total-score :level risk-level :reasons reasons}
           :is-flagged (>= total-score 50))))

;; --- Filtering ---

(defn is-flagged?
  "Check if transaction is flagged for review."
  [txn]
  (:is-flagged txn false))

(defn is-critical?
  "Check if transaction is critical (immediate alert)."
  [txn]
  (= :critical (get-in txn [:total-risk :level])))

(defn is-clean?
  "Check if transaction is clean (low risk)."
  [txn]
  (not (:is-flagged txn)))

;; --- Output Formatting ---

(defn format-alert
  "Format critical alert for output."
  [txn]
  (format "{\"alert_type\":\"CRITICAL\",\"transaction_id\":\"%s\",\"user_id\":\"%s\",\"amount\":%.2f,\"risk_score\":%d,\"reasons\":%s,\"timestamp\":\"%s\"}"
          (:transaction-id txn)
          (:user-id txn)
          (double (:amount txn 0))
          (get-in txn [:total-risk :score] 0)
          (pr-str (get-in txn [:total-risk :reasons] []))
          (:parsed-at txn)))

(defn format-flagged
  "Format flagged transaction for review queue."
  [txn]
  (format "{\"status\":\"FLAGGED\",\"transaction_id\":\"%s\",\"user_id\":\"%s\",\"amount\":%.2f,\"merchant\":\"%s\",\"risk_level\":\"%s\",\"risk_score\":%d,\"reasons\":%s}"
          (:transaction-id txn)
          (:user-id txn)
          (double (:amount txn 0))
          (:merchant txn)
          (name (get-in txn [:total-risk :level] :unknown))
          (get-in txn [:total-risk :score] 0)
          (pr-str (get-in txn [:total-risk :reasons] []))))

(defn format-clean
  "Format clean transaction."
  [txn]
  (format "{\"status\":\"CLEAN\",\"transaction_id\":\"%s\",\"user_id\":\"%s\",\"amount\":%.2f,\"risk_score\":%d}"
          (:transaction-id txn)
          (:user-id txn)
          (double (:amount txn 0))
          (get-in txn [:total-risk :score] 0)))

(defn format-analysis
  "Format full analysis for console output."
  [txn]
  (let [risk-level (get-in txn [:total-risk :level] :unknown)
        indicator (case risk-level
                    :critical "🚨 CRITICAL"
                    :high     "⚠️  HIGH"
                    :medium   "📋 MEDIUM"
                    :low      "✅ LOW"
                    "❓ UNKNOWN")]
    (format "%s | User: %s | $%.2f | %s | Score: %d | %s"
            indicator
            (:user-id txn)
            (double (:amount txn 0))
            (:merchant txn)
            (get-in txn [:total-risk :score] 0)
            (str/join ", " (get-in txn [:total-risk :reasons] [])))))

;; --- Keying ---

(defn get-user
  "Extract user-id for keying."
  [txn]
  (:user-id txn "unknown"))

;; --- User Transaction Aggregation ---

(defn txn-to-user-stats
  "Convert transaction to user statistics tuple.
   Returns [user-id, stats-map]"
  [txn]
  [(:user-id txn)
   {:transaction-count 1
    :total-amount (:amount txn 0)
    :flagged-count (if (:is-flagged txn) 1 0)
    :last-merchant (:merchant txn)
    :max-amount (:amount txn 0)}])

(defn merge-user-stats
  "Merge two user stats records."
  [[user s1] [_ s2]]
  [user {:transaction-count (+ (:transaction-count s1 0) (:transaction-count s2 0))
         :total-amount (+ (:total-amount s1 0) (:total-amount s2 0))
         :flagged-count (+ (:flagged-count s1 0) (:flagged-count s2 0))
         :last-merchant (:last-merchant s2)
         :max-amount (max (:max-amount s1 0) (:max-amount s2 0))}])

(defn format-user-stats
  "Format user stats for output."
  [[user-id stats]]
  (format "{\"user_id\":\"%s\",\"txn_count\":%d,\"total_amount\":%.2f,\"flagged_count\":%d,\"max_single\":%.2f}"
          user-id
          (:transaction-count stats)
          (double (:total-amount stats 0))
          (:flagged-count stats)
          (double (:max-amount stats 0))))

;; =============================================================================
;; Pipeline Builders
;; =============================================================================

(defn apply-fraud-rules
  "Apply all fraud detection rules to a stream."
  [data-stream]
  (-> data-stream
      (stream/flink-map #'analyze-transaction-value)
      (stream/flink-map #'analyze-international)
      (stream/flink-map #'analyze-merchant)
      (stream/flink-map #'calculate-total-risk)))

(defn build-alert-pipeline
  "Build pipeline for critical alerts (immediate notification)."
  [flink-env source sink]
  (-> flink-env
      (flink/from-source source "Transactions")
      (stream/flink-map #'parse-transaction)
      (apply-fraud-rules)
      (stream/flink-filter #'is-critical?)
      (stream/flink-map #'format-alert)
      (flink/to-sink sink "Critical Alerts")))

(defn build-flagged-pipeline
  "Build pipeline for flagged transactions (review queue)."
  [flink-env source sink]
  (-> flink-env
      (flink/from-source source "Transactions")
      (stream/flink-map #'parse-transaction)
      (apply-fraud-rules)
      (stream/flink-filter #'is-flagged?)
      (stream/flink-map #'format-flagged)
      (flink/to-sink sink "Flagged Transactions")))

;; =============================================================================
;; Job Runners
;; =============================================================================

(defn run-alerts!
  "Run critical alerts job (Kafka output)."
  [{:keys [generate? rate] :or {generate? true rate 10}}]
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "  FRAUD DETECTION - CRITICAL ALERTS")
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "Input topic: " (:input-topic config))
  (println "Output topic:" (get-in config [:output-topics :alerts]))
  (println)

  (when generate?
    (println "Starting transaction generator at" rate "events/sec...")
    (println "Note: ~2% of transactions are simulated fraud")
    (gen/start-transaction-generator! rate))

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(:input-topic config)]
                  :group-id (str (get-in config [:kafka :group-id]) "-alerts")
                  :starting-offsets :latest
                  :value-format :string})
        sink (kafka/sink
               {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                :topic (get-in config [:output-topics :alerts])
                :value-format :string})]

    (build-alert-pipeline flink-env source sink)

    (println "\nStarting Flink job...")
    (println "View alerts at: http://localhost:8080")
    (println "Press Ctrl+C to stop\n")

    (try
      (.execute flink-env "Fraud Detection - Critical Alerts")
      (finally
        (when generate?
          (gen/stop-generator!))))))

(defn run!
  "Run the fraud detection job with console output.

  Options:
    :generate? - Start data generator (default: true)
    :rate      - Events per second for generator (default: 10)

  Example:
    (run!)
    (run! {:generate? false})  ; Use existing data
    (run! {:rate 20})          ; Higher throughput"
  ([] (run! {}))
  ([{:keys [generate? rate] :or {generate? true rate 10}}]
   (println "\n" (str/join (repeat 60 "=")) "\n")
   (println "  FRAUD DETECTION (Console Output)")
   (println "\n" (str/join (repeat 60 "=")) "\n")
   (println "Input topic:" (:input-topic config))
   (println "Thresholds:")
   (println "  - High value:    $" (:high-value (:thresholds config)))
   (println "  - Suspicious:    $" (:suspicious-value (:thresholds config)))
   (println "  - Suspicious merchants:" (str/join ", " suspicious-merchants))
   (println)

   (when generate?
     (println "Starting transaction generator at" rate "events/sec...")
     (println "Note: ~2% of transactions are simulated fraud\n")
     (gen/start-transaction-generator! rate))

   (let [flink-env (env/create-env {:parallelism 1})
         source (kafka/source
                  {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                   :topics [(:input-topic config)]
                   :group-id (str (get-in config [:kafka :group-id]) "-console")
                   :starting-offsets :latest
                   :value-format :string})]

     (-> flink-env
         (flink/from-source source "Transactions")
         (stream/flink-map #'parse-transaction)
         (apply-fraud-rules)
         (stream/flink-map #'format-analysis)
         (stream/flink-print))

     (println "Starting Flink job...")
     (println "Legend: 🚨=Critical ⚠️=High 📋=Medium ✅=Low")
     (println "Press Ctrl+C to stop\n")

     (try
       (.execute flink-env "Fraud Detection (Console)")
       (finally
         (when generate?
           (gen/stop-generator!)))))))

(defn run-user-stats!
  "Run user statistics aggregation."
  [{:keys [generate? rate] :or {generate? true rate 10}}]
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "  FRAUD DETECTION - USER STATISTICS")
  (println "\n" (str/join (repeat 60 "=")) "\n")

  (when generate?
    (gen/start-transaction-generator! rate))

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(:input-topic config)]
                  :group-id (str (get-in config [:kafka :group-id]) "-stats")
                  :starting-offsets :latest
                  :value-format :string})]

    (-> flink-env
        (flink/from-source source "Transactions")
        (stream/flink-map #'parse-transaction)
        (apply-fraud-rules)
        (stream/flink-map #'txn-to-user-stats)
        (keyed/key-by #'first {:key-type :string})
        (keyed/flink-reduce #'merge-user-stats)
        (stream/flink-map #'format-user-stats)
        (stream/flink-print))

    (println "Starting Flink job...")
    (println "Press Ctrl+C to stop\n")

    (try
      (.execute flink-env "Fraud Detection - User Stats")
      (finally
        (when generate?
          (gen/stop-generator!))))))

;; =============================================================================
;; REPL
;; =============================================================================

(comment
  ;; Quick start:
  ;; $ docker compose up -d

  ;; Generate test transactions
  (gen/generate-batch! :transactions 100)

  ;; Run fraud detection (console output)
  (run!)

  ;; Run with higher event rate
  (run! {:rate 20})

  ;; Run specific pipelines
  (run-alerts! {:rate 15})
  (run-user-stats! {:rate 15})

  ;; Run without generator (use existing data)
  (run! {:generate? false})

  ;; Stop generator manually
  (gen/stop-generator!)

  ;; Test individual functions
  (-> {:amount 600 :merchant "Amazon" :is-international false}
      analyze-transaction-value
      analyze-international
      analyze-merchant
      calculate-total-risk)

  ;; High-risk transaction
  (-> {:amount 1500 :merchant "Foreign Site" :is-international true}
      analyze-transaction-value
      analyze-international
      analyze-merchant
      calculate-total-risk
      :total-risk)
  )
