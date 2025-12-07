(ns ecommerce-analytics-job
  "Demo Job 2: E-Commerce Real-Time Analytics

  A real-time analytics pipeline that demonstrates:
  - Reading e-commerce events from Kafka
  - Windowed aggregations (tumbling windows)
  - Multiple output metrics
  - KeyBy for grouping by category/user

  Metrics computed:
  - Revenue per category (5-minute windows)
  - Events per user (1-minute windows)
  - Purchase conversion rates

  Run:
    1. docker compose up -d
    2. lein with-profile +flink-1.20,+dev repl
    3. (require '[ecommerce-analytics-job :as ec])
    4. (ec/run!)"
  (:require [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.window :as w]
            [flink-clj.connectors.kafka :as kafka]
            [clojure.string :as str]
            [data-generator :as gen]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def config
  {:kafka {:bootstrap-servers "localhost:9092"
           :group-id "ecommerce-analytics"}
   :input-topic "ecommerce-events"
   :output-topics {:category-revenue "category-revenue"
                   :user-activity "user-activity"
                   :conversions "conversion-rates"}})

;; =============================================================================
;; Processing Functions (must be top-level for serialization)
;; =============================================================================

(defn parse-event
  "Parse EDN event from Kafka."
  [msg]
  (try
    (read-string msg)
    (catch Exception _
      {:event-type "unknown"
       :category "unknown"
       :user-id "unknown"
       :price 0.0
       :quantity 1})))

;; --- Category Revenue Aggregation ---

(defn event-to-category-revenue
  "Extract category and revenue from event.
   Returns [category, {:revenue amount :count 1}]"
  [event]
  (let [revenue (if (= (:event-type event) "purchase")
                  (* (:price event 0) (:quantity event 1))
                  0.0)]
    [(:category event "unknown")
     {:revenue revenue
      :views (if (= (:event-type event) "view") 1 0)
      :clicks (if (= (:event-type event) "click") 1 0)
      :purchases (if (= (:event-type event) "purchase") 1 0)
      :cart-adds (if (= (:event-type event) "add_to_cart") 1 0)}]))

(defn get-category
  "Extract category key from tuple."
  [[category _]] category)

(defn merge-category-metrics
  "Merge two category metric records."
  [[cat m1] [_ m2]]
  [cat {:revenue (+ (:revenue m1 0) (:revenue m2 0))
        :views (+ (:views m1 0) (:views m2 0))
        :clicks (+ (:clicks m1 0) (:clicks m2 0))
        :purchases (+ (:purchases m1 0) (:purchases m2 0))
        :cart-adds (+ (:cart-adds m1 0) (:cart-adds m2 0))}])

(defn format-category-revenue
  "Format category revenue for output."
  [[category {:keys [revenue views clicks purchases cart-adds]}]]
  (format "{\"category\":\"%s\",\"revenue\":%.2f,\"views\":%d,\"clicks\":%d,\"purchases\":%d,\"cart_adds\":%d,\"conversion_rate\":%.2f}"
          category
          (double revenue)
          views
          clicks
          purchases
          cart-adds
          (if (> views 0)
            (* 100.0 (/ purchases views))
            0.0)))

;; --- User Activity Aggregation ---

(defn event-to-user-activity
  "Extract user and activity from event.
   Returns [user-id, {:events 1 :value value}]"
  [event]
  [(:user-id event "unknown")
   {:events 1
    :total-value (if (= (:event-type event) "purchase")
                   (* (:price event 0) (:quantity event 1))
                   0.0)
    :last-action (:event-type event)
    :device (:device event "unknown")}])

(defn get-user-id
  "Extract user-id key from tuple."
  [[user-id _]] user-id)

(defn merge-user-activity
  "Merge two user activity records."
  [[user m1] [_ m2]]
  [user {:events (+ (:events m1 0) (:events m2 0))
         :total-value (+ (:total-value m1 0) (:total-value m2 0))
         :last-action (:last-action m2)
         :device (:device m2)}])

(defn format-user-activity
  "Format user activity for output."
  [[user-id {:keys [events total-value last-action device]}]]
  (format "{\"user_id\":\"%s\",\"events\":%d,\"total_value\":%.2f,\"last_action\":\"%s\",\"device\":\"%s\"}"
          user-id
          events
          (double total-value)
          last-action
          device))

;; --- Purchase Events Only ---

(defn is-purchase?
  "Filter for purchase events only."
  [event]
  (= (:event-type event) "purchase"))

(defn format-purchase
  "Format purchase event."
  [event]
  (format "{\"user_id\":\"%s\",\"product\":\"%s\",\"amount\":%.2f,\"city\":\"%s\"}"
          (:user-id event)
          (:product-name event)
          (* (:price event 0) (:quantity event 1))
          (:city event "unknown")))

;; =============================================================================
;; Pipeline Builders
;; =============================================================================

(defn build-category-revenue-pipeline
  "Build pipeline for category revenue aggregation with 10-second tumbling windows."
  [flink-env source sink]
  (-> flink-env
      (flink/from-source source "E-Commerce Events")
      (stream/flink-map #'parse-event)
      (stream/flink-map #'event-to-category-revenue)
      (keyed/key-by #'get-category {:key-type :string})
      (w/tumbling-processing-time (w/seconds 10))
      (w/flink-reduce #'merge-category-metrics)
      (stream/flink-map #'format-category-revenue)
      (flink/to-sink sink "Category Revenue Output")))

(defn build-user-activity-pipeline
  "Build pipeline for user activity aggregation with 5-second tumbling windows."
  [flink-env source sink]
  (-> flink-env
      (flink/from-source source "E-Commerce Events (User)")
      (stream/flink-map #'parse-event)
      (stream/flink-map #'event-to-user-activity)
      (keyed/key-by #'get-user-id {:key-type :string})
      (w/tumbling-processing-time (w/seconds 5))
      (w/flink-reduce #'merge-user-activity)
      (stream/flink-map #'format-user-activity)
      (flink/to-sink sink "User Activity Output")))

(defn build-purchases-pipeline
  "Build pipeline that filters and outputs only purchase events."
  [flink-env source sink]
  (-> flink-env
      (flink/from-source source "E-Commerce Events (Purchases)")
      (stream/flink-map #'parse-event)
      (stream/flink-filter #'is-purchase?)
      (stream/flink-map #'format-purchase)
      (flink/to-sink sink "Purchases Output")))

;; =============================================================================
;; Job Runners
;; =============================================================================

(defn run-category-revenue!
  "Run category revenue analytics job."
  [{:keys [generate? rate] :or {generate? true rate 10}}]
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "  CATEGORY REVENUE ANALYTICS")
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "Input topic:  " (:input-topic config))
  (println "Output topic: " (get-in config [:output-topics :category-revenue]))
  (println "Window size:   10 seconds (tumbling)")
  (println)

  (when generate?
    (println "Starting e-commerce data generator at" rate "events/sec...")
    (gen/start-ecommerce-generator! rate))

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(:input-topic config)]
                  :group-id (str (get-in config [:kafka :group-id]) "-category")
                  :starting-offsets :latest
                  :value-format :string})
        sink (kafka/sink
               {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                :topic (get-in config [:output-topics :category-revenue])
                :value-format :string})]

    (build-category-revenue-pipeline flink-env source sink)

    (println "\nStarting Flink job...")
    (println "View Kafka topics at: http://localhost:8080")
    (println "Press Ctrl+C to stop\n")

    (try
      (.execute flink-env "Category Revenue Analytics")
      (finally
        (when generate?
          (gen/stop-generator!))))))

(defn run-user-activity!
  "Run user activity analytics job."
  [{:keys [generate? rate] :or {generate? true rate 10}}]
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "  USER ACTIVITY ANALYTICS")
  (println "\n" (str/join (repeat 60 "=")) "\n")
  (println "Input topic:  " (:input-topic config))
  (println "Output topic: " (get-in config [:output-topics :user-activity]))
  (println "Window size:   5 seconds (tumbling)")
  (println)

  (when generate?
    (println "Starting e-commerce data generator at" rate "events/sec...")
    (gen/start-ecommerce-generator! rate))

  (let [flink-env (env/create-env {:parallelism 1})
        source (kafka/source
                 {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                  :topics [(:input-topic config)]
                  :group-id (str (get-in config [:kafka :group-id]) "-user")
                  :starting-offsets :latest
                  :value-format :string})
        sink (kafka/sink
               {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                :topic (get-in config [:output-topics :user-activity])
                :value-format :string})]

    (build-user-activity-pipeline flink-env source sink)

    (println "\nStarting Flink job...")
    (println "View Kafka topics at: http://localhost:8080")
    (println "Press Ctrl+C to stop\n")

    (try
      (.execute flink-env "User Activity Analytics")
      (finally
        (when generate?
          (gen/stop-generator!))))))

(defn run!
  "Run the full e-commerce analytics job (category revenue with console output).

  Options:
    :generate? - Start data generator (default: true)
    :rate      - Events per second for generator (default: 10)

  Example:
    (run!)
    (run! {:generate? false})  ; Use existing data"
  ([] (run! {}))
  ([{:keys [generate? rate] :or {generate? true rate 10}}]
   (println "\n" (str/join (repeat 60 "=")) "\n")
   (println "  E-COMMERCE ANALYTICS (Console Output)")
   (println "\n" (str/join (repeat 60 "=")) "\n")
   (println "Input topic:" (:input-topic config))
   (println "Aggregating by category with 10-second windows")
   (println)

   (when generate?
     (println "Starting e-commerce data generator at" rate "events/sec...")
     (gen/start-ecommerce-generator! rate))

   (let [flink-env (env/create-env {:parallelism 1})
         source (kafka/source
                  {:bootstrap-servers (get-in config [:kafka :bootstrap-servers])
                   :topics [(:input-topic config)]
                   :group-id (str (get-in config [:kafka :group-id]) "-console")
                   :starting-offsets :latest
                   :value-format :string})]

     (-> flink-env
         (flink/from-source source "E-Commerce Events")
         (stream/flink-map #'parse-event)
         (stream/flink-map #'event-to-category-revenue)
         (keyed/key-by #'get-category {:key-type :string})
         (w/tumbling-processing-time (w/seconds 10))
         (w/flink-reduce #'merge-category-metrics)
         (stream/flink-map #'format-category-revenue)
         (stream/flink-print))

     (println "\nStarting Flink job...")
     (println "View events at: http://localhost:8080")
     (println "Press Ctrl+C to stop\n")

     (try
       (.execute flink-env "E-Commerce Analytics (Console)")
       (finally
         (when generate?
           (gen/stop-generator!)))))))

;; =============================================================================
;; REPL
;; =============================================================================

(comment
  ;; Quick start:
  ;; $ docker compose up -d

  ;; Generate test data batch
  (gen/generate-batch! :ecommerce 100)

  ;; Run analytics (console output)
  (run!)

  ;; Run specific analytics jobs (Kafka output)
  (run-category-revenue! {:rate 15})
  (run-user-activity! {:rate 15})

  ;; Run without generator
  (run! {:generate? false})

  ;; Stop generator manually
  (gen/stop-generator!)
  )
