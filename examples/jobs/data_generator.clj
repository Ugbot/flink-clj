(ns data-generator
  "Data generator for flink-clj demo jobs.

  Generates realistic streaming data for various demo scenarios:
  - E-commerce events (clicks, purchases, cart actions)
  - User activity events
  - Sensor/IoT data
  - Log events

  Usage:
    (require '[data-generator :as gen])
    (gen/start-ecommerce-generator! 10)  ; 10 events/sec
    (gen/stop-generator!)"
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.admin AdminClient NewTopic]
           [java.util Properties UUID]
           [java.time Instant]
           [java.util.concurrent Executors TimeUnit ScheduledExecutorService]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def kafka-config
  {:bootstrap-servers "localhost:9092"})

(def topics
  {:ecommerce "ecommerce-events"
   :users "user-activity"
   :sensors "sensor-data"
   :logs "app-logs"
   :transactions "transactions"
   :words "word-stream"})

;; =============================================================================
;; Kafka Helpers
;; =============================================================================

(defn- create-producer []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" (:bootstrap-servers kafka-config))
                (.put "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
                (.put "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"))]
    (KafkaProducer. props)))

(defn- create-admin []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" (:bootstrap-servers kafka-config)))]
    (AdminClient/create props)))

(defn create-topics!
  "Create all demo topics."
  []
  (let [admin (create-admin)
        new-topics (map (fn [[_ topic]]
                          (NewTopic. topic 3 (short 1)))
                        topics)]
    (try
      (.get (.createTopics admin new-topics))
      (println "Created topics:" (str/join ", " (vals topics)))
      (catch Exception e
        (when-not (str/includes? (.getMessage e) "TopicExistsException")
          (println "Note:" (.getMessage e)))))
    (.close admin)))

(defn- send-event! [producer topic key value]
  (let [record (ProducerRecord. topic key (pr-str value))]
    (.send producer record)))

;; =============================================================================
;; Data Generators
;; =============================================================================

;; Sample data pools
(def users ["user-001" "user-002" "user-003" "user-004" "user-005"
            "user-006" "user-007" "user-008" "user-009" "user-010"])

(def products
  [{:id "prod-001" :name "Laptop" :category "Electronics" :price 999.99}
   {:id "prod-002" :name "Headphones" :category "Electronics" :price 149.99}
   {:id "prod-003" :name "Coffee Maker" :category "Home" :price 79.99}
   {:id "prod-004" :name "Running Shoes" :category "Sports" :price 129.99}
   {:id "prod-005" :name "Book: Clojure" :category "Books" :price 49.99}
   {:id "prod-006" :name "Keyboard" :category "Electronics" :price 89.99}
   {:id "prod-007" :name "Desk Lamp" :category "Home" :price 34.99}
   {:id "prod-008" :name "Yoga Mat" :category "Sports" :price 29.99}])

(def cities ["New York" "Los Angeles" "Chicago" "Houston" "Phoenix"
             "San Francisco" "Seattle" "Denver" "Boston" "Austin"])

(def log-levels ["INFO" "WARN" "ERROR" "DEBUG"])
(def services ["api-gateway" "user-service" "order-service" "payment-service" "inventory-service"])

;; E-commerce event generator
(defn generate-ecommerce-event []
  (let [user (rand-nth users)
        product (rand-nth products)
        event-type (rand-nth ["view" "view" "view" "click" "click"
                              "add_to_cart" "remove_from_cart" "purchase"])]
    {:event-id (str (UUID/randomUUID))
     :event-type event-type
     :timestamp (.toString (Instant/now))
     :user-id user
     :product-id (:id product)
     :product-name (:name product)
     :category (:category product)
     :price (:price product)
     :quantity (if (= event-type "purchase") (inc (rand-int 3)) 1)
     :session-id (str "session-" (mod (hash user) 100))
     :device (rand-nth ["mobile" "desktop" "tablet"])
     :city (rand-nth cities)}))

;; User activity generator
(defn generate-user-event []
  (let [user (rand-nth users)]
    {:event-id (str (UUID/randomUUID))
     :timestamp (.toString (Instant/now))
     :user-id user
     :action (rand-nth ["login" "logout" "page_view" "search" "profile_update"
                        "settings_change" "notification_read"])
     :page (rand-nth ["/home" "/products" "/cart" "/checkout" "/account" "/search"])
     :duration-ms (+ 100 (rand-int 5000))
     :ip-address (format "%d.%d.%d.%d"
                         (rand-int 256) (rand-int 256)
                         (rand-int 256) (rand-int 256))
     :user-agent (rand-nth ["Chrome/120" "Firefox/121" "Safari/17" "Edge/120"])}))

;; Sensor data generator
(defn generate-sensor-event []
  (let [sensor-id (format "sensor-%03d" (rand-int 50))]
    {:sensor-id sensor-id
     :timestamp (.toString (Instant/now))
     :temperature (+ 15.0 (* 20.0 (rand)))
     :humidity (+ 30.0 (* 50.0 (rand)))
     :pressure (+ 990.0 (* 40.0 (rand)))
     :battery-level (+ 10 (rand-int 90))
     :location (rand-nth cities)
     :status (if (< (rand) 0.95) "normal" "alert")}))

;; Log event generator
(defn generate-log-event []
  (let [level (rand-nth (concat (repeat 70 "INFO")
                                (repeat 20 "WARN")
                                (repeat 8 "ERROR")
                                (repeat 2 "DEBUG")))]
    {:timestamp (.toString (Instant/now))
     :level level
     :service (rand-nth services)
     :message (case level
                "ERROR" (rand-nth ["Connection timeout" "NullPointerException"
                                   "Database connection failed" "Authentication error"])
                "WARN" (rand-nth ["High latency detected" "Retry attempt"
                                  "Cache miss" "Rate limit approaching"])
                "INFO" (rand-nth ["Request processed" "User authenticated"
                                  "Order created" "Payment received"])
                "DEBUG" (rand-nth ["Entering method" "Variable state"
                                   "Query executed" "Response sent"]))
     :request-id (str (UUID/randomUUID))
     :latency-ms (if (= level "ERROR") (+ 1000 (rand-int 4000)) (+ 10 (rand-int 200)))}))

;; Transaction generator (for fraud detection)
(defn generate-transaction []
  (let [user (rand-nth users)
        is-fraud (< (rand) 0.02)  ; 2% fraud rate
        amount (if is-fraud
                 (+ 500 (* 2000 (rand)))  ; Fraudulent: higher amounts
                 (+ 10 (* 200 (rand))))]   ; Normal: typical amounts
    {:transaction-id (str (UUID/randomUUID))
     :timestamp (.toString (Instant/now))
     :user-id user
     :amount (double (/ (Math/round (* amount 100)) 100))
     :merchant (rand-nth ["Amazon" "Walmart" "Target" "BestBuy" "Costco"
                          "Unknown Merchant" "Foreign Site"])
     :category (rand-nth ["retail" "food" "travel" "entertainment" "utilities"])
     :card-type (rand-nth ["visa" "mastercard" "amex"])
     :location (rand-nth cities)
     :is-international (< (rand) 0.1)
     ;; Hidden label for verification (wouldn't be in real data)
     :_is-fraud is-fraud}))

;; Word stream generator
(defn generate-words []
  (let [sentences ["The quick brown fox jumps over the lazy dog"
                   "Flink processes streaming data in real time"
                   "Clojure is a functional programming language"
                   "Apache Kafka enables event streaming"
                   "Stream processing transforms data continuously"
                   "Window aggregations compute metrics over time"
                   "Stateful processing maintains application state"
                   "Exactly once semantics ensure data consistency"]]
    {:timestamp (.toString (Instant/now))
     :text (rand-nth sentences)
     :source (rand-nth ["twitter" "news" "blog" "forum"])}))

;; =============================================================================
;; Generator Control
;; =============================================================================

(def ^:private generator-state (atom nil))

(defn- start-generator [event-fn topic-key rate-per-sec]
  (let [producer (create-producer)
        scheduler (Executors/newScheduledThreadPool 1)
        topic (get topics topic-key)
        interval-ms (max 1 (/ 1000 rate-per-sec))]
    (println (format "Starting generator: %s events to '%s' at %d/sec"
                     (name topic-key) topic rate-per-sec))
    (.scheduleAtFixedRate
      scheduler
      (fn []
        (try
          (let [event (event-fn)
                key (or (:user-id event) (:sensor-id event) (str (UUID/randomUUID)))]
            (send-event! producer topic key event))
          (catch Exception e
            (println "Generator error:" (.getMessage e)))))
      0 interval-ms TimeUnit/MILLISECONDS)
    {:producer producer
     :scheduler scheduler
     :topic topic}))

(defn start-ecommerce-generator!
  "Start generating e-commerce events.
  rate-per-sec: number of events per second (default: 10)"
  ([] (start-ecommerce-generator! 10))
  ([rate-per-sec]
   (create-topics!)
   (reset! generator-state
           (start-generator generate-ecommerce-event :ecommerce rate-per-sec))))

(defn start-user-generator!
  "Start generating user activity events."
  ([] (start-user-generator! 5))
  ([rate-per-sec]
   (create-topics!)
   (reset! generator-state
           (start-generator generate-user-event :users rate-per-sec))))

(defn start-sensor-generator!
  "Start generating sensor data."
  ([] (start-sensor-generator! 20))
  ([rate-per-sec]
   (create-topics!)
   (reset! generator-state
           (start-generator generate-sensor-event :sensors rate-per-sec))))

(defn start-log-generator!
  "Start generating log events."
  ([] (start-log-generator! 50))
  ([rate-per-sec]
   (create-topics!)
   (reset! generator-state
           (start-generator generate-log-event :logs rate-per-sec))))

(defn start-transaction-generator!
  "Start generating transaction events (for fraud detection)."
  ([] (start-transaction-generator! 10))
  ([rate-per-sec]
   (create-topics!)
   (reset! generator-state
           (start-generator generate-transaction :transactions rate-per-sec))))

(defn start-word-generator!
  "Start generating word stream events."
  ([] (start-word-generator! 5))
  ([rate-per-sec]
   (create-topics!)
   (reset! generator-state
           (start-generator generate-words :words rate-per-sec))))

(defn stop-generator!
  "Stop the current generator."
  []
  (when-let [{:keys [producer scheduler]} @generator-state]
    (.shutdown scheduler)
    (.close producer)
    (reset! generator-state nil)
    (println "Generator stopped")))

(defn generator-running?
  "Check if a generator is currently running."
  []
  (some? @generator-state))

;; =============================================================================
;; Batch Generation (for quick testing)
;; =============================================================================

(defn generate-batch!
  "Generate a batch of events at once.

  event-type: :ecommerce, :users, :sensors, :logs, :transactions, :words
  count: number of events to generate"
  [event-type count]
  (create-topics!)
  (let [producer (create-producer)
        [event-fn topic-key] (case event-type
                               :ecommerce [generate-ecommerce-event :ecommerce]
                               :users [generate-user-event :users]
                               :sensors [generate-sensor-event :sensors]
                               :logs [generate-log-event :logs]
                               :transactions [generate-transaction :transactions]
                               :words [generate-words :words])
        topic (get topics topic-key)]
    (dotimes [_ count]
      (let [event (event-fn)
            key (or (:user-id event) (:sensor-id event) (str (UUID/randomUUID)))]
        (send-event! producer topic key event)))
    (.flush producer)
    (.close producer)
    (println (format "Generated %d %s events to '%s'" count (name event-type) topic))))

;; =============================================================================
;; REPL Helpers
;; =============================================================================

(comment
  ;; Start Docker first:
  ;; $ docker compose up -d

  ;; Create all topics
  (create-topics!)

  ;; Generate batches for testing
  (generate-batch! :ecommerce 100)
  (generate-batch! :transactions 50)
  (generate-batch! :words 200)

  ;; Start continuous generators
  (start-ecommerce-generator! 10)    ; 10 events/sec
  (start-transaction-generator! 5)    ; 5 events/sec
  (start-word-generator! 3)           ; 3 events/sec

  ;; Check status
  (generator-running?)

  ;; Stop generator
  (stop-generator!)

  ;; View in Kafka UI: http://localhost:8080
  )
