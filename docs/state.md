# State Management

Build stateful stream processing applications with fault-tolerant state.

## Why State?

Stateful processing is essential when your computation depends on history:
- Counting events per user
- Detecting patterns across multiple events
- Maintaining running aggregates
- Deduplication
- Sessionization

Flink automatically checkpoints state for fault tolerance.

## State Types

### ValueState

Store a single value per key:

```clojure
(require '[flink-clj.state :as state])
(require '[flink-clj.process :as process])

;; Create descriptor
(def counter-descriptor
  (state/value-state-descriptor "counter" :long))

;; In your process function
(defn count-events [event state-map ctx collector]
  (let [counter (:counter state-map)
        current (or (state/value counter) 0)
        new-count (inc current)]
    (state/update! counter new-count)
    (.collect collector {:key (process/current-key ctx)
                         :count new-count})))
```

### ListState

Store a list of values per key:

```clojure
(def history-descriptor
  (state/list-state-descriptor "history" :string))

(defn track-history [event state-map ctx collector]
  (let [history (:history state-map)]
    ;; Add to history
    (state/add! history (:action event))

    ;; Read all history
    (let [all-actions (state/values history)]
      (.collect collector {:key (process/current-key ctx)
                           :history (vec all-actions)}))))
```

### MapState

Store key-value pairs per key:

```clojure
(def metrics-descriptor
  (state/map-state-descriptor "metrics" :string :long))

(defn track-metrics [event state-map ctx collector]
  (let [metrics (:metrics state-map)
        metric-name (:metric event)
        current (or (state/get-value metrics metric-name) 0)]

    ;; Update specific metric
    (state/put! metrics metric-name (+ current (:value event)))

    ;; Iterate all metrics
    (let [all-metrics (into {} (state/entries metrics))]
      (.collect collector {:key (process/current-key ctx)
                           :metrics all-metrics}))))
```

## Process Functions

Use `keyed-process` to access state:

```clojure
(require '[flink-clj.process :as process])

(defn my-process-fn [event state-map ctx collector]
  ;; state-map contains your state objects
  ;; ctx provides access to timers, key, etc.
  ;; collector is used to emit output
  )

(-> stream
    (keyed/key-by :user-id {:key-type :string})
    (process/keyed-process
      {:state {:counter (state/value-state-descriptor "counter" :long)
               :history (state/list-state-descriptor "history" :map)}
       :process #'my-process-fn}))
```

## State Operations

### ValueState

```clojure
;; Get value (nil if not set)
(state/value my-state)

;; Update value
(state/update! my-state new-value)

;; Clear state
(state/clear! my-state)
```

### ListState

```clojure
;; Add single element
(state/add! my-list element)

;; Add multiple elements
(state/add-all! my-list [e1 e2 e3])

;; Get all elements (lazy sequence)
(state/values my-list)

;; Replace all elements
(state/update! my-list [new-e1 new-e2])

;; Clear list
(state/clear! my-list)
```

### MapState

```clojure
;; Get value for key
(state/get-value my-map "key")

;; Put key-value
(state/put! my-map "key" value)

;; Put multiple entries
(state/put-all! my-map {"k1" v1 "k2" v2})

;; Check if key exists
(state/contains? my-map "key")

;; Remove key
(state/remove! my-map "key")

;; Get all keys
(state/keys my-map)

;; Get all values
(state/values my-map)

;; Get all entries
(state/entries my-map)

;; Check if empty
(state/empty? my-map)

;; Clear map
(state/clear! my-map)
```

## Timers

Schedule callbacks for future processing:

```clojure
(defn process-with-timer [event state-map ctx collector]
  (let [counter (:counter state-map)
        current (or (state/value counter) 0)]
    (state/update! counter (inc current))

    ;; Schedule timer for 1 minute from now
    (process/register-processing-time-timer!
      ctx
      (+ (process/current-processing-time ctx) 60000))))

(defn on-timer [timestamp state-map ctx collector]
  ;; Called when timer fires
  (let [counter (:counter state-map)
        final-count (state/value counter)]
    (.collect collector {:key (process/current-key ctx)
                         :count final-count
                         :fired-at timestamp})
    (state/clear! counter)))

(-> stream
    (keyed/key-by :user-id)
    (process/keyed-process
      {:state {:counter (state/value-state-descriptor "counter" :long)}
       :process #'process-with-timer
       :on-timer #'on-timer}))
```

## Example: Session Tracker

Track user sessions with state and timers:

```clojure
(ns my-app.sessions
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.state :as state]
            [flink-clj.process :as process]))

(def session-timeout-ms (* 30 60 1000)) ; 30 minutes

(defn process-event [event state-map ctx collector]
  (let [session (:session state-map)
        current-session (or (state/value session)
                            {:start-time (process/current-processing-time ctx)
                             :event-count 0
                             :last-event nil})
        updated-session (-> current-session
                            (update :event-count inc)
                            (assoc :last-event event))]

    ;; Update session state
    (state/update! session updated-session)

    ;; Reset timeout timer
    (let [timeout-time (+ (process/current-processing-time ctx)
                          session-timeout-ms)]
      (process/register-processing-time-timer! ctx timeout-time))

    ;; Emit current session info
    (.collect collector {:user-id (process/current-key ctx)
                         :session updated-session})))

(defn on-session-timeout [timestamp state-map ctx collector]
  (let [session (:session state-map)
        final-session (state/value session)]
    (when final-session
      ;; Emit session summary
      (.collect collector {:user-id (process/current-key ctx)
                           :event :session-ended
                           :duration (- timestamp (:start-time final-session))
                           :event-count (:event-count final-session)})
      ;; Clear session
      (state/clear! session))))

(defn -main []
  (-> (env/create-env)
      (flink/register-clojure-types!)
      (flink/from-source kafka-source "User Events")
      (keyed/key-by :user-id {:key-type :string})
      (process/keyed-process
        {:state {:session (state/value-state-descriptor "session" :map)}
         :process #'process-event
         :on-timer #'on-session-timeout})
      (stream/flink-print)
      (flink/execute "Session Tracker")))
```

## Example: Deduplication

Deduplicate events within a time window:

```clojure
(defn deduplicate [event state-map ctx collector]
  (let [seen (:seen state-map)
        event-id (:id event)]
    (when-not (state/contains? seen event-id)
      ;; Mark as seen
      (state/put! seen event-id (process/current-processing-time ctx))

      ;; Schedule cleanup timer (1 hour)
      (process/register-processing-time-timer!
        ctx
        (+ (process/current-processing-time ctx) 3600000))

      ;; Emit deduplicated event
      (.collect collector event))))

(defn cleanup-old [timestamp state-map ctx collector]
  (let [seen (:seen state-map)
        one-hour-ago (- timestamp 3600000)]
    ;; Remove entries older than 1 hour
    (doseq [[event-id seen-time] (state/entries seen)]
      (when (< seen-time one-hour-ago)
        (state/remove! seen event-id)))))

(-> stream
    (keyed/key-by :id {:key-type :string})
    (process/keyed-process
      {:state {:seen (state/map-state-descriptor "seen" :string :long)}
       :process #'deduplicate
       :on-timer #'cleanup-old}))
```

## State Backends

Configure where state is stored:

```clojure
;; In-memory (default, for small state)
(env/create-env {:state-backend :memory})

;; RocksDB (for large state, production use)
(env/create-env {:state-backend :rocksdb
                 :checkpoint {:interval 60000
                              :mode :exactly-once}})
```

## Best Practices

1. **Keep state small**: Only store what you need
2. **Use appropriate state type**: ValueState for single values, MapState for lookups
3. **Clean up state**: Use timers to expire old state
4. **Set TTL**: Configure state TTL for automatic cleanup
5. **Enable checkpointing**: Required for fault tolerance

```clojure
(env/create-env
  {:parallelism 4
   :checkpoint {:interval 60000        ; Every minute
                :mode :exactly-once
                :timeout 600000        ; 10 minute timeout
                :min-pause 30000}})    ; 30 seconds between checkpoints
```
