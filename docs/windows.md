# Windows

Group unbounded streams into bounded chunks for aggregation.

## When to Use Windows

Windows are essential when you need to:
- Compute aggregates over time periods (hourly sales, 5-minute averages)
- Batch events for efficiency (write to database every 1000 records)
- Detect patterns within time bounds (3 failed logins in 5 minutes)

## Window Types

### Tumbling Windows

Fixed-size, non-overlapping windows. Each event belongs to exactly one window.

```
|  Window 1  |  Window 2  |  Window 3  |
|----10s-----|----10s-----|----10s-----|
  ^     ^        ^  ^           ^
  e1    e2       e3 e4          e5
```

```clojure
(require '[flink-clj.window :as w])

;; Time-based tumbling windows
(-> keyed-stream
    (w/tumbling-event-time (w/seconds 10))    ; Event time
    (w/flink-reduce #'merge-fn))

(-> keyed-stream
    (w/tumbling-processing-time (w/minutes 1)) ; Processing time
    (w/flink-reduce #'merge-fn))

;; With offset (align to clock)
(-> keyed-stream
    (w/tumbling-event-time (w/hours 1) (w/minutes 15))  ; :15 past each hour
    (w/flink-reduce #'merge-fn))
```

### Sliding Windows

Fixed-size, overlapping windows. Events can belong to multiple windows.

```
|----Window 1----|
     |----Window 2----|
          |----Window 3----|
|--10s--|--10s--|--10s--|
  ^  ^     ^
  e1 e2    e3
```

```clojure
;; 1-minute windows, sliding every 10 seconds
(-> keyed-stream
    (w/sliding-event-time (w/minutes 1) (w/seconds 10))
    (w/flink-reduce #'merge-fn))

(-> keyed-stream
    (w/sliding-processing-time (w/minutes 5) (w/minutes 1))
    (w/flink-reduce #'merge-fn))
```

### Session Windows

Dynamic windows based on activity gaps. Great for user sessions.

```
|--Session 1--|     |--Session 2--|
 ^  ^  ^      gap>5m   ^     ^
 e1 e2 e3              e4    e5
```

```clojure
;; New session after 5 minutes of inactivity
(-> keyed-stream
    (w/session-event-time (w/minutes 5))
    (w/flink-reduce #'merge-fn))

(-> keyed-stream
    (w/session-processing-time (w/minutes 10))
    (w/flink-reduce #'merge-fn))
```

### Global Windows

All events in one window. Requires a custom trigger to fire.

```clojure
;; Fire every 100 elements
(-> keyed-stream
    (w/global-window)
    (w/trigger (w/count-trigger 100))
    (w/flink-reduce #'merge-fn))
```

## Time Durations

Create duration values:

```clojure
(w/milliseconds 500)
(w/seconds 30)
(w/minutes 5)
(w/hours 1)
(w/days 1)

;; Or use vector syntax
(w/duration [10 :seconds])
(w/duration [5 :minutes])
(w/duration [1 :hours])
```

## Window Functions

### Reduce

Incrementally combine elements:

```clojure
(defn sum-amounts [[key a1] [_ a2]]
  [key (+ a1 a2)])

(-> keyed-stream
    (w/tumbling-processing-time (w/seconds 10))
    (w/flink-reduce #'sum-amounts))
```

### Aggregate

More control with accumulator pattern:

```clojure
(defn create-acc []
  {:count 0 :sum 0})

(defn add-element [acc event]
  (-> acc
      (update :count inc)
      (update :sum + (:value event))))

(defn get-result [acc]
  {:avg (/ (:sum acc) (:count acc))
   :count (:count acc)})

(defn merge-accs [a b]
  {:count (+ (:count a) (:count b))
   :sum (+ (:sum a) (:sum b))})

(-> keyed-stream
    (w/tumbling-processing-time (w/minutes 1))
    (w/aggregate {:create-accumulator #'create-acc
                  :add #'add-element
                  :get-result #'get-result
                  :merge #'merge-accs}))
```

### Simple Aggregations

Built-in aggregations for tuple/POJO fields:

```clojure
;; Sum field at index 1
(-> windowed-stream
    (w/sum 1))

;; Min/max by field
(-> windowed-stream
    (w/flink-min "amount"))

(-> windowed-stream
    (w/flink-max "timestamp"))

;; Get element with min/max
(-> windowed-stream
    (w/min-by "price"))

(-> windowed-stream
    (w/max-by "score"))
```

## Triggers

Control when windows fire:

```clojure
;; Fire after N elements
(-> windowed-stream
    (w/trigger (w/count-trigger 100))
    (w/flink-reduce #'merge-fn))

;; Fire at watermark (default for event time)
(-> windowed-stream
    (w/trigger (w/event-time-trigger))
    (w/flink-reduce #'merge-fn))

;; Fire based on processing time
(-> windowed-stream
    (w/trigger (w/processing-time-trigger))
    (w/flink-reduce #'merge-fn))

;; Purge window contents after firing
(-> windowed-stream
    (w/trigger (w/purging (w/count-trigger 100)))
    (w/flink-reduce #'merge-fn))
```

## Evictors

Remove elements before processing:

```clojure
;; Keep only last N elements
(-> windowed-stream
    (w/evictor (w/count-evictor 100))
    (w/flink-reduce #'merge-fn))

;; Keep elements within time range
(-> windowed-stream
    (w/evictor (w/time-evictor (w/seconds 30)))
    (w/flink-reduce #'merge-fn))
```

## Late Data Handling

Handle events that arrive after the window closes:

```clojure
;; Allow 10 seconds of lateness
(-> keyed-stream
    (w/tumbling-event-time (w/minutes 1))
    (w/allowed-lateness (w/seconds 10))
    (w/flink-reduce #'merge-fn))

;; Send late data to side output
(def late-tag (process/output-tag "late" :map))

(-> keyed-stream
    (w/tumbling-event-time (w/minutes 1))
    (w/allowed-lateness (w/seconds 30))
    (w/side-output-late-data late-tag)
    (w/flink-reduce #'merge-fn))

;; Access late data
(def late-stream (stream/get-side-output main-stream late-tag))
```

## Complete Example: Real-Time Metrics

```clojure
(ns my-app.metrics
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]
            [flink-clj.window :as w]))

(defn event->metric [event]
  [(:service event)
   {:requests 1
    :errors (if (:error event) 1 0)
    :latency-sum (:latency event 0)
    :latency-max (:latency event 0)}])

(defn merge-metrics [[svc m1] [_ m2]]
  [svc {:requests (+ (:requests m1) (:requests m2))
        :errors (+ (:errors m1) (:errors m2))
        :latency-sum (+ (:latency-sum m1) (:latency-sum m2))
        :latency-max (max (:latency-max m1) (:latency-max m2))}])

(defn get-service [[svc _]] svc)

(defn format-metric [[service {:keys [requests errors latency-sum latency-max]}]]
  (format "%s: %d req, %d err (%.1f%%), avg %.1fms, max %dms"
          service
          requests
          errors
          (* 100.0 (/ errors requests))
          (/ latency-sum (double requests))
          latency-max))

(defn -main []
  (-> (env/create-env {:parallelism 4})
      (flink/register-clojure-types!)
      (flink/from-source kafka-source "Requests")

      ;; Transform to metric pairs
      (stream/flink-map #'event->metric)

      ;; Aggregate per service in 10-second windows
      (keyed/key-by #'get-service {:key-type :string})
      (w/tumbling-processing-time (w/seconds 10))
      (w/flink-reduce #'merge-metrics)

      ;; Output
      (stream/flink-map #'format-metric)
      (stream/flink-print)

      (flink/execute "Service Metrics")))
```

Output every 10 seconds:
```
api-gateway: 1523 req, 12 err (0.8%), avg 45.2ms, max 892ms
user-service: 892 req, 3 err (0.3%), avg 23.1ms, max 156ms
order-service: 445 req, 8 err (1.8%), avg 89.4ms, max 2341ms
```
