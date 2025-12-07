# DataStream API

Transform unbounded streams of data.

## Transformations

### flink-map

Apply a function to each element:

```clojure
(require '[flink-clj.stream :as stream])

(defn parse-json [s]
  (json/read-str s :key-fn keyword))

(defn enrich-event [event]
  (assoc event :processed-at (System/currentTimeMillis)))

;; Basic map
(-> stream
    (stream/flink-map #'parse-json))

;; With type hint for better performance
(-> stream
    (stream/flink-map #'enrich-event {:returns :map}))
```

### flink-filter

Keep elements matching a predicate:

```clojure
(defn valid-event? [event]
  (and (:user-id event)
       (pos? (:amount event))))

(defn high-value? [event]
  (> (:amount event) 1000))

(-> stream
    (stream/flink-filter #'valid-event?)
    (stream/flink-filter #'high-value?))
```

### flat-map

One input to zero or more outputs:

```clojure
(defn tokenize [line]
  (str/split line #"\s+"))

(defn extract-tags [event]
  (for [tag (:tags event)]
    {:tag tag :event-id (:id event)}))

;; Split lines into words
(-> lines-stream
    (stream/flat-map #'tokenize))

;; Expand events by their tags
(-> events-stream
    (stream/flat-map #'extract-tags {:returns :map}))
```

## Keyed Streams

Partition by key for stateful operations:

```clojure
(require '[flink-clj.keyed :as keyed])

;; Key by a field
(-> stream
    (keyed/key-by :user-id))

;; Key by a function
(defn get-region [event]
  (subs (:zip-code event) 0 3))

(-> stream
    (keyed/key-by #'get-region {:key-type :string}))

;; Key by first element (for tuples/vectors)
(-> stream
    (keyed/key-by first))
```

### Rolling Aggregations

Compute running totals on keyed streams:

```clojure
;; Sum a field (for tuples or maps with numeric fields)
(-> keyed-stream
    (keyed/sum 1))        ; Sum field at index 1

;; Rolling min/max
(-> keyed-stream
    (keyed/flink-min "amount"))

(-> keyed-stream
    (keyed/flink-max "timestamp"))

;; Get element with min/max (returns whole element)
(-> keyed-stream
    (keyed/min-by "price"))

(-> keyed-stream
    (keyed/max-by "score"))
```

### Reduce

Custom rolling aggregation:

```clojure
(defn sum-amounts [[user a1] [_ a2]]
  [user (+ a1 a2)])

(defn merge-stats [[key s1] [_ s2]]
  [key {:count (+ (:count s1) (:count s2))
        :total (+ (:total s1) (:total s2))
        :max (max (:max s1) (:max s2))}])

(-> stream
    (keyed/key-by first)
    (keyed/flink-reduce #'sum-amounts))

;; With type hint
(-> stream
    (keyed/key-by first {:key-type :string})
    (keyed/flink-reduce #'merge-stats {:returns [:tuple :string :map]}))
```

## Combining Streams

### union

Merge streams of the same type:

```clojure
(def clicks-stream ...)
(def purchases-stream ...)
(def signups-stream ...)

;; All events in one stream
(-> clicks-stream
    (stream/union purchases-stream signups-stream))
```

## Partitioning

Control how data is distributed across parallel instances:

```clojure
;; Round-robin distribution
(-> stream
    (stream/rebalance))

;; Broadcast to all instances (use carefully!)
(-> stream
    (stream/broadcast))

;; Random distribution
(-> stream
    (stream/flink-shuffle))

;; Rescale (efficient when changing parallelism)
(-> stream
    (stream/rescale))

;; Forward (requires same parallelism)
(-> stream
    (stream/forward))

;; Send everything to one instance (bottleneck!)
(-> stream
    (stream/global))
```

## Operator Configuration

### Naming

Name operators for the Flink UI:

```clojure
(-> stream
    (stream/flink-map #'transform)
    (stream/flink-name "Parse Events"))
```

### Parallelism

Set per-operator parallelism:

```clojure
(-> stream
    (stream/flink-map #'heavy-computation)
    (stream/set-parallelism 8))
```

### Unique IDs

Set UIDs for state recovery (important for production):

```clojure
(-> stream
    (stream/flink-map #'transform)
    (stream/uid "transform-v1"))
```

## Type Information

Specify output types for better performance:

```clojure
;; Primitives
(stream/flink-map stream #'parse-long {:returns :long})
(stream/flink-map stream #'to-string {:returns :string})

;; Collections
(stream/flat-map stream #'tokenize {:returns :string})
(stream/flink-map stream #'to-vector {:returns [:vector :long]})

;; Maps
(stream/flink-map stream #'enrich {:returns :map})

;; Tuples
(stream/flink-map stream #'to-pair {:returns [:tuple :string :long]})
```

## Side Outputs

Emit elements to secondary outputs:

```clojure
(require '[flink-clj.process :as process])

;; Create output tag
(def late-data-tag (process/output-tag "late-data" :string))

;; Get side output stream
(def late-stream (stream/get-side-output main-stream late-data-tag))
```

## Complete Example

```clojure
(ns my-app.analytics
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]
            [flink-clj.keyed :as keyed]))

(defn parse-event [json]
  (json/read-str json :key-fn keyword))

(defn valid? [event]
  (and (:user-id event) (:event-type event)))

(defn event->pair [event]
  [(:event-type event) 1])

(defn sum-counts [[type c1] [_ c2]]
  [type (+ c1 c2)])

(defn format-output [[event-type count]]
  (format "%s: %d" event-type count))

(defn -main []
  (-> (env/create-env {:parallelism 4})
      (flink/register-clojure-types!)
      (flink/from-source kafka-source "Events")

      ;; Parse and validate
      (stream/flink-map #'parse-event)
      (stream/flink-filter #'valid?)

      ;; Transform to pairs
      (stream/flink-map #'event->pair)
      (stream/flink-name "To Pairs")

      ;; Aggregate by event type
      (keyed/key-by first {:key-type :string})
      (keyed/flink-reduce #'sum-counts)
      (stream/uid "event-counter")

      ;; Output
      (stream/flink-map #'format-output)
      (stream/flink-print)

      (flink/execute "Event Analytics")))
```
