# Idiomatic Clojure DSL

The `flink-clj.dsl` namespace provides syntactic sugar that makes flink-clj feel more like native Clojure.

## Why Use the DSL?

The core API requires passing vars with `#'` for serialization:

```clojure
;; Core API
(-> stream
    (stream/flink-map #'parse-event)
    (stream/flink-filter #'valid?))
```

The DSL eliminates this requirement and provides a cleaner API:

```clojure
;; DSL
(require '[flink-clj.dsl :as f])

(-> stream
    (f/map parse-event)
    (f/filter valid?))
```

## Getting Started

```clojure
(require '[flink-clj.dsl :as f])

(defn double-it [x] (* x 2))
(defn positive? [x] (pos? x))

(let [env (f/env {:parallelism 4})]
  (-> (f/source env (f/collection [1 2 3 4 5]))
      (f/map double-it)
      (f/filter positive?)
      (f/print)
      (f/run "My Job")))
```

## Stream Operations

All operations accept functions directly (no `#'` needed):

```clojure
;; Transform each element
(f/map stream double-it)

;; Keep elements matching predicate
(f/filter stream valid?)

;; One to many transformation
(f/flat-map stream tokenize)

;; Partition by key
(f/key-by stream :user-id)
(f/key-by stream get-key {:key-type :string})  ; Specify key type

;; Rolling reduce on keyed stream
(f/reduce keyed-stream sum-values)
```

## Pipeline Composition

Compose multiple operations into a single transformation:

```clojure
(def my-transform
  (f/pipeline
    (f/map parse-event)
    (f/filter valid?)
    (f/map enrich)))

(-> stream my-transform)
```

Or define as a named pipeline:

```clojure
(f/defpipeline word-count-transform
  (f/flat-map tokenize)
  (f/key-by first)
  (f/reduce sum-counts))

(-> stream word-count-transform)
```

## Conditional Steps

### when->

Apply operation only when condition is true:

```clojure
(let [should-filter? true]
  (-> stream
      (f/map parse)
      (f/when-> should-filter?
        (f/filter valid?))
      (f/map output)))
```

### if->

Branch based on condition:

```clojure
(let [use-windowing? config]
  (-> stream
      (f/if-> use-windowing?
        (f/windowed (f/tumbling 10 :seconds))
        identity)))
```

### cond->stream

Multiple conditional steps:

```clojure
(-> stream
    (f/cond->stream
      should-filter? (f/filter valid?)
      should-enrich? (f/map enrich)
      debug-mode?    (f/map log-event)))
```

## Windows

Cleaner window specification syntax:

```clojure
;; Tumbling windows
(f/tumbling 10 :seconds)
(f/tumbling 1 :minutes)
(f/tumbling 24 :hours)

;; Sliding windows
(f/sliding 1 :minutes 10 :seconds)  ; 1 minute window, 10 second slide

;; Session windows
(f/session 5 :minutes)  ; 5 minute gap
```

Apply to keyed stream:

```clojure
(-> stream
    (f/key-by :user-id {:key-type :string})
    (f/windowed (f/tumbling 10 :seconds))
    (f/reduce merge-fn))
```

## Sources

### Collection Source

```clojure
(f/source env (f/collection [1 2 3 4 5]))

;; With options
(f/source env (f/collection data {:type :string}))
```

### Kafka Source

```clojure
(f/source env (f/kafka-source
                {:servers "localhost:9092"
                 :topic "events"
                 :group "my-group"
                 :from :latest}))

;; Multiple topics
(f/source env (f/kafka-source
                {:servers "localhost:9092"
                 :topics ["topic1" "topic2"]
                 :group "my-group"}))
```

## Sinks

### Print Sink

```clojure
(-> stream (f/print))
```

### Kafka Sink

```clojure
(-> stream
    (f/map to-string)
    (f/into-kafka {:servers "localhost:9092"
                   :topic "output"}))

;; With delivery guarantee
(f/into-kafka stream {:servers "localhost:9092"
                      :topic "output"
                      :guarantee :exactly-once})
```

## Aggregations

Aggregate with a spec map:

```clojure
(f/aggregate stream
  {:init {}
   :add (fn [acc x] (update acc :count (fnil inc 0)))
   :result identity
   :merge (fn [a b] (merge-with + a b))})
```

## Operator Configuration

### Naming

```clojure
(-> stream
    (f/map transform)
    (f/named "Parse Events"))  ; Shows in Flink UI
```

### Parallelism

```clojure
(-> stream
    (f/map heavy-computation)
    (f/with-parallelism 8))
```

### UID for State Recovery

```clojure
(-> stream
    (f/reduce aggregate-fn)
    (f/with-uid "aggregation-v1"))
```

## Let Bindings for Complex Pipelines

```clojure
(f/let-streams [parsed (f/map raw-stream parse-event)
                valid (f/filter parsed valid?)
                enriched (f/map valid enrich)]
  (f/print enriched))
```

## Debugging with Tap

Apply side effects without changing the stream:

```clojure
(-> stream
    (f/tap #(println "Processing:" %))
    (f/map transform))
```

## Complete Example

```clojure
(ns my-app.pipeline
  (:require [flink-clj.dsl :as f]))

;; Define transformation functions
(defn parse-event [s] (read-string s))
(defn valid? [e] (and (:user-id e) (:value e)))
(defn to-pair [e] [(:user-id e) 1])
(defn sum-pairs [[k v1] [_ v2]] [k (+ v1 v2)])

(defn -main []
  (let [env (f/env {:parallelism 4})]
    (-> (f/source env (f/kafka-source
                        {:servers "localhost:9092"
                         :topic "user-events"
                         :group "counter"}))

        ;; Parse and validate
        (f/map parse-event)
        (f/filter valid?)

        ;; Count per user
        (f/map to-pair)
        (f/key-by first {:key-type :string})
        (f/reduce sum-pairs)

        ;; Output
        (f/into-kafka {:servers "localhost:9092"
                       :topic "user-counts"})

        (f/run "User Event Counter"))))
```

## When to Use Core vs DSL

| Use Case | Recommendation |
|----------|----------------|
| Simple pipelines | DSL |
| Complex state management | Core API |
| Process functions | Core API |
| Quick prototyping | DSL |
| Production jobs | Either works |
| Mixed with Java | Core API |

The DSL wraps the core API, so you can mix them:

```clojure
(require '[flink-clj.dsl :as f]
         '[flink-clj.process :as process])

(-> stream
    (f/map parse-event)
    (f/key-by :user-id {:key-type :string})
    (process/keyed-process {...})  ; Use core API for complex state
    (f/print))
```
