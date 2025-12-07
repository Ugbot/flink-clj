# Getting Started

Build your first streaming pipeline in 5 minutes.

## Quick Start with CLI

The fastest way to get started:

```bash
# Install the CLI
curl -sL https://raw.githubusercontent.com/Ugbot/flink-clj/main/install.sh | bash

# Create a new project
flink-clj new my-pipeline --template etl

# Start the REPL
cd my-pipeline
flink-clj repl --web-ui
```

In the REPL:

```clojure
(start!)                              ; Initialize Flink

;; Your environment is ready
env                                   ; StreamExecutionEnvironment
table-env                             ; TableEnvironment

;; Try a quick pipeline
(require '[flink-clj.dsl :as f])

(-> (f/source env (f/collection [1 2 3 4 5]))
    (f/map inc)
    (collect))
;; => [2 3 4 5 6]

(stop!)                               ; Cleanup
```

## Manual Installation

Add to your `project.clj`:

```clojure
:dependencies [[io.github.ugbot/flink-clj "0.1.0-SNAPSHOT"]
               [org.apache.flink/flink-streaming-java "1.20.0" :scope "provided"]
               [org.apache.flink/flink-clients "1.20.0" :scope "provided"]]
```

Or `deps.edn`:

```clojure
{:deps {io.github.ugbot/flink-clj {:mvn/version "0.1.0-SNAPSHOT"}
        org.apache.flink/flink-streaming-java {:mvn/version "1.20.0"}
        org.apache.flink/flink-clients {:mvn/version "1.20.0"}}}
```

## Your First Pipeline

### Using the DSL (Recommended)

```clojure
(ns my-app.core
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]))

(defn double-value [x] (* x 2))

(defn -main []
  (let [env (env/create-env {:parallelism 2})]
    (-> (f/source env (f/collection [1 2 3 4 5]))
        (f/map double-value)
        (f/print)
        (f/run "My First Job"))))
```

### Using the Core API

```clojure
(ns my-app.core
  (:require [flink-clj.core :as flink]
            [flink-clj.env :as env]
            [flink-clj.stream :as stream]))

(defn double-value [x] (* x 2))

(defn -main []
  (-> (env/create-env)
      (flink/register-clojure-types!)
      (flink/from-collection [1 2 3 4 5])
      (stream/flink-map #'double-value)
      (stream/flink-print)
      (flink/execute "My First Job")))
```

Run it:

```bash
lein run -m my-app.core
# Output: 2, 4, 6, 8, 10
```

## Key Concepts

### Functions Must Be Top-Level

Flink serializes your job and distributes it to workers. Transformation functions must be defined at the top level:

```clojure
;; CORRECT - top-level defn
(defn transform [x] (* x 2))

;; With DSL - just pass the function
(f/map stream transform)

;; With core API - pass as var
(stream/flink-map stream #'transform)

;; WRONG - anonymous functions won't serialize
(f/map stream #(* % 2))
```

### Thread-First Composition

All functions take the stream as the first argument:

```clojure
(-> stream
    (f/map parse)
    (f/filter valid?)
    (f/key-by :user-id)
    (f/reduce merge-fn))
```

### Register Clojure Types

For the core API, call `register-clojure-types!` once to enable serialization of Clojure data structures:

```clojure
(-> (env/create-env)
    (flink/register-clojure-types!)
    ...)
```

The DSL handles this automatically.

## Word Count Example

```clojure
(ns my-app.word-count
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]
            [clojure.string :as str]))

(defn tokenize [line]
  (for [word (str/split (str/lower-case line) #"\s+")
        :when (not (str/blank? word))]
    [word 1]))

(defn sum-counts [[word c1] [_ c2]]
  [word (+ c1 c2)])

(defn -main []
  (let [env (env/create-env {:parallelism 2})]
    (-> (f/source env (f/collection ["hello world"
                                      "hello flink"
                                      "flink streaming"]))
        (f/flat-map tokenize)
        (f/key-by first {:key-type :string})
        (f/reduce sum-counts)
        (f/print)
        (f/run "Word Count"))))
```

Output:
```
[hello, 2]
[world, 1]
[flink, 2]
[streaming, 1]
```

## Interactive Development

### Using the Shell

```bash
flink-clj repl --web-ui
```

```clojure
(start! {:web-ui true})           ; Web UI at localhost:8081

(require '[flink-clj.dsl :as f])

(defn process [x] (* x 2))

;; Build a pipeline and collect results
(-> (f/source env (f/collection [1 2 3 4 5]))
    (f/map process)
    (collect))
;; => [2 4 6 8 10]

;; Inspect a stream
(def my-stream (f/source env (f/collection [1 2 3])))
(desc my-stream)
;; => {:type "GenericType<Object>" :parallelism 2 :name "Collection Source"}

(stop!)
```

### Shell Commands

| Command | Description |
|---------|-------------|
| `(start!)` | Initialize local Flink environment |
| `(start! {:web-ui true})` | With Web UI at localhost:8081 |
| `(stop!)` | Cleanup and shutdown |
| `(collect stream)` | Execute and collect all results |
| `(take-n stream n)` | Collect first n results |
| `(desc stream)` | Describe stream type and parallelism |
| `(explain env)` | Print execution plan |
| `(help)` | Show all commands |

## Building and Running

### Build an Uberjar

```bash
flink-clj build
# or
lein uberjar
```

### Run Locally

```bash
flink-clj run
# or
java -jar target/my-pipeline-standalone.jar
```

### Deploy to a Cluster

```bash
# Standalone cluster
flink-clj deploy target/my-pipeline.jar --target standalone --host flink-cluster

# Kubernetes
flink-clj deploy target/my-pipeline.jar --target k8s --name my-pipeline
```

## Project Templates

Create projects from templates:

```bash
flink-clj new my-etl --template etl           # Extract, transform, load
flink-clj new my-analytics --template analytics  # Real-time aggregations
flink-clj new my-cdc --template cdc           # Change data capture
flink-clj new my-cep --template event-driven  # Pattern matching / CEP
```

## Next Steps

- [DataStream API](datastream-api.md) - All transformation operators
- [Windows](windows.md) - Time and count-based windowing
- [State Management](state.md) - Stateful stream processing
- [Connectors](connectors.md) - Kafka, files, JDBC, CDC
- [Deployment](deployment.md) - Running in production
- [Ververica Cloud](ververica-cloud.md) - Managed Flink deployment
