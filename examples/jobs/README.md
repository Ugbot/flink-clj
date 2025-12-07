# flink-clj Demo Jobs

Ready-to-run streaming job examples demonstrating flink-clj capabilities.

## Prerequisites

```bash
# Start Docker services (Kafka, PostgreSQL, etc.)
docker compose up -d

# Verify services
docker compose ps
```

## Quick Start

```bash
# Start REPL with Flink 1.20
lein with-profile +flink-1.20,+dev repl
```

```clojure
;; Load the runner
(require '[runner :as runner])

;; Show help
(runner/help)

;; Generate sample data
(runner/demo!)

;; Run a job
(runner/run! :word-count)
```

## Available Jobs

### 1. Word Count (`word_count_job.clj`)

Classic streaming word count demonstrating:
- Kafka source/sink
- FlatMap for tokenization
- KeyBy for grouping
- Reduce for aggregation

```clojure
(require '[jobs.word-count-job :as wc])
(wc/run!)
```

### 2. E-Commerce Analytics (`ecommerce_analytics_job.clj`)

Real-time analytics demonstrating:
- Windowed aggregations (tumbling windows)
- Multiple metrics (revenue, activity, conversions)
- KeyBy for category/user grouping

```clojure
(require '[jobs.ecommerce-analytics-job :as ec])
(ec/run!)                              ; Console output
(ec/run-category-revenue! {:rate 15})  ; Kafka output
(ec/run-user-activity! {:rate 15})     ; User stats
```

### 3. Fraud Detection (`fraud_detection_job.clj`)

Real-time fraud detection demonstrating:
- Rule-based risk scoring
- Multiple fraud indicators
- Alert generation
- Transaction filtering

```clojure
(require '[jobs.fraud-detection-job :as fraud])
(fraud/run!)                    ; Console output with risk levels
(fraud/run-alerts! {:rate 20})  ; Critical alerts to Kafka
(fraud/run-user-stats! {})      ; Per-user statistics
```

## Data Generator (`data_generator.clj`)

Generate realistic test data:

```clojure
(require '[jobs.data-generator :as gen])

;; Batch generation
(gen/generate-batch! :words 100)
(gen/generate-batch! :ecommerce 100)
(gen/generate-batch! :transactions 100)

;; Continuous generation
(gen/start-word-generator! 5)         ; 5 events/sec
(gen/start-ecommerce-generator! 10)   ; 10 events/sec
(gen/start-transaction-generator! 10) ; 10 events/sec

;; Stop generator
(gen/stop-generator!)
```

## Using the Runner

The unified runner provides a convenient interface:

```clojure
(require '[runner :as runner])

;; Information
(runner/help)        ; Full help
(runner/list-jobs)   ; Available jobs
(runner/status)      ; Service status

;; Running jobs
(runner/run! :word-count)
(runner/run! :ecommerce {:rate 20})
(runner/run! :fraud {:generate? false})

;; Manual data control
(runner/generate! :ecommerce 200)
(runner/start-generator! :fraud 15)
(runner/stop!)

;; Quick demo
(runner/demo!)       ; Generate sample data for all jobs
```

## Kafka Topics

| Topic | Job | Content |
|-------|-----|---------|
| `word-stream` | Word Count | Text messages |
| `word-counts` | Word Count | Aggregated counts |
| `ecommerce-events` | E-Commerce | Click, view, purchase events |
| `category-revenue` | E-Commerce | Revenue by category |
| `user-activity` | E-Commerce | User activity stats |
| `transactions` | Fraud | Transaction events |
| `fraud-alerts` | Fraud | Critical fraud alerts |
| `flagged-transactions` | Fraud | Flagged for review |

View all topics at: http://localhost:8080

## Job Options

All jobs accept the following options:

```clojure
{:generate? true   ; Auto-start data generator (default: true)
 :rate 10}         ; Events per second (default varies by job)
```

Examples:
```clojure
;; Use existing data (no generator)
(runner/run! :word-count {:generate? false})

;; High throughput
(runner/run! :fraud {:rate 50})

;; Custom settings
(runner/run! :ecommerce {:generate? true :rate 25})
```

## Architecture

```
Kafka (input) --> Flink Pipeline --> Kafka (output) / Console
                      |
                Data Generator
```

Each job demonstrates:
1. **Source**: Reading from Kafka with `kafka-source`
2. **Processing**: Stream transformations (`map`, `filter`, `flatMap`, `keyBy`, `reduce`)
3. **Windowing**: Time-based aggregations where applicable
4. **Sink**: Writing to Kafka with `kafka-sink` or console with `print`

## Troubleshooting

### Services not starting
```bash
docker compose logs kafka
docker compose down && docker compose up -d
```

### No events appearing
```clojure
;; Check if generator is running
(gen/generator-running?)

;; Start manually
(gen/start-ecommerce-generator! 10)
```

### ClassNotFoundException
Make sure to use the correct profile:
```bash
lein with-profile +flink-1.20,+dev repl
```
