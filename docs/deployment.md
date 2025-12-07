# Deployment

Run your Flink jobs in production.

## Building

### Using the CLI

```bash
# Build uberjar for Flink 1.20
flink-clj build

# Build for Flink 2.x
flink-clj build --flink-version 2.x
```

### Using Leiningen

```clojure
;; project.clj
(defproject my-flink-job "1.0.0"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [io.github.ugbot/flink-clj "0.1.0-SNAPSHOT"]]

  :profiles {:provided {:dependencies [[org.apache.flink/flink-streaming-java "1.20.0"]
                                       [org.apache.flink/flink-clients "1.20.0"]]}}

  :main my-app.core
  :aot [my-app.core]
  :uberjar-name "my-flink-job.jar")
```

```bash
lein uberjar
```

## Deployment Targets

### Ververica Cloud (Recommended)

Deploy to [Ververica Cloud](https://www.ververica.com/), a fully managed Apache Flink service:

```bash
# Login
flink-clj ververica login

# Select workspace
flink-clj ververica workspace list
flink-clj ververica workspace use <workspace-id>

# Deploy (uploads JAR automatically)
flink-clj ververica deployment create \
  --name my-pipeline \
  --jar target/my-job.jar \
  --entry-class my_app.job \
  --parallelism 4

# Start the job
flink-clj ververica job start --deployment <deployment-id>

# Monitor
flink-clj ververica job list
flink-clj ververica job status <job-id>

# Stop with savepoint
flink-clj ververica job stop <job-id> --kind STOP_WITH_SAVEPOINT

# Restart from savepoint
flink-clj ververica job start \
  --deployment <deployment-id> \
  --restore LATEST_SAVEPOINT
```

See [Ververica Cloud](ververica-cloud.md) for complete documentation.

### Local

Run in an embedded Flink cluster:

```bash
flink-clj deploy target/my-job.jar --target local
```

### Standalone Cluster

Deploy to a running Flink cluster via REST API:

```bash
flink-clj deploy target/my-job.jar --target standalone --host flink-cluster --port 8081
```

This uploads the JAR and submits the job automatically.

### Kubernetes

Deploy using the Flink Kubernetes Operator:

```bash
flink-clj deploy target/my-job.jar --target k8s --name my-pipeline --namespace production
```

This generates a FlinkDeployment manifest and applies it to your cluster.

## Job Management

### List Jobs

```bash
flink-clj jobs --host flink-cluster
flink-clj jobs --status running
```

### Job Details

```bash
flink-clj jobs info <job-id> --host flink-cluster
```

### Cancel Jobs

```bash
flink-clj jobs cancel <job-id> --host flink-cluster
```

## Job Structure

Organize your job for production:

```
my-flink-job/
├── src/
│   └── my_app/
│       ├── job.clj           # Main entry point
│       ├── transforms.clj    # Transformation functions
│       └── config.clj        # Configuration
├── resources/
│   └── config.edn            # Environment configs
├── test/
│   └── my_app/
│       └── job_test.clj
└── project.clj
```

### Entry Point

```clojure
(ns my-app.job
  (:require [flink-clj.dsl :as f]
            [flink-clj.env :as env]
            [my-app.transforms :as t]
            [my-app.config :as config])
  (:gen-class))

(defn build-pipeline [env cfg]
  (-> (f/source env (f/kafka {:servers (:kafka-servers cfg)
                               :topic (:input-topic cfg)
                               :group-id (:group-id cfg)}))
      (f/map t/parse-event)
      (f/filter t/valid?)
      (f/key-by :user-id)
      (f/reduce t/aggregate)
      (f/sink (f/kafka {:servers (:kafka-servers cfg)
                        :topic (:output-topic cfg)}))))

(defn -main [& args]
  (let [cfg (config/load-config args)
        env (env/create-env {:parallelism (:parallelism cfg 4)
                              :checkpoint {:interval 60000
                                           :mode :exactly-once}})]
    (build-pipeline env cfg)
    (f/run env (:job-name cfg "My Job"))))
```

### Configuration

```clojure
(ns my-app.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn load-config [args]
  (let [env (or (first args) "dev")
        file (str "config/" env ".edn")]
    (-> (io/resource file)
        slurp
        edn/read-string)))
```

```edn
;; resources/config/prod.edn
{:job-name "Production Pipeline"
 :parallelism 16
 :kafka-servers "kafka-prod:9092"
 :input-topic "events"
 :output-topic "processed"
 :group-id "prod-consumer"}
```

## Docker

```dockerfile
FROM flink:1.20-java11

COPY target/my-flink-job.jar /opt/flink/usrlib/

ENV FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  jobmanager:
    image: my-flink-job:latest
    command: jobmanager
    ports:
      - "8081:8081"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: my-flink-job:latest
    command: taskmanager
    depends_on:
      - jobmanager
    scale: 2
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
```

## Kubernetes

### Using the CLI

```bash
flink-clj deploy target/my-job.jar \
  --target k8s \
  --name my-pipeline \
  --namespace production \
  --parallelism 8
```

### Manual FlinkDeployment

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: my-flink-job
  namespace: production
spec:
  image: my-flink-job:latest
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend: rocksdb
    state.checkpoints.dir: s3://my-bucket/checkpoints
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    replicas: 4
    resource:
      memory: "4096m"
      cpu: 2
  job:
    jarURI: local:///opt/flink/usrlib/my-flink-job.jar
    entryClass: my_app.job
    args: ["prod"]
    parallelism: 16
    upgradeMode: savepoint
```

## Configuration

### Environment Settings

```clojure
(env/create-env
  {:parallelism 16

   :checkpoint {:interval 60000
                :mode :exactly-once
                :timeout 600000
                :min-pause 30000
                :max-concurrent 1}

   :state-backend :rocksdb

   :restart {:strategy :fixed-delay
             :attempts 3
             :delay 10000}})
```

### Operator UIDs

Set UIDs for stateful operators to enable savepoint recovery:

```clojure
(-> stream
    (f/key-by :user-id)
    (f/uid "user-key-by")
    (f/reduce aggregate-fn)
    (f/uid "user-aggregation-v1"))
```

### Parallelism

```clojure
;; Global parallelism
(env/create-env {:parallelism 16})

;; Per-operator parallelism
(-> stream
    (f/map light-transform)
    (f/parallelism 32)
    (f/map format-output)
    (f/parallelism 4))
```

## Monitoring

### Metrics

Configure Prometheus metrics in `flink-conf.yaml`:

```yaml
metrics.reporters: prometheus
metrics.reporter.prometheus.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prometheus.port: 9249
```

### Key Metrics

- `numRecordsIn/Out` - Throughput
- `currentInputWatermark` - Event time progress
- `lastCheckpointDuration` - Checkpoint health
- `numRestarts` - Job stability

## Savepoints

### Create Savepoint

```bash
flink savepoint <job-id> s3://my-bucket/savepoints/
```

### Restore from Savepoint

```bash
flink-clj deploy target/my-job.jar \
  --target standalone \
  --host flink-cluster \
  --savepoint s3://bucket/savepoints/savepoint-abc123
```

Or with the Flink CLI:

```bash
flink run -s s3://bucket/savepoints/savepoint-abc123 \
  -c my_app.job target/my-flink-job.jar prod
```

## Troubleshooting

### Out of Memory

- Increase TaskManager memory
- Reduce parallelism
- Enable incremental checkpoints for RocksDB
- Check for state growth

### Backpressure

- Check operator processing times in Web UI
- Increase parallelism for slow operators
- Add buffering with async I/O

### Checkpoint Failures

- Increase checkpoint timeout
- Check state size
- Verify checkpoint storage connectivity
- Consider incremental checkpoints
