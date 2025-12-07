# flink-clj Development Setup

This guide explains how to set up a local development environment with Kafka and PostgreSQL for testing flink-clj connectors.

## Prerequisites

- Docker and Docker Compose
- Leiningen
- Java 11+

## Quick Start

```bash
# 1. Start all services
docker compose up -d

# 2. Check services are healthy
docker compose ps

# 3. Start a REPL
lein with-profile +flink-1.20,+dev repl

# 4. Run the demo
(require '[examples.full-pipeline-example :as ex])
(ex/demo!)
```

## Services

The Docker Compose setup includes:

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Message broker |
| Zookeeper | 2181 | Kafka coordination |
| Kafka UI | 8080 | Web UI for Kafka |
| PostgreSQL | 5432 | Database |
| Schema Registry | 8081 | Avro schema management |

### Service URLs

- **Kafka Bootstrap**: `localhost:9092`
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: `jdbc:postgresql://localhost:5432/flink_test`
- **Schema Registry**: http://localhost:8081

### PostgreSQL Credentials

- **User**: `flink`
- **Password**: `flink`
- **Database**: `flink_test`

## JAR Loading (PyFlink-style)

flink-clj supports dynamic JAR loading similar to PyFlink's `add_jars()`:

```clojure
(require '[flink-clj.env :as env])

;; Option 1: Load JARs at environment creation
(def flink-env
  (env/create-env
    {:parallelism 2
     :jars [(env/jar-url "/path/to/flink-connector-kafka.jar")
            (env/jar-url "/path/to/kafka-clients.jar")]}))

;; Option 2: Add JARs dynamically
(-> (env/create-env)
    (env/add-jars! "/path/to/connector1.jar"
                   "/path/to/connector2.jar"))

;; Helper: Generate Maven Central URL
(env/maven-jar "org.apache.flink" "flink-connector-kafka" "3.3.0-1.20")
;; => "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar"
```

## Examples

### Kafka Example

```clojure
(require '[examples.kafka-example :as kafka-ex])

;; Set up topics and test data
(kafka-ex/setup!)

;; Run word count pipeline
(kafka-ex/run-word-count-example)

;; View results in Kafka UI: http://localhost:8080
```

### PostgreSQL Example

```clojure
(require '[examples.postgres-example :as pg-ex])

;; Test connection
(pg-ex/setup!)

;; Query data
(pg-ex/query "SELECT * FROM orders")

;; View word counts
(pg-ex/show-word-counts)
```

### Full Pipeline Example

```clojure
(require '[examples.full-pipeline-example :as ex])

;; Run complete demo
(ex/demo!)

;; Or run individual components:
(ex/check-services)
(ex/create-topics!)
(ex/generate-events! 100)
(ex/run-simple-pipeline)
(ex/run-windowed-pipeline)
```

## Connector JARs

### Kafka Connector (Flink 1.20)

Required JARs:
- `flink-connector-kafka-3.3.0-1.20.jar`
- `kafka-clients-3.6.1.jar`

```clojure
;; Download from Maven Central
(env/maven-jar "org.apache.flink" "flink-connector-kafka" "3.3.0-1.20")
(env/maven-jar "org.apache.kafka" "kafka-clients" "3.6.1")
```

### JDBC Connector

Required JARs:
- `flink-connector-jdbc-3.2.0-1.19.jar`
- `postgresql-42.7.1.jar`

```clojure
(env/maven-jar "org.apache.flink" "flink-connector-jdbc" "3.2.0-1.19")
(env/maven-jar "org.postgresql" "postgresql" "42.7.1")
```

### Downloading JARs Locally

For faster development, download JARs once:

```clojure
(require '[examples.full-pipeline-example :as ex])

;; Download all connector JARs to lib/connectors
(ex/download-jars! "lib/connectors")

;; Use local JARs
(def my-env
  (env/create-env
    {:jars (ex/local-jar-paths "lib/connectors")}))
```

## PostgreSQL Tables

The init script creates these tables:

### `events`
Stores processed events from the pipeline.

```sql
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id VARCHAR(100),
    payload JSONB,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `word_counts`
Stores word count results (for upsert pattern).

```sql
CREATE TABLE word_counts (
    word VARCHAR(255) PRIMARY KEY,
    count BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `orders`
Sample data for CDC testing.

```sql
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50) DEFAULT 'pending'
);
```

## Troubleshooting

### Services not starting

```bash
# Check logs
docker compose logs kafka
docker compose logs postgres

# Restart services
docker compose down
docker compose up -d
```

### Connection refused

Make sure services are healthy:

```bash
docker compose ps
```

All services should show "healthy" status.

### Kafka topics not created

Topics are auto-created on first use. Or create manually:

```bash
docker compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --topic flink-input
```

### PostgreSQL connection issues

Test connection:

```bash
docker compose exec postgres psql -U flink -d flink_test -c "SELECT 1"
```

### ClassNotFoundException for connectors

Make sure connector JARs are loaded:

```clojure
;; Check what JARs are loaded
(env/get-jars my-env)

;; Add missing JARs
(env/add-jars! my-env "/path/to/missing-connector.jar")
```

## Stopping Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes (fresh start)
docker compose down -v
```

## Flink Version Compatibility

| Flink Version | Kafka Connector | JDBC Connector |
|---------------|-----------------|----------------|
| 1.20.x | 3.3.0-1.20 | 3.2.0-1.19 |
| 2.0.x | TBD | TBD |

Note: Flink 2.x connector versions may differ. Check the official Flink documentation for compatible versions.
