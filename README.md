# flink-clj

A Clojure toolkit for Apache Flink.

flink-clj provides an idiomatic Clojure interface to Flink's DataStream and Table APIs, along with tooling for project scaffolding, building, and deployment. The goal is a complete experience for building stream processing applications on Flink using Clojure.

## Status

This library is in early development. The API may change. That said, the project prioritizes correctness and completeness over speed to market. Contributions and feedback are welcome.

## Features

- **DataStream API** - Streaming operators with thread-first composition
- **Table/SQL API** - Tables, SQL queries, stream-table conversions
- **Stateful Processing** - ValueState, ListState, MapState
- **Windowing** - Tumbling, sliding, session, and global windows
- **Connectors** - Kafka, filesystem, JDBC, CDC
- **Interactive Shell** - REPL with pre-configured Flink environments
- **CLI** - Project scaffolding, build, and deployment
- **Ververica Cloud** - Full integration with managed Flink
- **Multi-version** - Flink 1.20 and 2.x

## Installation

```clojure
;; project.clj
[io.github.ugbot/flink-clj "0.1.0-SNAPSHOT"]

;; deps.edn
io.github.ugbot/flink-clj {:mvn/version "0.1.0-SNAPSHOT"}
```

## Quick Start

### CLI

```bash
# Install
curl -sL https://raw.githubusercontent.com/Ugbot/flink-clj/main/install.sh | bash

# Create a project
flink-clj new my-pipeline --template etl

# Start REPL
cd my-pipeline
flink-clj repl --web-ui
```

### REPL

```clojure
(require '[flink-clj.dsl :as f])
(require '[flink-clj.env :as env])

(def env (env/create-local-env {:parallelism 2}))

(defn parse [s] (read-string s))
(defn valid? [e] (some? (:id e)))

(-> (f/source env (f/collection ["{:id 1}" "{:id 2}"]))
    (f/map parse)
    (f/filter valid?)
    (f/print))

(f/run env "Example")
```

### Interactive Shell

```clojure
(start!)                    ; Initialize
(start! {:web-ui true})     ; With Web UI

(-> (f/source env (f/collection [1 2 3]))
    (f/map inc)
    (collect))
;; => [2 3 4]

(stop!)
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `flink-clj new <name>` | Create project from template |
| `flink-clj repl` | Start REPL |
| `flink-clj run` | Run locally |
| `flink-clj build` | Build uberjar |
| `flink-clj deploy` | Deploy to cluster |
| `flink-clj jobs` | Manage jobs |
| `flink-clj ververica` | Ververica Cloud management |

## Ververica Cloud

Deploy and manage Flink jobs on Ververica Cloud:

```bash
# Login
flink-clj ververica login

# List workspaces
flink-clj ververica workspace list

# Set workspace
flink-clj ververica workspace use <id>

# Deploy
flink-clj ververica deployment create --name my-job --jar target/job.jar

# Start job
flink-clj ververica job start --deployment <id>

# Stop with savepoint
flink-clj ververica job stop <job-id> --kind STOP_WITH_SAVEPOINT
```

See [docs/ververica-cloud.md](docs/ververica-cloud.md) for complete documentation.

## Templates

- **etl** - Extract, transform, load
- **analytics** - Real-time aggregations
- **cdc** - Change data capture
- **event-driven** - Pattern matching / CEP

## Requirements

- Java 11+
- Clojure 1.11+
- Flink 1.20 or 2.x

## Development

```bash
lein test-1.20    # Test against Flink 1.20
lein test-2.x     # Test against Flink 2.x
lein shell        # Development REPL
```

## Documentation
 
- [docs/getting-started.md](docs/getting-started.md) - Getting started guide
- [docs/ververica-cloud.md](docs/ververica-cloud.md) - Ververica Cloud integration
- [docs/deployment.md](docs/deployment.md) - Deployment guide

## License

MIT
