# Ververica Cloud Integration

flink-clj provides full CLI integration with [Ververica Cloud Platform](https://www.ververica.com/), a managed Apache Flink service.

## Quick Start

```bash
# Login to Ververica Cloud
flink-clj ververica login

# List your workspaces
flink-clj ververica workspace list

# Set default workspace
flink-clj ververica workspace use <workspace-id>

# Deploy a job
flink-clj ververica deployment create --name my-job --jar target/my-job.jar

# Start the job
flink-clj ververica job start --deployment <deployment-id>
```

## Authentication

### Login

```bash
# Interactive login (prompts for credentials)
flink-clj ververica login

# Non-interactive login
flink-clj ververica login --username user@example.com --password 'secret'
```

Credentials are stored in `~/.flink-clj/ververica.json`.

### Session Management

```bash
# Show current user
flink-clj ververica whoami

# Logout
flink-clj ververica logout
```

## Workspace Management

### List Workspaces

```bash
# List all workspaces
flink-clj ververica workspace list

# Filter by status
flink-clj ververica workspace list --status OK

# JSON output
flink-clj ververica workspace list --json
```

### Set Default Workspace

```bash
flink-clj ververica workspace use q7760t0ym1d7xyso
```

All subsequent commands will use this workspace by default.

### Show Workspace Details

```bash
flink-clj ververica workspace info
flink-clj ververica workspace info <workspace-id>
```

## Deployment Management

### List Deployments

```bash
flink-clj ververica deployment list
flink-clj ververica deployment list --namespace default --json
```

### Create Deployment

```bash
# From local JAR (uploads automatically)
flink-clj ververica deployment create \
  --name my-job \
  --jar target/my-job.jar \
  --entry-class com.example.MyJob \
  --parallelism 4

# From remote JAR
flink-clj ververica deployment create \
  --name my-job \
  --jar s3://bucket/path/to/job.jar \
  --entry-class com.example.MyJob

# Deploy to session cluster
flink-clj ververica deployment create \
  --name my-job \
  --jar target/my-job.jar \
  --session-cluster my-session-cluster
```

### Describe Deployment

```bash
flink-clj ververica deployment describe <deployment-id>
flink-clj ververica deployment describe <deployment-id> --json
```

### Delete Deployment

```bash
flink-clj ververica deployment delete <deployment-id>
```

## Job Management

### List Jobs

```bash
flink-clj ververica job list
flink-clj ververica job list --json
```

### Start Job

```bash
# Start fresh (no state)
flink-clj ververica job start --deployment <deployment-id>

# Restore from latest savepoint
flink-clj ververica job start \
  --deployment <deployment-id> \
  --restore LATEST_SAVEPOINT

# Restore from specific savepoint
flink-clj ververica job start \
  --deployment <deployment-id> \
  --restore FROM_SAVEPOINT \
  --savepoint <savepoint-id>
```

Restore strategies:
- `NONE` - Start fresh without state
- `LATEST_SAVEPOINT` - Restore from most recent savepoint
- `LATEST_STATE` - Restore from most recent state
- `FROM_SAVEPOINT` - Restore from specific savepoint (requires `--savepoint`)

### Stop Job

```bash
# Cancel immediately
flink-clj ververica job stop <job-id>

# Stop with savepoint
flink-clj ververica job stop <job-id> --kind STOP_WITH_SAVEPOINT

# Suspend
flink-clj ververica job stop <job-id> --kind SUSPEND
```

### Job Status

```bash
flink-clj ververica job status <job-id>
```

## Savepoint Management

### List Savepoints

```bash
flink-clj ververica savepoint list <deployment-id>
```

### Create Savepoint

```bash
flink-clj ververica savepoint create <deployment-id>
```

## Artifact Management

### List Artifacts

```bash
flink-clj ververica artifact list
```

### Upload Artifact

```bash
flink-clj ververica artifact upload target/my-job.jar
```

## Command Aliases

For convenience, several command aliases are available:

| Full Command | Alias |
|--------------|-------|
| `ververica` | `vvc` |
| `workspace` | `ws` |
| `deployment list` | `deployment ls` |
| `deployment delete` | `deployment rm` |
| `deployment describe` | `deployment show` |
| `job list` | `job ls` |
| `savepoint` | `sp` |
| `artifact list` | `artifact ls` |

Example:
```bash
flink-clj vvc ws list
flink-clj vvc deploy ls
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VERVERICA_API_URL` | Override API base URL (default: `https://app.ververica.cloud`) |

## Configuration File

The CLI stores configuration in `~/.flink-clj/ververica.json`:

```json
{
  "access-token": "...",
  "refresh-token": "...",
  "user-id": "...",
  "workspace": "q7760t0ym1d7xyso"
}
```

## Programmatic Access

You can also use the Ververica client directly from Clojure:

```clojure
(require '[flink-clj.ververica.client :as vvc])

;; Login
(vvc/login {:username "user@example.com" :password "secret"})

;; List workspaces
(vvc/list-workspaces)

;; Create deployment
(vvc/create-deployment "workspace-id"
  {:name "my-job"
   :jar-uri "s3://bucket/job.jar"
   :entry-class "com.example.Main"
   :parallelism 4})

;; Start job
(vvc/start-job "workspace-id"
  {:deployment-id "dep-001"
   :restore-strategy "LATEST_SAVEPOINT"})

;; Stop job
(vvc/stop-job "workspace-id" "job-001" :kind "STOP_WITH_SAVEPOINT")
```

## Complete Workflow Example

```bash
# 1. Login
flink-clj ververica login

# 2. Select workspace
flink-clj ververica workspace list
flink-clj ververica workspace use q7760t0ym1d7xyso

# 3. Build your job
flink-clj build

# 4. Create deployment
flink-clj ververica deployment create \
  --name my-flink-job \
  --jar target/my-project-standalone.jar \
  --entry-class my-project.core \
  --parallelism 2

# 5. Start the job
flink-clj ververica job start --deployment <deployment-id>

# 6. Monitor
flink-clj ververica job list
flink-clj ververica job status <job-id>

# 7. Create savepoint before updates
flink-clj ververica savepoint create <deployment-id>

# 8. Stop with savepoint
flink-clj ververica job stop <job-id> --kind STOP_WITH_SAVEPOINT

# 9. Update deployment and restart
flink-clj ververica job start \
  --deployment <deployment-id> \
  --restore LATEST_SAVEPOINT
```
