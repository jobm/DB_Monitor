# Deployment

This guide covers the supported local and production-like deployment workflows
for DB Monitor.

For the pre-v1 customer isolation decision and rollout model, see
[pre-v1-tenant-isolation-strategy.md](pre-v1-tenant-isolation-strategy.md).

## 1. Choose Your Deployment Model

DB Monitor supports two deployment models depending on your isolation
requirements. The choice depends on whether you are monitoring databases
for a single team or serving multiple customers with strict data boundaries.

### Model A: Per-Startup / Single-Tenant Deployment

**Best for:** A single team, project, or organization that deploys DB Monitor
as a dedicated audit-log service for its own CDC pipeline.

- One DB Monitor instance, one monitor database, one broker namespace.
- Simplest to operate: single Helm release or Docker Compose stack.
- No cross-tenant isolation concerns — all data belongs to one tenant.
- Use this model when you don't need to segregate data by customer.

### Model B: Multi-Tenant with Isolated Customer Cells

**Best for:** A platform team offering DB Monitor as a service to multiple
internal or external customers.

- **Shared control plane:** onboarding API, deployment templates, aggregate
  monitoring, and operator tooling.
- **Isolated customer cells:** one DB Monitor runtime instance per customer,
  each with a dedicated Postgres database (or server), unique broker
  namespace, and per-customer credentials.
- This is *not* a fully shared multi-tenant data plane — each customer
  gets their own isolated deployment.

| Dimension | Single-Tenant | Isolated Cells |
|-----------|--------------|----------------|
| DB Monitor instances | 1 | 1 per customer |
| Postgres | 1 database | 1 database (or server) per customer |
| Broker namespace | 1 topic prefix | 1 topic prefix per customer |
| Secrets | 1 set | 1 set per customer |
| Operational complexity | Low | Medium (automated onboarding) |
| Isolation guarantee | N/A | Strong (dedicated per cell) |

This model is recorded as [ADR-007](adr/ADR-007-deployment-topology.md), which
also covers the blast-radius trade-off and the migration path from Model A.
See [`docs/pre-v1-tenant-isolation-strategy.md`](pre-v1-tenant-isolation-strategy.md)
for the full strategy document covering the recommended cell architecture,
Kubernetes namespace layout, security baseline, onboarding flow, and
operational guardrails.

---

## 2. Local Example Sandbox (Docker Compose)

The easiest way to evaluate DB Monitor locally is to run the bundled Docker Compose sandbox stack, which includes simulated source databases, Debezium configurations, and test data generators.

### Prerequisites

- Docker with Compose support
- `uv` for local helper commands

### Launching the Sandbox

```bash
./scripts/setup.sh
ALLOW_BOOTSTRAP=true make monitor-up-sandbox
```

Use `examples/sandbox/` for example-only generators and validation scripts.
The older top-level demo scripts are compatibility shims for existing tooling.

### Health Validation

```bash
curl http://localhost:8000/readyz
curl http://localhost:8000/health
```

### Bootstrap the First Admin Key

```bash
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

### Stopping the Sandbox

```bash
make monitor-down-sandbox
```

## 2. Local App Development Against Infrastructure

If you are developing or debugging DB Monitor locally, you can start the platform infrastructure in Docker and run the core Python application or consumer service on your local machine.

1. Start the supporting infrastructure core and optional sandbox sources.

```bash
# To start the baseline platform core AND the evaluation sandbox databases:
docker compose --profile sandbox up -d

# To start ONLY the core platform infrastructure (Kafka, Monitor DB, Grafana, etc.):
docker compose up -d
```

2. Run schema migrations locally on the monitor DB.

```bash
make monitor-migrate
```

3. Start the API or consumer service from the app directory.

```bash
cd app
PYTHONPATH=../src:. uv run python -m db_monitor.start_monitor
```

Expected local `.env` values are documented in [configuration.md](configuration.md).

## 3. Production-Like Expectations

### 3a. Kubernetes (Helm Chart)

DB Monitor ships a Helm chart under `deploy/helm/db-monitor/` for production
deployments on Kubernetes.

**Prerequisites:**
- Kubernetes 1.25+
- Helm 3.12+
- A PostgreSQL server (external or provisioned via the Terraform module)
- A Kafka or RabbitMQ cluster

**Install:**

```bash
# From the repo
helm install db-monitor ./deploy/helm/db-monitor \
  --set auth.jwtSecret="$(openssl rand -hex 32)" \
  --set database.host=postgres-monitor.example.com \
  --set database.password="<postgres-password>" \
  --set kafka.broker=kafka.example.com:9092

# With a values override file
helm install db-monitor ./deploy/helm/db-monitor \
  -f my-values.yaml
```

**Post-install:**
```
helm test db-monitor
kubectl port-forward svc/db-monitor-api 8000:8000
curl -X POST "http://localhost:8000/auth/bootstrap?owner_name=admin"
```

**API and consumer workloads:**

The chart runs two Deployments from the same image, matching
[ADR-007](adr/ADR-007-deployment-topology.md):

| Workload | `SERVICE_ROLE` | Responsibilities | Service |
|----------|----------------|------------------|---------|
| `<release>-db-monitor-api` | `api` | HTTP/WebSocket clients, audit-log writer | `<release>-db-monitor-api` |
| `<release>-db-monitor-consumer` | `consumer` | Broker consumers, retention cleanup, partition maintenance | `<release>-db-monitor-consumer` (metrics only) |

Both workloads share the ConfigMap and Secret but have their own replica
count, resources, HPA, PDB, NetworkPolicy, and ServiceMonitor. Route client
traffic to the `api` Service only. Keep the consumer replica count at or below
the topic partition count; extra replicas stay idle. Set `SERVICE_ROLE=all`
only for single-process deployments such as the Compose sandbox.

**Upgrade:**
```bash
helm upgrade db-monitor ./deploy/helm/db-monitor -f my-values.yaml
```

**Minimal production values:**
```yaml
app:
  env: production
  schemaMode: validate
  allowBootstrap: false

database:
  host: postgres-monitor.internal
  password: "<secure-password>"

auth:
  jwtSecret: "<random-64-char-hex>"

kafka:
  broker: kafka.internal:9092
  securityProtocol: SSL

api:
  autoscaling:
    enabled: true

consumer:
  replicaCount: 2

monitoring:
  serviceMonitor:
    enabled: true
```

See `deploy/helm/db-monitor/values.yaml` for all available options.

The Helm chart includes:

- Separate `api` and `consumer` deployments with per-role replica counts,
  resource limits, HPAs, and PDBs
- Configurable PostgreSQL connection (external or bundled)
- Kafka/RabbitMQ broker connection settings
- Secrets management via Kubernetes Secrets (mapped to `*_FILE` env vars)
- Prometheus `ServiceMonitor` / `PodMonitor`
- `NetworkPolicy` templates with deny-all-default posture
- Production security defaults: non-root user, read-only root filesystem,
  `securityContext`, capability drops

The Terraform module and Helm chart are designed to be used together:

```hcl
module "db_monitor_infra" {
  source = "github.com/jobm/DB_Monitor//deploy/terraform"
  ...
}
# → module output supplies Helm values for the chart
```

### 3b. Terraform (Companion Infrastructure)

A Terraform module is available under `deploy/terraform/` that provisions a
production-ready PostgreSQL flexible server for DB Monitor. The module outputs
connection details and Helm values for use with the chart above.

```hcl
module "db_monitor_infra" {
  source = "github.com/jobm/DB_Monitor//deploy/terraform"

  resource_group_name     = azurerm_resource_group.main.name
  location               = azurerm_resource_group.main.location
  postgres_admin_password = var.postgres_admin_password
  postgres_server_name    = "dbm-monitor-prod"
  postgres_sku            = "GP_Standard_D4s_v3"
  postgres_storage_mb     = 262144  # 256 GB
}

# Use the output to configure the Helm chart
output "db_monitor_helm_values" {
  value = module.db_monitor_infra.helm_values
}
```

### 3c. Production-Like Expectations (Non-Kubernetes)

The bundled `monitor-server` service already applies several production
constraints:

- non-root container execution
- read-only root filesystem
- tmpfs for writable scratch space
- capability drop and `no-new-privileges`
- explicit `APP_ENV=production`
- `DB_SCHEMA_MODE=validate`
- secrets loaded from mounted files

Before booting the app in production or production-like environments:

1. Apply migrations explicitly.

```bash
cd app
PYTHONPATH=../src:. uv run python -m db_monitor.cli migrate apply
```

2. Confirm bootstrap is disabled.

```text
ALLOW_BOOTSTRAP=false
```

3. Provide secrets through mounted files where possible.

Use the production `*_FILE` examples in [configuration.md](configuration.md) for the
authoritative variable list and naming.

4. Validate readiness after startup.

```bash
curl http://localhost:8000/readyz
```

5. Enable and validate retention cleanup policy.

Use the retention settings in [configuration.md](configuration.md) to define cleanup
windows and optional archival. For production-like deployments:

- keep `RETENTION_CLEANUP_ENABLED=true`
- set explicit per-table retention windows
- if `RETENTION_ARCHIVE_BEFORE_DELETE=true`, mount
	`RETENTION_ARCHIVE_DIR` to durable storage

6. Enable ingestion quota controls for burst protection.

Use ingestion quota settings from [configuration.md](configuration.md) when you need
per-source or per-tenant protection during traffic spikes:

- keep `INGESTION_QUOTA_ENABLED=true` for bounded ingestion
- use `INGESTION_QUOTA_MODE=throttle` to absorb spikes gradually
- use `INGESTION_QUOTA_MODE=drop` only for strict fail-fast pressure control

7. Configure Kafka topic partition count.

**Important:** Benchmark results show that with 1 partition per topic,
throughput plateaus at ~400-500 ev/s regardless of injection rate.
Production deployments should configure topics with multiple partitions
to enable consumer parallelism.

The recommended partition count depends on your throughput target:

| Target Throughput | Recommended Partitions | Consumer Replicas |
|------------------|----------------------|-------------------|
| < 500 ev/s | 1 | 1-2 |
| 500-2000 ev/s | 6 | 2-4 |
| 2000-5000 ev/s | 12 | 4-6 |
| > 5000 ev/s | 24+ | 6-10 |

**Via Helm chart:**

```yaml
kafka:
  topicPartitionCount: 6
  topicNames:
    - orderdb.public.orders
    - orderdb.public.customers
    - catalogdb.public.products
```

The Helm chart's pre-install Job will create topics with the configured
partition count if they don't exist, or increase partitions on existing
topics. Note that Kafka partition count can only be increased, never
decreased — choose an initial count that allows room for growth.

**Manual (Kafka CLI):**

```bash
# Create a topic with 6 partitions
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my-topic \
  --partitions 6 --replication-factor 1

# Increase partitions on an existing topic (cannot decrease)
kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic my-topic \
  --partitions 6
```

**With the Terraform module:**

The `deploy/terraform/main.tf` has a commented-out `kafka_topic` resource
block. Uncomment it and set `kafka_topic_partitions` variable to match your
target partition count.

**Consumer behavior:**

The consumer auto-assigns all partitions when `KAFKA_TOPIC_PARTITIONS` is
not set. With N partitions and a single consumer, the consumer processes
all N partitions sequentially (not in parallel). To achieve parallel
consumption, either:

- Run multiple consumer replicas (scale `replicaCount` or use HPA)
- Set `KAFKA_CLUSTERS` with multiple consumer instances per partition group

Each replica in the same consumer group will be assigned a subset of the
partitions, allowing parallel processing.

8. Understand partition management and migration safety.

The `events` and `column_changes` tables are automatically converted to
time-range-partitioned tables (monthly partitions) by migration 0013.
Retention cleanup uses partition `DROP` / `DETACH` rather than `DELETE`,
which keeps table bloat under control.

Migration execution uses PostgreSQL advisory locks (`pg_try_advisory_lock`).
Only one pod can run migrations at a time; concurrent attempts log a clear
"lock held by another process" message and exit cleanly. This is validated
by integration tests.

See [configuration.md](configuration.md) for retention settings and
[runbooks/recovery-and-validation.md](runbooks/recovery-and-validation.md) for operational workflows.

## 4. Post-Deploy Validation

Recommended order:

1. `curl http://localhost:8000/readyz`
2. `make monitor-test-integration`
3. `make monitor-test-smoke`
4. inspect `/admin/checkpoints` and `/admin/dlq` when validating ingestion

The detailed recovery flow is in `runbooks/recovery-and-validation.md`.