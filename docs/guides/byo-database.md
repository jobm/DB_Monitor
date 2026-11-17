# Bring Your Own Database — Onboarding Guide

This guide walks you through connecting **your own PostgreSQL database** to
DB Monitor — from setting up the server to seeing your first CDC event in the
API. No demo databases required.

---

## Prerequisites

- A running PostgreSQL database you want to monitor (version 14+ recommended)
- Docker and Docker Compose (or Podman Compose)
- Access to configure WAL-level replication on your source database

## 1. Deploy Core Platform

Start the minimal platform — just the monitor database, Kafka, and the app:

```bash
# Clone the repo
git clone https://github.com/jobm/DB_Monitor.git
cd DB_Monitor

# Create required secrets
cp secrets/monitor_postgres_url.example secrets/monitor_postgres_url
cp secrets/monitor_jwt_secret.example secrets/monitor_jwt_secret
cp secrets/monitor_jwt_secret_next.example secrets/monitor_jwt_secret_next

# Start core services (no demo databases)
make monitor-up
```

This starts:
- **Postgres** (monitor database) on port 5437
- **Kafka** on port 9093
- **FastAPI app** on port 8000
- **Prometheus + Grafana + Alertmanager** for monitoring

Wait about 30 seconds for migrations to apply, then check health:

```bash
curl http://localhost:8000/health
```

Expected response: `{"status":"ok"}`

> **Note:** `make monitor-up` uses the `docker-compose.yml` which does NOT
> include the sandbox databases (`postgres-order`, `postgres-catalog`,
> `postgres-shipping`) or Debezium Connect. Those are behind the `sandbox`
> profile.

## 2. Configure Your Source Database

### 2a. Enable Logical Replication

Your source PostgreSQL database must have logical replication enabled. Connect
to it and run:

```sql
-- Check if wal_level is already logical
SHOW wal_level;

-- If it's not 'logical', update postgresql.conf:
-- wal_level = logical
-- Then restart PostgreSQL.
```

### 2b. Create a Replication Slot (Optional — Debezium manages this)

Debezium will create the replication slot automatically when the connector
registers. You only need to ensure the connecting user has the `REPLICATION`
attribute and `CREATE` on the database.

```sql
CREATE USER db_monitor_user WITH REPLICATION PASSWORD 'your-strong-password';
GRANT CONNECT ON DATABASE your_db TO db_monitor_user;
GRANT USAGE ON SCHEMA public TO db_monitor_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_monitor_user;
```

## 3. Create Your Source Manifest

Copy the template and edit it with your database details:

```bash
cp connectors/sources.template.json connectors/my-sources.json
```

Edit `connectors/my-sources.json`:

```json
{
  "connectors": [
    {
      "source_name": "inventory_db",
      "database_hostname": "host.docker.internal",
      "tables": [
        "public.orders",
        "public.products"
      ],
      "enabled": true,
      "config_overrides": {
        "database.user": "db_monitor_user",
        "database.password": "your-strong-password",
        "database.dbname": "your_db",
        "database.port": "5432",
        "database.server.name": "inventory_db",
        "publication.autocreate.mode": "filtered",
        "plugin.name": "pgoutput",
        "slot.name": "db_monitor_inventory_slot"
      }
    }
  ]
}
```

> **Tip:** The `database_hostname` should be the address of your source
> database as visible from within the Docker network. Use `host.docker.internal`
> for databases running on the host machine, or the container's hostname if
> running in your compose network.

### Required Fields Reference

| Field | Value | Description |
|-------|-------|-------------|
| `source_name` | Unique string | Used as Kafka topic prefix and source identifier |
| `database_hostname` | Hostname/IP | Source DB address from Docker network |
| `tables` | Array of `schema.table` | Which tables to capture CDC events from |
| `config_overrides.database.user` | String | Source DB login user |
| `config_overrides.database.password` | String | Source DB login password |
| `config_overrides.database.dbname` | String | Source DB name |
| `config_overrides.database.port` | Port | Source DB port (default 5432) |
| `config_overrides.database.server.name` | String | Debezium server name — becomes Kafka topic prefix |
| `config_overrides.plugin.name` | `pgoutput` | PostgreSQL logical decoding plugin |

For the full contract, see the [JSON Schema](https://github.com/jobm/DB_Monitor/blob/main/connectors/sources.schema.json).

## 4. Start Debezium Connect and Register

Now start Debezium Connect with your manifest:

```bash
# Copy your manifest into place for the connector registrar
cp connectors/my-sources.json connectors/sources.json

# Debezium Connect needs the source DB hostname to be resolvable.
# If running on the host, create a docker-compose override:
cat > docker-compose.override.yml << 'EOF'
services:
  connect:
    extra_hosts:
      - "host.docker.internal:host-gateway"
EOF

# Start Debezium Connect and the connector registrar
docker compose --profile sandbox up -d connect connector-registrar
```

The `connector-registrar` will automatically register your Debezium connectors
based on the manifest. Check registration status:

```bash
# List registered connectors
curl http://localhost:8083/connectors

# Check a specific connector's status
curl http://localhost:8083/connectors/inventory_db-connector/status
```

Expected response includes `"state": "RUNNING"`.

## 5. Verify Ingestion

Once the connector is running, Debezium streams change events into Kafka.
DB Monitor's consumer automatically picks them up. Check that events are
flowing:

```bash
# Get the latest events (requires an API key — bootstrap one first)
curl -X POST http://localhost:8000/admin/bootstrap \
  -H "Content-Type: application/json" \
  -d '{"description":"dev key"}'

# Use the returned admin key to fetch events
curl http://localhost:8000/events?limit=5 \
  -H "X-API-Key: <your-admin-key>"
```

Expected response:

```json
{
  "events": [
    {
      "id": "uuid-here",
      "source": "inventory_db",
      "table": "public.orders",
      "event_type": "c",
      "payload": { ... }
    }
  ],
  "total": 42
}
```

## 6. Connect with the SDK

### Python

```bash
pip install db-monitor-sdk
```

```python
from db_monitor_sdk import DBMonitorClient

client = DBMonitorClient(base_url="http://localhost:8000", api_key="your-key")

# List monitored tables
tables = client.list_tables("inventory_db")
print(tables)

# Fetch recent events
events = client.list_events(limit=10)
for event in events["events"]:
    print(f"[{event['event_type']}] {event['table']} — {event['event_time']}")
```

### JavaScript

```javascript
import { DBMonitorClient } from "db-monitor-client";

const client = new DBMonitorClient({
  baseUrl: "http://localhost:8000",
  apiKey: "your-key",
});

const tables = await client.listTables("inventory_db");
console.log(tables);

const events = await client.listEvents({ limit: 10 });
console.log(events);
```

## 7. Next Steps

- **Monitoring:** Open Grafana at [http://localhost:3000](http://localhost:3000)
  (admin/admin) to see ingestion metrics and dashboards.
- **Alerts:** Configure alert thresholds in
  `monitoring/prometheus/alerts.yml`.
- **Retention:** Set event retention policies via environment variables (see
  [Configuration Reference](../configuration.md#retention-and-archival-settings)).
- **Horizontal Scaling:** Run multiple app replicas sharing the same Kafka
  consumer group and monitor database (see
  `docs/runbooks/recovery-and-validation.md`).

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| No events appearing | Connector not running | Check `http://localhost:8083/connectors/inventory_db-connector/status` |
| Connector fails to start | Source DB not reachable | Ensure `database_hostname` is correct from Docker network |
| "no pg_hba.conf entry" | Replication not allowed | Add `host replication all 0.0.0.0/0 md5` to `pg_hba.conf` |
| Consumer not connecting | Kafka topic mismatch | Verify `source_name` in manifest matches Kafka topic prefix |
| App healthcheck failing | Monitor DB not migrated | Check `docker compose logs monitor-server` for migration errors |

## Reference

- [Full Configuration Reference](../configuration.md)
- [Architecture Overview](../architecture.md)
- [Deployment Guide](../deployment.md)
- [Source Manifest Schema](https://github.com/jobm/DB_Monitor/blob/main/connectors/sources.schema.json)
- [API Reference](../api-reference.html)
