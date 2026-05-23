# Evaluation Sandbox Guide

This guide describes how to run and interact with the **optional, self-contained Evaluation Sandbox** bundled with DB Monitor. 

The sandbox allows you to run a complete local Change Data Capture (CDC) pipeline with simulated source databases, Debezium Connect workers, a message broker, and the core audit-log platform.

## Sandbox Architecture

When you spin up the evaluation stack, Docker Compose provisions the following components:

```text
               +-------------------- SANDBOX LAYER --------------------+
               |                                                       |
               |  +------------+   +--------------+   +-------------+  |
               |  |  orderdb   |   |  catalogdb   |   | shippingdb  |  |
               |  | (Postgres) |   |  (Postgres)  |   | (Postgres)  |  |
               |  +-----+------+   +------+-------+   +------+------+  |
               |        |                 |                  |         |
               +--------|-----------------|------------------|---------+
                        v                 v                  v
               +--------|-----------------|------------------|---------+
               |        +---------[ Debezium ]---------------+         |
               |                        |                              |
               |                        v                              |
               |            [ Kafka or RabbitMQ Broker ]               |
               |                        |                              |
               |                        v                              |
               |          +----------------------------+               |
               |          |       monitor-server       |               |
               |          |        (FastAPI Core)      |               |
               |          +--------------+-------------+               |
               |                         |                             |
               |                         v                             |
               |                [ postgres-monitor ]                   |
               |                 (Unified Audit DB)                    |
               |                                                       |
               +------------------ CORE PLATFORM ----------------------+
```

### Components

1. **Transactional Simulators (Sandbox)**:
   - `orderdb` (Postgres, port `5434`): Simulates e-commerce transactions (`public.orders`, `public.customers`).
   - `catalogdb` (Postgres, port `5435`): Simulates inventory catalogues (`public.categories`, `public.products`).
   - `shippingdb` (Postgres, port `5436`): Simulates shipment and fulfillment tracking (`public.shipments`, `public.drivers`).
2. **CDC Ingestion & Transport (Sandbox/Core)**:
   - `connect`: Debezium Connect worker capturing logical replication logs (WAL) from the three source databases.
   - `connector-registrar`: Utility tool that registers Debezium connectors based on the bundled source manifest (`connectors/sources.json`).
   - `kafka` / `zookeeper`: Message transportation backplane.
3. **Core Platform Services (Core)**:
   - `postgres-monitor` (Postgres, port `5437`): Stores normalized event payloads, autodiscovered table schemas, change histories, system checkpoints, and API audit data.
   - `monitor-server` (FastAPI, port `8000`): Runs the ingestion consumer engine, routes CDC payloads, manages system lifecycles, and exposes REST APIs, WebSocket streams, and Prometheus metrics.

---

## Getting Started

### 1. Launch the Sandbox Stack

Run the configuration setup script followed by the compose launcher:

```bash
# Prepare python environment, folders, and env configurations
./scripts/setup.sh

# Spin up all Docker containers (with API admin-key bootstrap enabled)
ALLOW_BOOTSTRAP=true make monitor-up-sandbox
```

### 2. Verify Health and Registrations

Wait approximately 30-45 seconds for Debezium Kafka Connect to fully initialize. You can check the registration logs:

```bash
docker compose logs connector-registrar
```

Verify that the platform endpoint is reachable and reports healthy state:

```bash
curl http://localhost:8000/health
```

---

## Seeding & Simulating Live Traffic

To simulate real-world e-commerce transactions and see DB Monitor capture CDC changes in real-time, you can use the bundled test data generators.

The `examples/sandbox/` directory is the canonical home for these example-only
assets. The legacy wrappers in `scripts/` remain available for backwards
compatibility, but new automation should call the `examples/sandbox/` paths
directly.

### Method A: Single-Pass Test Data Seed

Generate a batch of insert, update, and delete actions across all three simulated source databases:

```bash
# Runs the seed script via uv
uv run python examples/sandbox/generate_test_data.py --count 50
```

### Method B: Continuous Load Injection

Keep a background loop running to inject consistent transacting loads at an adjustable frequency:

```bash
# Injects edits/inserts every 1.5 seconds
uv run python examples/sandbox/load_test.py --workers 8 --requests 10
```

---

## Interacting With the Captures

As traffic is injected into `orderdb`, `catalogdb`, or `shippingdb`, Debezium captures the low-level row events and pushes them to Kafka. The `monitor-server` consumer ingests these events, updates the `postgres-monitor` log, and exposes them through various interfaces:

### 1. The REST API
Fetch the captured and normalized change log directly:

```bash
curl -H "Authorization: Bearer <YOUR_BOOTSTRAP_TOKEN>" http://localhost:8000/events
```

### 2. The WebSocket Stream
Connect a client to the live WebSocket feed to watch changes as they happen:

```bash
# Connect using the bundled TUI Client, wscat, or Python SDK
```

### 3. The Textual Terminal User Interface (TUI)
You can launch the bundled terminal dashboard to browse tables, watch stream health, and explore diff details:

```bash
cd tui
uv run python main.py
```

---

## Resetting and Tearing Down

When your evaluation is complete, you can fully clean up the container resources, volumes, and temporary networks:

```bash
# Stops and destroys all sandbox/core containers and volume storages
make monitor-down-sandbox
```
