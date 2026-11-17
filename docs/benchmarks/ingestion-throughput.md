# Ingestion Throughput Benchmark

This document describes how to benchmark DB Monitor ingestion throughput
and record results for release notes.

## Methodology

The benchmark measures end-to-end ingestion throughput by publishing
synthetic CDC events to Kafka and tracking how fast the consumer
processes them.

### Prerequisites

- Running DB Monitor stack (Kafka, Postgres, monitor-server)
- `aiokafka` installed (`pip install aiokafka`)
- Admin API key for the monitor-server

### Running

```bash
# 100 events/sec for 60 seconds (baseline)
python scripts/scale_test.py \
  --kafka-broker localhost:9093 \
  --rate 100 --duration 60 --json

# 500 events/sec for 300 seconds (target throughput)
python scripts/scale_test.py \
  --kafka-broker localhost:9093 \
  --rate 500 --duration 300 --json

# 1000 events/sec for 300 seconds (high throughput)
python scripts/scale_test.py \
  --kafka-broker localhost:9093 \
  --rate 1000 --duration 300 --json

# 5000 events/sec for 300 seconds (stress test)
python scripts/scale_test.py \
  --kafka-broker localhost:9093 \
  --rate 5000 --duration 300 --json
```

Or via Make:

```bash
make monitor-test-scale-target  # 500 ev/s, 300s, 10 sources
```

## Results (BM-2 — Baseline Matrix)

### Environment
- **Container host:** Local Docker Desktop (Linux)
- **CPU:** Host CPU (no CPU limits set on containers)
- **Memory:** No memory limits set on containers
- **Postgres:** Official `postgres:15` container
- **Kafka:** Confluent `cp-kafka:7.5.0` (single node, 1 partition per topic)
- **Deployment:** Single `monitor-server` container
- **Sources:** 10 simulated source databases
- **Date:** 2026-07-05

### Throughput

| Target Rate (ev/s) | Actual Throughput | p50 API (ms) | p95 API (ms) | p99 API (ms) | DLQ Events | Pass? |
|---|---|---|---|---|---|---|
| 100 | 98.5 ev/s | 2.10 | 3.98 | 5.21 | 0 | ✅ |
| 500 | 382 ev/s | 6.80 | 134 | 199 | 0 | ❌ (76%) |
| 1000 | 573 ev/s | 12.43 | 409 | 483 | 0 | ❌ (57%) |
| 5000 | 463 ev/s | 15.50 | 874 | 988 | 0 | ❌ (9%) |

### Target Thresholds
- p95 ingestion latency: < 500ms
- p95 API latency: < 200ms
- Zero DLQ events

### Analysis

**Root cause: Single-partition Kafka topics.** All benchmark topics have
exactly 1 partition. Kafka consumer groups can only assign one consumer
per partition, so the entire ingestion pipeline is serialized. The
consumer processes ~460-570 ev/s regardless of the injection rate above
that threshold.

At low rates (100 ev/s), the system performs well with sub-5ms p95
latency. At 500 ev/s, throughput plateaus at ~382 ev/s — limited by
the sequential per-message processing in the consumer.

**Next step:** Re-run benchmarks after increasing Kafka topic partitions
to 6+ and scaling consumer replicas.

### Raw Data

See `docs/benchmarks/results-{100,500,1000,5000}.json` for full details.

## Verification

These benchmarks establish the baseline on single-partition Kafka
infrastructure. The target throughput of ≥ 5000 events/sec sustained
with < 500ms p95 DB write latency requires topic partitioning to
achieve.
