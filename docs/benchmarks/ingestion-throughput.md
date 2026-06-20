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

## Results Template

Record results in release notes using this format:

```
## Ingestion Benchmark Results

### Reference Hardware
- CPU: [e.g., 4 vCPUs (AMD EPYC)]
- RAM: [e.g., 16 GB]
- Postgres: [e.g., Azure Flexible Server GP_Standard_D2s_v3]
- Kafka: [e.g., Confluent Cloud Standard]

### Throughput

| Rate (ev/s) | p95 API (ms) | p95 Commit Age (s) | p95 Ingestion (ms) | DLQ Events |
|---|---|---|---|---|
| 100 | — | — | — | 0 |
| 500 | — | — | — | 0 |
| 1000 | — | — | — | 0 |
| 5000 | — | — | — | 0 |

### Target Thresholds
- p95 ingestion latency: < 500ms
- p95 API latency: < 200ms
- Zero DLQ events
```

## Verification

These benchmarks confirm that the bulk-write ingestion path meets the
acceptance criteria from `docs/os-release-readiness-assessment.md`:
≥ 5000 events/sec sustained with < 500ms p95 DB write latency.
