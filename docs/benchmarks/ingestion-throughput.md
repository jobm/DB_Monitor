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
to 6+ and scaling consumer replicas. (Done — see the re-run below, which
changes the conclusion.)

### Raw Data

See `docs/benchmarks/results-{100,500,1000,5000}.json` for full details.

## Results (REL-1 re-run — 6 partitions, 2026-11-24)

### Environment
- Six partitions on `orderdb.public.orders`; offsets after the run were spread
  evenly (~50k new records per partition), so partitioning was exercised
- Single `monitor-server`, `INGESTION_BULK_WRITE_ENABLED=true`, `BATCH_SIZE=100`
- Load generator and monitor stack on the same host (local podman)

### Harness corrections required first

The original matrix could not test rates above ~460 ev/s because of the load
generator, not the pipeline:

- It slept per event instead of per batch, and slept a fixed interval **after**
  the work, so scheduling drift compounded
- Its API-sampling tasks ran blocking `urllib` calls on the same event loop as
  the producer, stalling publishing
- It used gzip compression for one small message at a time, dominating the
  producer's CPU budget

`scripts/scale_test.py` now paces whole batches against absolute deadlines, runs
sampling requests in a worker thread, and publishes uncompressed.

### Throughput

| Metric | Value |
|---|---|
| Target rate | 5,000 ev/s |
| Published | 299,000 events in 60 s (4,982 ev/s) |
| Processed | 11,227 events (190 ev/s) |
| Broker lag after run | 227,099 messages |
| `/health` reported lag (max) | 15,408 messages |
| p95 API latency | 1,673 ms |
| DLQ events | 0 |
| DB batch write duration | 95.7 ms mean per 100-event batch |

**Verdict: FAIL.** Partitioning is not the binding constraint. The batch write
on its own would support roughly 1,000 ev/s; the remaining time per batch is
per-event Python work in the consumer loop.

### Raw Data

[`results-5000-multipartition.json`](results-5000-multipartition.json)

## Verification

The single-partition matrix established the baseline. The 6-partition re-run
shows the ceiling is the per-event consumer path, so the ≥ 5,000 ev/s target is
still unmet and REL-1 stays open.
