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

**Verdict: FAIL at the time of the run.** Partitioning is not the binding
constraint, but the original explanation was wrong. Profiling on 2026-11-25
showed per-event Python work was not the ceiling: parsing, `KafkaEvent()`
construction, transform, metrics and span work together cost ~0.065 ms per
event (~15,000 ev/s of headroom). The cap was a `SELECT pg_notify(...)` round
trip per inserted event from the WebSocket backplane (`WS_BACKPLANE_ENABLED`
defaults to true), costing ~3.3 ms per event (~300 ev/s) even with no clients
connected. Treat the p95 API latency above as load-generator entrained rather
than consumer-caused.

### Measurement bugs found by this run (fixed)

- `/health` reported ~15k lag against a 274k broker backlog. Lag was derived
  from the per-message `highwater` cache, which only reflects the fetch that
  delivered that message. It now comes from a broker `end_offsets` snapshot per
  assigned partition, refreshed every 5 s, so `/readyz` reflects the real
  backlog.
- Ingestion latency always read ~0 ms because storage keeps `capture_time`
  equal to `event_time` (the partitioned unique constraint uses it for dedupe).
  Migration `0019_events_ingested_at` adds a real `ingested_at` write timestamp;
  latency is now `ingested_at - event_time`, exposed by `GET /events`.

### Post-fix throughput (2026-11-25)

The consumer now publishes one backplane notification per batch instead of one
per event (`ws_manager.publish_event_notifications`), with the listener fanning
out each event id. Same stack, same payload shape, 20,000 published events per
run, metrics sampled once per second:

| Run | Conditions | Mean drain | DB batch write | Kafka commit |
|---|---|---|---|---|
| 1 | Long-running stack | ~1,005 ev/s | 65–81 ms | ~2 ms |
| 2 | Cold, just restarted | 597 ev/s | 140.6 ms | 3–11 ms |
| 3 | Warm | 830 ev/s | 89.6 ms | 2.3 ms |
| 4 | Warm | 868 ev/s | 83.0 ms | — |

Warm steady state is ~830–870 ev/s, roughly 3x the 290 ev/s pre-fix drain and
4.5x the 190 ev/s recorded above. The limiter is now the DB batch write
(~85 ms per 100 events) plus commit; the backplane no longer costs anything
per event. Cold-cache runs immediately after a restart are not comparable.

### Raw Data

[`results-5000-multipartition.json`](results-5000-multipartition.json)

## Verification

The single-partition matrix established the baseline. The 6-partition re-run
shows the ceiling is the per-event consumer path, so the ≥ 5,000 ev/s target is
still unmet and REL-1 stays open.
