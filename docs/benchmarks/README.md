# Benchmark Results

This directory stores **machine-readable benchmark results** for DB Monitor
ingestion throughput at various event rates.

## Current Status

Recorded runs live in `results-*.json`. The single-partition baseline is below
target; the 2026-11-24 re-run with six partitions
(`results-5000-multipartition.json`) spread load evenly but still sustained only
190 ev/s, so partitioning is not the binding constraint. See
[Ingestion Throughput](ingestion-throughput.md) for methodology and the open
scaling item in [Backlog](../BACKLOG.md).

## How to Run Benchmarks

```bash
# Quick validation — 500 ev/s, 5 min, 10 sources
make monitor-test-scale-target

# Full matrix from ingestion-throughput.md
python scripts/scale_test.py --kafka-broker localhost:9093 --rate 100 --duration 60
python scripts/scale_test.py --kafka-broker localhost:9093 --rate 500 --duration 300
python scripts/scale_test.py --kafka-broker localhost:9093 --rate 1000 --duration 300
python scripts/scale_test.py --kafka-broker localhost:9093 --rate 5000 --duration 300
```

Results are saved as `.json` files in this directory and recorded in release
notes.

## Target Thresholds

| Metric | Target |
|--------|--------|
| Sustained ingestion rate | ≥ 5,000 events/sec |
| p95 ingestion latency | < 500 ms |
| p95 API latency | < 200 ms |
| DLQ events | Zero |
