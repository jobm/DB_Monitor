# Benchmark Results

This directory stores **machine-readable benchmark results** for DB Monitor
ingestion throughput at various event rates.

## Current Status

- `results-100.json` — Placeholder / initial run. Shows the benchmark harness
  detected the API as not ready (expected when the full stack is not running).
  Replace with real results after running the benchmark from an environment
  with a healthy stack.

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

## Target Thresholds (from os-release-readiness-assessment)

| Metric | Target |
|--------|--------|
| Sustained ingestion rate | ≥ 5,000 events/sec |
| p95 ingestion latency | < 500 ms |
| p95 API latency | < 200 ms |
| DLQ events | Zero |
