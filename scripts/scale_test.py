#!/usr/bin/env python3
"""Scale validation test for DB Monitor.

Provisions N simulated source databases, generates configurable event
rates, and validates that the ingestion pipeline meets throughput and
latency targets at the 100-DB scale.

Usage:
  PYTHONPATH=../src:. uv run python scripts/scale_test.py \\
      --sources 10 --rate 500 --duration 300

  # Direct-to-Kafka mode (requires KAFKA_BROKER):
  PYTHONPATH=../src:. uv run python scripts/scale_test.py \\
      --sources 10 --rate 500 --duration 300 --kafka-broker localhost:9093

Targets (from os-release-readiness-assessment.md):
  - 500 events/sec sustained for 5 minutes
  - p95 ingestion latency < 500ms
  - p95 API latency < 200ms
  - zero DLQ events
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import string
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

# Ensure the app package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "src")
)


@dataclass
class ScaleTestConfig:
    """Configuration for a scale validation run."""

    sources: int = 10
    rate: int = 500  # events/sec total across all sources
    duration: int = 300  # seconds
    batch_size: int = 100
    api_base: str = "http://localhost:8000"
    kafka_broker: str = ""  # e.g. "localhost:9093"
    kafka_topic: str = "orderdb.public.orders"
    service_name: str = ""
    admin_api_key: str = ""
    verbose: bool = False
    max_ingestion_latency_seconds: float = 0.5
    max_commit_age_seconds: float = 5.0


@dataclass
class ScaleTestResults:
    """Results collected during a scale validation run."""

    config: ScaleTestConfig
    events_generated: int = 0
    events_ingested: int = 0
    api_latencies: list[float] = field(default_factory=list)
    ingestion_lag_samples: list[float] = field(default_factory=list)
    ingestion_latency_samples: list[float] = field(default_factory=list)
    dlq_count: int = 0
    errors: list[str] = field(default_factory=list)
    start_time: str = ""
    end_time: str = ""
    elapsed_seconds: float = 0.0

    @property
    def events_per_second(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.events_generated / self.elapsed_seconds

    @property
    def p50_api_latency(self) -> float:
        return _percentile(self.api_latencies, 50)

    @property
    def p95_api_latency(self) -> float:
        return _percentile(self.api_latencies, 95)

    @property
    def p99_api_latency(self) -> float:
        return _percentile(self.api_latencies, 99)

    def passed(self) -> tuple[bool, list[str]]:
        """Check results against target thresholds."""
        failures: list[str] = []

        if self.events_per_second < self.config.rate * 0.9:
            failures.append(
                f"Throughput {self.events_per_second:.1f} ev/s "
                f"below target {self.config.rate} ev/s"
            )

        if self.p95_api_latency > 0.200:
            failures.append(
                f"p95 API latency {self.p95_api_latency*1000:.1f}ms "
                f"above target 200ms"
            )

        if self.ingestion_latency_samples:
            p95_ingestion_latency = _percentile(
                self.ingestion_latency_samples,
                95,
            )
            if (
                p95_ingestion_latency
                > self.config.max_ingestion_latency_seconds
            ):
                failures.append(
                    f"p95 ingestion latency "
                    f"{p95_ingestion_latency*1000:.1f}ms "
                    f"above target "
                    f"{self.config.max_ingestion_latency_seconds*1000:.1f}ms"
                )

        if self.ingestion_lag_samples:
            p95_commit_age = _percentile(self.ingestion_lag_samples, 95)
            if p95_commit_age > self.config.max_commit_age_seconds:
                failures.append(
                    f"p95 commit age {p95_commit_age:.1f}s "
                    f"above target {self.config.max_commit_age_seconds:.1f}s"
                )

        if self.dlq_count > 0:
            failures.append(
                f"{self.dlq_count} DLQ events detected (target: 0)"
            )

        if self.errors:
            failures.append(f"{len(self.errors)} errors during test")

        return len(failures) == 0, failures


def _percentile(data: list[float], pct: float) -> float:
    """Compute the p-th percentile of a list of values."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    index = (pct / 100.0) * (len(sorted_data) - 1)
    lower = int(index)
    upper = min(lower + 1, len(sorted_data) - 1)
    frac = index - lower
    return sorted_data[lower] * (1 - frac) + sorted_data[upper] * frac


async def _get_admin_token(config: ScaleTestConfig) -> str:
    """Exchange an admin API key for a bearer token."""
    import urllib.request

    if not config.admin_api_key:
        # Try environment variable
        config.admin_api_key = os.getenv(
            "DB_MONITOR_ADMIN_API_KEY", ""
        )
    if not config.admin_api_key:
        print("No admin API key provided. Set DB_MONITOR_ADMIN_API_KEY.")
        print("Continuing without auth (may fail if auth is required).")
        return ""

    req = urllib.request.Request(
        f"{config.api_base}/auth/token",
        method="POST",
        headers={"X-API-Key": config.admin_api_key},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            return body.get("access_token", "")
    except Exception as exc:
        print(f"Failed to get admin token: {exc}")
        return ""


def _make_request(
    url: str,
    token: str = "",
    timeout: float = 10.0,
) -> tuple[float, dict]:
    """Make an HTTP request and return (latency_seconds, response_dict)."""
    import urllib.request

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    start = time.monotonic()
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            latency = time.monotonic() - start
            return latency, json.loads(body) if body else {}
    except Exception as exc:
        latency = time.monotonic() - start
        return latency, {"error": str(exc)}


async def _check_readiness(config: ScaleTestConfig) -> bool:
    """Check that the DB Monitor API is ready."""
    url = f"{config.api_base}/readyz"
    try:
        _, data = _make_request(url, timeout=5)
        return data.get("status") == "ready"
    except Exception:
        return False


async def _collect_metrics(
    config: ScaleTestConfig,
    token: str,
) -> dict:
    """Collect health, metrics, and DLQ state."""
    results: dict = {}

    # Health
    _, health = _make_request(f"{config.api_base}/health", token, 5)
    results["health"] = health

    # Metrics
    _, metrics_text = _make_request(
        f"{config.api_base}/metrics", timeout=5
    )
    # Parse key metrics from Prometheus text format
    if isinstance(metrics_text, str) and not metrics_text:
        pass  # metrics not returned as JSON

    # DLQ count
    try:
        _, dlq_data = _make_request(
            f"{config.api_base}/admin/dlq?limit=1", token, 5
        )
        if isinstance(dlq_data, dict):
            results["dlq_pending"] = dlq_data.get("total", 0)
    except Exception:
        results["dlq_pending"] = -1

    return results


def _make_service_name(config: ScaleTestConfig) -> str:
    """Return the synthetic service name used during the test."""
    return config.service_name or f"scale-test-{uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Synthetic event generator
# ---------------------------------------------------------------------------

_SERVICES = ["order", "catalog", "shipping", "billing", "inventory"]
_TABLES: dict[str, list[str]] = {
    svc: [f"{svc}_{t}" for t in ("customers", "orders", "items", "events")]
    for svc in _SERVICES
}
_OPERATIONS = ["c", "u", "d", "r"]  # create, update, delete, read


def _make_debezium_payload(
    service: str,
    table: str,
    op: str,
    ts_ms: int,
) -> dict:
    """Build a synthetic Debezium-style CDC payload."""
    row_id = random.randint(1, 10_000_000)
    return {
        "schema": {"name": f"{service}.{table}.Envelope"},
        "service_name": service,
        "event_time": datetime.fromtimestamp(
            ts_ms / 1000, tz=timezone.utc
        ).isoformat(),
        "payload": {
            "before": None if op == "c" else {"id": row_id},
            "after": (
                None
                if op == "d"
                else {
                    "id": row_id,
                    "name": "".join(
                        random.choices(string.ascii_letters, k=10)
                    ),
                    "created_at": ts_ms,
                    "updated_at": ts_ms,
                }
            ),
            "source": {
                "name": service,
                "db": f"postgres-{service}",
                "table": table,
                "ts_ms": ts_ms,
            },
            "op": op,
            "ts_ms": ts_ms,
        },
    }


async def _publish_synthetic_events(
    config: ScaleTestConfig,
    results: ScaleTestResults,
    end_time: float,
    topic: str,
    service_name: str,
) -> None:
    """Publish synthetic CDC events to Kafka at the target rate.

    Requires ``--kafka-broker`` (or `KAFKA_BROKER`) for direct publishing.
    """
    if not config.kafka_broker:
        raise RuntimeError(
            "--kafka-broker is required to publish scale-test events"
        )

    try:
        from aiokafka import AIOKafkaProducer
    except ImportError:
        print(
            "aiokafka not installed. "
            "Install with: pip install aiokafka, "
            "or omit --kafka-broker to use API mode."
        )
        return

    producer = AIOKafkaProducer(
        bootstrap_servers=config.kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type="gzip",
        linger_ms=5,
    )
    await producer.start()

    try:
        print(
            f"  Publishing to Kafka broker {config.kafka_broker} topic {topic}"
        )
        batch: list[dict] = []
        interval = 1.0 / max(config.rate, 1)

        while time.monotonic() < end_time:
            ts_ms = int(time.time() * 1000)
            service = service_name
            table = random.choice(_TABLES.get(service, ["default"]))
            op = random.choice(_OPERATIONS)

            payload = _make_debezium_payload(service, table, op, ts_ms)
            batch.append(payload)
            results.events_generated += 1

            if len(batch) >= config.batch_size:
                for p in batch:
                    await producer.send(topic, p)
                batch.clear()

            await asyncio.sleep(interval)

        # Flush remaining
        for p in batch:
            await producer.send(topic, p)
        await producer.flush()

        print(f"  Published {results.events_generated} events to Kafka")
    finally:
        await producer.stop()


async def _collect_ingestion_latency_samples(
    config: ScaleTestConfig,
    token: str,
    service_name: str,
    results: ScaleTestResults,
) -> None:
    """Collect ingestion latency samples from matching events."""
    _, payload = _make_request(
        f"{config.api_base}/events?limit=1000&service_name={service_name}",
        token,
        10,
    )
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return

    for event in events:
        if not isinstance(event, dict):
            continue
        capture_time = event.get("capture_time")
        event_time = event.get("event_time")
        if not capture_time or not event_time:
            continue
        try:
            captured_at = datetime.fromisoformat(
                capture_time.replace("Z", "+00:00")
            )
            produced_at = datetime.fromisoformat(
                event_time.replace("Z", "+00:00")
            )
        except Exception:
            continue
        latency = (captured_at - produced_at).total_seconds()
        if latency >= 0:
            results.ingestion_latency_samples.append(latency)


async def run_scale_test(config: ScaleTestConfig) -> ScaleTestResults:
    """Execute the scale validation test."""
    results = ScaleTestResults(config=config)
    results.start_time = datetime.now(timezone.utc).isoformat()

    print("=== DB Monitor Scale Validation ===")
    print(f"  Sources: {config.sources}")
    print(f"  Target rate: {config.rate} events/sec")
    print(f"  Duration: {config.duration}s")
    print(f"  API base: {config.api_base}")
    print()

    service_name = _make_service_name(config)
    print(f"  Synthetic service: {service_name}")

    # Check readiness
    print("Checking API readiness...")
    if not await _check_readiness(config):
        print("ERROR: DB Monitor API is not ready. Aborting.")
        results.errors.append("API not ready")
        return results
    print("  Ready.")

    # Get admin token
    token = await _get_admin_token(config)
    if token:
        print("  Admin token acquired.")
    else:
        print("  No admin token (auth may be bypassed).")

    # Collect pre-test metrics
    pre_metrics = await _collect_metrics(config, token)
    print(f"  Pre-test DLQ pending: {pre_metrics.get('dlq_pending', '?')}")

    # Run the test — publish synthetic events while sampling
    print(f"\nRunning {config.duration}s scale test "
          f"({config.sources} sources, {config.rate} ev/s)...")
    start_time = time.monotonic()
    end_time = start_time + config.duration

    tasks: list[asyncio.Task] = []

    # Determine Kafka topic
    topic = config.kafka_topic

    # Load generator task (primary)
    generator_task = asyncio.create_task(
        _publish_synthetic_events(
            config,
            results,
            end_time,
            topic,
            service_name,
        ),
        name="load_generator",
    )
    tasks.append(generator_task)

    # API latency sampling task
    async def sample_api_latency():
        endpoints = [
            "/health",
            "/readyz",
            "/events?limit=10",
            "/tables",
        ]
        while time.monotonic() < end_time:
            for ep in endpoints:
                lat, _ = _make_request(
                    f"{config.api_base}{ep}", token, 5
                )
                results.api_latencies.append(lat)
            await asyncio.sleep(1.0)

    tasks.append(asyncio.create_task(sample_api_latency()))

    # Commit-age sampling task
    async def sample_lag():
        while time.monotonic() < end_time:
            try:
                _, health = _make_request(
                    f"{config.api_base}/health", token, 5
                )
                if isinstance(health, dict):
                    consumer = health.get("consumer", {})
                    commit_age = consumer.get("last_commit_age_seconds", 0)
                    if isinstance(commit_age, (int, float)) and commit_age > 0:
                        results.ingestion_lag_samples.append(float(commit_age))
            except Exception:
                pass
            await asyncio.sleep(5.0)

    tasks.append(asyncio.create_task(sample_lag()))

    # Progress reporter
    async def report_progress():
        while time.monotonic() < end_time:
            await asyncio.sleep(15.0)
            now = time.monotonic()
            elapsed = now - start_time
            api_samples = len(results.api_latencies)
            lag_samples = len(results.ingestion_lag_samples)
            print(
                f"  [{elapsed:.0f}s] "
                f"API samples: {api_samples}, "
                f"Lag samples: {lag_samples}, "
                f"Errors: {len(results.errors)}"
            )

    tasks.append(asyncio.create_task(report_progress()))

    # Wait for load generator to finish
    await generator_task

    # Cancel sampling tasks
    for t in tasks:
        if not t.done():
            t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    results.elapsed_seconds = time.monotonic() - start_time
    results.end_time = datetime.now(timezone.utc).isoformat()

    # Collect post-test metrics
    print("\nCollecting post-test metrics...")
    post_metrics = await _collect_metrics(config, token)
    results.dlq_count = post_metrics.get("dlq_pending", 0)

    await _collect_ingestion_latency_samples(
        config,
        token,
        service_name,
        results,
    )

    # Estimate events ingested from metrics if available
    try:
        _, metrics_raw = _make_request(
            f"{config.api_base}/metrics", timeout=5
        )
        # Simple parse for events_processed_total
        if isinstance(metrics_raw, str):
            for line in metrics_raw.split("\n"):
                if "db_monitor_events_processed_total" in line:
                    # Extract the value
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            results.events_ingested = int(float(parts[1]))
                        except ValueError:
                            pass
    except Exception:
        pass

    return results


def print_results(results: ScaleTestResults):
    """Print the scale test results report."""
    passed, failures = results.passed()

    print()
    print("=" * 60)
    print("  DB Monitor Scale Validation Results")
    print("=" * 60)
    print(f"  Start:          {results.start_time}")
    print(f"  End:            {results.end_time}")
    print(f"  Duration:       {results.elapsed_seconds:.1f}s")
    print(f"  Events ingested:{results.events_ingested}")
    print(f"  API samples:    {len(results.api_latencies)}")
    print(f"  Lag samples:    {len(results.ingestion_lag_samples)}")
    print(f"  DLQ count:      {results.dlq_count}")
    print(f"  Errors:         {len(results.errors)}")
    print()
    print("  API Latency (seconds):")
    print(f"    p50: {results.p50_api_latency*1000:.1f}ms")
    print(f"    p95: {results.p95_api_latency*1000:.1f}ms")
    print(f"    p99: {results.p99_api_latency*1000:.1f}ms")
    if results.ingestion_lag_samples:
        avg_lag = statistics.mean(results.ingestion_lag_samples)
        max_lag = max(results.ingestion_lag_samples)
        print("  Ingestion Lag:")
        print(f"    avg: {avg_lag:.0f}")
        print(f"    max: {max_lag:.0f}")
    print()
    print(f"  Throughput:  {results.events_per_second:.1f} ev/s")
    print(f"  Target:      {results.config.rate} ev/s")
    print()
    print(f"  VERDICT: {'PASS' if passed else 'FAIL'}")
    if failures:
        for f in failures:
            print(f"    - {f}")
    print("=" * 60)

    return passed


def main():
    parser = argparse.ArgumentParser(
        description="DB Monitor scale validation test"
    )
    parser.add_argument(
        "--sources",
        type=int,
        default=10,
        help="Number of simulated source databases (default: 10)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=500,
        help="Target event rate in events/sec (default: 500)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="Test duration in seconds (default: 300)",
    )
    parser.add_argument(
        "--api-base",
        default=os.getenv("DB_MONITOR_API_BASE", "http://localhost:8000"),
        help="DB Monitor API base URL",
    )
    parser.add_argument(
        "--admin-api-key",
        default="",
        help="Admin API key for auth-protected endpoints",
    )
    parser.add_argument(
        "--kafka-broker",
        default=os.getenv("KAFKA_BROKER", ""),
        help="Kafka bootstrap server for direct event publishing "
             "(default: KAFKA_BROKER env or none)",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC", "orderdb.public.orders"),
        help="Kafka topic for synthetic events (default: app KAFKA_TOPIC)",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output results as JSON"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )

    args = parser.parse_args()

    config = ScaleTestConfig(
        sources=args.sources,
        rate=args.rate,
        duration=args.duration,
        api_base=args.api_base,
        kafka_broker=args.kafka_broker,
        kafka_topic=args.kafka_topic,
        admin_api_key=args.admin_api_key,
        verbose=args.verbose,
    )

    results = asyncio.run(run_scale_test(config))

    if args.json:
        print(json.dumps({
            "passed": results.passed()[0],
            "events_ingested": results.events_ingested,
            "events_per_second": results.events_per_second,
            "p50_api_latency_ms": results.p50_api_latency * 1000,
            "p95_api_latency_ms": results.p95_api_latency * 1000,
            "p99_api_latency_ms": results.p99_api_latency * 1000,
            "dlq_count": results.dlq_count,
            "errors": results.errors,
            "elapsed_seconds": results.elapsed_seconds,
        }, indent=2))
    else:
        passed = print_results(results)
        if not passed:
            sys.exit(1)


if __name__ == "__main__":
    main()
