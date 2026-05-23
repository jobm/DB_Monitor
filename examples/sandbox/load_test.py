from __future__ import annotations

import argparse
import concurrent.futures
import importlib
import itertools
import json
import os
from pathlib import Path
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


BASE_URL = "http://localhost:8000"
PRESEEDED_ADMIN_KEY = os.getenv("DB_MONITOR_ADMIN_API_KEY")

_preflight = importlib.import_module("examples.sandbox.preflight")
require_live_sandbox = _preflight.require_live_sandbox
resolve_admin_api_key = _preflight.resolve_admin_api_key
_bootstrap_admin_api_key = _preflight._bootstrap_admin_api_key

DEFAULT_TARGETS = ["events", "stats", "tables", "checkpoints"]


def exchange_access_token(api_key: str) -> str:
    """Exchange an API key for a short-lived bearer token."""
    request = urllib.request.Request(
        f"{BASE_URL}/auth/token",
        method="POST",
        headers={"X-API-Key": api_key},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = response.read().decode("utf-8")

    return json.loads(payload)["access_token"]


def bootstrap_admin_key(
    owner_name: str = "load_tester",
    ttl_days: int = 1,
) -> str | None:
    """Attempt first-run bootstrap and return the composite API key."""
    return _bootstrap_admin_api_key(BASE_URL, owner_name, ttl_days)


def _authorized_get(path: str, access_token: str) -> tuple[bool, float]:
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=5):
            pass
        success = True
    except Exception:
        success = False
    duration = time.perf_counter() - start
    return success, duration


def fetch_events(access_token: str) -> tuple[bool, float]:
    return _authorized_get("/events?limit=50", access_token)


def fetch_stats(access_token: str) -> tuple[bool, float]:
    return _authorized_get("/events/stats", access_token)


def fetch_tables(access_token: str) -> tuple[bool, float]:
    return _authorized_get("/tables", access_token)


def fetch_checkpoints(access_token: str) -> tuple[bool, float]:
    return _authorized_get("/admin/checkpoints", access_token)


LOAD_TARGETS = {
    "events": fetch_events,
    "stats": fetch_stats,
    "tables": fetch_tables,
    "checkpoints": fetch_checkpoints,
}


def _p95_milliseconds(response_times: list[float]) -> float:
    """Return a stable p95 latency in milliseconds."""
    if len(response_times) == 1:
        return response_times[0] * 1000

    quantiles = statistics.quantiles(response_times, n=100)
    return quantiles[94] * 1000


def _build_task_cycle(
    targets: list[str],
    access_token: str,
) -> tuple[itertools.cycle, str]:
    """Create a deterministic target cycle for the requested endpoints."""
    selected_targets = [LOAD_TARGETS[target] for target in targets]
    return itertools.cycle(selected_targets), access_token


def _resolve_access_token() -> tuple[str, str]:
    """Return a bearer token and the auth source that provided it."""
    admin_key, auth_source = resolve_admin_api_key(
        base_url=BASE_URL,
        preseeded_api_key=PRESEEDED_ADMIN_KEY,
        owner_name="load_tester",
        ttl_days=1,
    )
    return exchange_access_token(admin_key), auth_source


def evaluate_thresholds(
    summary: dict[str, float | int | None],
    *,
    max_failures: int | None = None,
    min_success_rate: float | None = None,
    max_p95_ms: float | None = None,
) -> None:
    """Raise when a smoke-load summary violates configured limits."""
    failures: list[str] = []
    failure_count = int(summary["failure_count"])
    success_rate = float(summary["success_rate"])
    p95_ms = summary.get("p95_ms")

    if max_failures is not None and failure_count > max_failures:
        failures.append(
            f"failures {failure_count} exceeded max {max_failures}"
        )
    if min_success_rate is not None and success_rate < min_success_rate:
        failures.append(
            "success rate "
            f"{success_rate:.3f} fell below min {min_success_rate:.3f}"
        )
    if max_p95_ms is not None:
        if p95_ms is None:
            failures.append("p95 unavailable because no requests succeeded")
        elif float(p95_ms) > max_p95_ms:
            failures.append(
                f"p95 {float(p95_ms):.2f} ms exceeded max {max_p95_ms:.2f} ms"
            )

    if failures:
        raise ValueError("; ".join(failures))


def load_test(
    workers: int,
    requests_per_worker: int,
    targets: list[str],
) -> dict[str, float | int | None]:
    """Run the configured load test and return a summary payload."""
    print("Starting load test on DB Monitor API.")
    print(f"Workers: {workers}, Requests/Worker: {requests_per_worker}")
    print(f"Targets: {', '.join(targets)}")

    access_token, auth_source = _resolve_access_token()
    print(f"Using auth source: {auth_source}")
    task_cycle, access_token = _build_task_cycle(targets, access_token)

    success_count = 0
    failure_count = 0
    response_times: list[float] = []

    def load_task(_: int) -> tuple[bool, float]:
        return next(task_cycle)(access_token)

    start_time = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
    ) as executor:
        futures = [
            executor.submit(load_task, index)
            for index in range(workers * requests_per_worker)
        ]
        for future in concurrent.futures.as_completed(futures):
            success, duration = future.result()
            if success:
                success_count += 1
                response_times.append(duration)
            else:
                failure_count += 1

    duration_seconds = time.perf_counter() - start_time
    total_requests = success_count + failure_count
    tps = total_requests / duration_seconds

    print("\n--- Load Test Results ---")
    print(f"Total Time:   {duration_seconds:.2f} s")
    print(f"Successful:   {success_count}")
    print(f"Failed:       {failure_count}")
    print(f"TPS (Req/s):  {tps:.2f}")

    avg_ms = None
    max_ms = None
    min_ms = None
    p95_ms = None

    if response_times:
        avg_ms = statistics.mean(response_times) * 1000
        max_ms = max(response_times) * 1000
        min_ms = min(response_times) * 1000
        p95_ms = _p95_milliseconds(response_times)
        print(f"Avg Response: {avg_ms:.2f} ms")
        print(f"Max Response: {max_ms:.2f} ms")
        print(f"Min Response: {min_ms:.2f} ms")
        print(f"P95 Response: {p95_ms:.2f} ms")

    success_rate = success_count / total_requests if total_requests else 0.0
    return {
        "duration_seconds": duration_seconds,
        "success_count": success_count,
        "failure_count": failure_count,
        "success_rate": success_rate,
        "tps": tps,
        "avg_ms": avg_ms,
        "max_ms": max_ms,
        "min_ms": min_ms,
        "p95_ms": p95_ms,
    }


def _parse_targets(raw_targets: str) -> list[str]:
    """Validate and normalize the configured load-test target list."""
    targets = [
        target.strip()
        for target in raw_targets.split(",")
        if target.strip()
    ]
    invalid_targets = [
        target for target in targets if target not in LOAD_TARGETS
    ]
    if invalid_targets:
        raise ValueError(
            "Unknown load test targets: " + ", ".join(sorted(invalid_targets))
        )
    if not targets:
        raise ValueError("At least one load-test target must be provided")
    return targets


def main() -> int:
    """Parse CLI arguments, run the load test, and enforce thresholds."""
    parser = argparse.ArgumentParser(
        description="Load test DB monitor endpoints.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of concurrent workers",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=50,
        help="Requests per worker",
    )
    parser.add_argument(
        "--targets",
        default=",".join(DEFAULT_TARGETS),
        help=(
            "Comma-separated endpoint mix: "
            + ", ".join(sorted(LOAD_TARGETS))
        ),
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=None,
        help="Fail when request failures exceed this count.",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=None,
        help="Fail when successful request ratio drops below this value.",
    )
    parser.add_argument(
        "--max-p95-ms",
        type=float,
        default=None,
        help="Fail when p95 latency exceeds this threshold in milliseconds.",
    )
    args = parser.parse_args()

    try:
        require_live_sandbox(BASE_URL)
    except RuntimeError as exc:
        print(exc)
        return 1

    summary = load_test(
        args.workers,
        args.requests,
        _parse_targets(args.targets),
    )
    try:
        evaluate_thresholds(
            summary,
            max_failures=args.max_failures,
            min_success_rate=args.min_success_rate,
            max_p95_ms=args.max_p95_ms,
        )
    except ValueError as exc:
        print(f"Smoke load thresholds failed: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
