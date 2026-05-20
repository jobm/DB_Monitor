from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from consumer_service import _store_event_graph_with_retries
from sqlalchemy import select
from extensions import AsyncSessionLocal
from models import DeadLetterEvent, KafkaEvent


BASE_URL = "http://localhost:8000"
PRESEEDED_ADMIN_KEY = os.getenv("DB_MONITOR_ADMIN_API_KEY")


def request_json(
    path: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
):
    request = urllib.request.Request(f"{BASE_URL}{path}", method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)

    with urllib.request.urlopen(request) as response:
        body = response.read().decode()
        return response.getcode(), json.loads(body) if body else None


def request_text(
    path: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
):
    request = urllib.request.Request(f"{BASE_URL}{path}", method=method)
    for key, value in (headers or {}).items():
        request.add_header(key, value)

    with urllib.request.urlopen(request) as response:
        return response.getcode(), response.read().decode()


def exchange_access_token(api_key: str) -> str:
    """Exchange an API key for a short-lived bearer token."""
    status_code, payload = request_json(
        "/auth/token",
        method="POST",
        headers={"X-API-Key": api_key},
    )
    assert status_code == 200, f"Expected 200 on /auth/token, got {status_code}"
    return payload["access_token"]


async def verify_replay_guard(headers: dict[str, str]) -> None:
    """Verify replaying the same Kafka position does not add a second row."""

    marker = f"integration-replay-{uuid4()}"
    kafka_offset = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    payload = {
        "op": "c",
        "source": {
            "name": "orderdb",
            "db": "postgres",
            "table": "orders",
        },
        "after": {
            "id": marker,
            "status": "integration-test",
        },
    }
    raw_payload = json.dumps(payload)

    first_event = KafkaEvent(
        event_type="c",
        event_time=datetime.now(timezone.utc),
        user_id=None,
        service_name="orderdb.public.orders",
        kafka_topic="orderdb.public.orders",
        kafka_partition=0,
        kafka_offset=kafka_offset,
        event_data=payload,
        raw_payload=raw_payload,
        operation="INSERT",
    )
    second_event = KafkaEvent(
        event_type="c",
        event_time=first_event.event_time,
        user_id=None,
        service_name="orderdb.public.orders",
        kafka_topic="orderdb.public.orders",
        kafka_partition=0,
        kafka_offset=kafka_offset,
        event_data=payload,
        raw_payload=raw_payload,
        operation="INSERT",
    )

    print("Testing duplicate replay guard...")
    inserted_first = await _store_event_graph_with_retries(
        AsyncSessionLocal,
        first_event,
    )
    inserted_second = await _store_event_graph_with_retries(
        AsyncSessionLocal,
        second_event,
    )

    assert inserted_first is True, (
        "Initial integration replay event was not stored"
    )
    assert inserted_second is False, "Duplicate replay was not suppressed"

    status_code, replay_events = request_json(
        f"/events?search_term={urllib.parse.quote(marker)}&limit=10",
        headers=headers,
    )
    assert status_code == 200, (
        f"Replay verification /events failed: {status_code}"
    )
    assert replay_events["total"] == 1, (
        "Replay created more than one visible event"
    )


async def seed_dead_letter_event() -> int:
    """Create a DLQ record that can be exercised through admin endpoints."""
    marker = f"integration-dlq-{uuid4()}"
    kafka_offset = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    payload = {
        "op": "u",
        "source": {
            "name": "orderdb",
            "db": "postgres",
            "table": "orders",
        },
        "before": {
            "id": marker,
            "status": "pending",
        },
        "after": {
            "id": marker,
            "status": "replayed",
        },
    }
    raw_payload = json.dumps(payload)

    async with AsyncSessionLocal() as session:
        dlq_event = DeadLetterEvent(
            service_name="orderdb",
            kafka_topic="orderdb.public.orders",
            kafka_partition=0,
            kafka_offset=kafka_offset,
            operation="UPDATE",
            raw_payload=raw_payload,
            error_message="integration seeded replay",
        )
        session.add(dlq_event)
        await session.commit()
        await session.refresh(dlq_event)
        return int(dlq_event.id)


async def verify_recovery_endpoints(headers: dict[str, str]) -> None:
    """Exercise checkpoint inspection plus DLQ listing and replay APIs."""
    print("Testing /admin/checkpoints...")
    status_code, checkpoint_payload = request_json(
        "/admin/checkpoints",
        headers=headers,
    )
    assert status_code == 200, (
        f"/admin/checkpoints failed: {status_code}"
    )
    assert "checkpoints" in checkpoint_payload

    dlq_event_id = await seed_dead_letter_event()

    print("Testing /admin/dlq...")
    status_code, dlq_payload = request_json(
        "/admin/dlq?limit=20",
        headers=headers,
    )
    assert status_code == 200, f"/admin/dlq failed: {status_code}"
    assert any(
        event["id"] == dlq_event_id for event in dlq_payload["events"]
    ), "Seeded DLQ event not visible in admin listing"

    print("Testing /admin/dlq/{id}/replay...")
    status_code, replay_payload = request_json(
        f"/admin/dlq/{dlq_event_id}/replay",
        method="POST",
        headers=headers,
    )
    assert status_code == 200, (
        f"/admin/dlq/{dlq_event_id}/replay failed: {status_code}"
    )
    assert replay_payload["dlq_event_id"] == dlq_event_id
    assert replay_payload["status"] in {"replayed", "duplicate"}

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(DeadLetterEvent).where(DeadLetterEvent.id == dlq_event_id)
        )
        replayed_row = result.scalar_one()
        assert replayed_row.is_replayed is True
        assert replayed_row.replayed_at is not None


async def run_async_checks(headers: dict[str, str]) -> None:
    """Run all async integration checks on a single event loop."""
    await verify_replay_guard(headers)
    await verify_recovery_endpoints(headers)


def run_tests():
    print("Beginning Integration Tests...")

    try:
        status_code, health = request_json("/health")
        if status_code != 200 or health.get("status") != "healthy":
            print("Server not healthy!")
            sys.exit(1)
    except Exception as exc:
        print(f"Could not connect to {BASE_URL}: {exc}")
        sys.exit(1)

    admin_key = PRESEEDED_ADMIN_KEY
    if admin_key:
        print("Using pre-seeded admin key from DB_MONITOR_ADMIN_API_KEY.")
    else:
        try:
            encoded_owner = urllib.parse.quote("test_admin")
            status_code, data = request_json(
                f"/auth/bootstrap?owner_name={encoded_owner}",
                method="POST",
            )
            if status_code == 200:
                admin_key = data["api_key"]
                print("Successfully bootstrapped the system admin key.")
        except urllib.error.HTTPError as exc:
            if exc.code in {400, 403}:
                print(
                    (
                        "System already bootstrapped or bootstrap is disabled. "
                        "Provide DB_MONITOR_ADMIN_API_KEY or run with "
                        "ALLOW_BOOTSTRAP=true for first-time initialization."
                    )
                )
                sys.exit(0)
            print(f"Failed to bootstrap: {exc.code} {exc.read().decode()}")
            sys.exit(1)

    if not admin_key:
        print(
            "Cannot proceed with secured endpoint tests without an admin key."
        )
        sys.exit(0)

    access_token = exchange_access_token(admin_key)
    headers = {"Authorization": f"Bearer {access_token}"}

    print("Testing /metrics...")
    status_code, metrics_payload = request_text("/metrics")
    assert status_code == 200, f"Expected 200 on /metrics, got {status_code}"
    assert (
        "db_monitor" in metrics_payload or "http_requests" in metrics_payload
    ), "Prometheus metrics look empty"

    print("Testing /events...")
    status_code, events_data = request_json("/events?limit=5", headers=headers)
    assert status_code == 200, f"/events failed: {status_code}"
    assert "events" in events_data
    print(f"  Got {len(events_data['events'])} events")

    print("Testing /events?event_type=UPDATE...")
    status_code, filtered_events = request_json(
        "/events?event_type=UPDATE&limit=5",
        headers=headers,
    )
    assert status_code == 200, (
        f"/events?event_type=UPDATE failed: {status_code}"
    )
    for event in filtered_events["events"]:
        assert event["event_type"] == "UPDATE", "Filtering failed"

    print("Testing /tables...")
    status_code, tables_data = request_json("/tables", headers=headers)
    assert status_code == 200, f"/tables failed: {status_code}"
    assert "tables" in tables_data

    if tables_data["tables"]:
        table_info = tables_data["tables"][0]
        service_name = table_info["service_name"]
        table_name = table_info["table_name"]

        print(f"Testing /tables/{service_name}/{table_name}...")
        status_code, table_data = request_json(
            f"/tables/{service_name}/{table_name}",
            headers=headers,
        )
        assert (
            status_code == 200
        ), f"/tables/{service_name}/{table_name} failed: {status_code}"
        assert "columns" in table_data, (
            "Columns missing from single table response"
        )

    print("Testing /events/stats...")
    status_code, stats_data = request_json("/events/stats", headers=headers)
    assert status_code == 200, f"/events/stats failed: {status_code}"
    assert "total_events" in stats_data

    asyncio.run(run_async_checks(headers))

    print("All integration tests passed successfully.")


if __name__ == "__main__":
    run_tests()
