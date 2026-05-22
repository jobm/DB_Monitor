from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4

import websockets
from datetime import datetime, timedelta, timezone


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"

DEFAULT_LOCAL_POSTGRES_URL = (
    "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
)
DEFAULT_LOCAL_JWT_SECRET = "dev-insecure-secret-change-me-please-rotate"
DEFAULT_KAFKA_BROKER = "localhost:9093"
DEFAULT_UNUSED_TOPIC = "db-monitor-scaling-validation"
PRESEEDED_ADMIN_KEY = os.getenv("DB_MONITOR_ADMIN_API_KEY")

os.environ.setdefault("POSTGRES_URL", DEFAULT_LOCAL_POSTGRES_URL)
os.environ.setdefault("JWT_SECRET", DEFAULT_LOCAL_JWT_SECRET)

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


def request_json(
    base_url: str,
    path: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    timeout: float = 5.0,
) -> tuple[int, dict[str, object] | None]:
    request = urllib.request.Request(
        f"{base_url}{path}",
        method=method,
        headers=headers or {},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        payload = json.loads(body) if body else None
        return response.getcode(), payload


def bootstrap_admin_key(
    base_url: str,
    owner_name: str = "scale_validator",
    ttl_days: int = 1,
) -> str | None:
    encoded_owner = urllib.parse.quote(owner_name)
    request = urllib.request.Request(
        (
            f"{base_url}/auth/bootstrap"
            f"?owner_name={encoded_owner}&ttl_days={ttl_days}"
        ),
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=5.0) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code in {400, 403}:
            return None
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Failed to bootstrap admin key: {exc.code} {error_body}"
        ) from exc

    return json.loads(payload)["api_key"]


def create_ws_session_token(base_url: str, api_key: str) -> str:
    status_code, payload = request_json(
        base_url,
        "/auth/ws-token",
        method="POST",
        headers={"X-API-Key": api_key},
    )
    assert status_code == 200, (
        f"Expected 200 on /auth/ws-token, got {status_code}"
    )
    assert payload is not None
    return str(payload["session_token"])


def replay_dead_letter_event(
    base_url: str,
    api_key: str,
    dlq_event_id: int,
) -> dict[str, object]:
    status_code, payload = request_json(
        base_url,
        f"/admin/dlq/{dlq_event_id}/replay",
        method="POST",
        headers={"X-API-Key": api_key},
    )
    assert status_code == 200, (
        f"Expected 200 on /admin/dlq/{dlq_event_id}/replay, got "
        f"{status_code}"
    )
    assert payload is not None
    return payload


def verify_event_visible(base_url: str, api_key: str, marker: str) -> None:
    encoded_marker = urllib.parse.quote(marker)
    status_code, payload = request_json(
        base_url,
        f"/events?search_term={encoded_marker}&limit=10",
        headers={"X-API-Key": api_key},
    )
    assert status_code == 200, f"Expected 200 on /events, got {status_code}"
    assert payload is not None
    assert int(payload["total"]) >= 1, (
        f"Expected replayed event marker '{marker}' to be visible on "
        f"{base_url}"
    )


def wait_for_healthy(base_url: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            status_code, payload = request_json(
                base_url,
                "/health",
                timeout=2.0,
            )
            if status_code == 200 and payload is not None:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.5)

    raise RuntimeError(
        f"Timed out waiting for {base_url} to become healthy. "
        f"Last error: {last_error}"
    )


def build_instance_env(instance_name: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "APP_ENV": "development",
            "DB_SCHEMA_MODE": "skip",
            "POSTGRES_URL": os.getenv(
                "POSTGRES_URL",
                DEFAULT_LOCAL_POSTGRES_URL,
            ),
            "JWT_SECRET": os.getenv(
                "JWT_SECRET",
                DEFAULT_LOCAL_JWT_SECRET,
            ),
            "WS_BACKPLANE_ENABLED": "true",
            "KAFKA_CLUSTERS": json.dumps(
                [
                    {
                        "name": instance_name,
                        "bootstrap_servers": [
                            os.getenv(
                                "SCALING_VALIDATION_KAFKA_BROKER",
                                DEFAULT_KAFKA_BROKER,
                            )
                        ],
                        "topics": [
                            os.getenv(
                                "SCALING_VALIDATION_UNUSED_TOPIC",
                                DEFAULT_UNUSED_TOPIC,
                            )
                        ],
                        "consumer_group": (
                            f"db-monitor-scaling-validation-"
                            f"{instance_name}"
                        ),
                    }
                ]
            ),
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def start_instance(
    port: int,
    instance_name: str,
) -> tuple[subprocess.Popen[str], Path]:
    log_handle = tempfile.NamedTemporaryFile(
        mode="w+",
        prefix=f"{instance_name}_",
        suffix=".log",
        delete=False,
    )
    log_path = Path(log_handle.name)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(APP_ROOT),
        env=build_instance_env(instance_name),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log_handle.close()
    return process, log_path


def stop_instance(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5.0)


def print_log_tail(log_path: Path, label: str) -> None:
    if not log_path.exists():
        return
    log_text = log_path.read_text(encoding="utf-8", errors="replace")
    tail = "\n".join(log_text.splitlines()[-40:])
    print(f"\n--- {label} log tail ({log_path}) ---")
    print(tail)


async def create_ephemeral_admin_key() -> tuple[str, int]:
    from auth import generate_new_api_key, get_api_key_hash
    from extensions import AsyncSessionLocal
    from models import ApiKey

    raw_key = generate_new_api_key()
    key_hash = get_api_key_hash(raw_key)

    async with AsyncSessionLocal() as session:
        api_key = ApiKey(
            key_hash=key_hash,
            owner_name="scale-validator",
            role="admin",
            is_active=True,
            expires_at=(
                datetime.now(timezone.utc) + timedelta(hours=1)
            ),
        )
        session.add(api_key)
        await session.commit()
        await session.refresh(api_key)
        return f"{api_key.id}.{raw_key}", int(api_key.id)


async def revoke_ephemeral_admin_key(key_id: int) -> None:
    from extensions import AsyncSessionLocal
    from models import ApiKey

    async with AsyncSessionLocal() as session:
        api_key = await session.get(ApiKey, key_id)
        if api_key is None:
            return
        api_key.is_active = False
        api_key.revoked_at = datetime.now(timezone.utc)
        await session.commit()


async def resolve_admin_key(base_url: str) -> tuple[str, int | None]:
    if PRESEEDED_ADMIN_KEY:
        print("Using DB_MONITOR_ADMIN_API_KEY for scaling validation.")
        return PRESEEDED_ADMIN_KEY, None

    bootstrapped_key = bootstrap_admin_key(base_url)
    if bootstrapped_key is None:
        print(
            "Bootstrap already consumed; creating an ephemeral admin key "
            "directly in the monitor database for validation."
        )
        return await create_ephemeral_admin_key()

    print("Bootstrapped an admin key for scaling validation.")
    return bootstrapped_key, None


async def seed_dead_letter_event(marker: str) -> int:
    from extensions import AsyncSessionLocal
    from models import DeadLetterEvent

    kafka_offset = int(time.time() * 1_000_000)
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
            "status": "scaled-replay",
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
            error_message="horizontal scaling validation replay",
        )
        session.add(dlq_event)
        await session.commit()
        await session.refresh(dlq_event)
        return int(dlq_event.id)


async def run_validation(
    port_a: int,
    port_b: int,
    startup_timeout: float,
    event_timeout: float,
) -> None:
    base_url_a = f"http://127.0.0.1:{port_a}"
    base_url_b = f"http://127.0.0.1:{port_b}"
    instance_a, log_a = start_instance(port_a, "scale-a")
    instance_b, log_b = start_instance(port_b, "scale-b")
    ephemeral_key_id: int | None = None

    try:
        print(
            f"Starting validation instances on {base_url_a} and "
            f"{base_url_b}"
        )
        wait_for_healthy(base_url_a, startup_timeout)
        wait_for_healthy(base_url_b, startup_timeout)

        admin_key, ephemeral_key_id = await resolve_admin_key(base_url_a)
        session_token = create_ws_session_token(base_url_a, admin_key)

        marker = f"scaling-validation-{uuid4()}"
        dlq_event_id = await seed_dead_letter_event(marker)
        print(f"Seeded DLQ event #{dlq_event_id} for marker {marker}")

        websocket_url = (
            f"ws://127.0.0.1:{port_a}/ws/events"
            f"?session_token={session_token}"
        )

        async with websockets.connect(websocket_url) as websocket:
            await asyncio.sleep(1.0)
            replay_result = await asyncio.to_thread(
                replay_dead_letter_event,
                base_url_b,
                admin_key,
                dlq_event_id,
            )
            print(
                "Triggered replay on replica B with status "
                f"{replay_result.get('status')}"
            )

            raw_message = await asyncio.wait_for(
                websocket.recv(),
                timeout=event_timeout,
            )

        message = json.loads(raw_message)
        event = message.get("event") or {}
        event_data = event.get("event_data") or {}
        payload = event_data.get("payload") or event_data
        after = payload.get("after") or {}
        assert message.get("type") == "new_event", (
            f"Unexpected websocket message type: {message}"
        )
        assert after.get("id") == marker, (
            "Replica A websocket did not receive the replayed event from "
            "replica B."
        )

        verify_event_visible(base_url_a, admin_key, marker)
        verify_event_visible(base_url_b, admin_key, marker)
        print("Horizontal scaling validation passed.")
    except Exception:
        print_log_tail(log_a, "scale-a")
        print_log_tail(log_b, "scale-b")
        raise
    finally:
        if ephemeral_key_id is not None:
            await revoke_ephemeral_admin_key(ephemeral_key_id)
        stop_instance(instance_a)
        stop_instance(instance_b)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate horizontal scaling by launching two local DB Monitor "
            "replicas and confirming websocket backplane delivery."
        )
    )
    parser.add_argument(
        "--port-a",
        type=int,
        default=8011,
        help="Port for the first validation replica.",
    )
    parser.add_argument(
        "--port-b",
        type=int,
        default=8012,
        help="Port for the second validation replica.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=45.0,
        help="Seconds to wait for each validation replica to become healthy.",
    )
    parser.add_argument(
        "--event-timeout",
        type=float,
        default=20.0,
        help="Seconds to wait for the cross-replica websocket event.",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            run_validation(
                port_a=args.port_a,
                port_b=args.port_b,
                startup_timeout=args.startup_timeout,
                event_timeout=args.event_timeout,
            )
        )
    except Exception as exc:
        print(f"Horizontal scaling validation failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
