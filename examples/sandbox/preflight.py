from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_BASE_URL = os.getenv("DB_MONITOR_BASE_URL", "http://localhost:8000")
REPO_ROOT = Path(__file__).resolve().parents[2]


def is_stack_reachable(
    base_url: str = DEFAULT_BASE_URL,
) -> tuple[bool, str | None]:
    """Return whether the sandbox API is reachable at the configured URL."""
    try:
        with urllib.request.urlopen(
            f"{base_url}/health",
            timeout=5,
        ) as response:
            if response.getcode() == 200:
                return True, None
            return (
                False,
                f"Sandbox API at {base_url} returned HTTP "
                f"{response.getcode()}.",
            )
    except urllib.error.URLError as exc:
        return (
            False,
            f"Sandbox API at {base_url} is unreachable: {exc.reason}.",
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        return False, f"Sandbox API at {base_url} is unreachable: {exc}."


def is_docker_available() -> tuple[bool, str | None]:
    """Return whether the local Docker daemon can be reached."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except FileNotFoundError:
        return False, "Docker CLI is not installed."
    except subprocess.TimeoutExpired:
        return False, "Docker CLI timed out while checking daemon status."

    if result.returncode == 0:
        return True, None

    stderr = (
        result.stderr or result.stdout or "Docker daemon unavailable."
    ).strip()
    return False, stderr


def sandbox_pytest_skip_reason(
    base_url: str = DEFAULT_BASE_URL,
) -> str | None:
    """Return a skip reason for sandbox pytest checks when infra is missing."""
    stack_ok, stack_error = is_stack_reachable(base_url)
    if stack_ok:
        return None

    docker_ok, docker_error = is_docker_available()
    if not docker_ok:
        return (
            "Sandbox tests skipped because no live stack is reachable "
            "and Docker "
            "is unavailable. "
            f"Stack check: {stack_error} Docker check: {docker_error}"
        )

    return (
        "Sandbox tests skipped because the live stack is not reachable at "
        f"{base_url}. {stack_error} Start it with "
        "'make monitor-recovery' or "
        "set DB_MONITOR_BASE_URL to a reachable environment."
    )


def require_live_sandbox(
    base_url: str = DEFAULT_BASE_URL,
) -> None:
    """Raise a clear error when the live sandbox is unavailable."""
    stack_ok, stack_error = is_stack_reachable(base_url)
    if stack_ok:
        return

    docker_ok, docker_error = is_docker_available()
    if not docker_ok:
        raise RuntimeError(
            "Live sandbox checks require a reachable stack "
            "or a working Docker "
            "daemon. "
            f"Stack check: {stack_error} "
            f"Docker check: {docker_error}"
        )

    raise RuntimeError(
        "Live sandbox checks require the stack to be running at "
        f"{base_url}. {stack_error} "
        "Run 'make monitor-recovery' first."
    )


def _bootstrap_admin_api_key(
    base_url: str,
    owner_name: str,
    ttl_days: int,
) -> str | None:
    """Attempt first-run bootstrap for sandbox automation."""
    encoded_owner = urllib.parse.quote(owner_name)
    request = urllib.request.Request(
        (
            f"{base_url}/auth/bootstrap?"
            f"owner_name={encoded_owner}&ttl_days={ttl_days}"
        ),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code in {400, 403}:
            return None
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Failed to bootstrap admin key: {exc.code} {error_body}"
        ) from exc

    import json

    return json.loads(payload)["api_key"]


def _seed_admin_api_key(owner_name: str, ttl_days: int) -> str:
    """Create a temporary admin key directly against the monitor database."""
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "seed_admin_api_key.py"),
            "--owner-name",
            owner_name,
            "--ttl-days",
            str(ttl_days),
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=30,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"Failed to seed admin API key: {stderr}")

    return result.stdout.strip()


def resolve_admin_api_key(
    *,
    base_url: str = DEFAULT_BASE_URL,
    preseeded_api_key: str | None = None,
    owner_name: str,
    ttl_days: int = 1,
) -> tuple[str, str]:
    """Return an admin key plus the source used to obtain it."""
    if preseeded_api_key:
        return preseeded_api_key, "DB_MONITOR_ADMIN_API_KEY"

    bootstrapped_key = _bootstrap_admin_api_key(
        base_url,
        owner_name,
        ttl_days,
    )
    if bootstrapped_key is not None:
        return bootstrapped_key, "bootstrap"

    return _seed_admin_api_key(owner_name, ttl_days), "seed_admin_api_key"
