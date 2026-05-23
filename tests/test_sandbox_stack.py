from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import pytest


BASE_URL = os.getenv("DB_MONITOR_BASE_URL", "http://localhost:8000")
PRESEEDED_ADMIN_KEY = os.getenv("DB_MONITOR_ADMIN_API_KEY")


def _request_json(path: str) -> tuple[int, dict[str, object]]:
    """Return a parsed JSON response from the running sandbox stack."""
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return response.getcode(), payload


def _request_json_with_headers(
    path: str,
    headers: dict[str, str],
    method: str = "GET",
) -> tuple[int, dict[str, object]]:
    """Return a parsed JSON response for an authenticated sandbox request."""
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        method=method,
        headers=headers,
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
        return response.getcode(), payload


def _get_admin_api_key() -> str:
    """Return an admin API key for sandbox-authenticated test flows."""
    if PRESEEDED_ADMIN_KEY:
        return PRESEEDED_ADMIN_KEY

    encoded_owner = urllib.parse.quote("sandbox_pytest")
    try:
        _, payload = _request_json_with_headers(
            f"/auth/bootstrap?owner_name={encoded_owner}",
            headers={},
            method="POST",
        )
    except urllib.error.HTTPError as exc:
        if exc.code in {400, 403}:
            pytest.skip(
                "Sandbox authenticated checks require "
                "DB_MONITOR_ADMIN_API_KEY or ALLOW_BOOTSTRAP=true.",
            )
        raise

    return str(payload["api_key"])


def _exchange_access_token(api_key: str) -> str:
    """Exchange an admin API key for a short-lived bearer token."""
    _, payload = _request_json_with_headers(
        "/auth/token",
        headers={"X-API-Key": api_key},
        method="POST",
    )
    return str(payload["access_token"])


@pytest.mark.sandbox
def test_sandbox_health_endpoint_reports_healthy() -> None:
    """The bundled sandbox stack should expose a healthy process endpoint."""
    status_code, payload = _request_json("/health")

    assert status_code == 200
    assert payload["status"] == "healthy"


@pytest.mark.sandbox
def test_sandbox_readiness_reports_database_health() -> None:
    """The sandbox readiness check should include a healthy DB dependency."""
    status_code, payload = _request_json("/readyz")

    assert status_code == 200
    assert payload["checks"]["database"]["status"] == "healthy"


@pytest.mark.sandbox
def test_sandbox_authenticated_tables_access() -> None:
    """Sandbox verification should cover authenticated core API access."""
    api_key = _get_admin_api_key()
    access_token = _exchange_access_token(api_key)

    status_code, payload = _request_json_with_headers(
        "/tables",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert status_code == 200
    assert "tables" in payload


@pytest.mark.sandbox
def test_sandbox_authenticated_admin_checkpoint_access() -> None:
    """Sandbox verification should cover admin-only recovery endpoints."""
    api_key = _get_admin_api_key()
    access_token = _exchange_access_token(api_key)

    status_code, payload = _request_json_with_headers(
        "/admin/checkpoints",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert status_code == 200
    assert "checkpoints" in payload
