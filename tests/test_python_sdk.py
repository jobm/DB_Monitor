from __future__ import annotations

import json
import importlib
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


SDK_ROOT = Path(__file__).resolve().parents[1] / "sdk" / "python"


def _load_client_class():
    if str(SDK_ROOT) not in sys.path:
        sys.path.insert(0, str(SDK_ROOT))
    spec = spec_from_file_location(
        "db_monitor_client",
        SDK_ROOT / "db_monitor_client.py",
    )
    module = module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.DBMonitorClient


def _load_packaged_client_class():
    if str(SDK_ROOT) not in sys.path:
        sys.path.insert(0, str(SDK_ROOT))
    sys.modules.pop("db_monitor_sdk", None)
    packaged_module = importlib.import_module("db_monitor_sdk")
    return packaged_module.DBMonitorClient


DBMonitorClient = _load_client_class()
PackagedDBMonitorClient = _load_packaged_client_class()


class FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def test_exchange_access_token_uses_api_key(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["method"] = request.get_method()
        captured["timeout"] = timeout
        return FakeResponse({"access_token": "token-123"})

    monkeypatch.setattr(
        "urllib.request.urlopen",
        fake_urlopen,
    )

    client = DBMonitorClient("http://localhost:8000", api_key="1.secret")
    token = client.exchange_access_token()

    assert token == "token-123"
    assert client.access_token == "token-123"
    assert captured["url"] == "http://localhost:8000/auth/token"
    assert captured["method"] == "POST"
    assert captured["headers"]["X-api-key"] == "1.secret"


def test_get_changes_encodes_json_row_identity(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        del timeout
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        return FakeResponse({"changes": []})

    monkeypatch.setattr(
        "urllib.request.urlopen",
        fake_urlopen,
    )

    client = DBMonitorClient(
        "http://localhost:8000",
        access_token="token-123",
    )
    payload = client.get_changes(
        table_name="orderdb.sales.orders",
        row_identity={"id": 7},
        limit=5,
        offset=10,
    )

    assert payload == {"changes": []}
    assert "table_name=orderdb.sales.orders" in captured["url"]
    assert "row_identity=%7B%22id%22%3A+7%7D" in captured["url"]
    assert "limit=5" in captured["url"]
    assert "offset=10" in captured["url"]
    assert captured["headers"]["Authorization"] == "Bearer token-123"


def test_get_events_encodes_cursor(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        del timeout
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        return FakeResponse({"events": [], "total": None, "limit": 5, "offset": 0})

    monkeypatch.setattr(
        "urllib.request.urlopen",
        fake_urlopen,
    )

    client = DBMonitorClient(
        "http://localhost:8000",
        access_token="token-123",
    )
    payload = client.get_events(limit=5, cursor_id=88)

    assert payload["events"] == []
    assert "limit=5" in captured["url"]
    assert "cursor_id=88" in captured["url"]
    assert captured["headers"]["Authorization"] == "Bearer token-123"


def test_packaged_sdk_import_exposes_client() -> None:
    client = PackagedDBMonitorClient(
        "http://localhost:8000",
        api_key="1.secret",
    )

    assert client.base_url == "http://localhost:8000"
    assert client.api_key == "1.secret"
