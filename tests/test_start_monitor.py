from __future__ import annotations

import json
from pathlib import Path
import signal
from typing import Any, Callable

import pytest

import start_monitor


def test_derived_topics_from_manifest_skips_disabled_and_deduplicates(
    tmp_path: Path,
) -> None:
    """Enabled connectors should produce ordered unique topic names."""
    manifest_path = tmp_path / "sources.json"
    manifest_path.write_text(
        json.dumps(
            {
                "connectors": [
                    {
                        "source_name": "catalogdb",
                        "database_hostname": "postgres-catalog",
                        "tables": [
                            "public.categories",
                            "public.products",
                            "public.categories",
                        ],
                        "enabled": True,
                    },
                    {
                        "source_name": "shippingdb",
                        "database_hostname": "postgres-shipping",
                        "tables": ["public.shipments"],
                        "enabled": False,
                    },
                ]
            }
        )
    )

    assert start_monitor._derived_topics_from_manifest(manifest_path) == [
        "catalogdb.public.categories",
        "catalogdb.public.products",
    ]


def test_configure_topics_preserves_explicit_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit KAFKA_TOPICS should win over manifest-derived topics."""
    monkeypatch.setenv("KAFKA_TOPICS", "manual.topic")
    monkeypatch.setattr(
        start_monitor,
        "_derived_topics_from_manifest",
        lambda manifest_path: ["catalogdb.public.categories"],
    )

    start_monitor._configure_topics()

    assert start_monitor.os.environ["KAFKA_TOPICS"] == "manual.topic"


def test_derived_topics_from_manifest_handles_malformed_json(
    tmp_path: Path,
) -> None:
    """Malformed source manifests should not crash startup topic derivation."""
    manifest_path = tmp_path / "sources.json"
    manifest_path.write_text('{"connectors": [')

    assert start_monitor._derived_topics_from_manifest(manifest_path) == []


def test_derived_topics_from_manifest_logs_warning_for_bad_json(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Malformed JSON should be logged through the module logger."""
    manifest_path = tmp_path / "sources.json"
    manifest_path.write_text('{"connectors": [')

    with caplog.at_level("WARNING"):
        start_monitor._derived_topics_from_manifest(manifest_path)

    assert "not valid JSON" in caplog.text


def test_derived_topics_from_manifest_logs_warning_for_invalid_schema(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Schema-invalid manifests should not produce subscribed topics."""
    manifest_path = tmp_path / "sources.json"
    manifest_path.write_text(
        json.dumps(
            {
                "connectors": [
                    {
                        "source_name": "orderdb",
                        "database_hostname": "postgres-order",
                        "tables": ["not-a-valid-table-name"],
                        "unexpected": "value",
                    }
                ]
            }
        )
    )

    with caplog.at_level("WARNING"):
        topics = start_monitor._derived_topics_from_manifest(manifest_path)

    assert topics == []
    assert "failed schema validation" in caplog.text
    assert "unknown fields" in caplog.text


class _FakeServer:
    """Minimal server stub used to validate shutdown callbacks."""

    def __init__(self) -> None:
        self.should_exit = False


class _FakeLoop:
    """Capture signal handlers registered by start_monitor."""

    def __init__(self) -> None:
        self.handlers: dict[
            signal.Signals,
            tuple[Callable[..., Any], tuple[Any, ...]],
        ] = {}

    def add_signal_handler(
        self,
        sig: signal.Signals,
        callback: Callable[..., Any],
        *args: Any,
    ) -> None:
        self.handlers[sig] = (callback, args)


def test_register_shutdown_signals_requests_graceful_exit() -> None:
    """Registered signal handlers should mark the server for shutdown."""
    loop = _FakeLoop()
    server = _FakeServer()

    start_monitor._register_shutdown_signals(  # type: ignore[arg-type]
        loop,
        server,
    )

    assert signal.SIGINT in loop.handlers
    assert signal.SIGTERM in loop.handlers

    callback, args = loop.handlers[signal.SIGTERM]
    callback(*args)

    assert server.should_exit is True
