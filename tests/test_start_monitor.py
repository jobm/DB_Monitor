from __future__ import annotations

import json
from pathlib import Path

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


def test_configure_topics_preserves_explicit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit KAFKA_TOPICS should win over manifest-derived topics."""
    monkeypatch.setenv("KAFKA_TOPICS", "manual.topic")
    monkeypatch.setattr(
        start_monitor,
        "_derived_topics_from_manifest",
        lambda manifest_path: ["catalogdb.public.categories"],
    )

    start_monitor._configure_topics()

    assert start_monitor.os.environ["KAFKA_TOPICS"] == "manual.topic"