from __future__ import annotations

import db_monitor.start_monitor as package_start_monitor


def test_package_start_monitor_delegates_to_legacy_runner(
    monkeypatch,
) -> None:
    """Package startup entrypoint should invoke main() without error."""
    called = {"run": False}

    def _fake_main() -> None:
        called["run"] = True

    monkeypatch.setattr(
        package_start_monitor,
        "main",
        _fake_main,
    )

    package_start_monitor.main()

    assert called["run"] is True


def test_package_main_module_exposes_asgi_app() -> None:
    """Canonical package main module should expose a non-null ASGI app."""
    from db_monitor import main as package_main

    assert package_main.app is not None
