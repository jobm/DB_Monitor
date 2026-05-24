from __future__ import annotations

import db_monitor.cli as cli


def test_cli_migrate_dispatches_legacy_runner(
    monkeypatch,
) -> None:
    """The migrate command should delegate to the legacy migration bridge."""
    captured: dict[str, object] = {}

    def _fake_run_legacy_migrate(args):
        captured["args"] = list(args)
        return 7

    monkeypatch.setattr(cli, "run_legacy_migrate", _fake_run_legacy_migrate)

    assert cli.main(["migrate", "validate"]) == 7
    assert captured["args"] == ["validate"]


def test_cli_serve_dispatches_legacy_server(monkeypatch) -> None:
    """The serve command should delegate to the legacy server bridge."""
    called = {"serve": False}

    def _fake_run_legacy_server() -> None:
        called["serve"] = True

    monkeypatch.setattr(cli, "run_legacy_server", _fake_run_legacy_server)

    assert cli.main(["serve"]) == 0
    assert called["serve"] is True
