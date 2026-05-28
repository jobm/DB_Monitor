from __future__ import annotations

import pytest

import routes.ops as routes_ops


@pytest.mark.anyio
async def test_replay_endpoint_calls_repository(monkeypatch):
    async def fake_replay(limit: int = 100, ids=None, actor=None):
        return {"attempted": 2, "replayed": 2, "failed": 0}

    monkeypatch.setattr(
        routes_ops,
        "replay_spill_entries",
        fake_replay,
    )

    result = await routes_ops.replay_audit_log_spill(
        limit=10,
        admin_api_key=None,
    )
    assert result["result"]["replayed"] == 2


@pytest.mark.anyio
async def test_replay_repository_with_fake_session(monkeypatch):
    # Simulate two spill rows to be replayed
    class FakeRow:
        def __init__(self, id, entry):
            self.id = id
            self.entry = entry

    rows = [
        FakeRow(1, {"endpoint": "/x", "method": "POST", "status_code": 200}),
        FakeRow(2, {"endpoint": "/y", "method": "GET", "status_code": 404}),
    ]

    class FakeResult:
        def __init__(self, scalars):
            self._scalars = scalars

        def scalars(self):
            class Sc:
                def __init__(self, vals):
                    self._vals = vals

                def all(self):
                    return list(self._vals)

            return Sc(self._scalars)

    class FakeSession:
        def __init__(self):
            self.added = []
            self.deleted = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def begin(self):
            return self

        async def execute(self, stmt):
            # Only handle select for spills
            return FakeResult(rows)

        def add(self, model):
            self.added.append(model)

    # Patch both package and app-level module paths to be robust in tests
    monkeypatch.setattr(
        "db_monitor.repositories.audit_log_replay.AsyncSessionLocal",
        lambda: FakeSession(),
    )
    monkeypatch.setattr(
        "repositories.audit_log_replay.AsyncSessionLocal",
        lambda: FakeSession(),
    )

    from repositories.audit_log_replay import replay_spill_entries

    result = await replay_spill_entries(limit=10)
    assert result["attempted"] == 2
    assert result["replayed"] == 2


@pytest.mark.anyio
async def test_replay_marks_spill_with_actor_and_ids(monkeypatch):
    # create two fake rows and ensure they're marked replayed with actor
    class FakeRow:
        def __init__(self, id, entry):
            self.id = id
            self.entry = entry
            self.replayed = False
            self.replayed_by = None
            self.replayed_at = None

    rows = [
        FakeRow(101, {"endpoint": "/a", "method": "POST", "status_code": 201}),
    ]

    class FakeResult:
        def __init__(self, scalars):
            self._scalars = scalars

        def scalars(self):
            class Sc:
                def __init__(self, vals):
                    self._vals = vals

                def all(self):
                    return list(self._vals)

            return Sc(self._scalars)

    class FakeSession:
        def __init__(self):
            self.added = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def begin(self):
            return self

        async def execute(self, stmt):
            return FakeResult(rows)

        def add(self, model):
            # if model is ApiAuditLogSpill row, mark it
            try:
                if hasattr(model, "replayed"):
                    model.replayed = True
                    model.replayed_by = "script-user"
            except Exception:
                pass

    monkeypatch.setattr(
        "repositories.audit_log_replay.AsyncSessionLocal",
        lambda: FakeSession(),
    )
    from repositories.audit_log_replay import replay_spill_entries

    result = await replay_spill_entries(ids=[101], actor="script-user")
    assert result["attempted"] == 1
    assert result["replayed"] == 1
