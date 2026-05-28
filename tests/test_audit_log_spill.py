from __future__ import annotations

import pytest

import routes.ops as routes_ops


@pytest.mark.anyio
async def test_get_audit_log_spill_returns_rows(monkeypatch):
    sample = [
            {
                "id": 1,
                "entry": {"endpoint": "/events"},
                "error_message": "e",
                "spilled_at": "2026-01-01T00:00:00Z",
            }
    ]

    async def fake_list(limit: int = 100):
        return sample

    monkeypatch.setattr(routes_ops, "list_audit_log_spill_snapshot", fake_list)

    result = await routes_ops.get_audit_log_spill(limit=10, admin_api_key=None)
    assert result["count"] == 1
    assert result["events"] == sample
