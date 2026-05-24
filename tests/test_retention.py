from __future__ import annotations

from datetime import datetime, timezone

import retention


class _FakeColumn:
    def __init__(self, name: str):
        self.name = name


class _FakeTable:
    columns = [_FakeColumn("id"), _FakeColumn("captured_at"), _FakeColumn("payload")]


class _FakeRow:
    __table__ = _FakeTable()

    def __init__(self, row_id: int, captured_at: datetime, payload: dict[str, int]):
        self.id = row_id
        self.captured_at = captured_at
        self.payload = payload


def test_append_archive_rows_writes_jsonl(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(retention, "RETENTION_ARCHIVE_DIR", str(tmp_path))

    now = datetime(2026, 5, 24, 12, 0, tzinfo=timezone.utc)
    rows = [
        _FakeRow(1, datetime(2026, 5, 20, 10, 0, tzinfo=timezone.utc), {"x": 1}),
        _FakeRow(2, datetime(2026, 5, 20, 11, 0, tzinfo=timezone.utc), {"x": 2}),
    ]

    retention._append_archive_rows("events", rows, now)

    archive_path = tmp_path / "events" / "20260524.jsonl"
    assert archive_path.exists()

    lines = archive_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert '"id":1' in lines[0]
    assert '"id":2' in lines[1]
    assert '"captured_at":"2026-05-20T10:00:00+00:00"' in lines[0]
