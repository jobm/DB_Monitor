from __future__ import annotations

import pytest

from scripts import load_test


def test_parse_targets_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="Unknown load test targets"):
        load_test._parse_targets("events,unknown")


def test_evaluate_thresholds_accepts_healthy_summary() -> None:
    summary = {
        "failure_count": 0,
        "success_rate": 1.0,
        "p95_ms": 325.0,
    }

    load_test.evaluate_thresholds(
        summary,
        max_failures=0,
        min_success_rate=0.99,
        max_p95_ms=500.0,
    )


def test_evaluate_thresholds_reports_all_violations() -> None:
    summary = {
        "failure_count": 3,
        "success_rate": 0.85,
        "p95_ms": 1800.0,
    }

    with pytest.raises(ValueError) as exc_info:
        load_test.evaluate_thresholds(
            summary,
            max_failures=0,
            min_success_rate=0.95,
            max_p95_ms=1000.0,
        )

    message = str(exc_info.value)
    assert "failures 3 exceeded max 0" in message
    assert "success rate 0.850 fell below min 0.950" in message
    assert "p95 1800.00 ms exceeded max 1000.00 ms" in message
