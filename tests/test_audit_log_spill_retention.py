from __future__ import annotations

from datetime import datetime, timezone

from db_monitor import retention
from db_monitor.core.config import settings
from db_monitor.core.models import ApiAuditLogSpill


def test_spill_retention_policy_is_configured() -> None:
    """The spill retention policy should exist in the policy list."""
    policies = retention.retention_policies()
    spill_policy = [p for p in policies if p.name == "api_audit_log_spill"]
    assert len(spill_policy) == 1
    policy = spill_policy[0]

    assert policy.model is ApiAuditLogSpill
    assert policy.timestamp_column is ApiAuditLogSpill.replayed_at
    assert policy.retention_days == settings.retention.spill_retention_days
    assert policy.extra_filter is not None


def test_spill_retention_policy_only_targets_replayed_rows() -> None:
    """The extra_filter should filter for replayed=True only."""
    policies = retention.retention_policies()
    spill_policy = [p for p in policies if p.name == "api_audit_log_spill"][0]

    # The extra_filter should be equivalent to `replayed.is_(True)`
    expected = ApiAuditLogSpill.replayed.is_(True)
    assert str(spill_policy.extra_filter.compile(compile_kwargs={"literal_binds": True})) == str(
        expected.compile(compile_kwargs={"literal_binds": True})
    )


def test_retention_policies_include_extra_filter() -> None:
    """Verify which policies have extra_filter set."""
    policies = retention.retention_policies()
    for p in policies:
        if p.name == "api_audit_log_spill":
            assert p.extra_filter is not None
        else:
            # All other policies should have no extra_filter
            assert p.extra_filter is None, (
                f"Policy {p.name} has unexpected extra_filter={p.extra_filter}"
            )
