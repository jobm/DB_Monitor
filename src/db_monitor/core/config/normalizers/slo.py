"""Tenant cohort SLO policy loaders and defaults."""

from __future__ import annotations

import json
import os

from db_monitor.core.config_loader import config_error


def default_tenant_cohort_slo_policies() -> list[dict[str, object]]:
    """Return default SLO policy tiers for tenant cohorts."""
    return [
        {
            "name": "small",
            "max_sources": 10,
            "availability_target": 99.9,
            "error_budget_percent": 0.1,
            "max_commit_age_seconds": 180,
            "max_consumer_lag": 500,
            "max_dlq_messages": 0,
        },
        {
            "name": "medium",
            "max_sources": 50,
            "availability_target": 99.5,
            "error_budget_percent": 0.5,
            "max_commit_age_seconds": 300,
            "max_consumer_lag": 1000,
            "max_dlq_messages": 5,
        },
        {
            "name": "large",
            "max_sources": 100,
            "availability_target": 99.0,
            "error_budget_percent": 1.0,
            "max_commit_age_seconds": 600,
            "max_consumer_lag": 5000,
            "max_dlq_messages": 20,
        },
    ]


def load_tenant_cohort_slo_policies() -> list[dict[str, object]]:
    """Load and validate cohort SLO policy overrides from JSON."""
    raw_value = os.getenv("TENANT_COHORT_SLO_POLICIES")
    if not raw_value:
        return default_tenant_cohort_slo_policies()

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        config_error(
            "Invalid TENANT_COHORT_SLO_POLICIES format.",
            hint=(
                "Use a JSON array of cohort policy objects. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, list) or not parsed:
        config_error(
            "Invalid TENANT_COHORT_SLO_POLICIES format.",
            hint="Use a non-empty JSON array of cohort policy objects.",
        )

    normalized_policies: list[dict[str, object]] = []
    cohort_names: set[str] = set()
    for policy in parsed:
        if not isinstance(policy, dict):
            config_error(
                "Invalid TENANT_COHORT_SLO_POLICIES entry.",
                hint="Each cohort policy must be a JSON object.",
            )

        name = str(policy.get("name") or "").strip().lower()
        if not name:
            config_error(
                "Invalid TENANT_COHORT_SLO_POLICIES entry.",
                hint="Each cohort policy must include a non-empty name.",
            )
        if name in cohort_names:
            config_error(
                f"Duplicate cohort name '{name}' in TENANT_COHORT_SLO_POLICIES.",
                hint="Use unique cohort names.",
            )
        cohort_names.add(name)

        try:
            max_sources = int(policy.get("max_sources"))  # type: ignore[arg-type]
            max_commit_age_seconds = int(policy.get("max_commit_age_seconds"))  # type: ignore[arg-type]
            max_consumer_lag = int(policy.get("max_consumer_lag"))  # type: ignore[arg-type]
            max_dlq_messages = int(policy.get("max_dlq_messages"))  # type: ignore[arg-type]
            availability_target = float(policy.get("availability_target"))  # type: ignore[arg-type]
            error_budget_percent = float(policy.get("error_budget_percent"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            config_error(
                f"Invalid numeric SLO values for cohort '{name}'.",
                hint="Use numeric values for thresholds and percentages.",
            )

        if max_sources < 1:
            config_error(
                f"max_sources must be at least 1 for cohort '{name}'.",
                hint="Set max_sources to a positive integer.",
            )
        if not 0 < availability_target <= 100:
            config_error(
                f"availability_target must be in (0, 100] for cohort '{name}'.",
                hint="Use a percentage such as 99.9.",
            )
        if not 0 <= error_budget_percent <= 100:
            config_error(
                f"error_budget_percent must be in [0, 100] for cohort '{name}'.",
                hint="Use a percentage such as 0.1.",
            )
        if max_commit_age_seconds < 0:
            config_error(
                f"max_commit_age_seconds cannot be negative for cohort '{name}'.",
                hint="Use 0 or a positive integer.",
            )
        if max_consumer_lag < 0:
            config_error(
                f"max_consumer_lag cannot be negative for cohort '{name}'.",
                hint="Use 0 or a positive integer.",
            )
        if max_dlq_messages < 0:
            config_error(
                f"max_dlq_messages cannot be negative for cohort '{name}'.",
                hint="Use 0 or a positive integer.",
            )

        normalized_policies.append({
            "name": name,
            "max_sources": max_sources,
            "availability_target": availability_target,
            "error_budget_percent": error_budget_percent,
            "max_commit_age_seconds": max_commit_age_seconds,
            "max_consumer_lag": max_consumer_lag,
            "max_dlq_messages": max_dlq_messages,
        })

    return sorted(
        normalized_policies,
        key=lambda p: p["max_sources"],
    )
