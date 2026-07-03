"""Ingestion quota override loaders."""

from __future__ import annotations

import json
import os

from db_monitor.core.config_loader import config_error


def load_ingestion_quota_overrides(
    env_name: str,
) -> dict[str, int]:
    """Load per-identity ingestion quota overrides from JSON."""
    raw_value = os.getenv(env_name)
    if not raw_value:
        return {}

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        config_error(
            f"Invalid {env_name} format.",
            hint=(
                "Use a JSON object mapping identity names to quotas. "
                f"Original error: {exc}"
            ),
        )

    if not isinstance(parsed, dict):
        config_error(
            f"Invalid {env_name} format.",
            hint="Use a JSON object like {'orderdb': 1200}.",
        )

    normalized: dict[str, int] = {}
    for raw_name, raw_quota in parsed.items():
        identity_name = str(raw_name).strip()
        if not identity_name:
            config_error(
                f"{env_name} contains an empty identity name.",
                hint="Use non-empty identity names for each quota entry.",
            )
        try:
            quota = int(raw_quota)
        except (TypeError, ValueError):
            config_error(
                f"{env_name} contains an invalid quota for '{identity_name}'.",
                hint="Use integer quota values greater than or equal to zero.",
            )
        if quota < 0:
            config_error(
                f"{env_name} contains a negative quota for '{identity_name}'.",
                hint="Use 0 to disable one identity or a positive integer.",
            )
        normalized[identity_name] = quota

    return normalized
