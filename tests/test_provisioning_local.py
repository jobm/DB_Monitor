from __future__ import annotations

import pytest

import db_monitor.provisioning as provisioning
import db_monitor.provisioning_local as provisioning_local


@pytest.fixture(autouse=True)
def clear_providers() -> None:
    """Reset providers for tests and restore pre-test registry afterward."""
    original_providers = dict(provisioning.STEP_PROVIDERS)
    provisioning.clear_step_providers()
    yield
    provisioning.clear_step_providers()
    for step_name, provider in original_providers.items():
        provisioning.register_step_provider(step_name, provider)


def test_build_namespace_guardrails_contains_required_kinds() -> None:
    """Guardrail templates should include quota, limits, and network policy."""
    guardrails = provisioning_local.build_namespace_guardrails("Customer A")

    assert guardrails["resource_quota"]["kind"] == "ResourceQuota"
    assert guardrails["limit_range"]["kind"] == "LimitRange"
    assert guardrails["network_policy"]["kind"] == "NetworkPolicy"


def test_build_alert_routing_config_is_customer_scoped() -> None:
    """Alert routing should match on the tenant customer scope."""
    alert_routing = provisioning_local.build_alert_routing_config("customer-a")

    assert alert_routing["matchers"]["customer_id"] == "customer-a"
    assert alert_routing["receiver"] == "pagerduty-customer-a"


@pytest.mark.anyio
async def test_local_provider_registration_drives_step_execution() -> None:
    """Local provider registration should feed guardrails and labels."""
    provisioning_local.register_local_step_providers()

    namespace_result = await provisioning.execute_provisioning_step(
        step_name="generate_namespace",
        customer_id="customer-a",
        job_id="job-1",
    )
    secrets_result = await provisioning.execute_provisioning_step(
        step_name="create_secrets_and_config",
        customer_id="customer-a",
        job_id="job-1",
    )

    assert (
        namespace_result["guardrails"]["resource_quota"]["kind"]
        == "ResourceQuota"
    )
    assert (
        namespace_result["observability_labels"]["customer_id"]
        == "customer-a"
    )
    assert (
        secrets_result["alert_routing"]["matchers"]["customer_id"]
        == "customer-a"
    )
