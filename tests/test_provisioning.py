from __future__ import annotations

from collections.abc import Generator

import pytest

import db_monitor.provisioning as provisioning


@pytest.fixture(autouse=True)
def clear_registered_providers() -> Generator[None, None, None]:
    """Reset providers for the test and restore original registry afterward."""
    original_providers = dict(provisioning.STEP_PROVIDERS)
    provisioning.clear_step_providers()
    yield
    provisioning.clear_step_providers()
    for step_name, provider in original_providers.items():
        provisioning.register_step_provider(step_name, provider)


@pytest.mark.anyio
async def test_register_step_provider_overrides_default_namespace() -> None:
    """Registered providers should override default adapter behavior."""

    async def fake_namespace_provider(
        context: provisioning.ProvisioningContext,
    ) -> dict[str, object]:
        return {
            "detail": "provider namespace created",
            "namespace": f"ns-{context.customer_id}",
        }

    provisioning.register_step_provider(
        "generate_namespace",
        fake_namespace_provider,
    )

    result = await provisioning.execute_provisioning_step(
        step_name="generate_namespace",
        customer_id="customer-a",
        job_id="job-1",
    )

    assert result["detail"] == "provider namespace created"
    assert result["namespace"] == "ns-customer-a"


@pytest.mark.anyio
async def test_clear_step_providers_restores_default_behavior() -> None:
    """Clearing provider registrations should restore default adapter logic."""

    async def fake_namespace_provider(
        context: provisioning.ProvisioningContext,
    ) -> dict[str, object]:
        del context
        return {
            "detail": "provider namespace created",
            "namespace": "provider-namespace",
        }

    provisioning.register_step_provider(
        "generate_namespace",
        fake_namespace_provider,
    )
    provisioning.clear_step_providers()

    result = await provisioning.execute_provisioning_step(
        step_name="generate_namespace",
        customer_id="Customer A",
        job_id="job-1",
    )

    assert result["namespace"] == "dbm-customer-a"


def test_register_step_provider_rejects_unknown_step() -> None:
    """Unknown step names should be rejected during provider registration."""

    async def fake_provider(
        context: provisioning.ProvisioningContext,
    ) -> dict[str, object]:
        del context
        return {"detail": "ok"}

    with pytest.raises(
        provisioning.ProvisioningStepExecutionError,
    ) as exc_info:
        provisioning.register_step_provider("unknown_step", fake_provider)

    assert "Unknown provisioning step" in str(exc_info.value)


@pytest.mark.anyio
async def test_execute_step_wraps_unexpected_provider_exception() -> None:
    """Unexpected provider errors should be wrapped with execution context."""

    async def broken_provider(
        context: provisioning.ProvisioningContext,
    ) -> dict[str, object]:
        del context
        raise RuntimeError("provider exploded")

    provisioning.register_step_provider(
        "generate_namespace",
        broken_provider,
    )

    with pytest.raises(
        provisioning.ProvisioningStepExecutionError,
    ) as exc_info:
        await provisioning.execute_provisioning_step(
            step_name="generate_namespace",
            customer_id="customer-a",
            job_id="job-1",
        )

    assert "provider exploded" in str(exc_info.value)

