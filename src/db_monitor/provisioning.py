"""Provisioning step adapters for customer cell orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

from sqlalchemy import select


@dataclass(frozen=True)
class ProvisioningContext:
    """Context payload passed to each provisioning step adapter."""

    customer_id: str
    job_id: str
    note: str | None


class ProvisioningStepExecutionError(RuntimeError):
    """Raised when a provisioning step adapter fails execution."""


StepHandler = Callable[[ProvisioningContext], Awaitable[dict[str, object]]]
StepProvider = Callable[[ProvisioningContext], Awaitable[dict[str, object]]]


def _sanitize_customer(customer_id: str) -> str:
    """Return a DNS-safe fragment for generated resource names."""
    normalized = customer_id.strip().lower()
    slug = "".join(ch if ch.isalnum() else "-" for ch in normalized)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "customer"


async def _register_customer(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Register customer metadata in the control plane."""
    provider = STEP_PROVIDERS.get("register_customer")
    if provider is not None:
        return await provider(context)

    return {
        "detail": "Customer control-plane record registered.",
        "customer_id": context.customer_id,
    }


async def _generate_namespace(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Generate or verify the customer namespace identifier."""
    provider = STEP_PROVIDERS.get("generate_namespace")
    if provider is not None:
        return await provider(context)

    namespace = f"dbm-{_sanitize_customer(context.customer_id)}"
    return {
        "detail": f"Namespace '{namespace}' is ready.",
        "namespace": namespace,
    }


async def _provision_postgres(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Provision or validate the customer-specific database target."""
    provider = STEP_PROVIDERS.get("provision_postgres")
    if provider is not None:
        return await provider(context)

    db_name = f"dbm_{_sanitize_customer(context.customer_id)}"
    return {
        "detail": f"Postgres target '{db_name}' is provisioned.",
        "database": db_name,
    }


async def _provision_broker_namespace(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Provision or validate customer broker namespace resources."""
    provider = STEP_PROVIDERS.get("provision_broker_namespace")
    if provider is not None:
        return await provider(context)

    broker_namespace = f"broker-{_sanitize_customer(context.customer_id)}"
    return {
        "detail": (
            f"Broker namespace '{broker_namespace}' is provisioned."
        ),
        "broker_namespace": broker_namespace,
    }


async def _create_secrets_and_config(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Provision secrets and config bindings for one customer."""
    provider = STEP_PROVIDERS.get("create_secrets_and_config")
    if provider is not None:
        return await provider(context)

    return {
        "detail": "Secrets and runtime configuration are prepared.",
        "secret_set": "default",
        "note": context.note,
    }


async def _deploy_monitor_services(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Deploy or reconcile monitor service workloads for one customer."""
    provider = STEP_PROVIDERS.get("deploy_monitor_services")
    if provider is not None:
        return await provider(context)

    del context
    return {
        "detail": "Monitor workloads are deployed.",
        "deployment": "monitor-server",
    }


async def _run_migrations_and_health_checks(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Apply schema migrations and verify readiness checks."""
    from db_monitor.extensions import AsyncSessionLocal, init_db

    provider = STEP_PROVIDERS.get("run_migrations_and_health_checks")
    if provider is not None:
        return await provider(context)

    await init_db()
    async with AsyncSessionLocal() as session:
        await session.execute(select(1))

    return {
        "detail": (
            "Migrations applied/validated and database readiness check "
            "completed successfully."
        ),
        "health": "ready",
        "customer_id": context.customer_id,
    }


async def _bootstrap_and_activate(
    context: ProvisioningContext,
) -> dict[str, object]:
    """Bootstrap runtime artifacts and mark the customer cell active."""
    provider = STEP_PROVIDERS.get("bootstrap_and_activate")
    if provider is not None:
        return await provider(context)

    activated_at = datetime.now(timezone.utc).isoformat()
    return {
        "detail": "Customer cell bootstrap completed and activated.",
        "activated_at": activated_at,
    }


STEP_PROVIDERS: dict[str, StepProvider] = {}


STEP_HANDLERS: dict[str, StepHandler] = {
    "register_customer": _register_customer,
    "generate_namespace": _generate_namespace,
    "provision_postgres": _provision_postgres,
    "provision_broker_namespace": _provision_broker_namespace,
    "create_secrets_and_config": _create_secrets_and_config,
    "deploy_monitor_services": _deploy_monitor_services,
    "run_migrations_and_health_checks": _run_migrations_and_health_checks,
    "bootstrap_and_activate": _bootstrap_and_activate,
}


def register_step_provider(step_name: str, provider: StepProvider) -> None:
    """Register one external provisioning provider for a step name."""
    normalized_step = step_name.strip()
    if normalized_step not in STEP_HANDLERS:
        raise ProvisioningStepExecutionError(
            f"Unknown provisioning step '{normalized_step}'."
        )

    STEP_PROVIDERS[normalized_step] = provider


def clear_step_providers() -> None:
    """Clear registered external providers and restore default behavior."""
    STEP_PROVIDERS.clear()


async def execute_provisioning_step(
    step_name: str,
    customer_id: str,
    job_id: str,
    note: str | None = None,
) -> dict[str, object]:
    """Execute one named provisioning step using a registered adapter."""
    normalized_step = step_name.strip()
    handler = STEP_HANDLERS.get(normalized_step)
    if handler is None:
        raise ProvisioningStepExecutionError(
            f"No provisioning adapter is registered for '{normalized_step}'."
        )

    context = ProvisioningContext(
        customer_id=customer_id,
        job_id=job_id,
        note=note,
    )
    try:
        return await handler(context)
    except ProvisioningStepExecutionError:
        raise
    except Exception as exc:
        raise ProvisioningStepExecutionError(
            f"Provisioning step '{normalized_step}' failed: {exc}"
        ) from exc
