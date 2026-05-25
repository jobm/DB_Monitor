"""Local provider implementations for customer provisioning steps."""

from __future__ import annotations

from typing import Any

from db_monitor.provisioning import (
    ProvisioningContext,
    register_step_provider,
)


def _sanitize_customer(customer_id: str) -> str:
    """Return a deterministic DNS-safe slug for resource names."""
    normalized = customer_id.strip().lower()
    slug = "".join(ch if ch.isalnum() else "-" for ch in normalized)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "customer"


def build_observability_labels(customer_id: str) -> dict[str, str]:
    """Build standard labels used across metrics/logs/traces."""
    slug = _sanitize_customer(customer_id)
    return {
        "customer_id": customer_id,
        "customer_slug": slug,
        "tenant_scope": f"tenant:{slug}",
    }


def build_alert_routing_config(customer_id: str) -> dict[str, Any]:
    """Build customer-scoped alert routing metadata."""
    slug = _sanitize_customer(customer_id)
    route_key = f"db-monitor-customer-{slug}"
    return {
        "route_key": route_key,
        "matchers": {
            "customer_id": customer_id,
            "service": "db-monitor",
        },
        "receiver": f"pagerduty-{slug}",
    }


def build_namespace_guardrails(customer_id: str) -> dict[str, Any]:
    """Generate deployable namespace guardrail templates."""
    slug = _sanitize_customer(customer_id)
    namespace = f"dbm-{slug}"
    return {
        "resource_quota": {
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": f"rq-{slug}", "namespace": namespace},
            "spec": {
                "hard": {
                    "pods": "40",
                    "requests.cpu": "8",
                    "requests.memory": "16Gi",
                    "limits.cpu": "16",
                    "limits.memory": "32Gi",
                }
            },
        },
        "limit_range": {
            "apiVersion": "v1",
            "kind": "LimitRange",
            "metadata": {"name": f"limits-{slug}", "namespace": namespace},
            "spec": {
                "limits": [
                    {
                        "type": "Container",
                        "defaultRequest": {"cpu": "100m", "memory": "256Mi"},
                        "default": {"cpu": "500m", "memory": "1Gi"},
                    }
                ]
            },
        },
        "network_policy": {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": f"np-{slug}", "namespace": namespace},
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "db-monitor-customer": slug,
                                    }
                                }
                            }
                        ]
                    }
                ],
            },
        },
    }


async def _register_customer_provider(
    context: ProvisioningContext,
) -> dict[str, object]:
    labels = build_observability_labels(context.customer_id)
    return {
        "detail": "Customer record registered with local provider.",
        "customer_id": context.customer_id,
        "observability_labels": labels,
    }


async def _generate_namespace_provider(
    context: ProvisioningContext,
) -> dict[str, object]:
    slug = _sanitize_customer(context.customer_id)
    namespace = f"dbm-{slug}"
    labels = build_observability_labels(context.customer_id)
    return {
        "detail": f"Namespace '{namespace}' created with guardrails.",
        "namespace": namespace,
        "guardrails": build_namespace_guardrails(context.customer_id),
        "observability_labels": labels,
    }


async def _provision_postgres_provider(
    context: ProvisioningContext,
) -> dict[str, object]:
    db_name = f"dbm_{_sanitize_customer(context.customer_id)}"
    labels = build_observability_labels(context.customer_id)
    return {
        "detail": f"Provisioned Postgres database '{db_name}'.",
        "database": db_name,
        "observability_labels": labels,
    }


async def _provision_broker_namespace_provider(
    context: ProvisioningContext,
) -> dict[str, object]:
    broker_namespace = f"broker-{_sanitize_customer(context.customer_id)}"
    labels = build_observability_labels(context.customer_id)
    alert_routing = build_alert_routing_config(context.customer_id)
    return {
        "detail": f"Provisioned broker namespace '{broker_namespace}'.",
        "broker_namespace": broker_namespace,
        "alert_routing": alert_routing,
        "observability_labels": labels,
    }


async def _create_secrets_and_config_provider(
    context: ProvisioningContext,
) -> dict[str, object]:
    slug = _sanitize_customer(context.customer_id)
    labels = build_observability_labels(context.customer_id)
    alert_routing = build_alert_routing_config(context.customer_id)
    return {
        "detail": "Created customer secrets and runtime config set.",
        "secret_set": f"dbm-secrets-{slug}",
        "config_bundle": f"dbm-config-{slug}",
        "alert_routing": alert_routing,
        "observability_labels": labels,
    }


async def _deploy_monitor_services_provider(
    context: ProvisioningContext,
) -> dict[str, object]:
    slug = _sanitize_customer(context.customer_id)
    labels = build_observability_labels(context.customer_id)
    alert_routing = build_alert_routing_config(context.customer_id)
    return {
        "detail": "Deployed monitor services for customer namespace.",
        "deployment": "monitor-server",
        "namespace": f"dbm-{slug}",
        "observability_labels": labels,
        "alert_routing": alert_routing,
    }


def register_local_step_providers() -> None:
    """Register local provider implementations for core provisioning steps."""
    register_step_provider("register_customer", _register_customer_provider)
    register_step_provider("generate_namespace", _generate_namespace_provider)
    register_step_provider("provision_postgres", _provision_postgres_provider)
    register_step_provider(
        "provision_broker_namespace",
        _provision_broker_namespace_provider,
    )
    register_step_provider(
        "create_secrets_and_config",
        _create_secrets_and_config_provider,
    )
    register_step_provider(
        "deploy_monitor_services",
        _deploy_monitor_services_provider,
    )
