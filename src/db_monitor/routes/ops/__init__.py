"""Operational health/info/admin route sub-package."""

from __future__ import annotations

from fastapi import APIRouter

from db_monitor.extensions import AsyncSessionLocal
from db_monitor.lifecycle_manager import lifecycle_manager
from db_monitor.routes.ops.dlq import router as dlq_router
from db_monitor.routes.ops.health import router as health_router
from db_monitor.routes.ops.lifecycle import router as lifecycle_router
from db_monitor.core.config import settings
from db_monitor.routes.ops.metadata import router as metadata_router
from db_monitor.routes.ops.provisioning import router as provisioning_router
from db_monitor.routes.ops.provisioning import (
    CUSTOMER_PROVISION_JOBS,
    PROVISIONING_STEPS,
    get_customer_provision_job,
    list_customer_provision_jobs,
    start_customer_provision_job,
    update_customer_provision_step,
)
from db_monitor.routes.ops.execution import (
    execute_customer_provision_job,
    router as execution_router,
)
from db_monitor.routes.ops.lifecycle import (
    CUSTOMER_LIFECYCLE_STATE,
    get_customer_lifecycle_state,
    resume_customer_cell,
    suspend_customer_cell,
    upgrade_customer_cell,
)
from db_monitor.routes.ops.metadata import (
    get_customer_guardrails,
    get_customer_observability_profile,
    get_slo_policy,
)
from db_monitor.routes.ops.health import (
    health_check,
    live_check,
    readiness_check,
    readiness_payload,
    app_info,
)
from db_monitor.routes.ops.dlq import (
    get_consumer_checkpoints,
    get_audit_log_spill,
    replay_audit_log_spill,
    get_dead_letter_events,
    replay_dead_letter_events,
    replay_dead_letter_event,
)
from db_monitor.provisioning import ProvisioningStepExecutionError
from db_monitor.provisioning_local import register_local_step_providers

register_local_step_providers()

router = APIRouter()
router.include_router(health_router)
router.include_router(dlq_router)
router.include_router(lifecycle_router)
router.include_router(provisioning_router)
router.include_router(execution_router)
router.include_router(metadata_router)

__all__ = [
    "router",
    "health_check",
    "live_check",
    "readiness_check",
    "readiness_payload",
    "app_info",
    "get_consumer_checkpoints",
    "get_audit_log_spill",
    "replay_audit_log_spill",
    "get_dead_letter_events",
    "replay_dead_letter_events",
    "replay_dead_letter_event",
    "get_customer_lifecycle_state",
    "suspend_customer_cell",
    "resume_customer_cell",
    "upgrade_customer_cell",
    "start_customer_provision_job",
    "list_customer_provision_jobs",
    "get_customer_provision_job",
    "update_customer_provision_step",
    "execute_customer_provision_job",
    "get_customer_guardrails",
    "get_customer_observability_profile",
    "get_slo_policy",
]
