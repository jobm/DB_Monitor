"""Provisioning job persistence and in-memory cache."""

from __future__ import annotations

from sqlalchemy import select

from db_monitor.core.models import CustomerProvisionJob
from db_monitor.extensions import AsyncSessionLocal

CUSTOMER_PROVISION_JOBS: dict[str, dict[str, dict[str, object]]] = {}


def _get_customer_provision_jobs(
    customer_id: str,
) -> dict[str, dict[str, object]]:
    """Return the in-memory provisioning job map for one customer."""
    return CUSTOMER_PROVISION_JOBS.setdefault(customer_id, {})


async def _load_customer_provision_jobs_snapshot(
    customer_id: str,
) -> dict[str, dict[str, object]]:
    """Load all durable provisioning jobs for one customer."""
    cached = CUSTOMER_PROVISION_JOBS.get(customer_id)
    if cached:
        return {job_id: dict(job) for job_id, job in cached.items()}

    async with AsyncSessionLocal() as session:  # type: ignore[type-var,attr-defined]
        result = await session.execute(
            select(CustomerProvisionJob).where(
                CustomerProvisionJob.customer_id == customer_id,
            )
        )
        records = result.scalars().all()

    jobs: dict[str, dict[str, object]] = {}
    for record in records:
        state = dict(record.state or {})
        if state:
            jobs[str(record.job_id)] = state

    if jobs:
        CUSTOMER_PROVISION_JOBS[customer_id] = {
            job_id: dict(job) for job_id, job in jobs.items()
        }

    return jobs


async def _load_customer_provision_job(
    customer_id: str,
    job_id: str,
) -> dict[str, object] | None:
    """Load one durable provisioning job and refresh the cache."""
    jobs = await _load_customer_provision_jobs_snapshot(customer_id)
    job = jobs.get(job_id)
    if job is not None:
        return dict(job)
    return None


async def _persist_customer_provision_job(
    job: dict[str, object],
) -> dict[str, object]:
    """Persist one provisioning job and refresh the compatibility cache."""
    customer_id = str(job["customer_id"])
    job_id = str(job["job_id"])
    payload = dict(job)
    async with AsyncSessionLocal() as session:  # type: ignore[type-var,attr-defined]
        async with session.begin():
            result = await session.execute(
                select(CustomerProvisionJob).where(
                    CustomerProvisionJob.job_id == job_id,
                )
            )
            record = result.scalar_one_or_none()
            if record is None:
                session.add(  # type: ignore[union-attr,attr-defined]
                    CustomerProvisionJob(
                        job_id=job_id,
                        customer_id=customer_id,
                        state=payload,
                    )
                )
            else:
                record.customer_id = customer_id
                record.state = payload

    jobs = CUSTOMER_PROVISION_JOBS.setdefault(customer_id, {})
    jobs[job_id] = dict(payload)
    return payload
