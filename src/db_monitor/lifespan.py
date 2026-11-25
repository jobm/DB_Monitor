"""Application startup and shutdown orchestration helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from logging import Logger
from typing import Any, cast

from fastapi import FastAPI


@asynccontextmanager
async def managed_lifespan(
    app: FastAPI,
    *,
    lifecycle_manager: Any,
    audit_log_writer: Any,
    schema_cache_backplane: Any,
    ws_manager: Any,
    broker_clusters: list[dict[str, object]],
    consumer_task: Callable[..., Awaitable[None]],
    retention_cleanup_task: Callable[[], Awaitable[None]],
    partition_maintenance_task: Callable[[], Awaitable[None]] | None,
    retention_cleanup_enabled: bool,
    dlq_enabled: bool,
    batch_enabled: bool,
    batch_size: int,
    logger: Logger,
    service_role: str = "all",
):
    """Run startup task registration and coordinated shutdown.

    ``service_role`` splits the runtime per ADR-007: ``api`` pods serve
    clients but consume nothing, ``consumer`` pods ingest but serve no
    client traffic, and ``all`` keeps the original single-process behavior.
    """
    del app
    loop = asyncio.get_running_loop()
    lifecycle_manager.setup_signal_handlers(loop)
    runs_consumer = service_role != "api"
    serves_clients = service_role != "consumer"

    await lifecycle_manager.startup()

    try:
        if serves_clients:
            audit_log_task = asyncio.create_task(
                audit_log_writer.run(),
                name="audit_log_writer",
            )
            lifecycle_manager.register_task(audit_log_task, critical=False)

        schema_cache_backplane_task = (
            await schema_cache_backplane.start_listener()
        )
        if schema_cache_backplane_task is not None:
            lifecycle_manager.register_task(
                schema_cache_backplane_task, critical=False
            )

        if serves_clients:
            ws_backplane_task = await ws_manager.start_backplane_listener()
            if ws_backplane_task is not None:
                lifecycle_manager.register_task(
                    ws_backplane_task, critical=False
                )

        if runs_consumer:
            for cluster in broker_clusters:
                cluster_name = str(cluster["name"])
                task: asyncio.Task[None] = asyncio.create_task(  # type: ignore[var-annotated,arg-type]
                    consumer_task(  # type: ignore[arg-type]
                        cluster_name=cluster_name,
                        broker_kind=str(cluster["broker_kind"]),
                        bootstrap_servers=cast(
                            "str | list[str] | None",
                            cluster["bootstrap_servers"],
                        ),
                        consumer_group=str(cluster["consumer_group"]),
                        topics=cast("list[str]", cluster["topics"]),
                        topic_partitions=cast(
                            "dict[str, list[int]] | None",
                            cluster.get("topic_partitions"),
                        ),
                        connection_url=cast(
                            "str | None", cluster.get("connection_url")
                        ),
                        queue_names=cast(
                            "list[str] | None", cluster.get("queue_names")
                        ),
                        prefetch_count=int(
                            cast(int, cluster.get("prefetch_count") or 100)
                        ),
                        dlq_destination=str(
                            cluster.get("dlq_destination")
                            or "db-monitor-dlq"
                        ),
                        enable_dlq=dlq_enabled,
                        enable_batch=batch_enabled,
                        batch_size=batch_size,
                    ),
                    name=f"kafka_consumer_{cluster_name}",
                )
                lifecycle_manager.register_task(task)

            if retention_cleanup_enabled:
                retention_task: asyncio.Task[None] = asyncio.create_task(  # type: ignore[var-annotated,arg-type]
                    retention_cleanup_task(),  # type: ignore[arg-type]
                    name="retention_cleanup",
                )
                lifecycle_manager.register_task(
                    retention_task, critical=False
                )

            if partition_maintenance_task is not None:
                partition_task: asyncio.Task[None] = asyncio.create_task(  # type: ignore[var-annotated,arg-type]
                    partition_maintenance_task(),  # type: ignore[arg-type]
                    name="partition_maintenance",
                )
                lifecycle_manager.register_task(
                    partition_task, critical=False
                )

            logger.info(
                "Consumer tasks started",
                extra={
                    "clusters": [cluster["name"] for cluster in broker_clusters],
                },
            )
        else:
            logger.info("SERVICE_ROLE=api: consumer tasks disabled")

        yield

    except Exception as exc:
        logger.error(
            "Error during application lifespan",
            extra={"error": str(exc)},
        )
        raise
    finally:
        await lifecycle_manager.shutdown()
