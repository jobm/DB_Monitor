"""Application startup and shutdown orchestration helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from logging import Logger
from typing import Any

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
    dlq_enabled: bool,
    batch_enabled: bool,
    batch_size: int,
    logger: Logger,
):
    """Run startup task registration and coordinated shutdown."""
    del app
    loop = asyncio.get_running_loop()
    lifecycle_manager.setup_signal_handlers(loop)

    await lifecycle_manager.startup()

    try:
        audit_log_task = asyncio.create_task(
            audit_log_writer.run(),
            name="audit_log_writer",
        )
        lifecycle_manager.register_task(audit_log_task)

        schema_cache_backplane_task = (
            await schema_cache_backplane.start_listener()
        )
        if schema_cache_backplane_task is not None:
            lifecycle_manager.register_task(schema_cache_backplane_task)

        ws_backplane_task = await ws_manager.start_backplane_listener()
        if ws_backplane_task is not None:
            lifecycle_manager.register_task(ws_backplane_task)

        for cluster in broker_clusters:
            cluster_name = str(cluster["name"])
            task = asyncio.create_task(
                consumer_task(
                    cluster_name=cluster_name,
                    broker_kind=str(cluster["broker_kind"]),
                    bootstrap_servers=cluster["bootstrap_servers"],
                    consumer_group=str(cluster["consumer_group"]),
                    topics=list(cluster["topics"]),
                    topic_partitions=cluster.get("topic_partitions"),
                    connection_url=cluster.get("connection_url"),
                    queue_names=cluster.get("queue_names"),
                    prefetch_count=int(
                        cluster.get("prefetch_count") or 100
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

        logger.info(
            "Consumer tasks started",
            extra={
                "clusters": [cluster["name"] for cluster in broker_clusters],
            },
        )

        yield

    except Exception as exc:
        logger.error(
            "Error during application lifespan",
            extra={"error": str(exc)},
        )
        raise
    finally:
        await lifecycle_manager.shutdown()
