"""Database engine, session, and DB initialization for DB Monitor Server."""

from db_monitor.core.config import settings
from db_monitor.metrics import (
    active_connections,
    db_pool_checked_in_connections,
    db_pool_overflow_connections,
    db_pool_size,
    db_pool_utilization_ratio,
)
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


def _pool_metric_value(pool, attribute_name: str) -> int:
    """Return an integer pool metric value when the pool exposes it."""
    accessor = getattr(pool, attribute_name, None)
    if accessor is None:
        return 0
    if callable(accessor):
        return int(accessor())
    return int(accessor)


def _update_pool_metrics(pool) -> None:
    """Refresh Prometheus gauges from the current SQLAlchemy pool state."""
    pool_size_value = _pool_metric_value(pool, "size")
    checked_out_value = _pool_metric_value(pool, "checkedout")
    checked_in_value = _pool_metric_value(pool, "checkedin")
    overflow_value = max(_pool_metric_value(pool, "overflow"), 0)
    pool_capacity = pool_size_value + overflow_value

    active_connections.set(checked_out_value)
    db_pool_size.set(pool_size_value)
    db_pool_checked_in_connections.set(checked_in_value)
    db_pool_overflow_connections.set(overflow_value)
    if pool_capacity > 0:
        db_pool_utilization_ratio.set(checked_out_value / pool_capacity)
    else:
        db_pool_utilization_ratio.set(0)


def _instrument_pool_metrics(engine) -> None:
    """Attach pool event hooks so connection pressure is exported directly."""
    sync_engine = getattr(engine, "sync_engine", None)
    pool = getattr(sync_engine, "pool", None)
    if pool is None:
        return

    _update_pool_metrics(pool)

    def refresh_metrics(*_args, **_kwargs) -> None:
        _update_pool_metrics(pool)

    for event_name in (
        "connect",
        "checkout",
        "checkin",
        "close",
        "invalidate",
        "reset",
    ):
        event.listen(pool, event_name, refresh_metrics)


def build_engine():
    """Create the shared async engine with configurable pool controls."""
    engine = create_async_engine(
        settings.db.postgres_url,
        echo=settings.db.sqlalchemy_echo,
        future=True,
        pool_pre_ping=settings.db.pool_pre_ping,
        pool_size=settings.db.pool_size,
        max_overflow=settings.db.max_overflow,
        pool_timeout=settings.db.pool_timeout_seconds,
        pool_recycle=settings.db.pool_recycle_seconds,
    )
    _instrument_pool_metrics(engine)
    return engine


engine = build_engine()

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """Initialize database state according to the configured schema mode."""
    from db_monitor.core.migrations import (
        MIGRATIONS,
        apply_migrations,
        validate_migrations,
    )

    if settings.db_schema_mode == "skip":
        return

    if settings.db_schema_mode == "apply":
        await apply_migrations(AsyncSessionLocal, engine, MIGRATIONS)
        return

    await validate_migrations(engine, MIGRATIONS)
