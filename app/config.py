"""Configuration and environment variables for the DB Monitor server."""

from __future__ import annotations

import os
import ssl
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _default_schema_mode(app_env: str) -> str:
    """Return the safest default schema policy for the active environment."""
    if app_env in {"production", "prod"}:
        return "validate"
    return "apply"


def _get_env_or_file(name: str, default: str | None = None) -> str | None:
    """Return a config value from NAME or NAME_FILE."""
    value = os.getenv(name)
    if value is not None:
        return value

    file_path = os.getenv(f"{name}_FILE")
    if file_path:
        return Path(file_path).read_text(encoding="utf-8").strip()

    return default


DEFAULT_POSTGRES_URL = (
    "postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
)
DEFAULT_JWT_SECRET = "dev-insecure-secret-change-me-please-rotate"


APP_ENV = os.getenv("APP_ENV", "development").lower()
DB_SCHEMA_MODE = os.getenv(
    "DB_SCHEMA_MODE",
    _default_schema_mode(APP_ENV),
).lower()
if DB_SCHEMA_MODE not in {"apply", "validate", "skip"}:
    raise ValueError(
        "DB_SCHEMA_MODE must be one of: apply, validate, skip"
    )

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv(
    "KAFKA_SECURITY_PROTOCOL",
    "PLAINTEXT",
).upper()
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE")
KAFKA_SSL_CERTFILE = os.getenv("KAFKA_SSL_CERTFILE")
KAFKA_SSL_KEYFILE = os.getenv("KAFKA_SSL_KEYFILE")
KAFKA_SSL_PASSWORD = _get_env_or_file("KAFKA_SSL_PASSWORD")

KAFKA_SSL_CONTEXT = None
if KAFKA_SECURITY_PROTOCOL in ("SSL", "SASL_SSL"):
    KAFKA_SSL_CONTEXT = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=KAFKA_SSL_CAFILE,
    )
    if KAFKA_SSL_CERTFILE and KAFKA_SSL_KEYFILE:
        KAFKA_SSL_CONTEXT.load_cert_chain(
            certfile=KAFKA_SSL_CERTFILE,
            keyfile=KAFKA_SSL_KEYFILE,
            password=KAFKA_SSL_PASSWORD,
        )

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orderdb.public.orders")
KAFKA_TOPICS = (
    os.getenv("KAFKA_TOPICS", "").split(",")
    if os.getenv("KAFKA_TOPICS")
    else [KAFKA_TOPIC]
)
KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "fastapi-consumer-group",
)
POSTGRES_URL = _get_env_or_file(
    "POSTGRES_URL",
    DEFAULT_POSTGRES_URL,
)

if not POSTGRES_URL.startswith("postgresql+asyncpg://"):
    if POSTGRES_URL.startswith("postgresql://"):
        POSTGRES_URL = POSTGRES_URL.replace(
            "postgresql://",
            "postgresql+asyncpg://",
        )
    else:
        raise ValueError("Invalid PostgreSQL URL format")

SQLALCHEMY_ECHO = os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
DB_POOL_TIMEOUT_SECONDS = int(
    os.getenv("DB_POOL_TIMEOUT_SECONDS", "30")
)
DB_POOL_RECYCLE_SECONDS = int(
    os.getenv("DB_POOL_RECYCLE_SECONDS", "1800")
)
DB_POOL_PRE_PING = os.getenv(
    "DB_POOL_PRE_PING",
    "true",
).lower() == "true"
DLQ_ENABLED = os.getenv("DLQ_ENABLED", "true").lower() == "true"
BATCH_ENABLED = os.getenv("BATCH_ENABLED", "true").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
ALLOW_BOOTSTRAP = os.getenv(
    "ALLOW_BOOTSTRAP",
    "false" if APP_ENV in {"production", "prod"} else "true",
).lower() == "true"
API_KEY_DEFAULT_TTL_DAYS = int(
    os.getenv("API_KEY_DEFAULT_TTL_DAYS", "90")
)
READINESS_MAX_COMMIT_AGE_SECONDS = int(
    os.getenv("READINESS_MAX_COMMIT_AGE_SECONDS", "300")
)
READINESS_MAX_CONSUMER_LAG = int(
    os.getenv("READINESS_MAX_CONSUMER_LAG", "1000")
)
READINESS_MAX_DLQ_MESSAGES = int(
    os.getenv("READINESS_MAX_DLQ_MESSAGES", "0")
)
JWT_SECRET = _get_env_or_file(
    "JWT_SECRET",
    DEFAULT_JWT_SECRET,
)
JWT_SECRET_NEXT = _get_env_or_file("JWT_SECRET_NEXT")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_TTL_MINUTES = int(
    os.getenv("ACCESS_TOKEN_TTL_MINUTES", "15")
)
WS_SESSION_TOKEN_TTL_SECONDS = int(
    os.getenv("WS_SESSION_TOKEN_TTL_SECONDS", "60")
)
WS_BACKPLANE_ENABLED = os.getenv(
    "WS_BACKPLANE_ENABLED",
    "true",
).lower() == "true"
WS_BACKPLANE_CHANNEL = os.getenv(
    "WS_BACKPLANE_CHANNEL",
    "db_monitor_ws_events",
)
AUDIT_LOG_QUEUE_MAXSIZE = int(
    os.getenv("AUDIT_LOG_QUEUE_MAXSIZE", "10000")
)
AUDIT_LOG_BATCH_SIZE = int(
    os.getenv("AUDIT_LOG_BATCH_SIZE", "100")
)
AUDIT_LOG_FLUSH_INTERVAL_SECONDS = float(
    os.getenv("AUDIT_LOG_FLUSH_INTERVAL_SECONDS", "1.0")
)
APP_SHUTDOWN_TIMEOUT_SECONDS = float(
    os.getenv("APP_SHUTDOWN_TIMEOUT_SECONDS", "45")
)

if APP_ENV in {"production", "prod"}:
    if DB_POOL_SIZE < 1:
        raise ValueError("DB_POOL_SIZE must be at least 1")
    if DB_MAX_OVERFLOW < 0:
        raise ValueError("DB_MAX_OVERFLOW cannot be negative")
    if DB_POOL_TIMEOUT_SECONDS < 1:
        raise ValueError(
            "DB_POOL_TIMEOUT_SECONDS must be at least 1"
        )
    if DB_POOL_RECYCLE_SECONDS < 0:
        raise ValueError(
            "DB_POOL_RECYCLE_SECONDS cannot be negative"
        )
    if POSTGRES_URL == DEFAULT_POSTGRES_URL:
        raise ValueError(
            "POSTGRES_URL must be explicitly configured in production"
        )
    if JWT_SECRET == DEFAULT_JWT_SECRET:
        raise ValueError(
            "JWT_SECRET must be explicitly configured in production"
        )
    if len(JWT_SECRET) < 32:
        raise ValueError(
            "JWT_SECRET must be at least 32 characters long in production"
        )
    if JWT_SECRET_NEXT and len(JWT_SECRET_NEXT) < 32:
        raise ValueError(
            "JWT_SECRET_NEXT must be at least 32 characters long in production"
        )
