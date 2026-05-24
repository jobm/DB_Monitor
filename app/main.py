import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime

from api.factory import create_application
from audit_log import AuditLogEntry, audit_log_writer
from auth import authenticate_credentials
from core.config import (
    BATCH_ENABLED,
    BATCH_SIZE,
    BROKER_CLUSTERS,
    DLQ_ENABLED,
    OTEL_EXPORTER,
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_HEADERS,
    OTEL_SERVICE_NAME,
    OTEL_TRACING_ENABLED,
)
from core.db import engine
from core.lifecycle import lifecycle_manager
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from ingestion.consumer import consumer_task
from ingestion.schema import schema_cache_backplane
from lifespan import managed_lifespan
from metrics import api_request_duration_seconds, failed_auth_attempts_total
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from api.router import router
from tracing import current_trace_context, initialize_tracing
from ws_manager import ws_manager


class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        log_data.update(current_trace_context())
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)


def _configure_tracing(app: FastAPI) -> None:
    """Initialize optional distributed tracing for the app."""
    if not OTEL_TRACING_ENABLED:
        logger.debug(
            "OpenTelemetry tracing disabled - "
            "set OTEL_TRACING_ENABLED=true to enable"
        )
        return

    initialize_tracing(
        app=app,
        engine=engine,
        service_name=OTEL_SERVICE_NAME,
        exporter=OTEL_EXPORTER,
        otlp_endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
        otlp_headers=OTEL_EXPORTER_OTLP_HEADERS,
    )


def _credential_type(
    api_key_header_value: str | None,
    authorization_header_value: str | None,
    session_token: str | None = None,
) -> str | None:
    """Return the presented credential type, if any."""
    if session_token:
        return "session_token"
    if authorization_header_value:
        scheme, _, token = authorization_header_value.partition(" ")
        if scheme.lower() == "bearer" and token:
            return "bearer"
    if api_key_header_value:
        return "api_key"
    return None


@asynccontextmanager
async def lifespan_manager(app: FastAPI):
    async with managed_lifespan(
        app,
        lifecycle_manager=lifecycle_manager,
        audit_log_writer=audit_log_writer,
        schema_cache_backplane=schema_cache_backplane,
        ws_manager=ws_manager,
        broker_clusters=BROKER_CLUSTERS,
        consumer_task=consumer_task,
        dlq_enabled=DLQ_ENABLED,
        batch_enabled=BATCH_ENABLED,
        batch_size=BATCH_SIZE,
        logger=logger,
    ):
        yield


app = create_application(
    title="DB Monitor",
    description="Database Change Data Capture Monitoring System",
    version="1.0.0",
    lifespan=lifespan_manager,
)

_configure_tracing(app)

app.include_router(router)


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


_client_id_counter = 0


@app.websocket("/ws/events")
async def ws_events(
    websocket: WebSocket,
    session_token: str | None = Query(default=None),
):
    global _client_id_counter
    _client_id_counter += 1
    client_id = _client_id_counter

    credential_type = _credential_type(
        websocket.headers.get("x-api-key"),
        websocket.headers.get("authorization"),
        session_token,
    )
    api_key_record = await authenticate_credentials(
        api_key_header_value=websocket.headers.get("x-api-key"),
        authorization_header_value=websocket.headers.get("authorization"),
        session_token=session_token,
        expected_token_type="ws" if session_token else "access",
    )
    if not api_key_record:
        if credential_type:
            failed_auth_attempts_total.labels(
                surface="websocket",
                credential_type=credential_type,
            ).inc()
        await websocket.close(code=4001, reason="Invalid API key")
        return

    await ws_manager.connect(websocket, client_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(client_id)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Structured logging for HTTP requests and database API Audit Logging."""
    start_time = datetime.utcnow()

    api_key_record = await authenticate_credentials(
        api_key_header_value=request.headers.get("x-api-key"),
        authorization_header_value=request.headers.get("authorization"),
    )
    credential_type = _credential_type(
        request.headers.get("x-api-key"),
        request.headers.get("authorization"),
    )
    if api_key_record is None and credential_type:
        failed_auth_attempts_total.labels(
            surface="http",
            credential_type=credential_type,
        ).inc()

    response = await call_next(request)
    duration = (datetime.utcnow() - start_time).total_seconds()

    api_request_duration_seconds.labels(
        endpoint=request.url.path, method=request.method
    ).observe(duration)

    audit_log_writer.enqueue(
        AuditLogEntry(
            api_key_id=api_key_record.id if api_key_record else None,
            endpoint=request.url.path,
            method=request.method,
            status_code=response.status_code,
            ip_address=request.client.host if request.client else None,
        )
    )

    # Standard STDOUT log
    logger.info(
        "HTTP request",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_seconds": duration,
            "api_key_owner": api_key_record.owner_name
            if api_key_record
            else "anonymous",
        },
    )
    return response
