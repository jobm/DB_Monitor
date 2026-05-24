import json
import importlib
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from _namespace_bridge import ensure_src_namespace_path

ensure_src_namespace_path()

_api_factory = importlib.import_module("db_monitor.api.factory")
_api_router = importlib.import_module("db_monitor.api.router")
_audit_log = importlib.import_module("db_monitor.audit_log")
_auth = importlib.import_module("db_monitor.auth")
_core_config = importlib.import_module("db_monitor.core.config")
_core_db = importlib.import_module("db_monitor.core.db")
_core_lifecycle = importlib.import_module("db_monitor.core.lifecycle")
_ingestion_consumer = importlib.import_module("db_monitor.ingestion.consumer")
_ingestion_schema = importlib.import_module("db_monitor.ingestion.schema")
_lifespan = importlib.import_module("db_monitor.lifespan")
_metrics = importlib.import_module("db_monitor.metrics")
_tracing = importlib.import_module("db_monitor.tracing")
_ws_manager = importlib.import_module("db_monitor.ws_manager")

create_application = _api_factory.create_application
router = _api_router.router
AuditLogEntry = _audit_log.AuditLogEntry
audit_log_writer = _audit_log.audit_log_writer
authenticate_credentials = _auth.authenticate_credentials

BATCH_ENABLED = _core_config.BATCH_ENABLED
BATCH_SIZE = _core_config.BATCH_SIZE
BROKER_CLUSTERS = _core_config.BROKER_CLUSTERS
DLQ_ENABLED = _core_config.DLQ_ENABLED
OTEL_EXPORTER = _core_config.OTEL_EXPORTER
OTEL_EXPORTER_OTLP_ENDPOINT = _core_config.OTEL_EXPORTER_OTLP_ENDPOINT
OTEL_EXPORTER_OTLP_HEADERS = _core_config.OTEL_EXPORTER_OTLP_HEADERS
OTEL_SERVICE_NAME = _core_config.OTEL_SERVICE_NAME
OTEL_TRACING_ENABLED = _core_config.OTEL_TRACING_ENABLED

engine = _core_db.engine
lifecycle_manager = _core_lifecycle.lifecycle_manager
consumer_task = _ingestion_consumer.consumer_task
schema_cache_backplane = _ingestion_schema.schema_cache_backplane
managed_lifespan = _lifespan.managed_lifespan
api_request_duration_seconds = _metrics.api_request_duration_seconds
failed_auth_attempts_total = _metrics.failed_auth_attempts_total
current_trace_context = _tracing.current_trace_context
initialize_tracing = _tracing.initialize_tracing
ws_manager = _ws_manager.ws_manager


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
