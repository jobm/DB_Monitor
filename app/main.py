import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from config import KAFKA_TOPICS, DLQ_ENABLED, BATCH_ENABLED, BATCH_SIZE
from audit_log import AuditLogEntry, audit_log_writer
from auth import authenticate_credentials, get_current_api_key
from consumer_service import consumer_task
from routes import router
from lifecycle_manager import lifecycle_manager
from extensions import AsyncSessionLocal
from metrics import api_request_duration_seconds, failed_auth_attempts_total
from ws_manager import ws_manager


class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)


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
    loop = asyncio.get_running_loop()
    lifecycle_manager.setup_signal_handlers(loop)

    await lifecycle_manager.startup()

    consumer_task_instance = None
    audit_log_task = None
    ws_backplane_task = None
    try:
        audit_log_task = asyncio.create_task(
            audit_log_writer.run(),
            name="audit_log_writer",
        )
        lifecycle_manager.register_task(audit_log_task)

        ws_backplane_task = await ws_manager.start_backplane_listener()
        if ws_backplane_task is not None:
            lifecycle_manager.register_task(ws_backplane_task)

        consumer_task_instance = asyncio.create_task(
            consumer_task(
                topics=KAFKA_TOPICS,
                enable_dlq=DLQ_ENABLED,
                enable_batch=BATCH_ENABLED,
                batch_size=BATCH_SIZE,
            ),
            name="kafka_consumer",
        )
        lifecycle_manager.register_task(consumer_task_instance)
        logger.info("Consumer task started", extra={"topics": KAFKA_TOPICS})

        yield

    except Exception as e:
        logger.error("Error during application lifespan", extra={"error": str(e)})
        raise
    finally:
        await lifecycle_manager.shutdown()


app = FastAPI(
    title="DB Monitor",
    description="Database Change Data Capture Monitoring System",
    version="1.0.0",
    lifespan=lifespan_manager,
)

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
