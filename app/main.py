import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from config import KAFKA_TOPICS, DLQ_ENABLED, BATCH_ENABLED, BATCH_SIZE
from consumer_service import consumer_task
from routes import router
from lifecycle_manager import lifecycle_manager
from models import ApiAuditLog
from auth import get_current_api_key
from extensions import AsyncSessionLocal
from metrics import api_request_duration_seconds
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


@asynccontextmanager
async def lifespan_manager(app: FastAPI):
    loop = asyncio.get_running_loop()
    lifecycle_manager.setup_signal_handlers(loop)

    await lifecycle_manager.startup()

    consumer_task_instance = None
    try:
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
async def ws_events(websocket: WebSocket, api_key: str = Query(...)):
    global _client_id_counter
    _client_id_counter += 1
    client_id = _client_id_counter

    api_key_record = await get_current_api_key(api_key)
    if not api_key_record:
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

    # Attempt to resolve an API key without outright rejecting (routes handle auth rejection)
    api_key_header_value = request.headers.get("x-api-key")
    api_key_record = await get_current_api_key(api_key_header_value)

    response = await call_next(request)
    duration = (datetime.utcnow() - start_time).total_seconds()

    api_request_duration_seconds.labels(
        endpoint=request.url.path, method=request.method
    ).observe(duration)

    # Log to DB
    async with AsyncSessionLocal() as session:
        async with session.begin():
            audit_log = ApiAuditLog(
                api_key_id=api_key_record.id if api_key_record else None,
                endpoint=request.url.path,
                method=request.method,
                status_code=response.status_code,
                ip_address=request.client.host if request.client else None,
            )
            session.add(audit_log)

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
