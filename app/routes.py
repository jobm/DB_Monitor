from fastapi import APIRouter
from sqlalchemy import select
from extensions import AsyncSessionLocal
from models import KafkaEvent
from lifecycle_manager import lifecycle_manager

router = APIRouter()

@router.get("/events")
async def get_events():
    # breakpoint()
    async with AsyncSessionLocal() as session:
        stmt = select(KafkaEvent).order_by(KafkaEvent.id.desc())
        result = await session.execute(stmt)
        events = result.scalars().all()
        return [{"id": event.id, "value": event.value} for event in events]


# Health check endpoint
@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if lifecycle_manager.is_healthy else "unhealthy",
        "shutting_down": lifecycle_manager.is_shutting_down
    }


# Application info endpoint
@router.get("/info")
async def app_info():
    """Application information endpoint"""
    return {
        "title": router.app.title,
        "description": router.app.description,
        "healthy": lifecycle_manager.is_healthy,
        "shutting_down": lifecycle_manager.is_shutting_down
    }
