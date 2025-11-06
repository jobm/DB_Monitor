import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from consumer_service import consumer_task
from routes import router
from lifecycle_manager import lifecycle_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan_manager(app: FastAPI):
    """Manage application lifespan using lifecycle manager"""
    
    # Setup signal handlers
    loop = asyncio.get_running_loop()
    lifecycle_manager.setup_signal_handlers(loop)
    
    # Startup phase
    await lifecycle_manager.startup()
    
    # Start consumer task
    consumer_task_instance = None
    try:
        consumer_task_instance = asyncio.create_task(
            consumer_task(), 
            name="kafka_consumer"
        )
        lifecycle_manager.register_task(consumer_task_instance)
        logger.info("Consumer task started")
        
        # Application is ready
        yield
        
    except Exception as e:
        logger.error(f"Error during application lifespan: {e}")
        raise
    finally:
        # Shutdown phase
        await lifecycle_manager.shutdown()


# Create FastAPI app with lifecycle management
app = FastAPI(lifespan=lifespan_manager)

# Include routes
app.include_router(router)
