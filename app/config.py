# Configuration and environment variables for the DB Monitor Server
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orderdb.public.orders")
POSTGRES_URL = os.getenv(
	"POSTGRES_URL",
	"postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
)

# Ensure the URL explicitly uses asyncpg
if not POSTGRES_URL.startswith("postgresql+asyncpg://"):
	if POSTGRES_URL.startswith("postgresql://"):
		POSTGRES_URL = POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
	else:
		raise ValueError("Invalid PostgreSQL URL format")
