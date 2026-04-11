# Configuration and environment variables for the DB Monitor Server
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
KAFKA_SSL_CAFILE = os.getenv("KAFKA_SSL_CAFILE")
KAFKA_SSL_CERTFILE = os.getenv("KAFKA_SSL_CERTFILE")
KAFKA_SSL_KEYFILE = os.getenv("KAFKA_SSL_KEYFILE")
KAFKA_SSL_PASSWORD = os.getenv("KAFKA_SSL_PASSWORD")

# Build SSL Context globally if enabled.
import ssl

KAFKA_SSL_CONTEXT = None
if KAFKA_SECURITY_PROTOCOL in ("SSL", "SASL_SSL"):
    KAFKA_SSL_CONTEXT = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=KAFKA_SSL_CAFILE
    )
    if KAFKA_SSL_CERTFILE and KAFKA_SSL_KEYFILE:
        KAFKA_SSL_CONTEXT.load_cert_chain(
            certfile=KAFKA_SSL_CERTFILE,
            keyfile=KAFKA_SSL_KEYFILE,
            password=KAFKA_SSL_PASSWORD
        )

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orderdb.public.orders")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "").split(",") if os.getenv("KAFKA_TOPICS") else [KAFKA_TOPIC]
POSTGRES_URL = os.getenv(
	"POSTGRES_URL",
	"postgresql+asyncpg://postgres:postgres@localhost:5437/postgres"
)

DLQ_ENABLED = os.getenv("DLQ_ENABLED", "true").lower() == "true"
BATCH_ENABLED = os.getenv("BATCH_ENABLED", "true").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

if not POSTGRES_URL.startswith("postgresql+asyncpg://"):
	if POSTGRES_URL.startswith("postgresql://"):
		POSTGRES_URL = POSTGRES_URL.replace("postgresql://", "postgresql+asyncpg://")
	else:
		raise ValueError("Invalid PostgreSQL URL format")
