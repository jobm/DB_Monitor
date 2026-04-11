"""Database engine, session, and DB initialization for DB Monitor Server."""

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from config import POSTGRES_URL
from models import Base

engine = create_async_engine(
	POSTGRES_URL,
	echo=True,
	future=True
)

AsyncSessionLocal = sessionmaker(
	bind=engine,
	class_=AsyncSession,
	expire_on_commit=False
)

async def init_db():
	async with engine.begin() as conn:
		await conn.run_sync(Base.metadata.create_all)

	# Best-effort backfill from legacy raw table into the new structured table.
	# This is safe to run repeatedly.
	try:
		from migrations import migrate_legacy_kafka_events

		await migrate_legacy_kafka_events(AsyncSessionLocal)
	except Exception:
		# Avoid blocking startup on migration issues; logs will surface root cause.
		import logging

		logging.getLogger(__name__).exception(
			"Legacy event migration failed during startup"
		)
