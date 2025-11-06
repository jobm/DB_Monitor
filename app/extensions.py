# Database engine, session, and DB initialization for DB Monitor Server
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
