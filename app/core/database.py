import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

logger = logging.getLogger("temporallayr.database")

def _normalize_async_database_url(database_url: str) -> str:
    """Ensure SQLAlchemy async engine always uses the asyncpg dialect."""
    if database_url.startswith("postgresql+asyncpg://"):
        return database_url
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+asyncpg://", 1)
    return database_url


# Explicit mapping connecting downstream async batching
DATABASE_URL = _normalize_async_database_url(
    os.environ.get(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/temporallayr"
    )
)

try:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=20,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        echo=False,
    )
    async_session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    Base = declarative_base()
except Exception as e:
    logger.error(f"Failed configuring Async Engine mappings: {e}")
    # Don't crash immediately on load, allow retry architectures to hook failures sequentially over worker ticks natively
    engine = None
    async_session_maker = None
    Base = declarative_base()


async def get_db_session() -> AsyncSession:
    """Dependency injector yielding active sessions natively."""
    if not async_session_maker:
        raise RuntimeError("Database engine not initialized successfully.")

    async with async_session_maker() as session:
        yield session
