import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from app.config import DATABASE_URL as RAW_DATABASE_URL

logger = logging.getLogger("temporallayr.database")


def _normalize_async_database_url(database_url: str) -> str:
    """Ensure SQLAlchemy async engine always uses the asyncpg dialect."""
    if not database_url:
        return ""
    if database_url.startswith("postgresql+asyncpg://"):
        return database_url
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+asyncpg://", 1)
    return database_url


# Explicit mapping connecting downstream async batching
DATABASE_URL = _normalize_async_database_url(
    RAW_DATABASE_URL
    or "postgresql+asyncpg://postgres:postgres@localhost:5432/temporallayr"
)

try:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=5,
        pool_timeout=10,  # Connect timeout basically
        pool_recycle=300,
        pool_pre_ping=True,
        echo=False,
        connect_args={"command_timeout": 5.0},
    )

    # Add structured logging for every DB query
    import time
    from sqlalchemy import event

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        conn.info.setdefault("query_start_time", []).append(time.time())

    @event.listens_for(engine.sync_engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        start_time = conn.info["query_start_time"].pop(-1)
        total = time.time() - start_time
        logger.info(
            f"DB_QUERY SUCCESS | duration_ms={total * 1000:.2f} | query={statement[:200]}..."
        )

    @event.listens_for(engine.sync_engine, "handle_error")
    def handle_error(context):
        if (
            "query_start_time" in context.connection.info
            and context.connection.info["query_start_time"]
        ):
            start_time = context.connection.info["query_start_time"].pop(-1)
            total = time.time() - start_time
            logger.error(
                f"DB_QUERY ERROR | duration_ms={total * 1000:.2f} | error={context.original_exception}"
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
        raise Exception("Database engine not initialized successfully.")

    async with async_session_maker() as session:
        yield session
