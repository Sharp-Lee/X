"""Database connection and table definitions."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

from app.config import get_settings

Base = declarative_base()


class KlineTable(Base):
    """K-line data table (TimescaleDB hypertable)."""

    __tablename__ = "klines"

    symbol = Column(String(20), primary_key=True)
    timeframe = Column(String(10), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    open = Column(Numeric(20, 8), nullable=False)
    high = Column(Numeric(20, 8), nullable=False)
    low = Column(Numeric(20, 8), nullable=False)
    close = Column(Numeric(20, 8), nullable=False)
    volume = Column(Numeric(30, 8), nullable=False)

    __table_args__ = (
        Index("idx_klines_symbol_timeframe", "symbol", "timeframe"),
    )


class AggTradeTable(Base):
    """Aggregated trade data table (TimescaleDB hypertable)."""

    __tablename__ = "aggtrades"

    symbol = Column(String(20), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    agg_trade_id = Column(BigInteger, primary_key=True)
    price = Column(Numeric(20, 8), nullable=False)
    quantity = Column(Numeric(30, 8), nullable=False)
    is_buyer_maker = Column(Boolean, nullable=False)

    __table_args__ = (
        Index("idx_aggtrades_symbol_tradeid", "symbol", "agg_trade_id"),
    )


class MsrSignalTable(Base):
    """MSR strategy signal records table."""

    __tablename__ = "msr_signals"

    id = Column(String(36), primary_key=True)
    strategy = Column(String(50), nullable=False, default="msr_retest_capture")
    symbol = Column(String(20), nullable=False)
    timeframe = Column(String(10), nullable=False)
    signal_time = Column(DateTime(timezone=True), nullable=False)
    direction = Column(Integer, nullable=False)  # 1 = LONG, -1 = SHORT
    entry_price = Column(Numeric(20, 8), nullable=False)
    tp_price = Column(Numeric(20, 8), nullable=False)
    sl_price = Column(Numeric(20, 8), nullable=False)
    atr_at_signal = Column(Numeric(20, 8), default=0)
    max_atr = Column(Numeric(20, 8), default=0)
    streak_at_signal = Column(Integer, default=0)
    mae_ratio = Column(Numeric(10, 6), default=0)
    mfe_ratio = Column(Numeric(10, 6), default=0)
    outcome = Column(String(10), default="active")
    outcome_time = Column(DateTime(timezone=True), nullable=True)
    outcome_price = Column(Numeric(20, 8), nullable=True)

    __table_args__ = (
        Index("idx_msr_signals_symbol_time", "symbol", "signal_time"),
        Index("idx_msr_signals_outcome", "outcome"),
        Index("idx_msr_signals_symbol_tf_outcome", "symbol", "timeframe", "outcome"),
    )


# Backward-compat alias so existing code referencing SignalTable still works
SignalTable = MsrSignalTable


class EmaSignalTable(Base):
    """EMA Crossover strategy signal records table."""

    __tablename__ = "ema_signals"

    id = Column(String(36), primary_key=True)
    strategy = Column(String(50), nullable=False, default="ema_crossover")
    symbol = Column(String(20), nullable=False)
    timeframe = Column(String(10), nullable=False)
    signal_time = Column(DateTime(timezone=True), nullable=False)
    direction = Column(Integer, nullable=False)  # 1 = LONG, -1 = SHORT
    entry_price = Column(Numeric(20, 8), nullable=False)
    tp_price = Column(Numeric(20, 8), nullable=False)
    sl_price = Column(Numeric(20, 8), nullable=False)
    ema_fast = Column(Numeric(20, 8), default=0)
    ema_slow = Column(Numeric(20, 8), default=0)
    atr_at_signal = Column(Numeric(20, 8), default=0)
    mae_ratio = Column(Numeric(10, 6), default=0)
    mfe_ratio = Column(Numeric(10, 6), default=0)
    outcome = Column(String(10), default="active")
    outcome_time = Column(DateTime(timezone=True), nullable=True)
    outcome_price = Column(Numeric(20, 8), nullable=True)

    __table_args__ = (
        Index("idx_ema_signals_symbol_time", "symbol", "signal_time"),
        Index("idx_ema_signals_outcome", "outcome"),
        Index("idx_ema_signals_symbol_tf_outcome", "symbol", "timeframe", "outcome"),
    )


class ProcessingStateTable(Base):
    """Processing state table for tracking K-line replay progress.

    Used to ensure signal determinism across restarts:
    - system_start_time: First-ever startup time (never changes)
    - last_processed_time: Last successfully processed kline timestamp
    - state_status: 'pending' during replay, 'confirmed' after commit
    """

    __tablename__ = "processing_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    timeframe = Column(String(10), nullable=False)
    system_start_time = Column(DateTime(timezone=True), nullable=False)
    last_processed_time = Column(DateTime(timezone=True), nullable=False)
    state_status = Column(String(20), default="confirmed")  # 'pending' | 'confirmed'
    created_at = Column(DateTime(timezone=True), server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), server_default=text("NOW()"), onupdate=text("NOW()"))

    __table_args__ = (
        Index("idx_processing_state_symbol_tf", "symbol", "timeframe", unique=True),
    )


class Database:
    """Database connection manager."""

    def __init__(self, database_url: str | None = None):
        settings = get_settings()
        url = database_url or settings.database_url

        # Convert postgresql:// to postgresql+asyncpg://
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

        # Connection pool configuration for high-concurrency trading system
        # - pool_size: Base number of persistent connections
        # - max_overflow: Additional connections allowed under load
        # - pool_pre_ping: Validate connections before use (detect stale connections)
        # - pool_recycle: Recycle connections after 1 hour to prevent stale connections
        # - pool_timeout: Max wait time for a connection from pool
        #
        # Sizing: 5 symbols × 5 timeframes = 25 concurrent operations possible
        # Plus signal tracking, position updates, and burst operations
        self.engine = create_async_engine(
            url,
            echo=settings.debug,
            pool_size=20,          # Base connections for steady-state operations
            max_overflow=30,       # Allow burst up to 50 total connections
            pool_pre_ping=True,    # Validate before use
            pool_recycle=3600,     # Recycle every hour
            pool_timeout=30,       # Wait max 30s for connection
            connect_args={
                "timeout": 10,                 # Connection timeout
                "command_timeout": 60,         # Query timeout
                "server_settings": {
                    "statement_timeout": "60000",  # 60s statement timeout
                },
            },
        )
        self.session_factory = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def create_tables(self) -> None:
        """Create all tables and configure TimescaleDB hypertables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

            # Create TimescaleDB hypertables
            # Note: These will fail silently if already created
            try:
                await conn.execute(
                    text(
                        "SELECT create_hypertable('klines', 'timestamp', "
                        "if_not_exists => TRUE, migrate_data => TRUE)"
                    )
                )
            except Exception:
                pass  # Already a hypertable

            try:
                await conn.execute(
                    text(
                        "SELECT create_hypertable('aggtrades', 'timestamp', "
                        "if_not_exists => TRUE, migrate_data => TRUE)"
                    )
                )
            except Exception:
                pass  # Already a hypertable

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a database session."""
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def close(self) -> None:
        """Close database connection."""
        await self.engine.dispose()


# Global database instance
_db: Database | None = None


def get_database() -> Database:
    """Get the global database instance."""
    global _db
    if _db is None:
        _db = Database()
    return _db


async def init_database() -> Database:
    """Initialize the database and create tables."""
    db = get_database()
    await db.create_tables()
    return db
