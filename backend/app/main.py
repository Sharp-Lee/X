"""Main application entry point."""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager

# Configure logging FIRST, before any other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Reduce noise from third-party libraries (must be set before importing them)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("picows").setLevel(logging.WARNING)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

# Try to use uvloop for better performance (Unix only)
try:
    import uvloop
    uvloop.install()
    _UVLOOP_ENABLED = True
except ImportError:
    _UVLOOP_ENABLED = False

from app.api import router, manager, websocket_endpoint
from app.config import get_settings
from app.core import is_talib_available
from app.models import Outcome, SignalRecord
from app.services import DataCollector, SignalGenerator, PositionTracker
from app.storage import init_database, get_database, cache, price_cache

logger = logging.getLogger(__name__)

# Global services
data_collector: DataCollector | None = None
signal_generator: SignalGenerator | None = None
position_tracker: PositionTracker | None = None
_price_flush_task: asyncio.Task | None = None


async def _periodic_price_flush():
    """Background task to periodically flush price cache."""
    while True:
        try:
            await asyncio.sleep(2.0)  # Flush every 2 seconds
            await price_cache.flush_pending_prices()
        except asyncio.CancelledError:
            # Final flush on shutdown
            await price_cache.flush_pending_prices()
            break
        except Exception as e:
            logger.warning(f"Price cache flush error: {e}")


async def on_new_signal(signal: SignalRecord) -> None:
    """Handle new signal from signal generator."""
    # Add to position tracker
    if position_tracker:
        await position_tracker.add_signal(signal)

    # Broadcast via WebSocket
    await manager.send_signal({
        "id": signal.id,
        "symbol": signal.symbol,
        "timeframe": signal.timeframe,
        "signal_time": signal.signal_time.isoformat(),
        "direction": signal.direction.name,
        "entry_price": float(signal.entry_price),
        "tp_price": float(signal.tp_price),
        "sl_price": float(signal.sl_price),
        "streak_at_signal": signal.streak_at_signal,
    })


async def on_outcome(signal: SignalRecord, outcome: Outcome) -> None:
    """Handle signal outcome (TP/SL hit)."""
    # Update streak tracker and release position lock
    if signal_generator:
        await signal_generator.record_outcome(
            outcome,
            symbol=signal.symbol,
            timeframe=signal.timeframe,
        )

    # Broadcast via WebSocket
    await manager.send_outcome(
        signal_id=signal.id,
        outcome=outcome.value,
        exit_price=float(signal.outcome_price) if signal.outcome_price else 0,
    )


async def on_kline_update(kline) -> None:
    """Handle kline update from data collector.

    This is called for both raw 1m klines and aggregated klines (3m, 5m, 15m, 30m).
    """
    if not signal_generator or not data_collector:
        return

    # Skip processing during replay (replay service handles signal generation)
    if data_collector.is_replaying or data_collector.is_buffering:
        return

    # Get buffer for the specific timeframe of this kline
    buffer = data_collector.get_kline_buffer(kline.symbol, kline.timeframe)
    if buffer and kline.is_closed:
        result = await signal_generator.process_kline(kline, buffer)

        # Update max_atr for active signals of this symbol/timeframe
        if position_tracker and result.atr is not None:
            await position_tracker.update_max_atr(
                symbol=kline.symbol,
                timeframe=kline.timeframe,
                current_atr=result.atr,
            )


async def on_aggtrade_update(trade) -> None:
    """Handle aggTrade update from data collector."""
    if position_tracker:
        await position_tracker.process_trade(trade)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global data_collector, signal_generator, position_tracker, _price_flush_task

    logger.info("Starting MSR Retest Capture system...")
    logger.info(f"Event loop: {'uvloop' if _UVLOOP_ENABLED else 'asyncio'}")
    logger.info(f"Indicators: {'TA-Lib' if is_talib_available() else 'NumPy fallback'}")

    # Track initialization state for proper cleanup on failure
    db_initialized = False
    cache_initialized = False
    services_started = False

    try:
        # Initialize database
        await init_database()
        db_initialized = True
        logger.info("Database initialized")

        # Initialize Redis cache
        await cache.init_cache()
        cache_initialized = True
        if cache.is_cache_available():
            logger.info("Redis cache initialized")
        else:
            logger.warning("Redis cache unavailable - running without caching")

        # Initialize services
        data_collector = DataCollector()
        signal_generator = SignalGenerator()
        position_tracker = PositionTracker()

        # Load streak tracker from cache
        await signal_generator.init()

        # Register callbacks
        signal_generator.on_signal(on_new_signal)
        position_tracker.on_outcome(on_outcome)
        data_collector.on_kline(on_kline_update)
        data_collector.on_aggtrade(on_aggtrade_update)

        # Connect signal generator to data collector for replay processing
        data_collector.set_signal_generator(signal_generator)

        # Load active signals
        await position_tracker.load_active_signals()

        # Start data collection (includes gap detection, backfill, and replay)
        await data_collector.start()
        services_started = True
        logger.info("Data collection started")

        # Start background price cache flush task
        _price_flush_task = asyncio.create_task(_periodic_price_flush())
        logger.info("Price cache flush task started")

    except Exception as e:
        logger.error(f"Startup failed: {e}")
        # Cleanup on startup failure
        if services_started and data_collector:
            try:
                await data_collector.stop()
            except Exception as cleanup_err:
                logger.warning(f"Error stopping data collector: {cleanup_err}")
        if cache_initialized:
            try:
                await cache.close_cache()
            except Exception as cleanup_err:
                logger.warning(f"Error closing cache: {cleanup_err}")
        if db_initialized:
            try:
                db = get_database()
                await db.close()
            except Exception as cleanup_err:
                logger.warning(f"Error closing database: {cleanup_err}")
        raise  # Re-raise to prevent app from starting in broken state

    yield

    # Shutdown
    logger.info("Shutting down...")

    # Stop background tasks
    if _price_flush_task:
        _price_flush_task.cancel()
        try:
            await _price_flush_task
        except asyncio.CancelledError:
            pass

    # Stop data collector
    if data_collector:
        await data_collector.stop()

    # Close Redis cache
    await cache.close_cache()

    # Close database connections
    try:
        db = get_database()
        await db.close()
        logger.info("Database connections closed")
    except Exception as e:
        logger.warning(f"Error closing database: {e}")

    logger.info("Shutdown complete")


# Create FastAPI app with orjson for faster JSON serialization
app = FastAPI(
    title="MSR Retest Capture",
    description="Trading signal system for crypto futures",
    version="0.1.0",
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include REST routes
app.include_router(router, prefix="/api")

# WebSocket endpoint
app.websocket("/ws")(websocket_endpoint)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "MSR Retest Capture",
        "version": "0.1.0",
        "docs": "/docs",
        "event_loop": "uvloop" if _UVLOOP_ENABLED else "asyncio",
        "indicators": "talib" if is_talib_available() else "numpy",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


def main():
    """Run the application."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()
