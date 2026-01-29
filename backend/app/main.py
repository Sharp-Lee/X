"""Main application entry point."""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager

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
from app.storage import init_database, cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global services
data_collector: DataCollector | None = None
signal_generator: SignalGenerator | None = None
position_tracker: PositionTracker | None = None


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
    # Update streak tracker
    if signal_generator:
        await signal_generator.record_outcome(outcome)

    # Broadcast via WebSocket
    await manager.send_outcome(
        signal_id=signal.id,
        outcome=outcome.value,
        exit_price=float(signal.outcome_price) if signal.outcome_price else 0,
    )


async def on_kline_update(kline) -> None:
    """Handle kline update from data collector."""
    if not signal_generator or not data_collector:
        return

    buffer = data_collector.get_kline_buffer(kline.symbol)
    if buffer and kline.is_closed:
        await signal_generator.process_kline(kline, buffer)


async def on_aggtrade_update(trade) -> None:
    """Handle aggTrade update from data collector."""
    if position_tracker:
        await position_tracker.process_trade(trade)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global data_collector, signal_generator, position_tracker

    logger.info("Starting MSR Retest Capture system...")
    logger.info(f"Event loop: {'uvloop' if _UVLOOP_ENABLED else 'asyncio'}")
    logger.info(f"Indicators: {'TA-Lib' if is_talib_available() else 'NumPy fallback'}")

    # Initialize database
    await init_database()
    logger.info("Database initialized")

    # Initialize Redis cache
    await cache.init_cache()
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

    # Load active signals
    await position_tracker.load_active_signals()

    # Start data collection
    await data_collector.start()
    logger.info("Data collection started")

    yield

    # Shutdown
    logger.info("Shutting down...")
    if data_collector:
        await data_collector.stop()
    await cache.close_cache()
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
