"""Price cache for fast access to latest prices.

Stores the latest price for each symbol in Redis with TTL.
Used for quick price lookups without querying the exchange.

Data structure:
- price:{symbol} -> JSON {price, timestamp, volume}

Uses batched updates to reduce Redis connection pressure.
"""

from __future__ import annotations

import asyncio
import logging
import time

import orjson

from app.storage import cache

logger = logging.getLogger(__name__)

# TTL for price data (60 seconds - prices become stale quickly)
PRICE_TTL = 60

# Batch flush interval (in seconds)
# Flush all pending updates to Redis at this interval
BATCH_FLUSH_INTERVAL = 1.0

# Memory cleanup settings
# Maximum symbols to keep in memory (prevents unbounded growth)
MAX_CACHED_SYMBOLS = 100
# Cleanup interval (check for stale entries every N flushes)
CLEANUP_INTERVAL_FLUSHES = 60
# Maximum age for in-memory prices (seconds) - older entries are removed
MAX_PRICE_AGE = 300  # 5 minutes

# Track latest price per symbol (in-memory, for batched updates)
_pending_prices: dict[str, dict] = {}
# Track which symbols have pending updates
_dirty_symbols: set[str] = set()
# Last flush time
_last_flush: float = 0.0
# Flush counter for periodic cleanup
_flush_count: int = 0
# Lock for thread safety (protects all global state)
_state_lock: asyncio.Lock | None = None


def _price_key(symbol: str) -> str:
    """Get the cache key for a symbol's price."""
    return f"{cache.KEY_PREFIX_PRICE}{symbol}"


def _get_state_lock() -> asyncio.Lock:
    """Get or create the state lock.

    Note: This uses lazy initialization which has a small race window on first call.
    In practice, this is acceptable because:
    1. The lock is created once and reused
    2. Even if two locks are created, only one will be stored
    """
    global _state_lock
    if _state_lock is None:
        _state_lock = asyncio.Lock()
    return _state_lock


def _cleanup_stale_prices() -> int:
    """Remove stale prices from in-memory cache.

    Called periodically to prevent unbounded memory growth.
    Removes entries older than MAX_PRICE_AGE or when cache exceeds MAX_CACHED_SYMBOLS.

    Note: Must be called while holding _state_lock.

    Returns:
        Number of entries removed
    """
    global _pending_prices

    if not _pending_prices:
        return 0

    now = time.time()
    removed = 0

    # Remove entries older than MAX_PRICE_AGE
    stale_symbols = [
        symbol for symbol, data in _pending_prices.items()
        if now - data.get("timestamp", 0) > MAX_PRICE_AGE
    ]
    for symbol in stale_symbols:
        del _pending_prices[symbol]
        _dirty_symbols.discard(symbol)
        removed += 1

    # If still over limit, remove oldest entries
    if len(_pending_prices) > MAX_CACHED_SYMBOLS:
        # Sort by timestamp and keep only the most recent
        sorted_symbols = sorted(
            _pending_prices.keys(),
            key=lambda s: _pending_prices[s].get("timestamp", 0),
            reverse=True
        )
        for symbol in sorted_symbols[MAX_CACHED_SYMBOLS:]:
            del _pending_prices[symbol]
            _dirty_symbols.discard(symbol)
            removed += 1

    if removed > 0:
        logger.debug(f"Cleaned up {removed} stale price entries")

    return removed


async def update_price(
    symbol: str,
    price: float,
    timestamp: float | None = None,
    volume: float | None = None,
) -> bool:
    """Update the latest price for a symbol (batched).

    To avoid overwhelming Redis with updates (aggTrades can be 1000+/sec),
    we batch updates and flush them periodically using Redis pipeline.
    The most recent price is always stored in memory for immediate access.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        price: Latest price
        timestamp: Unix timestamp (defaults to now)
        volume: Trade volume (optional)

    Returns:
        True if updated successfully
    """
    global _last_flush
    now = time.time()
    ts = timestamp or now

    # Update in-memory cache with lock protection
    async with _get_state_lock():
        _pending_prices[symbol] = {
            "price": price,
            "timestamp": ts,
            "volume": volume,
        }
        _dirty_symbols.add(symbol)
        should_flush = now - _last_flush >= BATCH_FLUSH_INTERVAL

    # Flush outside lock to avoid blocking other updates
    if should_flush:
        await flush_pending_prices()

    return True


async def flush_pending_prices() -> bool:
    """Flush all pending price updates to Redis using pipeline.

    Uses Redis pipeline to batch multiple SET operations into a single
    network round-trip, drastically reducing connection pressure.

    Also performs periodic memory cleanup to prevent unbounded growth.

    Returns:
        True if flush was successful
    """
    global _last_flush, _flush_count

    if not cache.is_cache_available():
        return False

    async with _get_state_lock():
        # Early exit if nothing to flush
        if not _dirty_symbols:
            return True

        # Periodic cleanup to prevent memory leak
        _flush_count += 1
        if _flush_count >= CLEANUP_INTERVAL_FLUSHES:
            _cleanup_stale_prices()
            _flush_count = 0

        client = cache.get_client()
        if client is None:
            return False

        try:
            # Snapshot data while holding lock
            symbols_to_flush = list(_dirty_symbols)
            data_to_flush = {
                symbol: orjson.dumps(_pending_prices[symbol])
                for symbol in symbols_to_flush
                if symbol in _pending_prices
            }

            # Clear dirty set before releasing lock
            _dirty_symbols.clear()
            _last_flush = time.time()

        except Exception as e:
            logger.warning(f"Failed to prepare flush data: {e}")
            return False

    # Execute Redis operations outside lock to avoid blocking updates
    try:
        async with client.pipeline(transaction=False) as pipe:
            for symbol, data in data_to_flush.items():
                key = _price_key(symbol)
                pipe.setex(key, PRICE_TTL, data)

            # Execute all commands in one round-trip
            await pipe.execute()

        return True

    except Exception as e:
        logger.warning(f"Failed to flush prices to Redis: {e}")
        return False


def get_price_immediate(symbol: str) -> dict | None:
    """Get the latest price from in-memory cache (non-async, instant).

    This returns the most recent price even if it hasn't been synced to Redis yet.

    Note: This is a synchronous function that reads from the dict without lock.
    This is safe because dict.get() is atomic in Python (GIL protected),
    and we only need a consistent snapshot of a single symbol's data.

    Args:
        symbol: Trading symbol

    Returns:
        Dict with price, timestamp, volume or None if not found
    """
    # dict.get() is atomic in Python due to GIL - safe for read
    return _pending_prices.get(symbol)


async def get_price(symbol: str) -> dict | None:
    """Get the latest price for a symbol.

    Args:
        symbol: Trading symbol

    Returns:
        Dict with price, timestamp, volume or None if not found
    """
    if not cache.is_cache_available():
        return None

    key = _price_key(symbol)
    return await cache.get_json(key)


async def get_price_value(symbol: str) -> float | None:
    """Get just the price value for a symbol.

    Args:
        symbol: Trading symbol

    Returns:
        Price as float or None if not found
    """
    data = await get_price(symbol)
    if data is None:
        return None
    return data.get("price")


async def get_prices(symbols: list[str]) -> dict[str, dict | None]:
    """Get prices for multiple symbols.

    Args:
        symbols: List of trading symbols

    Returns:
        Dict mapping symbol to price data (or None if not found)
    """
    if not cache.is_cache_available() or not symbols:
        return {s: None for s in symbols}

    try:
        keys = [_price_key(s) for s in symbols]
        results = await cache.mget(keys)

        prices = {}
        for symbol, data in zip(symbols, results):
            if data is not None:
                try:
                    prices[symbol] = orjson.loads(data)
                except orjson.JSONDecodeError:
                    prices[symbol] = None
            else:
                prices[symbol] = None

        return prices

    except Exception as e:
        logger.warning(f"Failed to get prices: {e}")
        return {s: None for s in symbols}


async def delete_price(symbol: str) -> bool:
    """Delete the cached price for a symbol.

    Args:
        symbol: Trading symbol

    Returns:
        True if deleted successfully
    """
    if not cache.is_cache_available():
        return False

    return await cache.delete(_price_key(symbol))


async def get_all_prices() -> dict[str, dict]:
    """Get all cached prices.

    Returns:
        Dict mapping symbol to price data
    """
    if not cache.is_cache_available():
        return {}

    try:
        # Scan for all price keys
        client = cache.get_client()
        if client is None:
            return {}

        prices = {}
        pattern = f"{cache.KEY_PREFIX_PRICE}*"

        async for key in client.scan_iter(match=pattern):
            # Extract symbol from key
            if isinstance(key, bytes):
                key = key.decode()
            symbol = key.replace(cache.KEY_PREFIX_PRICE, "")

            # Get price data
            data = await cache.get_json(key)
            if data is not None:
                prices[symbol] = data

        return prices

    except Exception as e:
        logger.warning(f"Failed to get all prices: {e}")
        return {}


def is_price_fresh(price_data: dict, max_age_seconds: float = 30.0) -> bool:
    """Check if a price is fresh (not stale).

    Args:
        price_data: Price data dict with timestamp
        max_age_seconds: Maximum age in seconds

    Returns:
        True if price is fresh
    """
    if price_data is None:
        return False

    timestamp = price_data.get("timestamp")
    if timestamp is None:
        return False

    age = time.time() - timestamp
    return age <= max_age_seconds
