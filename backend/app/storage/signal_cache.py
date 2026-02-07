"""Signal cache for fast access to active signals.

Stores active signals in Redis for:
- Fast read access (< 1ms)
- Persistence across restarts
- Shared state between processes

Data structure:
- signal:{id} -> JSON serialized FastSignal
- signals:{symbol} -> Set of signal IDs for that symbol
- signals:all -> Set of all active signal IDs
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import TYPE_CHECKING

import orjson

from app.storage import cache
from core.models.fast import FastSignal

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# TTL for signal data (24 hours - signals should resolve before this)
SIGNAL_TTL = 86400


def _signal_key(signal_id: str) -> str:
    """Get the cache key for a signal."""
    return f"{cache.KEY_PREFIX_SIGNAL}{signal_id}"


def _symbol_set_key(symbol: str) -> str:
    """Get the cache key for a symbol's signal set."""
    return f"{cache.KEY_PREFIX_SIGNALS}{symbol}"


def _all_signals_key() -> str:
    """Get the cache key for all signals set."""
    return f"{cache.KEY_PREFIX_SIGNALS}all"


def _serialize_signal(signal: FastSignal) -> bytes:
    """Serialize a FastSignal to JSON bytes."""
    data = asdict(signal)
    return orjson.dumps(data)


def _deserialize_signal(data: bytes) -> FastSignal | None:
    """Deserialize JSON bytes to a FastSignal."""
    try:
        obj = orjson.loads(data)
        return FastSignal(**obj)
    except (orjson.JSONDecodeError, TypeError, KeyError) as e:
        logger.warning(f"Failed to deserialize signal: {e}")
        return None


async def cache_signal(signal: FastSignal) -> bool:
    """Cache an active signal.

    Uses Redis pipeline to batch 3 operations into 1 network round-trip:
    - Store signal data
    - Add to symbol set
    - Add to all signals set

    Args:
        signal: The FastSignal to cache

    Returns:
        True if cached successfully
    """
    if not cache.is_cache_available():
        return False

    client = cache.get_client()
    if client is None:
        return False

    try:
        # Serialize signal
        data = _serialize_signal(signal)
        signal_key = _signal_key(signal.id)
        symbol_key = _symbol_set_key(signal.symbol)
        all_key = _all_signals_key()

        # Use pipeline to batch all 3 operations
        async with client.pipeline(transaction=False) as pipe:
            pipe.setex(signal_key, SIGNAL_TTL, data)
            pipe.sadd(symbol_key, signal.id)
            pipe.sadd(all_key, signal.id)
            await pipe.execute()

        logger.debug(f"Cached signal {signal.id}")
        return True

    except Exception as e:
        logger.warning(f"Failed to cache signal {signal.id}: {e}")
        return False


async def get_signal(signal_id: str) -> FastSignal | None:
    """Get a cached signal by ID.

    Args:
        signal_id: The signal ID

    Returns:
        FastSignal or None if not found
    """
    if not cache.is_cache_available():
        return None

    data = await cache.get(_signal_key(signal_id))
    if data is None:
        return None

    return _deserialize_signal(data)


async def update_signal(signal: FastSignal) -> bool:
    """Update a cached signal.

    Args:
        signal: The updated FastSignal

    Returns:
        True if updated successfully
    """
    if not cache.is_cache_available():
        return False

    try:
        data = _serialize_signal(signal)
        signal_key = _signal_key(signal.id)
        await cache.set(signal_key, data, ttl=SIGNAL_TTL)
        return True
    except Exception as e:
        logger.warning(f"Failed to update cached signal {signal.id}: {e}")
        return False


async def remove_signal(signal_id: str, symbol: str) -> bool:
    """Remove a signal from cache.

    Uses Redis pipeline to batch 3 operations into 1 network round-trip:
    - Delete signal data
    - Remove from symbol set
    - Remove from all signals set

    Args:
        signal_id: The signal ID
        symbol: The symbol (needed to remove from symbol set)

    Returns:
        True if removed successfully
    """
    if not cache.is_cache_available():
        return False

    client = cache.get_client()
    if client is None:
        return False

    try:
        signal_key = _signal_key(signal_id)
        symbol_key = _symbol_set_key(symbol)
        all_key = _all_signals_key()

        # Use pipeline to batch all 3 operations
        async with client.pipeline(transaction=False) as pipe:
            pipe.delete(signal_key)
            pipe.srem(symbol_key, signal_id)
            pipe.srem(all_key, signal_id)
            await pipe.execute()

        logger.debug(f"Removed signal {signal_id} from cache")
        return True

    except Exception as e:
        logger.warning(f"Failed to remove signal {signal_id}: {e}")
        return False


async def get_signals_by_symbol(symbol: str) -> list[FastSignal]:
    """Get all active signals for a symbol.

    Args:
        symbol: The trading symbol

    Returns:
        List of FastSignals (empty if none found)
    """
    if not cache.is_cache_available():
        return []

    try:
        # Get signal IDs for this symbol
        signal_ids = await cache.smembers(_symbol_set_key(symbol))
        if not signal_ids:
            return []

        # Get all signal data
        keys = [_signal_key(sid) for sid in signal_ids]
        results = await cache.mget(keys)

        signals = []
        for data in results:
            if data is not None:
                signal = _deserialize_signal(data)
                if signal is not None:
                    signals.append(signal)

        return signals

    except Exception as e:
        logger.warning(f"Failed to get signals for {symbol}: {e}")
        return []


async def get_all_signals() -> list[FastSignal]:
    """Get all active signals from cache.

    Returns:
        List of all FastSignals (empty if none found)
    """
    if not cache.is_cache_available():
        return []

    try:
        # Get all signal IDs
        signal_ids = await cache.smembers(_all_signals_key())
        if not signal_ids:
            return []

        # Get all signal data
        keys = [_signal_key(sid) for sid in signal_ids]
        results = await cache.mget(keys)

        signals = []
        for data in results:
            if data is not None:
                signal = _deserialize_signal(data)
                if signal is not None:
                    signals.append(signal)

        return signals

    except Exception as e:
        logger.warning(f"Failed to get all signals: {e}")
        return []


async def get_signal_count() -> int:
    """Get the count of active signals in cache.

    Returns:
        Number of active signals
    """
    if not cache.is_cache_available():
        return 0

    try:
        signal_ids = await cache.smembers(_all_signals_key())
        return len(signal_ids)
    except Exception:
        return 0


async def clear_all_signals() -> int:
    """Clear all signals from cache.

    Returns:
        Number of signals cleared
    """
    if not cache.is_cache_available():
        return 0

    try:
        # Get all signal IDs first
        signal_ids = await cache.smembers(_all_signals_key())
        count = len(signal_ids)

        if signal_ids:
            # Delete all signal keys
            await cache.delete_pattern(f"{cache.KEY_PREFIX_SIGNAL}*")
            # Delete all symbol sets
            await cache.delete_pattern(f"{cache.KEY_PREFIX_SIGNALS}*")

        return count

    except Exception as e:
        logger.warning(f"Failed to clear signals: {e}")
        return 0


async def sync_from_db(signals: list[FastSignal]) -> int:
    """Sync signals from database to cache.

    Used on startup to populate cache from DB.

    Args:
        signals: List of active FastSignals from database

    Returns:
        Number of signals cached
    """
    if not cache.is_cache_available():
        return 0

    count = 0
    for signal in signals:
        if await cache_signal(signal):
            count += 1

    logger.info(f"Synced {count} signals to cache")
    return count
