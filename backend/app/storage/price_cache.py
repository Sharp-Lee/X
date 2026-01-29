"""Price cache for fast access to latest prices.

Stores the latest price for each symbol in Redis with TTL.
Used for quick price lookups without querying the exchange.

Data structure:
- price:{symbol} -> JSON {price, timestamp, volume}
"""

from __future__ import annotations

import logging
import time

import orjson

from app.storage import cache

logger = logging.getLogger(__name__)

# TTL for price data (60 seconds - prices become stale quickly)
PRICE_TTL = 60


def _price_key(symbol: str) -> str:
    """Get the cache key for a symbol's price."""
    return f"{cache.KEY_PREFIX_PRICE}{symbol}"


async def update_price(
    symbol: str,
    price: float,
    timestamp: float | None = None,
    volume: float | None = None,
) -> bool:
    """Update the latest price for a symbol.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        price: Latest price
        timestamp: Unix timestamp (defaults to now)
        volume: Trade volume (optional)

    Returns:
        True if updated successfully
    """
    if not cache.is_cache_available():
        return False

    try:
        data = {
            "price": price,
            "timestamp": timestamp or time.time(),
            "volume": volume,
        }
        key = _price_key(symbol)
        return await cache.set_json(key, data, ttl=PRICE_TTL)
    except Exception as e:
        logger.warning(f"Failed to update price for {symbol}: {e}")
        return False


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
