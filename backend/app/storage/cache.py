"""Redis cache layer for hot data.

Provides caching for:
- Active signals (FastSignal)
- Latest prices by symbol
- Streak tracker state

Uses orjson for fast serialization/deserialization.
"""

from __future__ import annotations

import logging
from typing import Any

import orjson
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from app.config import get_settings

logger = logging.getLogger(__name__)

# Global connection pool
_pool: ConnectionPool | None = None
_client: redis.Redis | None = None


# =============================================================================
# Key prefixes for different data types
# =============================================================================

KEY_PREFIX_SIGNAL = "signal:"        # Active signals: signal:{id}
KEY_PREFIX_SIGNALS = "signals:"      # Signal list by symbol: signals:{symbol}
KEY_PREFIX_PRICE = "price:"          # Latest price: price:{symbol}
KEY_PREFIX_STREAK = "streak"         # Streak tracker state


# =============================================================================
# Connection management
# =============================================================================

async def init_cache() -> None:
    """Initialize Redis connection pool."""
    global _pool, _client

    if _client is not None:
        return

    settings = get_settings()
    _pool = ConnectionPool.from_url(
        settings.redis_url,
        max_connections=20,
        decode_responses=False,  # We handle encoding ourselves with orjson
    )
    _client = redis.Redis(connection_pool=_pool)

    # Test connection
    try:
        await _client.ping()
        logger.info(f"Redis connected: {settings.redis_url}")
    except redis.ConnectionError as e:
        logger.warning(f"Redis connection failed: {e}. Cache will be disabled.")
        _client = None
        _pool = None


async def close_cache() -> None:
    """Close Redis connection pool."""
    global _pool, _client

    if _client is not None:
        await _client.close()
        _client = None

    if _pool is not None:
        await _pool.disconnect()
        _pool = None

    logger.info("Redis connection closed")


def get_client() -> redis.Redis | None:
    """Get the Redis client instance."""
    return _client


def is_cache_available() -> bool:
    """Check if cache is available."""
    return _client is not None


# =============================================================================
# Basic operations
# =============================================================================

async def get(key: str) -> bytes | None:
    """Get a value from cache.

    Args:
        key: Cache key

    Returns:
        Raw bytes or None if not found/cache unavailable
    """
    if _client is None:
        return None

    try:
        return await _client.get(key)
    except redis.RedisError as e:
        logger.warning(f"Redis GET error: {e}")
        return None


async def set(
    key: str,
    value: bytes,
    ttl: int | None = None,
) -> bool:
    """Set a value in cache.

    Args:
        key: Cache key
        value: Raw bytes to store
        ttl: Time-to-live in seconds (None for no expiry)

    Returns:
        True if successful, False otherwise
    """
    if _client is None:
        return False

    try:
        if ttl:
            await _client.setex(key, ttl, value)
        else:
            await _client.set(key, value)
        return True
    except redis.RedisError as e:
        logger.warning(f"Redis SET error: {e}")
        return False


async def delete(key: str) -> bool:
    """Delete a key from cache.

    Args:
        key: Cache key

    Returns:
        True if deleted, False otherwise
    """
    if _client is None:
        return False

    try:
        await _client.delete(key)
        return True
    except redis.RedisError as e:
        logger.warning(f"Redis DELETE error: {e}")
        return False


async def exists(key: str) -> bool:
    """Check if a key exists in cache.

    Args:
        key: Cache key

    Returns:
        True if exists, False otherwise
    """
    if _client is None:
        return False

    try:
        return await _client.exists(key) > 0
    except redis.RedisError as e:
        logger.warning(f"Redis EXISTS error: {e}")
        return False


# =============================================================================
# JSON operations (using orjson)
# =============================================================================

async def get_json(key: str) -> Any | None:
    """Get a JSON value from cache.

    Args:
        key: Cache key

    Returns:
        Deserialized object or None
    """
    data = await get(key)
    if data is None:
        return None

    try:
        return orjson.loads(data)
    except orjson.JSONDecodeError as e:
        logger.warning(f"JSON decode error for key {key}: {e}")
        return None


async def set_json(
    key: str,
    value: Any,
    ttl: int | None = None,
) -> bool:
    """Set a JSON value in cache.

    Args:
        key: Cache key
        value: Object to serialize and store
        ttl: Time-to-live in seconds

    Returns:
        True if successful, False otherwise
    """
    try:
        data = orjson.dumps(value)
        return await set(key, data, ttl)
    except (TypeError, orjson.JSONEncodeError) as e:
        logger.warning(f"JSON encode error for key {key}: {e}")
        return False


# =============================================================================
# Set operations (for signal lists)
# =============================================================================

async def sadd(key: str, *members: str) -> int:
    """Add members to a set.

    Args:
        key: Set key
        members: Members to add

    Returns:
        Number of members added
    """
    if _client is None:
        return 0

    try:
        return await _client.sadd(key, *members)
    except redis.RedisError as e:
        logger.warning(f"Redis SADD error: {e}")
        return 0


async def srem(key: str, *members: str) -> int:
    """Remove members from a set.

    Args:
        key: Set key
        members: Members to remove

    Returns:
        Number of members removed
    """
    if _client is None:
        return 0

    try:
        return await _client.srem(key, *members)
    except redis.RedisError as e:
        logger.warning(f"Redis SREM error: {e}")
        return 0


async def smembers(key: str) -> set[str]:
    """Get all members of a set.

    Args:
        key: Set key

    Returns:
        Set of members (empty if not found)
    """
    if _client is None:
        return set()

    try:
        result = await _client.smembers(key)
        return {m.decode() if isinstance(m, bytes) else m for m in result}
    except redis.RedisError as e:
        logger.warning(f"Redis SMEMBERS error: {e}")
        return set()


# =============================================================================
# Batch operations
# =============================================================================

async def mget(keys: list[str]) -> list[bytes | None]:
    """Get multiple values at once.

    Args:
        keys: List of cache keys

    Returns:
        List of values (None for missing keys)
    """
    if _client is None or not keys:
        return [None] * len(keys)

    try:
        return await _client.mget(keys)
    except redis.RedisError as e:
        logger.warning(f"Redis MGET error: {e}")
        return [None] * len(keys)


async def mset(mapping: dict[str, bytes]) -> bool:
    """Set multiple values at once.

    Args:
        mapping: Dict of key-value pairs

    Returns:
        True if successful, False otherwise
    """
    if _client is None or not mapping:
        return False

    try:
        await _client.mset(mapping)
        return True
    except redis.RedisError as e:
        logger.warning(f"Redis MSET error: {e}")
        return False


async def delete_pattern(pattern: str) -> int:
    """Delete all keys matching a pattern.

    Args:
        pattern: Key pattern (e.g., "signal:*")

    Returns:
        Number of keys deleted
    """
    if _client is None:
        return 0

    try:
        keys = []
        async for key in _client.scan_iter(match=pattern):
            keys.append(key)

        if keys:
            return await _client.delete(*keys)
        return 0
    except redis.RedisError as e:
        logger.warning(f"Redis DELETE pattern error: {e}")
        return 0


# =============================================================================
# Health check
# =============================================================================

async def ping() -> bool:
    """Check if Redis is responsive.

    Returns:
        True if Redis responds to PING
    """
    if _client is None:
        return False

    try:
        return await _client.ping()
    except redis.RedisError:
        return False


async def get_info() -> dict:
    """Get Redis server info.

    Returns:
        Dict with server info or empty dict if unavailable
    """
    if _client is None:
        return {"status": "disconnected"}

    try:
        info = await _client.info()
        return {
            "status": "connected",
            "redis_version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "total_connections_received": info.get("total_connections_received"),
        }
    except redis.RedisError as e:
        return {"status": "error", "error": str(e)}
