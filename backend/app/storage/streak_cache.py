"""Streak tracker cache for fast access to win/loss statistics.

Stores per-symbol/timeframe streak tracker state in Redis for:
- Fast read access (< 1ms)
- Persistence across restarts
- Shared state between processes

Data structure:
- streak:{symbol}_{timeframe} -> JSON {current_streak, total_wins, total_losses}
"""

from __future__ import annotations

import logging

from app.storage import cache
from app.models import StreakTracker

logger = logging.getLogger(__name__)


def _streak_key(symbol: str, timeframe: str) -> str:
    """Get the cache key for a symbol/timeframe streak tracker."""
    return f"{cache.KEY_PREFIX_STREAK}{symbol}_{timeframe}"


async def save_streak(symbol: str, timeframe: str, tracker: StreakTracker) -> bool:
    """Save streak tracker state to cache.

    Args:
        symbol: Trading symbol (e.g. BTCUSDT)
        timeframe: Timeframe (e.g. 1m, 5m)
        tracker: The StreakTracker to cache

    Returns:
        True if saved successfully
    """
    if not cache.is_cache_available():
        return False

    try:
        data = {
            "current_streak": tracker.current_streak,
            "total_wins": tracker.total_wins,
            "total_losses": tracker.total_losses,
        }
        return await cache.set_json(_streak_key(symbol, timeframe), data)
    except Exception as e:
        logger.warning(f"Failed to save streak to cache: {e}")
        return False


async def load_streak(symbol: str, timeframe: str) -> StreakTracker | None:
    """Load streak tracker state from cache.

    Args:
        symbol: Trading symbol
        timeframe: Timeframe

    Returns:
        StreakTracker or None if not found
    """
    if not cache.is_cache_available():
        return None

    try:
        data = await cache.get_json(_streak_key(symbol, timeframe))
        if data is None:
            return None

        return StreakTracker(
            current_streak=data.get("current_streak", 0),
            total_wins=data.get("total_wins", 0),
            total_losses=data.get("total_losses", 0),
        )
    except Exception as e:
        logger.warning(f"Failed to load streak from cache: {e}")
        return None


async def load_all_streaks() -> dict[str, StreakTracker]:
    """Load all streak trackers from cache.

    Scans for all streak:* keys and returns a dict keyed by symbol_timeframe.

    Returns:
        Dict mapping "symbol_timeframe" to StreakTracker
    """
    if not cache.is_cache_available():
        return {}

    client = cache.get_client()
    if client is None:
        return {}

    trackers: dict[str, StreakTracker] = {}
    try:
        prefix = cache.KEY_PREFIX_STREAK
        async for key in client.scan_iter(match=f"{prefix}*"):
            key_str = key.decode() if isinstance(key, bytes) else key
            # Extract symbol_timeframe from "streak:BTCUSDT_1m"
            symbol_tf = key_str[len(prefix):]
            data = await cache.get_json(key_str)
            if data:
                trackers[symbol_tf] = StreakTracker(
                    current_streak=data.get("current_streak", 0),
                    total_wins=data.get("total_wins", 0),
                    total_losses=data.get("total_losses", 0),
                )
        return trackers
    except Exception as e:
        logger.warning(f"Failed to load all streaks from cache: {e}")
        return {}


async def clear_streak(symbol: str, timeframe: str) -> bool:
    """Clear streak tracker from cache.

    Args:
        symbol: Trading symbol
        timeframe: Timeframe

    Returns:
        True if cleared successfully
    """
    if not cache.is_cache_available():
        return False

    return await cache.delete(_streak_key(symbol, timeframe))


async def clear_all_streaks() -> int:
    """Clear all streak trackers from cache.

    Returns:
        Number of keys deleted
    """
    if not cache.is_cache_available():
        return 0

    return await cache.delete_pattern(f"{cache.KEY_PREFIX_STREAK}*")


async def get_streak_stats(symbol: str, timeframe: str) -> dict:
    """Get streak statistics from cache.

    Args:
        symbol: Trading symbol
        timeframe: Timeframe

    Returns:
        Dict with current_streak, total_wins, total_losses, win_rate
    """
    tracker = await load_streak(symbol, timeframe)
    if tracker is None:
        return {
            "current_streak": 0,
            "total_wins": 0,
            "total_losses": 0,
            "win_rate": 0.0,
        }

    return {
        "current_streak": tracker.current_streak,
        "total_wins": tracker.total_wins,
        "total_losses": tracker.total_losses,
        "win_rate": tracker.win_rate,
    }
