"""Streak tracker cache for fast access to win/loss statistics.

Stores streak tracker state in Redis for:
- Fast read access (< 1ms)
- Persistence across restarts
- Shared state between processes

Data structure:
- streak -> JSON {current_streak, total_wins, total_losses}
"""

from __future__ import annotations

import logging

from app.storage import cache
from app.models import StreakTracker

logger = logging.getLogger(__name__)


def _streak_key() -> str:
    """Get the cache key for streak tracker."""
    return cache.KEY_PREFIX_STREAK


async def save_streak(tracker: StreakTracker) -> bool:
    """Save streak tracker state to cache.

    Args:
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
        return await cache.set_json(_streak_key(), data)
    except Exception as e:
        logger.warning(f"Failed to save streak to cache: {e}")
        return False


async def load_streak() -> StreakTracker | None:
    """Load streak tracker state from cache.

    Returns:
        StreakTracker or None if not found
    """
    if not cache.is_cache_available():
        return None

    try:
        data = await cache.get_json(_streak_key())
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


async def clear_streak() -> bool:
    """Clear streak tracker from cache.

    Returns:
        True if cleared successfully
    """
    if not cache.is_cache_available():
        return False

    return await cache.delete(_streak_key())


async def get_streak_stats() -> dict:
    """Get streak statistics from cache.

    Returns:
        Dict with current_streak, total_wins, total_losses, win_rate
    """
    tracker = await load_streak()
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
