"""Tests for streak cache."""

import pytest
from unittest.mock import AsyncMock, patch

from app.models import Outcome, StreakTracker
from app.storage import streak_cache


class TestStreakCache:
    """Tests for streak_cache module (per-symbol/timeframe)."""

    @pytest.fixture
    def sample_tracker(self):
        """Create a sample streak tracker."""
        tracker = StreakTracker()
        tracker.current_streak = 3
        tracker.total_wins = 10
        tracker.total_losses = 5
        return tracker

    @pytest.mark.asyncio
    async def test_save_and_load_streak(self, sample_tracker):
        """Test saving and loading streak tracker for a specific symbol/timeframe."""
        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'set_json', new_callable=AsyncMock, return_value=True) as mock_set:
                result = await streak_cache.save_streak("BTCUSDT", "5m", sample_tracker)
                assert result is True
                mock_set.assert_called_once()

                call_args = mock_set.call_args
                key = call_args[0][0]
                saved_data = call_args[0][1]
                assert key == "streak:BTCUSDT_5m"
                assert saved_data['current_streak'] == 3
                assert saved_data['total_wins'] == 10
                assert saved_data['total_losses'] == 5

    @pytest.mark.asyncio
    async def test_load_streak_success(self):
        """Test loading streak from cache for a specific symbol/timeframe."""
        cached_data = {
            'current_streak': 2,
            'total_wins': 8,
            'total_losses': 4,
        }

        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'get_json', new_callable=AsyncMock, return_value=cached_data):
                tracker = await streak_cache.load_streak("BTCUSDT", "1m")

                assert tracker is not None
                assert tracker.current_streak == 2
                assert tracker.total_wins == 8
                assert tracker.total_losses == 4

    @pytest.mark.asyncio
    async def test_load_streak_cache_unavailable(self):
        """Test loading streak when cache is unavailable."""
        with patch.object(streak_cache.cache, 'is_cache_available', return_value=False):
            tracker = await streak_cache.load_streak("BTCUSDT", "1m")
            assert tracker is None

    @pytest.mark.asyncio
    async def test_load_streak_not_found(self):
        """Test loading streak when not in cache."""
        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'get_json', new_callable=AsyncMock, return_value=None):
                tracker = await streak_cache.load_streak("ETHUSDT", "3m")
                assert tracker is None

    @pytest.mark.asyncio
    async def test_get_streak_stats_with_data(self):
        """Test getting streak stats when data exists."""
        cached_data = {
            'current_streak': -2,
            'total_wins': 5,
            'total_losses': 10,
        }

        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'get_json', new_callable=AsyncMock, return_value=cached_data):
                stats = await streak_cache.get_streak_stats("BTCUSDT", "5m")

                assert stats['current_streak'] == -2
                assert stats['total_wins'] == 5
                assert stats['total_losses'] == 10
                assert stats['win_rate'] == pytest.approx(5 / 15, rel=1e-6)

    @pytest.mark.asyncio
    async def test_get_streak_stats_empty(self):
        """Test getting streak stats when no data."""
        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'get_json', new_callable=AsyncMock, return_value=None):
                stats = await streak_cache.get_streak_stats("BTCUSDT", "1m")

                assert stats['current_streak'] == 0
                assert stats['total_wins'] == 0
                assert stats['total_losses'] == 0
                assert stats['win_rate'] == 0.0

    @pytest.mark.asyncio
    async def test_clear_streak(self):
        """Test clearing streak from cache for a specific symbol/timeframe."""
        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'delete', new_callable=AsyncMock, return_value=True) as mock_delete:
                result = await streak_cache.clear_streak("BTCUSDT", "5m")
                assert result is True
                mock_delete.assert_called_once_with("streak:BTCUSDT_5m")

    @pytest.mark.asyncio
    async def test_clear_all_streaks(self):
        """Test clearing all streaks from cache."""
        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'delete_pattern', new_callable=AsyncMock, return_value=3) as mock_del:
                result = await streak_cache.clear_all_streaks()
                assert result == 3
                mock_del.assert_called_once_with("streak:*")

    @pytest.mark.asyncio
    async def test_different_symbols_have_different_keys(self, sample_tracker):
        """Test that different symbol/timeframe combos use different cache keys."""
        calls = []

        async def mock_set_json(key, data):
            calls.append(key)
            return True

        with patch.object(streak_cache.cache, 'is_cache_available', return_value=True):
            with patch.object(streak_cache.cache, 'set_json', side_effect=mock_set_json):
                await streak_cache.save_streak("BTCUSDT", "1m", sample_tracker)
                await streak_cache.save_streak("BTCUSDT", "5m", sample_tracker)
                await streak_cache.save_streak("ETHUSDT", "1m", sample_tracker)

                assert calls == [
                    "streak:BTCUSDT_1m",
                    "streak:BTCUSDT_5m",
                    "streak:ETHUSDT_1m",
                ]
