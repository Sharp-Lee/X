"""Kline data source for backtesting.

Reads klines from PostgreSQL via the shared asyncpg pool.
No app/ dependency.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Protocol

import asyncpg

from core.models.kline import Kline

logger = logging.getLogger(__name__)


class KlineSource(Protocol):
    """Protocol for kline data access."""

    async def get_range(
        self, symbol: str, timeframe: str, start: datetime, end: datetime
    ) -> list[Kline]: ...


class PostgresKlineSource:
    """Read klines from PostgreSQL via shared asyncpg pool.

    Uses the same pool as BacktestDatabase — no extra connections.
    asyncpg returns NUMERIC columns as Decimal natively.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_range(
        self, symbol: str, timeframe: str, start: datetime, end: datetime
    ) -> list[Kline]:
        """Fetch klines in ascending time order."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT symbol, timeframe, timestamp,
                          open, high, low, close, volume
                   FROM klines
                   WHERE symbol=$1 AND timeframe=$2
                     AND timestamp >= $3 AND timestamp <= $4
                   ORDER BY timestamp ASC""",
                symbol,
                timeframe,
                start,
                end,
            )

        # asyncpg returns Numeric as Decimal natively
        return [
            Kline(
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                timestamp=row["timestamp"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
            )
            for row in rows
        ]
