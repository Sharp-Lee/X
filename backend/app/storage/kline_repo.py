"""K-line data repository."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, delete, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Kline
from app.storage.database import KlineTable, get_database


class KlineRepository:
    """Repository for K-line data operations."""

    def __init__(self, session: AsyncSession | None = None):
        self._session = session

    async def _get_session(self) -> AsyncSession:
        if self._session:
            return self._session
        db = get_database()
        return db.session_factory()

    async def save(self, kline: Kline) -> None:
        """Save a single K-line (upsert)."""
        async with get_database().session() as session:
            stmt = insert(KlineTable).values(
                symbol=kline.symbol,
                timeframe=kline.timeframe,
                timestamp=kline.timestamp,
                open=kline.open,
                high=kline.high,
                low=kline.low,
                close=kline.close,
                volume=kline.volume,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol", "timeframe", "timestamp"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                },
            )
            await session.execute(stmt)

    async def save_batch(self, klines: list[Kline], chunk_size: int = 1000) -> None:
        """Save multiple K-lines in batch (upsert).

        Args:
            klines: List of Kline objects to save
            chunk_size: Maximum number of klines per insert (to avoid PostgreSQL
                        32767 parameter limit - 8 columns * 4000 = 32000)
        """
        if not klines:
            return

        async with get_database().session() as session:
            # Process in chunks to avoid PostgreSQL parameter limit
            for i in range(0, len(klines), chunk_size):
                chunk = klines[i:i + chunk_size]
                values = [
                    {
                        "symbol": k.symbol,
                        "timeframe": k.timeframe,
                        "timestamp": k.timestamp,
                        "open": k.open,
                        "high": k.high,
                        "low": k.low,
                        "close": k.close,
                        "volume": k.volume,
                    }
                    for k in chunk
                ]
                stmt = insert(KlineTable).values(values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol", "timeframe", "timestamp"],
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                    },
                )
                await session.execute(stmt)

    async def get_latest(
        self, symbol: str, timeframe: str, limit: int = 200
    ) -> list[Kline]:
        """Get the latest K-lines for a symbol."""
        async with get_database().session() as session:
            stmt = (
                select(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                )
                .order_by(KlineTable.timestamp.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                Kline(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=Decimal(str(row.open)),
                    high=Decimal(str(row.high)),
                    low=Decimal(str(row.low)),
                    close=Decimal(str(row.close)),
                    volume=Decimal(str(row.volume)),
                )
                for row in reversed(rows)
            ]

    async def get_range(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[Kline]:
        """Get K-lines within a time range."""
        async with get_database().session() as session:
            stmt = (
                select(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                    KlineTable.timestamp >= start,
                    KlineTable.timestamp <= end,
                )
                .order_by(KlineTable.timestamp.asc())
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                Kline(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=Decimal(str(row.open)),
                    high=Decimal(str(row.high)),
                    low=Decimal(str(row.low)),
                    close=Decimal(str(row.close)),
                    volume=Decimal(str(row.volume)),
                )
                for row in rows
            ]

    async def get_last_timestamp(
        self, symbol: str, timeframe: str
    ) -> datetime | None:
        """Get the timestamp of the most recent K-line."""
        async with get_database().session() as session:
            stmt = (
                select(KlineTable.timestamp)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                )
                .order_by(KlineTable.timestamp.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            return row

    async def delete_range(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> int:
        """Delete K-lines within a time range. Returns count deleted."""
        async with get_database().session() as session:
            stmt = (
                delete(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                    KlineTable.timestamp >= start,
                    KlineTable.timestamp <= end,
                )
                .returning(KlineTable.timestamp)
            )
            result = await session.execute(stmt)
            return len(result.all())

    async def get_all_timestamps(
        self,
        symbol: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[datetime]:
        """Get all K-line timestamps for gap detection.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            start: Optional start time filter
            end: Optional end time filter

        Returns:
            List of timestamps in ascending order
        """
        async with get_database().session() as session:
            stmt = (
                select(KlineTable.timestamp)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                )
            )
            if start:
                stmt = stmt.where(KlineTable.timestamp >= start)
            if end:
                stmt = stmt.where(KlineTable.timestamp <= end)
            stmt = stmt.order_by(KlineTable.timestamp.asc())

            result = await session.execute(stmt)
            return [row[0] for row in result.all()]

    async def get_before(
        self,
        symbol: str,
        timeframe: str,
        before_time: datetime,
        limit: int = 200,
    ) -> list[Kline]:
        """Get K-lines before a specific timestamp (exclusive).

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            before_time: Get klines strictly before this timestamp
            limit: Maximum number of klines to return

        Returns:
            List of klines in ascending time order (oldest first)
        """
        async with get_database().session() as session:
            stmt = (
                select(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                    KlineTable.timestamp < before_time,
                )
                .order_by(KlineTable.timestamp.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                Kline(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=Decimal(str(row.open)),
                    high=Decimal(str(row.high)),
                    low=Decimal(str(row.low)),
                    close=Decimal(str(row.close)),
                    volume=Decimal(str(row.volume)),
                )
                for row in reversed(rows)  # Return in ascending order
            ]

    async def get_latest_until(
        self,
        symbol: str,
        timeframe: str,
        until_time: datetime,
        limit: int = 200,
    ) -> list[Kline]:
        """Get K-lines up to and including a specific timestamp.

        Used to restore buffer state - includes the checkpoint kline.

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            until_time: Get klines up to and including this timestamp
            limit: Maximum number of klines to return

        Returns:
            List of klines in ascending time order (oldest first)
        """
        async with get_database().session() as session:
            stmt = (
                select(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                    KlineTable.timestamp <= until_time,  # Inclusive
                )
                .order_by(KlineTable.timestamp.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                Kline(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=Decimal(str(row.open)),
                    high=Decimal(str(row.high)),
                    low=Decimal(str(row.low)),
                    close=Decimal(str(row.close)),
                    volume=Decimal(str(row.volume)),
                )
                for row in reversed(rows)  # Return in ascending order
            ]

    async def get_after(
        self,
        symbol: str,
        timeframe: str,
        after_time: datetime,
        end_time: datetime | None = None,
    ) -> list[Kline]:
        """Get K-lines after a specific timestamp.

        Used for replay - gets klines AFTER the checkpoint (not including it).

        Args:
            symbol: Trading pair
            timeframe: K-line interval
            after_time: Get klines strictly after this timestamp
            end_time: Optional end time (defaults to no limit)

        Returns:
            List of klines in ascending time order (oldest first)
        """
        async with get_database().session() as session:
            stmt = (
                select(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                    KlineTable.timestamp > after_time,  # Strictly greater than
                )
            )
            if end_time:
                stmt = stmt.where(KlineTable.timestamp <= end_time)

            stmt = stmt.order_by(KlineTable.timestamp.asc())
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                Kline(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=Decimal(str(row.open)),
                    high=Decimal(str(row.high)),
                    low=Decimal(str(row.low)),
                    close=Decimal(str(row.close)),
                    volume=Decimal(str(row.volume)),
                )
                for row in rows
            ]

    async def get_first_timestamp(
        self, symbol: str, timeframe: str
    ) -> datetime | None:
        """Get the timestamp of the earliest K-line."""
        async with get_database().session() as session:
            stmt = (
                select(KlineTable.timestamp)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                )
                .order_by(KlineTable.timestamp.asc())
                .limit(1)
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            return row

    async def count_range(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> int:
        """Count K-lines within a time range."""
        async with get_database().session() as session:
            from sqlalchemy import func
            stmt = (
                select(func.count())
                .select_from(KlineTable)
                .where(
                    KlineTable.symbol == symbol,
                    KlineTable.timeframe == timeframe,
                    KlineTable.timestamp >= start,
                    KlineTable.timestamp <= end,
                )
            )
            result = await session.execute(stmt)
            return result.scalar_one()
