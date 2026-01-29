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

    async def save_batch(self, klines: list[Kline]) -> None:
        """Save multiple K-lines in batch (upsert)."""
        if not klines:
            return

        async with get_database().session() as session:
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
                for k in klines
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
