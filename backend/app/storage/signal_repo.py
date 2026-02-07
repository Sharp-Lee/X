"""Signal and AggTrade data repositories."""

from datetime import datetime
from decimal import Decimal

import asyncio

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AggTrade, Direction, Outcome, SignalRecord
from app.storage.database import AggTradeTable, SignalTable, get_database


class SignalRepository:
    """Repository for signal data operations."""

    async def save(self, signal: SignalRecord) -> None:
        """Save a new signal record."""
        async with get_database().session() as session:
            stmt = insert(SignalTable).values(
                id=signal.id,
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                signal_time=signal.signal_time,
                direction=signal.direction.value,
                entry_price=signal.entry_price,
                tp_price=signal.tp_price,
                sl_price=signal.sl_price,
                atr_at_signal=signal.atr_at_signal,
                max_atr=signal.max_atr,
                streak_at_signal=signal.streak_at_signal,
                mae_ratio=signal.mae_ratio,
                mfe_ratio=signal.mfe_ratio,
                outcome=signal.outcome.value,
                outcome_time=signal.outcome_time,
                outcome_price=signal.outcome_price,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "max_atr": stmt.excluded.max_atr,
                    "mae_ratio": stmt.excluded.mae_ratio,
                    "mfe_ratio": stmt.excluded.mfe_ratio,
                    "outcome": stmt.excluded.outcome,
                    "outcome_time": stmt.excluded.outcome_time,
                    "outcome_price": stmt.excluded.outcome_price,
                },
            )
            await session.execute(stmt)

    async def update_outcome(
        self,
        signal_id: str,
        mae_ratio: Decimal,
        mfe_ratio: Decimal,
        outcome: Outcome,
        outcome_time: datetime | None = None,
        outcome_price: Decimal | None = None,
        max_atr: Decimal | None = None,
    ) -> None:
        """Update signal outcome, MAE/MFE ratios, and max_atr."""
        async with get_database().session() as session:
            values = {
                "mae_ratio": mae_ratio,
                "mfe_ratio": mfe_ratio,
                "outcome": outcome.value,
                "outcome_time": outcome_time,
                "outcome_price": outcome_price,
            }
            if max_atr is not None:
                values["max_atr"] = max_atr

            stmt = (
                update(SignalTable)
                .where(SignalTable.id == signal_id)
                .values(**values)
            )
            await session.execute(stmt)

    async def get_active(self, symbol: str | None = None) -> list[SignalRecord]:
        """Get all active signals, optionally filtered by symbol."""
        async with get_database().session() as session:
            stmt = select(SignalTable).where(SignalTable.outcome == "active")
            if symbol:
                stmt = stmt.where(SignalTable.symbol == symbol)
            stmt = stmt.order_by(SignalTable.signal_time.desc())

            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [self._row_to_signal(row) for row in rows]

    async def get_recent(
        self, limit: int = 100, symbol: str | None = None
    ) -> list[SignalRecord]:
        """Get recent signals."""
        async with get_database().session() as session:
            stmt = select(SignalTable)
            if symbol:
                stmt = stmt.where(SignalTable.symbol == symbol)
            stmt = stmt.order_by(SignalTable.signal_time.desc()).limit(limit)

            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [self._row_to_signal(row) for row in rows]

    async def get_by_id(self, signal_id: str) -> SignalRecord | None:
        """Get a signal by ID."""
        async with get_database().session() as session:
            stmt = select(SignalTable).where(SignalTable.id == signal_id)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()

            if row is None:
                return None
            return self._row_to_signal(row)

    async def get_range(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> list[SignalRecord]:
        """Get signals within a time range."""
        async with get_database().session() as session:
            stmt = (
                select(SignalTable)
                .where(
                    SignalTable.symbol == symbol,
                    SignalTable.signal_time >= start,
                    SignalTable.signal_time <= end,
                )
                .order_by(SignalTable.signal_time.asc())
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [self._row_to_signal(row) for row in rows]

    async def get_stats(self, symbol: str | None = None) -> dict:
        """Get signal statistics (win/loss counts).

        Args:
            symbol: Optional symbol filter

        Returns:
            Dict with tp_count, sl_count, active_count, total_count, win_rate
        """
        async with get_database().session() as session:
            # Count by outcome
            stmt = select(
                SignalTable.outcome,
                func.count().label("count"),
            ).group_by(SignalTable.outcome)

            if symbol:
                stmt = stmt.where(SignalTable.symbol == symbol)

            result = await session.execute(stmt)
            rows = result.all()

            counts = {"tp": 0, "sl": 0, "active": 0}
            for row in rows:
                counts[row.outcome] = row.count

            total = counts["tp"] + counts["sl"]
            win_rate = counts["tp"] / total if total > 0 else 0.0

            return {
                "tp_count": counts["tp"],
                "sl_count": counts["sl"],
                "active_count": counts["active"],
                "total_count": total,
                "win_rate": win_rate,
            }

    # ---------- Analytics Methods ----------

    async def get_analytics_summary(self, days: int = 30) -> dict:
        """Get combined analytics summary in one call.

        Runs all analytics queries concurrently for minimal latency.
        """
        (
            by_symbol,
            by_timeframe,
            by_direction,
            expectancy,
            daily,
            mae_mfe,
        ) = await asyncio.gather(
            self._get_by_symbol(),
            self._get_by_timeframe(),
            self._get_by_direction(),
            self._get_expectancy(),
            self._get_daily(days=days),
            self._get_mae_mfe(),
        )
        return {
            "by_symbol": by_symbol,
            "by_timeframe": by_timeframe,
            "by_direction": by_direction,
            "expectancy": expectancy,
            "daily": daily,
            "mae_mfe": mae_mfe,
        }

    async def _get_by_symbol(self) -> list[dict]:
        """Win rate breakdown by symbol."""
        async with get_database().session() as session:
            result = await session.execute(text("""
                SELECT
                    symbol,
                    COUNT(*) FILTER (WHERE outcome = 'tp') AS wins,
                    COUNT(*) FILTER (WHERE outcome = 'sl') AS losses,
                    COUNT(*) AS total,
                    ROUND(
                        COUNT(*) FILTER (WHERE outcome = 'tp')::numeric
                        / NULLIF(COUNT(*), 0) * 100, 2
                    ) AS win_rate
                FROM signals
                WHERE outcome != 'active'
                GROUP BY symbol
                ORDER BY win_rate DESC
            """))
            return [dict(row._mapping) for row in result.all()]

    async def _get_by_timeframe(self) -> list[dict]:
        """Win rate breakdown by timeframe."""
        async with get_database().session() as session:
            result = await session.execute(text("""
                SELECT
                    timeframe,
                    COUNT(*) FILTER (WHERE outcome = 'tp') AS wins,
                    COUNT(*) FILTER (WHERE outcome = 'sl') AS losses,
                    COUNT(*) AS total,
                    ROUND(
                        COUNT(*) FILTER (WHERE outcome = 'tp')::numeric
                        / NULLIF(COUNT(*), 0) * 100, 2
                    ) AS win_rate
                FROM signals
                WHERE outcome != 'active'
                GROUP BY timeframe
                ORDER BY win_rate DESC
            """))
            return [dict(row._mapping) for row in result.all()]

    async def _get_by_direction(self) -> list[dict]:
        """Win rate breakdown by direction."""
        async with get_database().session() as session:
            result = await session.execute(text("""
                SELECT
                    CASE WHEN direction = 1 THEN 'LONG' ELSE 'SHORT' END AS direction,
                    COUNT(*) FILTER (WHERE outcome = 'tp') AS wins,
                    COUNT(*) FILTER (WHERE outcome = 'sl') AS losses,
                    COUNT(*) AS total,
                    ROUND(
                        COUNT(*) FILTER (WHERE outcome = 'tp')::numeric
                        / NULLIF(COUNT(*), 0) * 100, 2
                    ) AS win_rate
                FROM signals
                WHERE outcome != 'active'
                GROUP BY direction
                ORDER BY direction
            """))
            return [dict(row._mapping) for row in result.all()]

    async def _get_expectancy(self) -> dict:
        """Overall expectancy in R-multiples.

        TP = +4.42R, SL = -1R.
        Expectancy = (win_rate * 4.42) - (loss_rate * 1.0)
        """
        async with get_database().session() as session:
            result = await session.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE outcome = 'tp') AS wins,
                    COUNT(*) FILTER (WHERE outcome = 'sl') AS losses,
                    ROUND(
                        COUNT(*) FILTER (WHERE outcome = 'tp')::numeric
                        / NULLIF(COUNT(*), 0) * 100, 2
                    ) AS win_rate,
                    ROUND(
                        (COUNT(*) FILTER (WHERE outcome = 'tp')::numeric
                         / NULLIF(COUNT(*), 0) * 4.42)
                        - (COUNT(*) FILTER (WHERE outcome = 'sl')::numeric
                           / NULLIF(COUNT(*), 0) * 1.0),
                        4
                    ) AS expectancy_r,
                    ROUND(
                        COUNT(*) FILTER (WHERE outcome = 'tp')::numeric * 4.42
                        - COUNT(*) FILTER (WHERE outcome = 'sl')::numeric * 1.0,
                        2
                    ) AS total_r,
                    ROUND(
                        (COUNT(*) FILTER (WHERE outcome = 'tp')::numeric * 4.42)
                        / NULLIF(COUNT(*) FILTER (WHERE outcome = 'sl')::numeric * 1.0, 0),
                        2
                    ) AS profit_factor
                FROM signals
                WHERE outcome != 'active'
            """))
            row = result.one()
            return dict(row._mapping)

    async def _get_daily(self, days: int = 30) -> list[dict]:
        """Daily performance with cumulative R curve."""
        async with get_database().session() as session:
            result = await session.execute(text("""
                WITH daily AS (
                    SELECT
                        DATE(outcome_time AT TIME ZONE 'UTC') AS date,
                        COUNT(*) FILTER (WHERE outcome = 'tp') AS wins,
                        COUNT(*) FILTER (WHERE outcome = 'sl') AS losses,
                        COUNT(*) AS total,
                        ROUND(
                            COUNT(*) FILTER (WHERE outcome = 'tp')::numeric * 4.42
                            - COUNT(*) FILTER (WHERE outcome = 'sl')::numeric * 1.0,
                            2
                        ) AS daily_r
                    FROM signals
                    WHERE outcome != 'active'
                      AND outcome_time >= NOW() - MAKE_INTERVAL(days => :days)
                    GROUP BY DATE(outcome_time AT TIME ZONE 'UTC')
                    ORDER BY date
                )
                SELECT
                    date::text AS date,
                    wins,
                    losses,
                    total,
                    daily_r,
                    SUM(daily_r) OVER (ORDER BY date) AS cumulative_r
                FROM daily
            """), {"days": days})
            return [dict(row._mapping) for row in result.all()]

    async def _get_mae_mfe(self) -> dict:
        """MAE/MFE distribution statistics for wins and losses."""
        async with get_database().session() as session:
            result = await session.execute(text("""
                SELECT
                    outcome,
                    COUNT(*) AS count,
                    ROUND(AVG(mae_ratio)::numeric, 4) AS avg_mae,
                    ROUND(AVG(mfe_ratio)::numeric, 4) AS avg_mfe,
                    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mae_ratio)::numeric, 4) AS mae_p25,
                    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mae_ratio)::numeric, 4) AS mae_p50,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mae_ratio)::numeric, 4) AS mae_p75,
                    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mae_ratio)::numeric, 4) AS mae_p90,
                    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mfe_ratio)::numeric, 4) AS mfe_p25,
                    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mfe_ratio)::numeric, 4) AS mfe_p50,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mfe_ratio)::numeric, 4) AS mfe_p75,
                    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mfe_ratio)::numeric, 4) AS mfe_p90
                FROM signals
                WHERE outcome != 'active'
                GROUP BY outcome
            """))
            rows = result.all()
            return {row._mapping["outcome"]: dict(row._mapping) for row in rows}

    def _row_to_signal(self, row: SignalTable) -> SignalRecord:
        """Convert database row to SignalRecord model."""
        return SignalRecord(
            id=row.id,
            symbol=row.symbol,
            timeframe=row.timeframe,
            signal_time=row.signal_time,
            direction=Direction(row.direction),
            entry_price=Decimal(str(row.entry_price)),
            tp_price=Decimal(str(row.tp_price)),
            sl_price=Decimal(str(row.sl_price)),
            atr_at_signal=Decimal(str(row.atr_at_signal)) if row.atr_at_signal else Decimal("0"),
            max_atr=Decimal(str(row.max_atr)) if row.max_atr else Decimal("0"),
            streak_at_signal=row.streak_at_signal,
            mae_ratio=Decimal(str(row.mae_ratio)),
            mfe_ratio=Decimal(str(row.mfe_ratio)),
            outcome=Outcome(row.outcome),
            outcome_time=row.outcome_time,
            outcome_price=Decimal(str(row.outcome_price)) if row.outcome_price else None,
        )


class AggTradeRepository:
    """Repository for aggregated trade data operations."""

    async def save_batch(self, trades: list[AggTrade]) -> None:
        """Save multiple trades in batch using INSERT."""
        if not trades:
            return

        async with get_database().session() as session:
            values = [
                {
                    "symbol": t.symbol,
                    "agg_trade_id": t.agg_trade_id,
                    "timestamp": t.timestamp,
                    "price": t.price,
                    "quantity": t.quantity,
                    "is_buyer_maker": t.is_buyer_maker,
                }
                for t in trades
            ]
            stmt = insert(AggTradeTable).values(values)
            # Use primary key (symbol, timestamp, agg_trade_id) for conflict resolution
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "timestamp", "agg_trade_id"]
            )
            await session.execute(stmt)

    async def copy_batch(self, trades: list[AggTrade]) -> int:
        """Save trades using PostgreSQL COPY command (5-10x faster than INSERT).

        Note: COPY doesn't handle conflicts, so use for fresh data only.
        Returns number of rows copied.
        """
        if not trades:
            return 0

        import asyncpg
        from app.config import get_settings

        settings = get_settings()
        db_url = settings.database_url

        # Connect directly with asyncpg (bypass SQLAlchemy)
        conn = await asyncpg.connect(db_url)
        try:
            # Prepare records as tuples
            records = [
                (
                    t.symbol,
                    t.timestamp,
                    t.agg_trade_id,
                    float(t.price),
                    float(t.quantity),
                    t.is_buyer_maker,
                )
                for t in trades
            ]

            # Use COPY
            result = await conn.copy_records_to_table(
                "aggtrades",
                records=records,
                columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
            )
            # result format: "COPY <count>"
            return int(result.split()[1])
        finally:
            await conn.close()

    async def get_range(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> list[AggTrade]:
        """Get trades within a time range."""
        async with get_database().session() as session:
            stmt = (
                select(AggTradeTable)
                .where(
                    AggTradeTable.symbol == symbol,
                    AggTradeTable.timestamp >= start,
                    AggTradeTable.timestamp <= end,
                )
                .order_by(AggTradeTable.timestamp.asc())
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                AggTrade(
                    symbol=row.symbol,
                    agg_trade_id=row.agg_trade_id,
                    timestamp=row.timestamp,
                    price=Decimal(str(row.price)),
                    quantity=Decimal(str(row.quantity)),
                    is_buyer_maker=row.is_buyer_maker,
                )
                for row in rows
            ]

    async def get_last_trade_id(self, symbol: str) -> int | None:
        """Get the last trade ID for a symbol."""
        async with get_database().session() as session:
            stmt = (
                select(AggTradeTable.agg_trade_id)
                .where(AggTradeTable.symbol == symbol)
                .order_by(AggTradeTable.agg_trade_id.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def get_last_timestamp(self, symbol: str) -> datetime | None:
        """Get the timestamp of the most recent trade."""
        async with get_database().session() as session:
            stmt = (
                select(AggTradeTable.timestamp)
                .where(AggTradeTable.symbol == symbol)
                .order_by(AggTradeTable.timestamp.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
