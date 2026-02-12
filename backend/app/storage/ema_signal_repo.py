"""EMA Crossover signal repository for live trading."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Direction, Outcome, SignalRecord
from app.storage.database import EmaSignalTable, get_database


class EmaSignalRepository:
    """Repository for EMA Crossover signal data operations."""

    async def save(self, signal: SignalRecord) -> None:
        """Save a new EMA signal record."""
        async with get_database().session() as session:
            stmt = insert(EmaSignalTable).values(
                id=signal.id,
                strategy=signal.strategy,
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                signal_time=signal.signal_time,
                direction=signal.direction.value,
                entry_price=signal.entry_price,
                tp_price=signal.tp_price,
                sl_price=signal.sl_price,
                ema_fast=getattr(signal, "ema_fast", Decimal("0")),
                ema_slow=getattr(signal, "ema_slow", Decimal("0")),
                atr_at_signal=signal.atr_at_signal,
                mae_ratio=signal.mae_ratio,
                mfe_ratio=signal.mfe_ratio,
                outcome=signal.outcome.value,
                outcome_time=signal.outcome_time,
                outcome_price=signal.outcome_price,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
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
    ) -> None:
        """Update signal outcome and MAE/MFE ratios."""
        async with get_database().session() as session:
            stmt = (
                update(EmaSignalTable)
                .where(EmaSignalTable.id == signal_id)
                .values(
                    mae_ratio=mae_ratio,
                    mfe_ratio=mfe_ratio,
                    outcome=outcome.value,
                    outcome_time=outcome_time,
                    outcome_price=outcome_price,
                )
            )
            await session.execute(stmt)

    async def get_active(self, symbol: str | None = None) -> list[SignalRecord]:
        """Get all active EMA signals."""
        async with get_database().session() as session:
            stmt = select(EmaSignalTable).where(
                EmaSignalTable.outcome == "active"
            )
            if symbol:
                stmt = stmt.where(EmaSignalTable.symbol == symbol)
            stmt = stmt.order_by(EmaSignalTable.signal_time.desc())

            result = await session.execute(stmt)
            rows = result.scalars().all()
            return [self._row_to_signal(row) for row in rows]

    async def get_by_id(self, signal_id: str) -> SignalRecord | None:
        """Get a signal by ID."""
        async with get_database().session() as session:
            stmt = select(EmaSignalTable).where(
                EmaSignalTable.id == signal_id
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return self._row_to_signal(row)

    def _row_to_signal(self, row: EmaSignalTable) -> SignalRecord:
        """Convert database row to SignalRecord."""
        return SignalRecord(
            id=row.id,
            strategy=row.strategy or "ema_crossover",
            symbol=row.symbol,
            timeframe=row.timeframe,
            signal_time=row.signal_time,
            direction=Direction(row.direction),
            entry_price=Decimal(str(row.entry_price)),
            tp_price=Decimal(str(row.tp_price)),
            sl_price=Decimal(str(row.sl_price)),
            atr_at_signal=Decimal(str(row.atr_at_signal)) if row.atr_at_signal else Decimal("0"),
            mae_ratio=Decimal(str(row.mae_ratio)),
            mfe_ratio=Decimal(str(row.mfe_ratio)),
            outcome=Outcome(row.outcome),
            outcome_time=row.outcome_time,
            outcome_price=Decimal(str(row.outcome_price)) if row.outcome_price else None,
        )
