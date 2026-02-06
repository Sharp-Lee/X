"""Processing state repository for tracking K-line replay progress."""

from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from app.models import ProcessingState
from app.storage.database import ProcessingStateTable, get_database


class ProcessingStateRepository:
    """Repository for processing state operations.

    Tracks K-line processing progress to ensure signal determinism across restarts.
    """

    async def get_state(self, symbol: str, timeframe: str) -> ProcessingState | None:
        """Get processing state for a symbol/timeframe pair."""
        async with get_database().session() as session:
            stmt = select(ProcessingStateTable).where(
                ProcessingStateTable.symbol == symbol,
                ProcessingStateTable.timeframe == timeframe,
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()

            if row is None:
                return None

            return ProcessingState(
                symbol=row.symbol,
                timeframe=row.timeframe,
                system_start_time=row.system_start_time,
                last_processed_time=row.last_processed_time,
                state_status=row.state_status,
            )

    async def upsert_state(self, state: ProcessingState) -> None:
        """Insert or update processing state."""
        async with get_database().session() as session:
            stmt = insert(ProcessingStateTable).values(
                symbol=state.symbol,
                timeframe=state.timeframe,
                system_start_time=state.system_start_time,
                last_processed_time=state.last_processed_time,
                state_status=state.state_status,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol", "timeframe"],
                set_={
                    "last_processed_time": stmt.excluded.last_processed_time,
                    "state_status": stmt.excluded.state_status,
                },
            )
            await session.execute(stmt)

    async def mark_pending(self, symbol: str, timeframe: str) -> None:
        """Mark state as pending (during replay)."""
        async with get_database().session() as session:
            stmt = (
                update(ProcessingStateTable)
                .where(
                    ProcessingStateTable.symbol == symbol,
                    ProcessingStateTable.timeframe == timeframe,
                )
                .values(state_status="pending")
            )
            await session.execute(stmt)

    async def mark_confirmed(self, symbol: str, timeframe: str) -> None:
        """Mark state as confirmed (after successful commit)."""
        async with get_database().session() as session:
            stmt = (
                update(ProcessingStateTable)
                .where(
                    ProcessingStateTable.symbol == symbol,
                    ProcessingStateTable.timeframe == timeframe,
                )
                .values(state_status="confirmed")
            )
            await session.execute(stmt)

    async def update_last_processed(
        self, symbol: str, timeframe: str, last_processed_time: datetime
    ) -> None:
        """Update last processed time for a symbol/timeframe."""
        async with get_database().session() as session:
            stmt = (
                update(ProcessingStateTable)
                .where(
                    ProcessingStateTable.symbol == symbol,
                    ProcessingStateTable.timeframe == timeframe,
                )
                .values(
                    last_processed_time=last_processed_time,
                    state_status="confirmed",
                )
            )
            await session.execute(stmt)

    async def get_all_states(self) -> list[ProcessingState]:
        """Get all processing states."""
        async with get_database().session() as session:
            stmt = select(ProcessingStateTable).order_by(
                ProcessingStateTable.symbol,
                ProcessingStateTable.timeframe,
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                ProcessingState(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    system_start_time=row.system_start_time,
                    last_processed_time=row.last_processed_time,
                    state_status=row.state_status,
                )
                for row in rows
            ]

    async def get_pending_states(self) -> list[ProcessingState]:
        """Get all states that are still pending (crashed during replay)."""
        async with get_database().session() as session:
            stmt = select(ProcessingStateTable).where(
                ProcessingStateTable.state_status == "pending"
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            return [
                ProcessingState(
                    symbol=row.symbol,
                    timeframe=row.timeframe,
                    system_start_time=row.system_start_time,
                    last_processed_time=row.last_processed_time,
                    state_status=row.state_status,
                )
                for row in rows
            ]
