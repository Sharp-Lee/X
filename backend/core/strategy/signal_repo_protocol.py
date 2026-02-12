"""Signal repository protocol for strategy-agnostic persistence.

Any storage backend (live PostgreSQL, backtest in-memory, etc.) can implement
this protocol to be used by strategies and engine code.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable

from core.models.signal import Outcome, SignalRecord


@runtime_checkable
class SignalRepository(Protocol):
    """Protocol that signal storage backends must implement."""

    async def save_signal(self, signal: SignalRecord) -> None:
        """Persist a new or updated signal."""
        ...

    async def update_outcome(
        self,
        signal_id: str,
        outcome: Outcome,
        outcome_time: datetime | None = None,
        outcome_price: Decimal | None = None,
    ) -> None:
        """Update the outcome of an existing signal."""
        ...

    async def get_active(
        self,
        symbol: str | None = None,
        timeframe: str | None = None,
    ) -> list[SignalRecord]:
        """Get all active (non-resolved) signals."""
        ...

    async def get_by_id(self, signal_id: str) -> SignalRecord | None:
        """Get a single signal by its ID."""
        ...
