"""Base signal record shared by all strategies.

Defines the common fields that every strategy's signal must have.
Strategy-specific subclasses add their own fields (e.g., MsrSignalRecord).
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from core.models.signal import Direction, Outcome


def _generate_signal_id(
    strategy: str,
    symbol: str,
    timeframe: str,
    signal_time: datetime,
    direction: int,
) -> str:
    """Generate deterministic signal ID based on signal attributes.

    Includes strategy name so that different strategies processing
    the same symbol/timeframe/time produce different IDs.
    """
    ts_str = signal_time.strftime("%Y%m%d%H%M%S%f")
    key = f"{strategy}:{symbol}:{timeframe}:{ts_str}:{direction}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


class BaseSignalRecord(BaseModel):
    """Base trading signal record with fields common to ALL strategies.

    Strategy-specific models should subclass this and add their own fields.
    """

    id: str = ""
    strategy: str  # e.g. "msr_retest_capture"
    symbol: str
    timeframe: str
    direction: Direction
    signal_time: datetime
    entry_price: Decimal
    tp_price: Decimal
    sl_price: Decimal
    outcome: Outcome = Outcome.ACTIVE
    outcome_time: datetime | None = None
    outcome_price: Decimal | None = None
    mae_ratio: Decimal = Decimal("0")
    mfe_ratio: Decimal = Decimal("0")

    def model_post_init(self, __context) -> None:
        """Generate deterministic ID after model initialization."""
        if not self.id:
            object.__setattr__(
                self,
                "id",
                _generate_signal_id(
                    self.strategy,
                    self.symbol,
                    self.timeframe,
                    self.signal_time,
                    self.direction.value,
                ),
            )

    @property
    def risk_amount(self) -> Decimal:
        """Get the risk amount (distance to stop loss)."""
        if self.direction == Direction.LONG:
            return self.entry_price - self.sl_price
        return self.sl_price - self.entry_price

    @property
    def reward_amount(self) -> Decimal:
        """Get the reward amount (distance to take profit)."""
        if self.direction == Direction.LONG:
            return self.tp_price - self.entry_price
        return self.entry_price - self.tp_price
