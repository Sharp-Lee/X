"""Signal and trade data models."""

import hashlib
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class Direction(int, Enum):
    """Trade direction."""

    LONG = 1
    SHORT = -1


class Outcome(str, Enum):
    """Signal outcome status."""

    ACTIVE = "active"
    TP = "tp"  # Take profit hit
    SL = "sl"  # Stop loss hit


def _generate_signal_id(symbol: str, timeframe: str, signal_time: datetime, direction: int) -> str:
    """Generate deterministic signal ID based on signal attributes.

    This ensures the same signal generates the same ID during replay,
    preventing duplicate signals after crash recovery.
    """
    # Format timestamp to microsecond precision for uniqueness
    ts_str = signal_time.strftime("%Y%m%d%H%M%S%f")
    key = f"{symbol}:{timeframe}:{ts_str}:{direction}"
    # Use first 32 chars of SHA256 for a UUID-like format
    return hashlib.sha256(key.encode()).hexdigest()[:32]


class SignalRecord(BaseModel):
    """Trading signal record."""

    id: str = ""  # Will be set in model_post_init
    symbol: str
    timeframe: str
    signal_time: datetime
    direction: Direction
    entry_price: Decimal
    tp_price: Decimal
    sl_price: Decimal
    atr_at_signal: Decimal = Decimal("0")  # ATR value when signal was generated
    max_atr: Decimal = Decimal("0")  # Maximum ATR during signal lifetime
    streak_at_signal: int = 0  # Positive = win streak, negative = loss streak
    mae_ratio: Decimal = Decimal("0")  # Maximum Adverse Excursion ratio
    mfe_ratio: Decimal = Decimal("0")  # Maximum Favorable Excursion ratio
    outcome: Outcome = Outcome.ACTIVE
    outcome_time: datetime | None = None
    outcome_price: Decimal | None = None

    def model_post_init(self, __context) -> None:
        """Generate deterministic ID after model initialization."""
        if not self.id:
            object.__setattr__(
                self,
                "id",
                _generate_signal_id(
                    self.symbol, self.timeframe, self.signal_time, self.direction.value
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

    def update_mae(self, current_price: Decimal) -> bool:
        """
        Update MAE ratio based on current price.
        Returns True if outcome changed (hit TP or SL).
        """
        if self.outcome != Outcome.ACTIVE:
            return False

        risk = self.risk_amount
        if risk == 0:
            return False

        # Calculate adverse excursion
        if self.direction == Direction.LONG:
            adverse = self.entry_price - current_price
            favorable = current_price - self.entry_price
        else:
            adverse = current_price - self.entry_price
            favorable = self.entry_price - current_price

        # Update MAE/MFE ratios
        adverse_ratio = adverse / risk
        favorable_ratio = favorable / risk

        if adverse_ratio > self.mae_ratio:
            self.mae_ratio = adverse_ratio

        if favorable_ratio > self.mfe_ratio:
            self.mfe_ratio = favorable_ratio

        return False

    def check_outcome(self, price: Decimal, timestamp: datetime) -> bool:
        """
        Check if price hits TP or SL.
        Returns True if outcome changed.
        """
        if self.outcome != Outcome.ACTIVE:
            return False

        if self.direction == Direction.LONG:
            if price >= self.tp_price:
                self.outcome = Outcome.TP
                self.outcome_time = timestamp
                self.outcome_price = price
                return True
            if price <= self.sl_price:
                self.outcome = Outcome.SL
                self.outcome_time = timestamp
                self.outcome_price = price
                return True
        else:  # SHORT
            if price <= self.tp_price:
                self.outcome = Outcome.TP
                self.outcome_time = timestamp
                self.outcome_price = price
                return True
            if price >= self.sl_price:
                self.outcome = Outcome.SL
                self.outcome_time = timestamp
                self.outcome_price = price
                return True

        return False


class AggTrade(BaseModel):
    """Aggregated trade data from exchange."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    agg_trade_id: int
    price: Decimal
    quantity: Decimal
    timestamp: datetime
    is_buyer_maker: bool


class StreakTracker(BaseModel):
    """Track win/loss streaks."""

    current_streak: int = 0  # Positive = wins, negative = losses
    total_wins: int = 0
    total_losses: int = 0

    def record_outcome(self, outcome: Outcome) -> None:
        """Record a signal outcome and update streak."""
        if outcome == Outcome.TP:
            self.total_wins += 1
            if self.current_streak >= 0:
                self.current_streak += 1
            else:
                self.current_streak = 1
        elif outcome == Outcome.SL:
            self.total_losses += 1
            if self.current_streak <= 0:
                self.current_streak -= 1
            else:
                self.current_streak = -1

    @property
    def win_rate(self) -> float:
        """Calculate win rate."""
        total = self.total_wins + self.total_losses
        if total == 0:
            return 0.0
        return self.total_wins / total
