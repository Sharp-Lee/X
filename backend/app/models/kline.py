"""K-line (candlestick) data models."""

from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, Field


class Kline(BaseModel):
    """K-line (candlestick) data model."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    is_closed: bool = True

    @property
    def is_bullish(self) -> bool:
        """Check if this is a bullish (green) candle."""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """Check if this is a bearish (red) candle."""
        return self.close < self.open

    @property
    def body_size(self) -> Decimal:
        """Get the absolute size of the candle body."""
        return abs(self.close - self.open)

    @property
    def range_size(self) -> Decimal:
        """Get the full range (high - low) of the candle."""
        return self.high - self.low


class KlineBuffer(BaseModel):
    """Buffer for storing recent K-lines for indicator calculation."""

    symbol: str
    timeframe: str
    klines: list[Kline] = Field(default_factory=list)
    max_size: int = 200

    def add(self, kline: Kline) -> None:
        """Add a K-line to the buffer, maintaining max size."""
        if self.klines and kline.timestamp <= self.klines[-1].timestamp:
            # Update existing kline (same timestamp)
            if kline.timestamp == self.klines[-1].timestamp:
                self.klines[-1] = kline
            return

        self.klines.append(kline)
        if len(self.klines) > self.max_size:
            self.klines = self.klines[-self.max_size :]

    def get_closes(self) -> list[Decimal]:
        """Get list of close prices."""
        return [k.close for k in self.klines]

    def get_highs(self) -> list[Decimal]:
        """Get list of high prices."""
        return [k.high for k in self.klines]

    def get_lows(self) -> list[Decimal]:
        """Get list of low prices."""
        return [k.low for k in self.klines]

    def get_volumes(self) -> list[Decimal]:
        """Get list of volumes."""
        return [k.volume for k in self.klines]

    def __len__(self) -> int:
        return len(self.klines)
