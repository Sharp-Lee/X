"""Trading configuration loaded from trading.yaml.

Supports:
- Portfolio selection: "A", "B", or "custom" (inline strategies)
- Multiple Binance trading accounts, each mapped to strategy subsets
- Backward compatible: no YAML file = Portfolio B, no auto-trading
"""

import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, model_validator

from core.models.config import SignalFilterConfig, PORTFOLIO_A, PORTFOLIO_B

logger = logging.getLogger(__name__)


class StrategyEntry(BaseModel):
    """A single strategy entry in the YAML config (custom portfolio)."""

    symbol: str
    timeframe: str
    enabled: bool = True
    streak_lo: int = -999
    streak_hi: int = 999
    atr_pct_threshold: float = 0.0
    position_qty: float = 0.0
    max_consecutive_loss_months: int = 3

    def to_filter_config(self) -> SignalFilterConfig:
        return SignalFilterConfig(
            symbol=self.symbol,
            timeframe=self.timeframe,
            enabled=self.enabled,
            streak_lo=self.streak_lo,
            streak_hi=self.streak_hi,
            atr_pct_threshold=self.atr_pct_threshold,
            position_qty=self.position_qty,
            max_consecutive_loss_months=self.max_consecutive_loss_months,
        )


class AccountConfig(BaseModel):
    """A Binance trading account configuration."""

    name: str
    api_key_env: str = ""
    api_secret_env: str = ""
    testnet: bool = True
    enabled: bool = True
    auto_trade: bool = False
    leverage: int = 5
    strategies: list[str] = []  # empty = all strategies in portfolio

    @property
    def api_key(self) -> str:
        if not self.api_key_env:
            return ""
        return os.environ.get(self.api_key_env, "")

    @property
    def api_secret(self) -> str:
        if not self.api_secret_env:
            return ""
        return os.environ.get(self.api_secret_env, "")


_VALID_PORTFOLIOS = ("A", "B", "custom")


class TradingConfig(BaseModel):
    """Top-level trading.yaml configuration."""

    portfolio: str = "B"
    strategies: list[StrategyEntry] = []
    accounts: list[AccountConfig] = []

    @model_validator(mode="after")
    def _validate(self):
        if self.portfolio not in _VALID_PORTFOLIOS:
            raise ValueError(
                f"portfolio must be one of {_VALID_PORTFOLIOS}, got '{self.portfolio}'"
            )
        if self.portfolio == "custom" and not self.strategies:
            raise ValueError(
                "portfolio='custom' requires at least one entry in 'strategies'"
            )
        return self

    def get_signal_filters(self) -> list[SignalFilterConfig]:
        """Resolve portfolio selection to a list of SignalFilterConfig."""
        if self.portfolio == "A":
            return list(PORTFOLIO_A)
        elif self.portfolio == "B":
            return list(PORTFOLIO_B)
        else:
            return [s.to_filter_config() for s in self.strategies]

    def get_enabled_accounts(self) -> list[AccountConfig]:
        """Return accounts with enabled=True and auto_trade=True."""
        return [a for a in self.accounts if a.enabled and a.auto_trade]


_DEFAULT_PATH = Path(__file__).parent.parent / "trading.yaml"


def load_trading_config(path: Path | None = None) -> TradingConfig:
    """Load trading config from YAML file.

    Falls back to defaults (portfolio B, no accounts) if file doesn't exist.
    """
    config_path = path or _DEFAULT_PATH

    # Load .env into os.environ so AccountConfig.api_key/api_secret can read them
    env_path = config_path.parent / ".env"
    load_dotenv(env_path, override=False)

    if not config_path.exists():
        logger.info(
            "No trading.yaml found at %s, using defaults (portfolio B)",
            config_path,
        )
        return TradingConfig()

    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}

    config = TradingConfig(**raw)
    logger.info(
        "Loaded trading config: portfolio=%s, %d strategies, %d accounts (%d auto-trade)",
        config.portfolio,
        len(config.get_signal_filters()),
        len(config.accounts),
        len(config.get_enabled_accounts()),
    )
    return config
