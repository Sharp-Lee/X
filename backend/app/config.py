"""Application configuration."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql://localhost/crypto_data"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Binance API
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_testnet: bool = False

    # Trading Configuration
    symbols: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    timeframes: list[str] = ["1m", "3m", "5m", "15m", "30m"]

    # Strategy Parameters (match TradingView Pine Script)
    ema_period: int = 50
    fib_period: int = 9   # Pine Script: lookback = 9
    atr_period: int = 9   # Pine Script: atrPeriod = 9
    tp_atr_mult: float = 2.0
    sl_atr_mult: float = 8.84  # 2 * 4.42
    max_risk_percent: float = 2.53  # Maximum risk per trade as % of equity

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
