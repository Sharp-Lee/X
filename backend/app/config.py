"""Application configuration."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
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

    # Strategy Parameters
    ema_period: int = 50
    fib_period: int = 9
    atr_period: int = 9
    tp_atr_mult: float = 2.0
    sl_atr_mult: float = 8.84  # 2 * 4.42

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
