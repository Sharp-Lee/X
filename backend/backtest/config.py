"""Backtest-specific configuration.

Independent of app/config.py — only needs a database URL.
Both kline reading and signal storage use the same PostgreSQL.
"""

from __future__ import annotations

import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class BacktestSettings(BaseSettings):
    """Backtest configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="BACKTEST_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # PostgreSQL — shared for klines (read) and backtest results (write)
    database_url: str = os.environ.get(
        "DATABASE_URL", "postgresql://localhost/crypto_live"
    )


_settings: BacktestSettings | None = None


def get_backtest_settings() -> BacktestSettings:
    """Get cached backtest settings instance."""
    global _settings
    if _settings is None:
        _settings = BacktestSettings()
    return _settings
