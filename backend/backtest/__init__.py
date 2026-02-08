"""Backtesting system for MSR Retest Capture strategy.

Fully independent of app/ — only depends on core/ for business logic.

Storage:
- Klines: read from PostgreSQL via asyncpg (no SQLAlchemy)
- Signals: written to SQLite with run_id tracking

Usage:
    python -m backtest --start 2025-01-01 --end 2025-12-31
    python -m backtest --list-runs
"""

from backtest.runner import BacktestConfig, BacktestRunner
from backtest.stats import BacktestResult

__all__ = ["BacktestConfig", "BacktestRunner", "BacktestResult"]
