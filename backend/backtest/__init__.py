"""Backtesting system for MSR Retest Capture strategy.

Downloads official 1m klines from data.binance.vision, aggregates higher
timeframes locally via KlineAggregator, generates signals via SignalGenerator,
and determines outcomes using 1m kline high/low.

Usage:
    python -m backtest --start 2025-01-01 --end 2025-12-31
    python -m backtest --download --symbols BTCUSDT --start 2025-06-01 --end 2025-12-31
"""

from backtest.runner import BacktestConfig, BacktestRunner
from backtest.stats import BacktestResult

__all__ = ["BacktestConfig", "BacktestRunner", "BacktestResult"]
