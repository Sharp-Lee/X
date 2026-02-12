"""PostgreSQL database for backtest results.

Manages a single asyncpg connection pool shared by both kline reading
and signal storage. Creates backtest-specific tables (backtest_runs,
backtest_signals) separate from the live signals table.
"""

from __future__ import annotations

import logging

import asyncpg

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    id              TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    start_date      TIMESTAMPTZ NOT NULL,
    end_date        TIMESTAMPTZ NOT NULL,
    symbols         JSONB NOT NULL,
    timeframes      JSONB NOT NULL,
    strategy_config JSONB NOT NULL,
    total_signals   INTEGER DEFAULT 0,
    wins            INTEGER DEFAULT 0,
    losses          INTEGER DEFAULT 0,
    active          INTEGER DEFAULT 0,
    win_rate        DOUBLE PRECISION DEFAULT 0.0,
    expectancy_r    DOUBLE PRECISION DEFAULT 0.0,
    total_r         DOUBLE PRECISION DEFAULT 0.0,
    profit_factor   DOUBLE PRECISION DEFAULT 0.0,
    status          TEXT DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS backtest_msr_signals (
    id              TEXT NOT NULL,
    run_id          TEXT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    strategy        VARCHAR(50) NOT NULL DEFAULT 'msr_retest_capture',
    symbol          VARCHAR(20) NOT NULL,
    timeframe       VARCHAR(10) NOT NULL,
    signal_time     TIMESTAMPTZ NOT NULL,
    direction       INTEGER NOT NULL,
    entry_price     NUMERIC(20,8) NOT NULL,
    tp_price        NUMERIC(20,8) NOT NULL,
    sl_price        NUMERIC(20,8) NOT NULL,
    atr_at_signal   NUMERIC(20,8) DEFAULT 0,
    max_atr         NUMERIC(20,8) DEFAULT 0,
    streak_at_signal INTEGER DEFAULT 0,
    mae_ratio       NUMERIC(10,6) DEFAULT 0,
    mfe_ratio       NUMERIC(10,6) DEFAULT 0,
    outcome         VARCHAR(10) DEFAULT 'active',
    outcome_time    TIMESTAMPTZ,
    outcome_price   NUMERIC(20,8),
    PRIMARY KEY (run_id, id)
);

-- Indexes for analytical queries
CREATE INDEX IF NOT EXISTS idx_bt_msr_signals_run_outcome
    ON backtest_msr_signals(run_id, outcome);
CREATE INDEX IF NOT EXISTS idx_bt_msr_signals_run_symbol_tf
    ON backtest_msr_signals(run_id, symbol, timeframe, outcome);

CREATE TABLE IF NOT EXISTS backtest_ema_signals (
    id              TEXT NOT NULL,
    run_id          TEXT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    strategy        VARCHAR(50) NOT NULL DEFAULT 'ema_crossover',
    symbol          VARCHAR(20) NOT NULL,
    timeframe       VARCHAR(10) NOT NULL,
    signal_time     TIMESTAMPTZ NOT NULL,
    direction       INTEGER NOT NULL,
    entry_price     NUMERIC(20,8) NOT NULL,
    tp_price        NUMERIC(20,8) NOT NULL,
    sl_price        NUMERIC(20,8) NOT NULL,
    ema_fast        NUMERIC(20,8) DEFAULT 0,
    ema_slow        NUMERIC(20,8) DEFAULT 0,
    atr_at_signal   NUMERIC(20,8) DEFAULT 0,
    mae_ratio       NUMERIC(10,6) DEFAULT 0,
    mfe_ratio       NUMERIC(10,6) DEFAULT 0,
    outcome         VARCHAR(10) DEFAULT 'active',
    outcome_time    TIMESTAMPTZ,
    outcome_price   NUMERIC(20,8),
    PRIMARY KEY (run_id, id)
);

CREATE INDEX IF NOT EXISTS idx_bt_ema_signals_run_outcome
    ON backtest_ema_signals(run_id, outcome);
CREATE INDEX IF NOT EXISTS idx_bt_ema_signals_run_symbol_tf
    ON backtest_ema_signals(run_id, symbol, timeframe, outcome);
"""


class BacktestDatabase:
    """Asyncpg connection pool for backtest operations.

    Shared by KlineSource (read klines) and SignalRepo (write results).
    """

    def __init__(self, database_url: str):
        self._database_url = database_url
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database not initialized. Call init() first.")
        return self._pool

    async def init(self) -> None:
        """Create connection pool and ensure backtest tables exist."""
        self._pool = await asyncpg.create_pool(
            self._database_url,
            min_size=2,
            max_size=10,
            command_timeout=120,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)
        logger.info("Backtest database initialized")

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
