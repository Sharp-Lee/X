"""Backtest signal repository — PostgreSQL-backed, with run_id tracking.

Uses the shared asyncpg pool. Writes to backtest_runs / backtest_msr_signals
tables, completely separate from the live signals table.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import asyncpg

from core.models.config import StrategyConfig
from core.models.signal import SignalRecord

from backtest.stats import BacktestResult

logger = logging.getLogger(__name__)


class BacktestSignalRepo:
    """Persist and query backtest runs and signals in PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # ── Run management ──────────────────────────────────────────

    async def create_run(
        self,
        run_id: str,
        start_date: datetime,
        end_date: datetime,
        symbols: list[str],
        timeframes: list[str],
        strategy: StrategyConfig,
    ) -> None:
        """Create a new backtest run record."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO backtest_runs
                   (id, start_date, end_date, symbols, timeframes,
                    strategy_config, status)
                   VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, 'running')""",
                run_id,
                start_date,
                end_date,
                json.dumps(symbols),
                json.dumps(timeframes),
                strategy.model_dump_json(),
            )

    async def complete_run(self, run_id: str, result: BacktestResult) -> None:
        """Update run with final statistics."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """UPDATE backtest_runs SET
                    total_signals=$2, wins=$3, losses=$4, active=$5,
                    win_rate=$6, expectancy_r=$7, total_r=$8, profit_factor=$9,
                    status='completed'
                   WHERE id=$1""",
                run_id,
                result.total_signals,
                result.wins,
                result.losses,
                result.active,
                result.win_rate,
                result.expectancy_r,
                result.total_r,
                result.profit_factor,
            )

    async def fail_run(self, run_id: str) -> None:
        """Mark a run as failed."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE backtest_runs SET status='failed' WHERE id=$1",
                run_id,
            )

    # ── Signal persistence ──────────────────────────────────────

    async def save_signals(
        self, run_id: str, signals: list[SignalRecord]
    ) -> int:
        """Batch-insert all signals for a run.

        Uses executemany for high throughput.
        Returns number of signals saved.
        """
        if not signals:
            return 0

        rows = [
            (
                s.id,
                run_id,
                s.strategy,
                s.symbol,
                s.timeframe,
                s.signal_time,
                s.direction.value,
                s.entry_price,
                s.tp_price,
                s.sl_price,
                s.atr_at_signal,
                s.max_atr,
                s.streak_at_signal,
                s.mae_ratio,
                s.mfe_ratio,
                s.outcome.value,
                s.outcome_time,
                s.outcome_price,
            )
            for s in signals
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """INSERT INTO backtest_msr_signals
                   (id, run_id, strategy, symbol, timeframe, signal_time, direction,
                    entry_price, tp_price, sl_price, atr_at_signal, max_atr,
                    streak_at_signal, mae_ratio, mfe_ratio, outcome,
                    outcome_time, outcome_price)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                   ON CONFLICT (run_id, id) DO UPDATE SET
                    outcome=EXCLUDED.outcome,
                    outcome_time=EXCLUDED.outcome_time,
                    outcome_price=EXCLUDED.outcome_price,
                    mae_ratio=EXCLUDED.mae_ratio,
                    mfe_ratio=EXCLUDED.mfe_ratio,
                    max_atr=EXCLUDED.max_atr""",
                rows,
            )
        return len(rows)

    # ── Query methods ───────────────────────────────────────────

    async def list_runs(self) -> list[dict]:
        """List all backtest runs, newest first."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, created_at, start_date, end_date,
                          symbols, timeframes,
                          total_signals, wins, losses, win_rate,
                          expectancy_r, total_r, profit_factor, status
                   FROM backtest_runs
                   ORDER BY created_at DESC"""
            )
        return [dict(r) for r in rows]

    async def get_run(self, run_id: str) -> dict | None:
        """Get a single run with full details."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM backtest_runs WHERE id=$1", run_id
            )
        return dict(row) if row else None

    async def get_run_signals_stats(self, run_id: str) -> list[dict]:
        """Get per-symbol/timeframe breakdown for a run.

        Leverages PostgreSQL's native analytical functions.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT
                    symbol, timeframe,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE outcome='tp') AS wins,
                    COUNT(*) FILTER (WHERE outcome='sl') AS losses,
                    ROUND(
                        COUNT(*) FILTER (WHERE outcome='tp')::numeric
                        / NULLIF(COUNT(*) FILTER (WHERE outcome IN ('tp','sl')), 0)
                        * 100, 1
                    ) AS win_rate
                   FROM backtest_msr_signals
                   WHERE run_id=$1
                   GROUP BY symbol, timeframe
                   ORDER BY symbol, timeframe""",
                run_id,
            )
        return [dict(r) for r in rows]

    async def delete_run(self, run_id: str) -> bool:
        """Delete a run and all its signals (CASCADE)."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM backtest_runs WHERE id=$1", run_id
            )
        return result == "DELETE 1"
