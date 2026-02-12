"""Backtest EMA signal repository — PostgreSQL-backed, with run_id tracking.

Uses the shared asyncpg pool. Writes to backtest_ema_signals table,
completely separate from the live ema_signals table.
"""

from __future__ import annotations

import logging

import asyncpg

from core.models.signal import SignalRecord

logger = logging.getLogger(__name__)


class BacktestEmaSignalRepo:
    """Persist and query backtest EMA signals in PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def save_signals(
        self, run_id: str, signals: list[SignalRecord]
    ) -> int:
        """Batch-insert all EMA signals for a run.

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
                """INSERT INTO backtest_ema_signals
                   (id, run_id, strategy, symbol, timeframe, signal_time, direction,
                    entry_price, tp_price, sl_price, atr_at_signal,
                    mae_ratio, mfe_ratio, outcome, outcome_time, outcome_price)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                   ON CONFLICT (run_id, id) DO UPDATE SET
                    outcome=EXCLUDED.outcome,
                    outcome_time=EXCLUDED.outcome_time,
                    outcome_price=EXCLUDED.outcome_price,
                    mae_ratio=EXCLUDED.mae_ratio,
                    mfe_ratio=EXCLUDED.mfe_ratio""",
                rows,
            )
        return len(rows)

    async def get_run_signals_stats(self, run_id: str) -> list[dict]:
        """Get per-symbol/timeframe breakdown for a run."""
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
                   FROM backtest_ema_signals
                   WHERE run_id=$1
                   GROUP BY symbol, timeframe
                   ORDER BY symbol, timeframe""",
                run_id,
            )
        return [dict(r) for r in rows]
