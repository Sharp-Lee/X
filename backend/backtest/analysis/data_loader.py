"""Load backtest signals from PostgreSQL into pandas DataFrame.

Central data loading module used by all analysis phases.
Converts Decimal columns to float64 and computes actual P&L
using real entry/outcome prices.

All analysis is per (symbol, timeframe) group, so P&L uses raw
price differences — no normalization by entry_price needed.
"""

from __future__ import annotations

import asyncio
import logging

import asyncpg
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Strategy parameter constants (from Pine Script)
TP_ATR_MAX = 2.0    # Maximum TP distance = 2.0 * ATR (may be capped by kline high/low)
SL_ATR_MULT = 8.84  # SL distance = 8.84 * ATR (always exact, no cap)


async def _fetch_signals(
    database_url: str,
    run_id: str,
) -> pd.DataFrame:
    """Fetch resolved signals from PostgreSQL."""
    conn = await asyncpg.connect(database_url)
    try:
        rows = await conn.fetch(
            """
            SELECT
                id, symbol, timeframe, direction, signal_time,
                entry_price, tp_price, sl_price,
                atr_at_signal, max_atr, streak_at_signal,
                mae_ratio, mfe_ratio,
                outcome, outcome_time, outcome_price
            FROM backtest_signals
            WHERE run_id = $1
              AND outcome IN ('tp', 'sl')
            ORDER BY signal_time
            """,
            run_id,
        )
        logger.info(f"Fetched {len(rows)} resolved signals for run={run_id}")
        return pd.DataFrame([dict(r) for r in rows])
    finally:
        await conn.close()


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns based on actual prices.

    P&L uses raw price differences (not percentages).
    Analysis should always group by (symbol, timeframe).
    """
    # Convert Decimal columns to float64
    decimal_cols = [
        "entry_price", "tp_price", "sl_price",
        "atr_at_signal", "max_atr", "mae_ratio", "mfe_ratio",
        "outcome_price",
    ]
    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # --- Primary P&L: raw price difference ---
    # pnl = direction * (outcome_price - entry_price)
    # Positive = profit, Negative = loss
    df["pnl"] = df["direction"] * (df["outcome_price"] - df["entry_price"])

    # --- TP/SL distances in price units (always positive) ---
    df["tp_dist"] = df["direction"] * (df["tp_price"] - df["entry_price"])
    df["sl_dist"] = -df["direction"] * (df["sl_price"] - df["entry_price"])

    # --- TP distance in ATR units (≤ 2.0 due to kline high/low cap) ---
    df["tp_atr"] = df["tp_dist"] / df["atr_at_signal"]

    # --- MAE/MFE in price units (always positive) ---
    # mae_ratio/mfe_ratio are relative to sl_dist (risk_amount)
    df["mae"] = df["mae_ratio"] * df["sl_dist"]
    df["mfe"] = df["mfe_ratio"] * df["sl_dist"]

    # --- MAE/MFE in ATR units ---
    df["mae_atr"] = df["mae_ratio"] * SL_ATR_MULT
    df["mfe_atr"] = df["mfe_ratio"] * SL_ATR_MULT

    # --- Binary win indicator ---
    df["win"] = (df["outcome"] == "tp").astype(int)

    # --- Trade duration in minutes ---
    if "outcome_time" in df.columns and "signal_time" in df.columns:
        df["duration_min"] = (
            df["outcome_time"] - df["signal_time"]
        ).dt.total_seconds() / 60.0

    # --- Time components for temporal analysis ---
    df["hour_utc"] = df["signal_time"].dt.hour
    df["day_of_week"] = df["signal_time"].dt.dayofweek  # 0=Mon
    df["month"] = df["signal_time"].dt.month
    df["year"] = df["signal_time"].dt.year

    return df


def load_signals(
    database_url: str = "postgresql://localhost/crypto_live",
    run_id: str = "2e3728409f3a1717",
) -> pd.DataFrame:
    """Load and prepare signals DataFrame.

    Returns DataFrame with columns:
        Raw:    id, symbol, timeframe, direction, signal_time,
                entry_price, tp_price, sl_price,
                atr_at_signal, max_atr, streak_at_signal,
                mae_ratio, mfe_ratio, outcome, outcome_time, outcome_price
        P&L:    pnl (raw price diff, direction-aware)
        Dist:   tp_dist, sl_dist (price units, always positive)
                tp_atr (TP in ATR units, ≤ 2.0 due to cap)
        MAE:    mae, mfe (price units, always positive)
                mae_atr, mfe_atr (in ATR units)
        Meta:   win, duration_min, hour_utc, day_of_week, month, year

    Note: pnl/tp_dist/sl_dist/mae/mfe are in the symbol's price units.
          Always group by (symbol, timeframe) before aggregating.
    """
    df = asyncio.run(_fetch_signals(database_url, run_id))
    df = _add_derived_columns(df)
    logger.info(
        f"Prepared {len(df)} signals: "
        f"{df['win'].sum()} TP + {(~df['win'].astype(bool)).sum()} SL, "
        f"WR={df['win'].mean():.4f}"
    )
    return df
