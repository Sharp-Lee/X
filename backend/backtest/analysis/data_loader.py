"""Load backtest signals from PostgreSQL into pandas DataFrame.

Central data loading module used by all analysis phases.
Converts Decimal columns to float64 and adds derived columns.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Strategy constants
TP_ATR_MULT = 2.0    # TP = 2.0 * ATR
SL_ATR_MULT = 8.84   # SL = 8.84 * ATR
TP_R = 1.0            # +1.0R per TP
SL_R = 4.42           # -4.42R per SL
BREAKEVEN_WR = SL_R / (TP_R + SL_R)  # 0.81549...


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
    """Add computed columns used across all analysis phases."""
    # Convert Decimal columns to float64
    decimal_cols = [
        "entry_price", "tp_price", "sl_price",
        "atr_at_signal", "max_atr", "mae_ratio", "mfe_ratio",
        "outcome_price",
    ]
    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Binary win indicator
    df["win"] = (df["outcome"] == "tp").astype(int)

    # R-multiple per trade
    df["trade_r"] = np.where(df["outcome"] == "tp", TP_R, -SL_R)

    # Trade duration in minutes
    if "outcome_time" in df.columns and "signal_time" in df.columns:
        df["duration_min"] = (
            df["outcome_time"] - df["signal_time"]
        ).dt.total_seconds() / 60.0

    # MAE/MFE in ATR units (from risk units)
    df["mae_atr"] = df["mae_ratio"] * SL_ATR_MULT
    df["mfe_atr"] = df["mfe_ratio"] * SL_ATR_MULT

    # Time components for temporal analysis
    df["hour_utc"] = df["signal_time"].dt.hour
    df["day_of_week"] = df["signal_time"].dt.dayofweek  # 0=Mon
    df["month"] = df["signal_time"].dt.month
    df["year"] = df["signal_time"].dt.year
    df["quarter"] = df["signal_time"].dt.to_period("Q").astype(str)

    return df


def load_signals(
    database_url: str = "postgresql://localhost/crypto_live",
    run_id: str = "2e3728409f3a1717",
) -> pd.DataFrame:
    """Load and prepare signals DataFrame.

    Returns DataFrame with columns:
        id, symbol, timeframe, direction, signal_time,
        entry_price, tp_price, sl_price,
        atr_at_signal, max_atr, streak_at_signal,
        mae_ratio, mfe_ratio, outcome, outcome_time, outcome_price,
        win, trade_r, duration_min, mae_atr, mfe_atr,
        hour_utc, day_of_week, month, year, quarter
    """
    df = asyncio.run(_fetch_signals(database_url, run_id))
    df = _add_derived_columns(df)
    logger.info(
        f"Prepared {len(df)} signals: "
        f"{df['win'].sum()} TP + {(~df['win'].astype(bool)).sum()} SL, "
        f"WR={df['win'].mean():.4f}"
    )
    return df
