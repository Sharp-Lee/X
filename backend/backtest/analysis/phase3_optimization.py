"""Phase 3: Optimization & Risk Analysis.

Analyses included:
  1. MAE distribution (TP vs SL) + survival curve
  2. MFE distribution for SL trades + TP-reach analysis
  3. TP/SL grid search with pessimistic ambiguity handling
  4. Drawdown analysis (historical + Monte Carlo)
  5. Kelly criterion with confidence-interval sensitivity
  6. Equity curve metrics (Sharpe, Sortino, Calmar, per-symbol, per-timeframe)
  7. Time-in-trade analysis (duration distributions, R/hour)
  8. Breakeven sensitivity table

Entry point: run_phase3(df) -> dict
"""

from __future__ import annotations

import sys
from typing import Any

import numpy as np
import pandas as pd

from backtest.analysis.data_loader import (
    BREAKEVEN_WR,
    SL_ATR_MULT,
    SL_R,
    TP_ATR_MULT,
    TP_R,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _percentile_stats(series: pd.Series) -> dict[str, float]:
    """Return summary statistics for a numeric series."""
    if series.empty:
        return {k: np.nan for k in [
            "count", "mean", "std", "min",
            "p25", "p50", "p75", "p90", "p95", "p99", "max",
        ]}
    return {
        "count": int(len(series)),
        "mean": float(series.mean()),
        "std": float(series.std()),
        "min": float(series.min()),
        "p25": float(np.percentile(series, 25)),
        "p50": float(np.percentile(series, 50)),
        "p75": float(np.percentile(series, 75)),
        "p90": float(np.percentile(series, 90)),
        "p95": float(np.percentile(series, 95)),
        "p99": float(np.percentile(series, 99)),
        "max": float(series.max()),
    }


def _progress(msg: str) -> None:
    print(f"  [Phase 3] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 1. MAE Distribution Analysis
# ---------------------------------------------------------------------------

def _mae_distribution(df: pd.DataFrame) -> dict[str, Any]:
    _progress("MAE distribution analysis ...")

    tp_mae = df.loc[df["win"] == 1, "mae_ratio"]
    sl_mae = df.loc[df["win"] == 0, "mae_ratio"]

    tp_stats = _percentile_stats(tp_mae)
    sl_stats = _percentile_stats(sl_mae)

    # Survival curve: for each SL threshold, what % of TP trades survive?
    thresholds = np.arange(0.10, 1.005, 0.01)
    survival_pcts = []
    n_tp = len(tp_mae)
    for t in thresholds:
        # TP trade survives if its MAE is below the threshold
        surviving = (tp_mae < t).sum() if n_tp > 0 else 0
        pct = surviving / n_tp * 100.0 if n_tp > 0 else 0.0
        survival_pcts.append(pct)

    survival_df = pd.DataFrame({
        "threshold": np.round(thresholds, 4),
        "survival_pct": np.round(survival_pcts, 4),
    })

    # Find threshold where we start losing >5% of TP trades
    threshold_5pct = np.nan
    for i, pct in enumerate(survival_pcts):
        if pct < 95.0:
            threshold_5pct = float(thresholds[i])
            break

    tp_stats["median"] = tp_stats["p50"]
    sl_stats["median"] = sl_stats["p50"]

    return {
        "tp_stats": tp_stats,
        "sl_stats": sl_stats,
        "survival_curve": survival_df,
        "threshold_5pct_loss": threshold_5pct,
    }


# ---------------------------------------------------------------------------
# 2. MFE Distribution Analysis
# ---------------------------------------------------------------------------

def _mfe_distribution(df: pd.DataFrame) -> dict[str, Any]:
    _progress("MFE distribution analysis ...")

    sl_trades = df[df["win"] == 0]
    sl_mfe = sl_trades["mfe_ratio"]
    sl_mfe_atr = sl_trades["mfe_atr"]

    sl_stats = _percentile_stats(sl_mfe)
    sl_stats["median"] = sl_stats["p50"]

    # What % of SL trades reached the current TP level in MFE?
    # TP in risk units = TP_ATR_MULT / SL_ATR_MULT
    tp_in_risk_units = TP_ATR_MULT / SL_ATR_MULT  # 2.0 / 8.84 = 0.22624...
    n_sl = len(sl_mfe)
    pct_reached_tp = float(
        (sl_mfe >= tp_in_risk_units).sum() / n_sl * 100.0
    ) if n_sl > 0 else 0.0

    # MFE thresholds in ATR units for SL trades
    atr_levels = [0.5, 1.0, 1.5, 2.0]
    pct_reaching = []
    for level in atr_levels:
        reached = (sl_mfe_atr >= level).sum() if n_sl > 0 else 0
        pct_reaching.append(float(reached / n_sl * 100.0) if n_sl > 0 else 0.0)

    mfe_thresholds_df = pd.DataFrame({
        "atr_level": atr_levels,
        "pct_reaching": np.round(pct_reaching, 4),
    })

    return {
        "sl_stats": sl_stats,
        "pct_sl_reached_tp": round(pct_reached_tp, 4),
        "mfe_thresholds": mfe_thresholds_df,
    }


# ---------------------------------------------------------------------------
# 3. TP/SL Grid Search
# ---------------------------------------------------------------------------

def _grid_search(df: pd.DataFrame) -> dict[str, Any]:
    _progress("TP/SL grid search (this may take a moment) ...")

    mae_atr = df["mae_atr"].values
    mfe_atr = df["mfe_atr"].values
    n_total = len(df)

    tp_values = np.arange(1.0, 4.25, 0.25)
    sl_values = np.arange(4.0, 12.5, 0.5)

    rows = []
    for tp_new in tp_values:
        for sl_new in sl_values:
            hits_tp = mfe_atr >= tp_new
            hits_sl = mae_atr >= sl_new

            case1 = hits_tp & ~hits_sl   # definite TP
            case2 = hits_sl & ~hits_tp   # definite SL
            case3 = hits_tp & hits_sl    # ambiguous -> pessimistic = SL
            case4 = ~hits_tp & ~hits_sl  # indeterminate -> excluded

            wins = int(case1.sum())
            losses = int(case2.sum() + case3.sum())
            ambiguous = int(case3.sum())
            excluded = int(case4.sum())
            resolved = wins + losses

            if resolved == 0:
                continue

            wr = wins / resolved
            rr = tp_new / sl_new
            exp = wr * rr - (1.0 - wr) * 1.0
            total_r = wins * rr - losses * 1.0
            pf = (wins * rr) / losses if losses > 0 else float("inf")
            be_wr = 1.0 / (1.0 + rr)

            rows.append({
                "tp": round(tp_new, 2),
                "sl": round(sl_new, 2),
                "wins": wins,
                "losses": losses,
                "ambiguous": ambiguous,
                "excluded": excluded,
                "resolved": resolved,
                "wr": round(wr, 6),
                "rr": round(rr, 6),
                "expectancy": round(exp, 6),
                "profit_factor": round(pf, 6) if pf != float("inf") else float("inf"),
                "total_r": round(total_r, 2),
                "breakeven_wr": round(be_wr, 6),
            })

    results_df = pd.DataFrame(rows)

    # Best configurations
    best_exp = (
        results_df.loc[results_df["expectancy"].idxmax()].to_dict()
        if not results_df.empty else {}
    )
    best_total_r = (
        results_df.loc[results_df["total_r"].idxmax()].to_dict()
        if not results_df.empty else {}
    )
    # For profit factor, filter out infinite values for a meaningful "best"
    finite_pf = results_df[results_df["profit_factor"] != float("inf")]
    best_pf = (
        finite_pf.loc[finite_pf["profit_factor"].idxmax()].to_dict()
        if not finite_pf.empty else {}
    )

    # Current config row
    current_mask = (
        (results_df["tp"] == TP_ATR_MULT) & (results_df["sl"] == SL_ATR_MULT)
    )
    if current_mask.any():
        current_config = results_df.loc[current_mask].iloc[0].to_dict()
    else:
        # SL_ATR_MULT=8.84 may not be on the 0.5-step grid; find closest
        closest_sl = sl_values[np.argmin(np.abs(sl_values - SL_ATR_MULT))]
        closest_tp = tp_values[np.argmin(np.abs(tp_values - TP_ATR_MULT))]
        fallback = results_df[
            (results_df["tp"] == closest_tp) & (results_df["sl"] == closest_sl)
        ]
        current_config = fallback.iloc[0].to_dict() if not fallback.empty else {
            "note": f"Current config TP={TP_ATR_MULT}, SL={SL_ATR_MULT} not on grid"
        }

    _progress(
        f"Grid search complete: {len(results_df)} combinations evaluated, "
        f"best expectancy={best_exp.get('expectancy', 'N/A')}"
    )

    return {
        "results": results_df,
        "best_expectancy": best_exp,
        "best_total_r": best_total_r,
        "best_pf": best_pf,
        "current_config": current_config,
    }


# ---------------------------------------------------------------------------
# 4. Drawdown Analysis
# ---------------------------------------------------------------------------

def _drawdown_analysis(df: pd.DataFrame) -> dict[str, Any]:
    _progress("Drawdown analysis ...")

    sorted_df = df.sort_values("outcome_time").reset_index(drop=True)
    trade_r = sorted_df["trade_r"].values
    cum_r = np.cumsum(trade_r)
    running_max = np.maximum.accumulate(cum_r)
    drawdown = cum_r - running_max  # always <= 0

    max_dd = float(np.min(drawdown))
    max_dd_idx = int(np.argmin(drawdown))

    # Find the peak before max drawdown
    peak_idx = int(np.argmax(cum_r[:max_dd_idx + 1])) if max_dd_idx > 0 else 0
    max_dd_start = str(sorted_df.iloc[peak_idx]["outcome_time"])
    max_dd_end = str(sorted_df.iloc[max_dd_idx]["outcome_time"])

    # Find all drawdown periods > 100R depth
    major_drawdowns = []
    in_dd = False
    dd_peak_idx = 0
    dd_trough = 0.0
    dd_trough_idx = 0

    for i in range(len(drawdown)):
        if drawdown[i] < 0 and not in_dd:
            # Start of drawdown
            in_dd = True
            dd_peak_idx = i - 1 if i > 0 else 0
            dd_trough = drawdown[i]
            dd_trough_idx = i
        elif in_dd:
            if drawdown[i] < dd_trough:
                dd_trough = drawdown[i]
                dd_trough_idx = i
            if drawdown[i] == 0:
                # Drawdown ended (recovered)
                if dd_trough < -100.0:
                    major_drawdowns.append({
                        "depth_r": round(float(dd_trough), 2),
                        "peak_time": str(sorted_df.iloc[dd_peak_idx]["outcome_time"]),
                        "trough_time": str(sorted_df.iloc[dd_trough_idx]["outcome_time"]),
                        "recovery_time": str(sorted_df.iloc[i]["outcome_time"]),
                        "n_trades": i - dd_peak_idx,
                    })
                in_dd = False

    # Check if we ended in a drawdown > 100R
    if in_dd and dd_trough < -100.0:
        major_drawdowns.append({
            "depth_r": round(float(dd_trough), 2),
            "peak_time": str(sorted_df.iloc[dd_peak_idx]["outcome_time"]),
            "trough_time": str(sorted_df.iloc[dd_trough_idx]["outcome_time"]),
            "recovery_time": "ongoing",
            "n_trades": len(drawdown) - dd_peak_idx,
        })

    # Monte Carlo max drawdown simulation
    _progress("Monte Carlo drawdown simulation (10,000 sims) ...")
    mc_result = _monte_carlo_drawdown(df)

    return {
        "max_drawdown_r": round(max_dd, 2),
        "max_dd_start": max_dd_start,
        "max_dd_end": max_dd_end,
        "major_drawdowns": major_drawdowns,
        "monte_carlo": mc_result,
    }


def _monte_carlo_drawdown(df: pd.DataFrame) -> dict[str, float]:
    """Monte Carlo simulation for max drawdown distribution.

    Strategy: for each of 10,000 simulations, generate n binary trades
    with the observed win rate, compute equity curve and max drawdown.

    Uses the actual trade count capped at 50,000 per sim for memory
    efficiency. With n=50,000 and 10K sims, peak memory ~4GB of float64
    which is manageable.
    """
    n_actual = len(df)
    n_sims = 10_000
    # Cap per-sim trade count for memory; 50K trades per sim is sufficient
    # to capture drawdown characteristics
    n_per_sim = min(n_actual, 50_000)

    observed_wr = float(df["win"].mean())
    rng = np.random.default_rng(seed=42)

    max_drawdowns = np.empty(n_sims, dtype=np.float64)

    # Process in batches to manage memory
    batch_size = 500
    for batch_start in range(0, n_sims, batch_size):
        batch_end = min(batch_start + batch_size, n_sims)
        batch_n = batch_end - batch_start

        # Generate random outcomes: shape (batch_n, n_per_sim)
        wins = rng.random((batch_n, n_per_sim)) < observed_wr
        trade_r = np.where(wins, TP_R, -SL_R)

        # Cumulative R
        cum_r = np.cumsum(trade_r, axis=1)

        # Running max along trade axis
        running_max = np.maximum.accumulate(cum_r, axis=1)

        # Drawdown at each point
        dd = cum_r - running_max  # <= 0

        # Max drawdown per sim (most negative value)
        max_drawdowns[batch_start:batch_end] = np.min(dd, axis=1)

        if (batch_start // batch_size) % 5 == 0:
            _progress(
                f"  MC batch {batch_start // batch_size + 1}"
                f"/{(n_sims + batch_size - 1) // batch_size}"
            )

    return {
        "n_sims": n_sims,
        "n_trades_per_sim": n_per_sim,
        "observed_wr": round(observed_wr, 6),
        "mean": round(float(np.mean(max_drawdowns)), 2),
        "p5": round(float(np.percentile(max_drawdowns, 5)), 2),
        "p1": round(float(np.percentile(max_drawdowns, 1)), 2),
        "p01": round(float(np.percentile(max_drawdowns, 0.1)), 2),
    }


# ---------------------------------------------------------------------------
# 5. Kelly Criterion
# ---------------------------------------------------------------------------

def _kelly_criterion(df: pd.DataFrame) -> dict[str, Any]:
    _progress("Kelly criterion ...")

    n = len(df)
    p = float(df["win"].mean())  # observed win rate
    q = 1.0 - p
    b = TP_R / SL_R  # odds ratio = 1.0 / 4.42

    # Full Kelly: f* = (p*b - q) / b
    full_kelly = (p * b - q) / b if b > 0 else 0.0

    # 95% CI on win rate (normal approximation)
    se = np.sqrt(p * q / n) if n > 0 else 0.0
    ci_lower = p - 1.96 * se
    ci_upper = p + 1.96 * se

    # Kelly at CI lower bound
    kelly_ci_lower = (ci_lower * b - (1.0 - ci_lower)) / b if b > 0 else 0.0

    return {
        "full": round(full_kelly, 6),
        "half": round(full_kelly / 2.0, 6),
        "quarter": round(full_kelly / 4.0, 6),
        "at_ci_lower": round(kelly_ci_lower, 6),
        "edge_significant": kelly_ci_lower > 0,
        "win_rate": round(p, 6),
        "ci_95_lower": round(ci_lower, 6),
        "ci_95_upper": round(ci_upper, 6),
        "odds_ratio_b": round(b, 6),
    }


# ---------------------------------------------------------------------------
# 6. Equity Curve Metrics
# ---------------------------------------------------------------------------

def _equity_curve_metrics(df: pd.DataFrame) -> dict[str, Any]:
    _progress("Equity curve metrics ...")

    sorted_df = df.sort_values("outcome_time").copy()

    # Daily R returns
    sorted_df["outcome_date"] = sorted_df["outcome_time"].dt.date
    daily_r = sorted_df.groupby("outcome_date")["trade_r"].sum()

    # Trading-day count and total R
    n_days = len(daily_r)
    total_r = float(daily_r.sum())

    # Annualized R: scale by 365 / actual_days
    if n_days > 1:
        date_range = (daily_r.index[-1] - daily_r.index[0]).days
        annualized_r = total_r * (365.0 / date_range) if date_range > 0 else total_r
    else:
        annualized_r = total_r
        date_range = 1

    # Sharpe ratio = mean(daily_R) / std(daily_R) * sqrt(365)
    mean_daily = float(daily_r.mean())
    std_daily = float(daily_r.std())
    sharpe = (mean_daily / std_daily * np.sqrt(365)) if std_daily > 0 else 0.0

    # Sortino ratio = mean(daily_R) / downside_std * sqrt(365)
    downside = daily_r[daily_r < 0]
    downside_std = float(downside.std()) if len(downside) > 1 else 0.0
    sortino = (mean_daily / downside_std * np.sqrt(365)) if downside_std > 0 else 0.0

    # Max drawdown for Calmar
    cum_r = np.cumsum(sorted_df["trade_r"].values)
    running_max = np.maximum.accumulate(cum_r)
    max_dd = float(np.min(cum_r - running_max))
    calmar = (annualized_r / abs(max_dd)) if max_dd != 0 else 0.0

    # Per-symbol equity curves
    by_symbol = (
        sorted_df.groupby("symbol")["trade_r"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "total_r", "count": "n_trades"})
        .sort_values("total_r", ascending=False)
        .reset_index()
    )

    # Per-timeframe equity curves
    by_timeframe = (
        sorted_df.groupby("timeframe")["trade_r"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "total_r", "count": "n_trades"})
        .sort_values("total_r", ascending=False)
        .reset_index()
    )

    return {
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "calmar": round(calmar, 4),
        "annualized_r": round(annualized_r, 2),
        "total_r": round(total_r, 2),
        "n_trading_days": n_days,
        "date_range_days": date_range if isinstance(date_range, int) else int(date_range),
        "by_symbol": by_symbol,
        "by_timeframe": by_timeframe,
    }


# ---------------------------------------------------------------------------
# 7. Time-in-Trade Analysis
# ---------------------------------------------------------------------------

def _time_in_trade(df: pd.DataFrame) -> dict[str, Any]:
    _progress("Time-in-trade analysis ...")

    tp_dur = df.loc[df["win"] == 1, "duration_min"]
    sl_dur = df.loc[df["win"] == 0, "duration_min"]

    tp_stats = _percentile_stats(tp_dur)
    sl_stats = _percentile_stats(sl_dur)
    tp_stats["median"] = tp_stats["p50"]
    sl_stats["median"] = sl_stats["p50"]

    # Win rate by duration bucket
    bins = [0, 5, 15, 60, 240, 1440, float("inf")]
    labels = ["<5min", "5-15min", "15-60min", "1-4hr", "4-24hr", "24hr+"]

    df_copy = df.copy()
    df_copy["duration_bucket"] = pd.cut(
        df_copy["duration_min"], bins=bins, labels=labels, right=False,
    )

    bucket_stats = (
        df_copy.groupby("duration_bucket", observed=True)
        .agg(
            n=("win", "count"),
            wins=("win", "sum"),
            total_r=("trade_r", "sum"),
        )
    )
    bucket_stats["win_rate"] = (bucket_stats["wins"] / bucket_stats["n"]).round(6)
    bucket_stats["expectancy"] = (bucket_stats["total_r"] / bucket_stats["n"]).round(6)
    bucket_stats = bucket_stats.reset_index().rename(
        columns={"duration_bucket": "bucket"}
    )

    # Aggregate R per hour
    total_duration_hours = df["duration_min"].sum() / 60.0
    total_r = df["trade_r"].sum()
    r_per_hour = float(total_r / total_duration_hours) if total_duration_hours > 0 else 0.0

    return {
        "tp_duration_stats": tp_stats,
        "sl_duration_stats": sl_stats,
        "by_duration_bucket": bucket_stats,
        "aggregate_r_per_hour": round(r_per_hour, 6),
    }


# ---------------------------------------------------------------------------
# 8. Breakeven Sensitivity
# ---------------------------------------------------------------------------

def _breakeven_sensitivity(df: pd.DataFrame) -> dict[str, Any]:
    _progress("Breakeven sensitivity ...")

    n = len(df)
    observed_wr = float(df["win"].mean())
    margin_pct = observed_wr - BREAKEVEN_WR

    # Impact of 1% WR change on total R
    # delta_total_R = n * 0.01 * (TP_R + SL_R)
    impact_per_1pct = n * 0.01 * (TP_R + SL_R)

    # Sensitivity table: WR from 79% to 86%
    wr_range = np.arange(0.79, 0.8605, 0.005)
    rows = []
    for wr in wr_range:
        exp = wr * TP_R - (1.0 - wr) * SL_R
        total_r = n * exp
        # Annualized: estimate from the dataset's time span
        date_range = (
            df["outcome_time"].max() - df["outcome_time"].min()
        ).total_seconds() / 86400.0
        annual_r = total_r * (365.0 / date_range) if date_range > 0 else total_r

        rows.append({
            "win_rate": round(float(wr), 4),
            "expectancy_per_trade": round(float(exp), 6),
            "total_r": round(float(total_r), 2),
            "annual_r": round(float(annual_r), 2),
        })

    sensitivity_df = pd.DataFrame(rows)

    return {
        "breakeven_wr": round(BREAKEVEN_WR, 6),
        "observed_wr": round(observed_wr, 6),
        "margin_pct": round(margin_pct, 6),
        "impact_per_1pct": round(impact_per_1pct, 2),
        "sensitivity_table": sensitivity_df,
    }


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def run_phase3(df: pd.DataFrame) -> dict[str, Any]:
    """Run all Phase 3 analyses.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns: win, trade_r, symbol, timeframe, direction,
        signal_time, outcome_time, mae_ratio, mfe_ratio, mae_atr, mfe_atr,
        atr_at_signal, duration_min, year, quarter.

    Returns
    -------
    dict
        Nested dictionary with keys: mae_distribution, mfe_distribution,
        grid_search, drawdown, kelly, equity_curve, time_in_trade, breakeven.
    """
    n = len(df)
    wr = df["win"].mean() if n > 0 else 0.0
    print(f"\n{'='*60}", flush=True)
    print(f"Phase 3: Optimization & Risk Analysis", flush=True)
    print(f"  Signals: {n:,} (TP={df['win'].sum():,}, SL={(1-df['win']).sum():,.0f})", flush=True)
    print(f"  Win rate: {wr:.4f}  (breakeven: {BREAKEVEN_WR:.4f})", flush=True)
    print(f"{'='*60}\n", flush=True)

    results: dict[str, Any] = {}

    results["mae_distribution"] = _mae_distribution(df)
    results["mfe_distribution"] = _mfe_distribution(df)
    results["grid_search"] = _grid_search(df)
    results["drawdown"] = _drawdown_analysis(df)
    results["kelly"] = _kelly_criterion(df)
    results["equity_curve"] = _equity_curve_metrics(df)
    results["time_in_trade"] = _time_in_trade(df)
    results["breakeven"] = _breakeven_sensitivity(df)

    print(f"\n{'='*60}", flush=True)
    print(f"Phase 3 complete.", flush=True)
    print(f"{'='*60}\n", flush=True)

    return results
