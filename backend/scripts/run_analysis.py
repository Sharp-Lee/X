#!/usr/bin/env python3
"""Run comprehensive statistical analysis on backtest signals.

Usage:
    python scripts/run_analysis.py
    python scripts/run_analysis.py --phase 1
    python scripts/run_analysis.py --phase 1 2 3
"""

import argparse
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def format_results_phase1(r: dict) -> str:
    """Format Phase 1 results for display."""
    lines = []
    lines.append("\n" + "=" * 70)
    lines.append("  PHASE 1: STATISTICAL SIGNIFICANCE ANALYSIS")
    lines.append("=" * 70)

    # Autocorrelation
    ac = r["autocorrelation"]["aggregate"]
    lines.append("\n--- Autocorrelation (Aggregate) ---")
    lines.append(f"  Lag-1 autocorrelation: {ac['lag1_autocorr']:.6f}")
    lines.append(f"  Ljung-Box p (lag=10):  {ac['ljung_box_p_10']:.6f}")
    lines.append(f"  Ljung-Box p (lag=50):  {ac['ljung_box_p_50']:.6f}")
    lines.append(f"  Runs test z={ac['runs_test_z']:.4f}, p={ac['runs_test_p']:.6f}")
    lines.append(f"  n_raw={ac['n_raw']:,}  n_eff={ac['n_eff']:,}  "
                 f"(ratio={ac['n_eff']/ac['n_raw']:.4f})")

    # Per-segment autocorrelation summary
    seg_ac = r["autocorrelation"]["per_segment"]
    if seg_ac:
        sig_count = sum(1 for s in seg_ac
                        if s.get("ljung_box_p_10") is not None
                        and not np.isnan(s["ljung_box_p_10"])
                        and s["ljung_box_p_10"] < 0.05)
        lines.append(f"\n  Per-segment: {sig_count}/{len(seg_ac)} segments "
                     f"show significant autocorrelation (LB p<0.05)")
        # Show top 5 most autocorrelated
        valid = [s for s in seg_ac if s.get("lag1_autocorr") is not None
                 and not np.isnan(s.get("lag1_autocorr", float("nan")))]
        if valid:
            sorted_segs = sorted(valid,
                                 key=lambda x: abs(x["lag1_autocorr"]),
                                 reverse=True)[:5]
            lines.append("  Top 5 by |autocorr|:")
            for s in sorted_segs:
                lines.append(
                    f"    {s['symbol']:8s} {s['timeframe']:4s} "
                    f"dir={s['direction']:+d}  "
                    f"lag1={s['lag1_autocorr']:+.4f}  "
                    f"n_eff={s['n_eff']:,}/{s['n_raw']:,}"
                )

    # Aggregate significance
    sig = r["aggregate_significance"]
    lines.append("\n--- Aggregate Significance ---")
    lines.append(f"  Win rate: {sig['win_rate']:.4f} "
                 f"({sig['win_rate']*100:.2f}%)")
    lines.append(f"  Breakeven: {0.8155:.4f} ({81.55:.2f}%)")
    lines.append(f"  z-stat (raw n):      {sig['z_stat_raw']:.4f}  "
                 f"p={sig['p_value_raw']:.2e}")
    lines.append(f"  z-stat (adjusted):   {sig['z_stat_adjusted']:.4f}  "
                 f"p={sig['p_value_adjusted']:.2e}")
    wr_ci = sig["win_rate_ci_95"]
    lines.append(f"  Win rate 95% CI:     [{wr_ci[0]:.4f}, {wr_ci[1]:.4f}]")
    e_ci = sig["expectancy_ci_95"]
    lines.append(f"  Expectancy:          {sig['expectancy']:.4f}R")
    lines.append(f"  Expectancy 95% CI:   [{e_ci[0]:.4f}, {e_ci[1]:.4f}]R")
    pf_ci = sig["profit_factor_ci_95"]
    lines.append(f"  Profit factor:       {sig['profit_factor']:.4f}")
    lines.append(f"  PF 95% CI:           [{pf_ci[0]:.4f}, {pf_ci[1]:.4f}]")

    # Per-segment
    seg_df = r["per_segment"]
    if isinstance(seg_df, pd.DataFrame) and not seg_df.empty:
        lines.append("\n--- Per-Segment Classification ---")
        if "classification" in seg_df.columns:
            vc = seg_df["classification"].value_counts()
            for cls, cnt in vc.items():
                lines.append(f"  {cls}: {cnt} segments")

        # Show segments below breakeven
        below = seg_df[seg_df["win_rate"] < 0.8155]
        if not below.empty:
            lines.append(f"\n  Segments BELOW breakeven ({len(below)}):")
            for _, row in below.iterrows():
                lines.append(
                    f"    {row['symbol']:8s} {row['timeframe']:4s} "
                    f"dir={int(row['direction']):+d}  "
                    f"WR={row['win_rate']:.4f}  "
                    f"n={int(row['n']):,}  "
                    f"p_bh={row.get('p_bh', float('nan')):.4f}  "
                    f"{row.get('classification', '')}"
                )

    # Stationarity
    st = r["stationarity"]
    lines.append("\n--- Non-Stationarity Tests ---")
    lines.append(f"  Year-over-year chi²: {st['yearly_chi2']:.2f}  "
                 f"p={st['yearly_chi2_p']:.6f}")
    lines.append(f"  Half-period z-test:  z={st['half_period_z']:.4f}  "
                 f"p={st['half_period_p']:.6f}")

    yearly = st.get("yearly_data")
    if isinstance(yearly, pd.DataFrame) and not yearly.empty:
        lines.append("\n  Year-by-year win rates:")
        for _, row in yearly.iterrows():
            lines.append(
                f"    {int(row['year'])}: "
                f"WR={row['win_rate']:.4f}  "
                f"n={int(row['n']):,}"
            )

    # Bootstrap
    bs = r["bootstrap"]
    lines.append("\n--- Bootstrap Validation (10,000 resamples) ---")
    lines.append(f"  Win rate 95% CI:   [{bs['win_rate_ci_95'][0]:.4f}, "
                 f"{bs['win_rate_ci_95'][1]:.4f}]")
    lines.append(f"  Expectancy 95% CI: [{bs['expectancy_ci_95'][0]:.4f}, "
                 f"{bs['expectancy_ci_95'][1]:.4f}]R")
    lines.append(f"  PF 95% CI:         [{bs['pf_ci_95'][0]:.4f}, "
                 f"{bs['pf_ci_95'][1]:.4f}]")
    lines.append(f"  P(below breakeven): {bs['p_below_breakeven']:.4f}")

    return "\n".join(lines)


def format_results_phase2(r: dict) -> str:
    """Format Phase 2 results for display."""
    lines = []
    lines.append("\n" + "=" * 70)
    lines.append("  PHASE 2: CONDITIONAL ANALYSIS")
    lines.append("=" * 70)

    # ATR Regime
    atr = r["atr_regime"]
    lines.append("\n--- ATR Volatility Regime ---")
    lines.append(f"  Chi² test p={atr['chi2_p']:.6f}")
    lines.append(f"  Cochran-Armitage trend p={atr['trend_p']:.6f}")
    qdf = atr["quintile_data"]
    if isinstance(qdf, pd.DataFrame) and not qdf.empty:
        lines.append("  Quintile | n       | Win Rate | Expectancy")
        lines.append("  " + "-" * 50)
        for _, row in qdf.iterrows():
            lines.append(
                f"  Q{int(row.get('atr_quintile', row.get('quintile', 0))):d}      | "
                f"{int(row['n']):>7,} | "
                f"{row['win_rate']:.4f}   | "
                f"{row['expectancy']:+.4f}R"
            )

    # Temporal
    tmp = r["temporal"]
    lines.append("\n--- Temporal Patterns ---")
    lines.append(f"  Hourly chi² p={tmp['hourly_chi2_p']:.6f}")
    lines.append(f"  Daily chi² p={tmp['daily_chi2_p']:.6f}")
    lines.append(f"  Monthly chi² p={tmp['monthly_chi2_p']:.6f}")

    session = tmp.get("session")
    if isinstance(session, pd.DataFrame) and not session.empty:
        lines.append("  Session performance:")
        for _, row in session.iterrows():
            lines.append(
                f"    {row.get('session', row.get('hour_utc', '?')):<25s} "
                f"WR={row['win_rate']:.4f}  "
                f"n={int(row['n']):,}"
            )

    # Streak
    strk = r["streak"]
    lines.append("\n--- Streak Analysis ---")
    lines.append(f"  Chi² test p={strk['chi2_p']:.6f}")
    lines.append(f"  Logistic: streak coeff={strk['logistic_streak_coeff']:.6f} "
                 f"p={strk['logistic_streak_p']:.6f}")
    lines.append(f"  Logistic: streak² coeff={strk['logistic_streak2_coeff']:.6f} "
                 f"p={strk['logistic_streak2_p']:.6f}")

    bucket_df = strk.get("by_bucket")
    if isinstance(bucket_df, pd.DataFrame) and not bucket_df.empty:
        lines.append("  Streak buckets:")
        for _, row in bucket_df.iterrows():
            marker = " !" if row["win_rate"] < 0.8155 else ""
            lines.append(
                f"    {str(row.get('streak_bucket', row.get('bucket', '?'))):<20s} "
                f"WR={row['win_rate']:.4f}  "
                f"n={int(row['n']):>7,}  "
                f"E={row['expectancy']:+.4f}R{marker}"
            )

    # Direction × Regime
    dr = r["direction_regime"]
    lines.append("\n--- Direction × Market Regime ---")
    dr_data = dr.get("data")
    if isinstance(dr_data, pd.DataFrame) and not dr_data.empty:
        for _, row in dr_data.iterrows():
            d_label = "LONG" if row["direction"] == 1 else "SHORT"
            lines.append(
                f"  {row.get('regime', row.get('market_regime', '?')):<15s} "
                f"{d_label:<5s}  "
                f"WR={row['win_rate']:.4f}  "
                f"n={int(row['n']):>7,}"
            )

    # Yearly
    yr = r["yearly"]
    lines.append("\n--- Yearly Performance ---")
    yr_data = yr.get("by_year")
    if isinstance(yr_data, pd.DataFrame) and not yr_data.empty:
        for _, row in yr_data.iterrows():
            marker = " !" if row["win_rate"] < 0.8155 else ""
            lines.append(
                f"  {int(row['year'])}: "
                f"WR={row['win_rate']:.4f}  "
                f"E={row['expectancy']:+.4f}R  "
                f"n={int(row['n']):>7,}{marker}"
            )
    lines.append(f"  Best quarter:  {yr.get('best_quarter', '?')}")
    lines.append(f"  Worst quarter: {yr.get('worst_quarter', '?')}")

    # Density
    den = r["density"]
    lines.append("\n--- Signal Density ---")
    lines.append(f"  Spearman r={den['spearman_r']:.4f}  p={den['spearman_p']:.6f}")

    return "\n".join(lines)


def format_results_phase3(r: dict) -> str:
    """Format Phase 3 results for display."""
    lines = []
    lines.append("\n" + "=" * 70)
    lines.append("  PHASE 3: OPTIMIZATION & RISK ANALYSIS")
    lines.append("=" * 70)

    # MAE
    mae = r["mae_distribution"]
    lines.append("\n--- MAE Distribution ---")
    for label, stats in [("TP trades", mae["tp_stats"]),
                          ("SL trades", mae["sl_stats"])]:
        lines.append(f"  {label}:")
        lines.append(
            f"    mean={stats['mean']:.4f}  "
            f"median={stats['p50']:.4f}  "
            f"p90={stats['p90']:.4f}  "
            f"p99={stats['p99']:.4f}"
        )
    lines.append(f"  SL threshold where 5% TP killed: "
                 f"{mae['threshold_5pct_loss']:.4f} risk "
                 f"({mae['threshold_5pct_loss'] * 8.84:.2f} ATR)")

    # MFE
    mfe = r["mfe_distribution"]
    lines.append("\n--- MFE Distribution (SL trades) ---")
    lines.append(f"  SL trades that reached TP level: "
                 f"{mfe['pct_sl_reached_tp']:.2f}%")
    mfe_th = mfe.get("mfe_thresholds")
    if isinstance(mfe_th, pd.DataFrame) and not mfe_th.empty:
        lines.append("  MFE reach thresholds:")
        for _, row in mfe_th.iterrows():
            lines.append(
                f"    {row.get('atr_level', row.iloc[0]):.1f} ATR: "
                f"{row.get('pct_reaching', row.iloc[1]):.1f}%"
            )

    # Grid Search
    gs = r["grid_search"]
    lines.append("\n--- TP/SL Grid Search ---")
    current = gs.get("current_config", {})
    if current:
        lines.append(
            f"  Current (TP=2.0, SL=8.84): "
            f"WR={current.get('win_rate', current.get('wr', 0)):.2f}%  "
            f"E={current.get('expectancy', current.get('exp', 0)):.4f}R  "
            f"PF={current.get('profit_factor', current.get('pf', 0)):.3f}"
        )
    for label, key in [("Best expectancy", "best_expectancy"),
                        ("Best total R", "best_total_r"),
                        ("Best PF", "best_pf")]:
        d = gs.get(key, {})
        if d:
            lines.append(
                f"  {label}: "
                f"TP={d.get('tp', d.get('tp_atr', '?'))}, "
                f"SL={d.get('sl', d.get('sl_atr', '?'))}  "
                f"WR={d.get('win_rate', d.get('wr', 0)):.2f}%  "
                f"E={d.get('expectancy', d.get('exp', 0)):.4f}R  "
                f"PF={d.get('profit_factor', d.get('pf', 0)):.3f}  "
                f"TotalR={d.get('total_r', 0):.0f}"
            )

    # Drawdown
    dd = r["drawdown"]
    lines.append("\n--- Drawdown Analysis ---")
    lines.append(f"  Historical max drawdown: {dd['max_drawdown_r']:.2f}R")
    lines.append(f"  DD period: {dd.get('max_dd_start', '?')} → "
                 f"{dd.get('max_dd_end', '?')}")
    mc = dd.get("monte_carlo", {})
    if mc:
        lines.append(f"  Monte Carlo (10K sims):")
        lines.append(f"    Mean max DD: {mc.get('mean', 0):.2f}R")
        lines.append(f"    5th pctile:  {mc.get('p5', 0):.2f}R")
        lines.append(f"    1st pctile:  {mc.get('p1', 0):.2f}R")
        lines.append(f"    0.1 pctile:  {mc.get('p01', 0):.2f}R")

    # Kelly
    kelly = r["kelly"]
    lines.append("\n--- Kelly Criterion ---")
    lines.append(f"  Full Kelly:    {kelly['full']*100:.2f}%")
    lines.append(f"  Half Kelly:    {kelly['half']*100:.2f}%")
    lines.append(f"  Quarter Kelly: {kelly['quarter']*100:.2f}%")
    lines.append(f"  At CI lower:   {kelly['at_ci_lower']*100:.2f}%")
    lines.append(f"  Edge significant for sizing: {kelly['edge_significant']}")

    # Equity
    eq = r["equity_curve"]
    lines.append("\n--- Equity Curve Metrics ---")
    lines.append(f"  Sharpe ratio:    {eq['sharpe']:.3f}")
    lines.append(f"  Sortino ratio:   {eq['sortino']:.3f}")
    lines.append(f"  Calmar ratio:    {eq['calmar']:.3f}")
    lines.append(f"  Annualized R:    {eq['annualized_r']:.1f}R")

    sym_df = eq.get("by_symbol")
    if isinstance(sym_df, pd.DataFrame) and not sym_df.empty:
        lines.append("  By symbol:")
        for _, row in sym_df.iterrows():
            lines.append(f"    {row['symbol']:8s}  {row['total_r']:+.1f}R")

    tf_df = eq.get("by_timeframe")
    if isinstance(tf_df, pd.DataFrame) and not tf_df.empty:
        lines.append("  By timeframe:")
        for _, row in tf_df.iterrows():
            lines.append(f"    {row['timeframe']:4s}  {row['total_r']:+.1f}R")

    # Time in trade
    tit = r["time_in_trade"]
    lines.append("\n--- Time-in-Trade ---")
    for label, stats in [("TP", tit["tp_duration_stats"]),
                          ("SL", tit["sl_duration_stats"])]:
        lines.append(
            f"  {label} duration: "
            f"median={stats.get('p50', stats.get('median', 0)):.1f}min  "
            f"mean={stats.get('mean', 0):.1f}min  "
            f"p90={stats.get('p90', 0):.1f}min"
        )
    lines.append(f"  Aggregate R/hour: {tit['aggregate_r_per_hour']:.4f}")

    # Breakeven
    be = r["breakeven"]
    lines.append("\n--- Breakeven Sensitivity ---")
    lines.append(f"  Breakeven WR:  {be['breakeven_wr']*100:.2f}%")
    lines.append(f"  Observed WR:   {be['observed_wr']*100:.2f}%")
    lines.append(f"  Margin:        {be['margin_pct']*100:.2f}%")
    lines.append(f"  1% WR change = {be['impact_per_1pct']:.0f}R impact")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Run backtest signal analysis")
    parser.add_argument(
        "--phase", nargs="*", type=int, default=[1, 2, 3, 4],
        help="Which phases to run (default: all)",
    )
    parser.add_argument(
        "--run-id", default="2e3728409f3a1717",
        help="Backtest run ID",
    )
    parser.add_argument(
        "--db-url", default="postgresql://localhost/crypto_live",
        help="Database URL",
    )
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("  BACKTEST SIGNAL STATISTICAL ANALYSIS")
    print("=" * 70)
    print(f"  Run ID: {args.run_id}")
    print(f"  Phases: {args.phase}")
    print()

    # Load data
    t0 = time.time()
    print("Loading signals from database...")
    from backtest.analysis.data_loader import load_signals
    df = load_signals(database_url=args.db_url, run_id=args.run_id)
    print(f"Loaded {len(df):,} signals in {time.time() - t0:.1f}s")
    print(f"  TP: {df['win'].sum():,}  SL: {(~df['win'].astype(bool)).sum():,}  "
          f"WR: {df['win'].mean():.4f}")
    print()

    # Phase 1
    if 1 in args.phase:
        print("-" * 70)
        print("Running Phase 1: Statistical Significance...")
        print("-" * 70)
        t1 = time.time()
        from backtest.analysis.phase1_significance import run_phase1
        r1 = run_phase1(df)
        print(f"\nPhase 1 completed in {time.time() - t1:.1f}s")
        print(format_results_phase1(r1))

    # Phase 2
    if 2 in args.phase:
        print("\n" + "-" * 70)
        print("Running Phase 2: Conditional Analysis...")
        print("-" * 70)
        t2 = time.time()
        from backtest.analysis.phase2_conditional import run_phase2
        r2 = run_phase2(df)
        print(f"\nPhase 2 completed in {time.time() - t2:.1f}s")
        print(format_results_phase2(r2))

    # Phase 3
    if 3 in args.phase:
        print("\n" + "-" * 70)
        print("Running Phase 3: Optimization & Risk...")
        print("-" * 70)
        t3 = time.time()
        from backtest.analysis.phase3_optimization import run_phase3
        r3 = run_phase3(df)
        print(f"\nPhase 3 completed in {time.time() - t3:.1f}s")
        print(format_results_phase3(r3))

    # Phase 4
    if 4 in args.phase:
        print("\n" + "-" * 70)
        print("Running Phase 4: Walk-Forward Validation...")
        print("-" * 70)
        t4 = time.time()
        from backtest.analysis.phase4_validation import run_phase4
        r4 = run_phase4(df)
        print(f"\nPhase 4 completed in {time.time() - t4:.1f}s")

    total = time.time() - t0
    print("\n" + "=" * 70)
    print(f"  ANALYSIS COMPLETE — Total time: {total:.1f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
