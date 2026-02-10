"""Phase 4: Walk-Forward Validation of Signal Filters.

Validates that filters discovered in Phase 2 (streak > K, exclude 30m)
generalize out-of-sample.

Validation methods:
  1. Fixed split: train 2020-2023, test 2024-2025
  2. Rolling walk-forward: 2-year train, 6-month test windows
  3. Leave-one-year-out cross-validation

Entry point: run_phase4(df) -> dict
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from statsmodels.stats.proportion import proportion_confint

from backtest.analysis.data_loader import BREAKEVEN_WR, TP_R, SL_R

TOTAL_R = TP_R + SL_R  # 5.42


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metrics(df: pd.DataFrame) -> dict:
    """Compute standard metrics for a signal subset."""
    n = len(df)
    if n == 0:
        return {
            "n": 0, "wins": 0, "losses": 0,
            "win_rate": np.nan, "expectancy": np.nan,
            "total_r": 0.0, "profit_factor": np.nan,
        }
    wins = int(df["win"].sum())
    losses = n - wins
    wr = wins / n
    exp = wr * TP_R - (1 - wr) * SL_R
    total_r = wins * TP_R - losses * SL_R
    pf = (wins * TP_R) / (losses * SL_R) if losses > 0 else float("inf")
    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "win_rate": wr,
        "expectancy": exp,
        "total_r": total_r,
        "profit_factor": pf,
    }


def _two_proportion_z(n1: int, w1: int, n2: int, w2: int) -> tuple[float, float]:
    """Two-proportion z-test. Returns (z_stat, p_value_two_sided)."""
    if n1 == 0 or n2 == 0:
        return np.nan, np.nan
    p1 = w1 / n1
    p2 = w2 / n2
    p_pool = (w1 + w2) / (n1 + n2)
    se = np.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    if se == 0:
        return 0.0, 1.0
    z = (p1 - p2) / se
    p = 2 * (1 - scipy_stats.norm.cdf(abs(z)))
    return z, p


def _apply_filters(
    df: pd.DataFrame,
    max_streak: int | None = None,
    exclude_30m: bool = False,
    exclude_15m_short: bool = False,
) -> pd.DataFrame:
    """Apply signal filters and return filtered DataFrame."""
    mask = pd.Series(True, index=df.index)
    if max_streak is not None:
        mask &= df["streak_at_signal"] <= max_streak
    if exclude_30m:
        mask &= df["timeframe"] != "30m"
    if exclude_15m_short:
        mask &= ~((df["timeframe"] == "15m") & (df["direction"] == -1))
    return df[mask]


# ---------------------------------------------------------------------------
# Filter definitions
# ---------------------------------------------------------------------------

FILTERS = {
    "baseline": {
        "label": "No filter (baseline)",
        "params": {},
    },
    "streak_10": {
        "label": "Exclude streak > 10",
        "params": {"max_streak": 10},
    },
    "no_30m": {
        "label": "Exclude 30m",
        "params": {"exclude_30m": True},
    },
    "streak_10_no_30m": {
        "label": "Exclude streak > 10 + no 30m",
        "params": {"max_streak": 10, "exclude_30m": True},
    },
    "streak_7": {
        "label": "Exclude streak > 7",
        "params": {"max_streak": 7},
    },
    "streak_7_no_30m": {
        "label": "Exclude streak > 7 + no 30m",
        "params": {"max_streak": 7, "exclude_30m": True},
    },
    "streak_10_no_30m_no_15ms": {
        "label": "Streak ≤10 + no 30m + no 15m SHORT",
        "params": {"max_streak": 10, "exclude_30m": True, "exclude_15m_short": True},
    },
}


# ---------------------------------------------------------------------------
# Validation Method 1: Fixed Train/Test Split
# ---------------------------------------------------------------------------

def _fixed_split_validation(df: pd.DataFrame) -> dict:
    """Train on 2020-2023, test on 2024-2025."""
    print("  [4.1] Fixed split: train=2020-2023, test=2024-2025")

    train = df[df["year"] <= 2023]
    test = df[df["year"] >= 2024]
    print(f"    Train: {len(train):,} signals ({train['year'].min()}-{train['year'].max()})")
    print(f"    Test:  {len(test):,} signals ({test['year'].min()}-{test['year'].max()})")

    results = []
    for fkey, fdef in FILTERS.items():
        train_f = _apply_filters(train, **fdef["params"])
        test_f = _apply_filters(test, **fdef["params"])

        train_m = _metrics(train_f)
        test_m = _metrics(test_f)

        # Improvement vs baseline on test set
        test_base = _metrics(test)
        improvement_wr = (test_m["win_rate"] - test_base["win_rate"]) if test_m["n"] > 0 else np.nan
        improvement_exp = (test_m["expectancy"] - test_base["expectancy"]) if test_m["n"] > 0 else np.nan

        results.append({
            "filter": fkey,
            "label": fdef["label"],
            "train_n": train_m["n"],
            "train_wr": train_m["win_rate"],
            "train_exp": train_m["expectancy"],
            "test_n": test_m["n"],
            "test_wr": test_m["win_rate"],
            "test_exp": test_m["expectancy"],
            "test_total_r": test_m["total_r"],
            "test_pf": test_m["profit_factor"],
            "test_wr_improvement": improvement_wr,
            "test_exp_improvement": improvement_exp,
            "signals_excluded_pct": (1 - test_m["n"] / len(test)) * 100 if len(test) > 0 else 0,
        })

        marker = " ***" if (test_m["expectancy"] or 0) > (test_base["expectancy"] or 0) else ""
        print(
            f"    {fdef['label']:<40s} "
            f"Train WR={train_m['win_rate']:.4f} E={train_m['expectancy']:+.4f} | "
            f"Test WR={test_m['win_rate']:.4f} E={test_m['expectancy']:+.4f} "
            f"({test_m['n']:,} sigs){marker}"
        )

    return {"data": pd.DataFrame(results)}


# ---------------------------------------------------------------------------
# Validation Method 2: Rolling Walk-Forward
# ---------------------------------------------------------------------------

def _rolling_walkforward(df: pd.DataFrame) -> dict:
    """Rolling 2-year train, 1-year test windows."""
    print("  [4.2] Rolling walk-forward: 2yr train → 1yr test")

    years = sorted(df["year"].unique())
    results = []

    for test_year in years:
        if test_year < years[0] + 2:
            continue  # need at least 2 years of training data

        train_years = [y for y in years if y < test_year and y >= test_year - 2]
        if len(train_years) < 2:
            continue

        train = df[df["year"].isin(train_years)]
        test = df[df["year"] == test_year]

        if len(train) == 0 or len(test) == 0:
            continue

        for fkey, fdef in FILTERS.items():
            train_f = _apply_filters(train, **fdef["params"])
            test_f = _apply_filters(test, **fdef["params"])

            train_m = _metrics(train_f)
            test_m = _metrics(test_f)

            results.append({
                "filter": fkey,
                "train_years": f"{min(train_years)}-{max(train_years)}",
                "test_year": test_year,
                "train_n": train_m["n"],
                "train_wr": train_m["win_rate"],
                "train_exp": train_m["expectancy"],
                "test_n": test_m["n"],
                "test_wr": test_m["win_rate"],
                "test_exp": test_m["expectancy"],
                "test_total_r": test_m["total_r"],
            })

    results_df = pd.DataFrame(results)

    # Summarize: average test expectancy per filter
    if not results_df.empty:
        summary = (
            results_df.groupby("filter")
            .agg(
                avg_test_exp=("test_exp", "mean"),
                avg_test_wr=("test_wr", "mean"),
                total_test_r=("test_total_r", "sum"),
                n_windows=("test_year", "count"),
                min_test_exp=("test_exp", "min"),
                all_positive=("test_exp", lambda x: (x > 0).all()),
            )
            .reset_index()
        )
        # Add filter labels
        summary["label"] = summary["filter"].map(
            {k: v["label"] for k, v in FILTERS.items()}
        )
        print("\n    Walk-forward summary (avg across test windows):")
        for _, row in summary.iterrows():
            pos_flag = "✓" if row["all_positive"] else "✗"
            print(
                f"    {row['label']:<40s} "
                f"avg_E={row['avg_test_exp']:+.4f}  "
                f"total_R={row['total_test_r']:+.0f}  "
                f"min_E={row['min_test_exp']:+.4f}  "
                f"all_positive={pos_flag}"
            )
    else:
        summary = pd.DataFrame()

    return {
        "detail": results_df,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Validation Method 3: Leave-One-Year-Out
# ---------------------------------------------------------------------------

def _leave_one_year_out(df: pd.DataFrame) -> dict:
    """Leave-one-year-out cross-validation."""
    print("  [4.3] Leave-one-year-out cross-validation")

    years = sorted(df["year"].unique())
    results = []

    for held_out_year in years:
        train = df[df["year"] != held_out_year]
        test = df[df["year"] == held_out_year]

        for fkey, fdef in FILTERS.items():
            train_f = _apply_filters(train, **fdef["params"])
            test_f = _apply_filters(test, **fdef["params"])

            train_m = _metrics(train_f)
            test_m = _metrics(test_f)

            results.append({
                "filter": fkey,
                "held_out_year": held_out_year,
                "train_n": train_m["n"],
                "train_wr": train_m["win_rate"],
                "train_exp": train_m["expectancy"],
                "test_n": test_m["n"],
                "test_wr": test_m["win_rate"],
                "test_exp": test_m["expectancy"],
                "test_total_r": test_m["total_r"],
            })

    results_df = pd.DataFrame(results)

    # Summary per filter
    if not results_df.empty:
        summary = (
            results_df.groupby("filter")
            .agg(
                avg_test_exp=("test_exp", "mean"),
                avg_test_wr=("test_wr", "mean"),
                total_test_r=("test_total_r", "sum"),
                min_test_exp=("test_exp", "min"),
                years_positive=("test_exp", lambda x: (x > 0).sum()),
                years_total=("test_exp", "count"),
            )
            .reset_index()
        )
        summary["label"] = summary["filter"].map(
            {k: v["label"] for k, v in FILTERS.items()}
        )
        print("\n    LOYO summary:")
        for _, row in summary.iterrows():
            print(
                f"    {row['label']:<40s} "
                f"avg_E={row['avg_test_exp']:+.4f}  "
                f"total_R={row['total_test_r']:+.0f}  "
                f"positive_years={int(row['years_positive'])}/{int(row['years_total'])}"
            )
    else:
        summary = pd.DataFrame()

    return {
        "detail": results_df,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Statistical significance of filter improvement
# ---------------------------------------------------------------------------

def _filter_significance(df: pd.DataFrame) -> dict:
    """Test whether each filter significantly improves on baseline."""
    print("  [4.4] Statistical significance of filter improvements")

    results = []
    baseline = _metrics(df)

    for fkey, fdef in FILTERS.items():
        if fkey == "baseline":
            continue

        filtered = _apply_filters(df, **fdef["params"])
        excluded = df.loc[~df.index.isin(filtered.index)]

        fm = _metrics(filtered)
        em = _metrics(excluded)

        # Test: is filtered WR > baseline WR?
        # Two-proportion z-test: filtered vs excluded
        z, p = _two_proportion_z(
            fm["n"], fm["wins"], em["n"], em["wins"]
        )

        # Wilson CI for filtered win rate
        if fm["n"] > 0:
            wr_lower, wr_upper = proportion_confint(
                fm["wins"], fm["n"], alpha=0.05, method="wilson"
            )
        else:
            wr_lower, wr_upper = np.nan, np.nan

        # Expectancy CI from WR CI
        exp_lower = TOTAL_R * wr_lower - SL_R
        exp_upper = TOTAL_R * wr_upper - SL_R

        results.append({
            "filter": fkey,
            "label": fdef["label"],
            "included_n": fm["n"],
            "excluded_n": em["n"],
            "included_wr": fm["win_rate"],
            "excluded_wr": em["win_rate"],
            "included_exp": fm["expectancy"],
            "excluded_exp": em["expectancy"],
            "wr_improvement": fm["win_rate"] - baseline["win_rate"],
            "exp_improvement": fm["expectancy"] - baseline["expectancy"],
            "z_stat": z,
            "p_value": p,
            "included_wr_ci_lower": wr_lower,
            "included_wr_ci_upper": wr_upper,
            "included_exp_ci_lower": exp_lower,
            "included_exp_ci_upper": exp_upper,
            "exp_ci_above_zero": exp_lower > 0,
            "wr_ci_above_breakeven": wr_lower > BREAKEVEN_WR,
        })

        sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
        print(
            f"    {fdef['label']:<40s} "
            f"included={fm['win_rate']:.4f} vs excluded={em['win_rate']:.4f}  "
            f"z={z:+.3f} p={p:.4f} {sig}"
        )
        print(
            f"    {'':40s} "
            f"E[included]={fm['expectancy']:+.4f}R  "
            f"E[excluded]={em['expectancy']:+.4f}R  "
            f"CI=[{exp_lower:+.4f}, {exp_upper:+.4f}]"
        )

    return {"data": pd.DataFrame(results)}


# ---------------------------------------------------------------------------
# Yearly detail for recommended filter
# ---------------------------------------------------------------------------

def _yearly_filter_detail(df: pd.DataFrame) -> dict:
    """Year-by-year detail for each filter."""
    print("  [4.5] Year-by-year detail for all filters")

    years = sorted(df["year"].unique())
    results = []

    for year in years:
        year_df = df[df["year"] == year]
        for fkey, fdef in FILTERS.items():
            filtered = _apply_filters(year_df, **fdef["params"])
            m = _metrics(filtered)
            results.append({
                "year": year,
                "filter": fkey,
                "label": fdef["label"],
                "n": m["n"],
                "win_rate": m["win_rate"],
                "expectancy": m["expectancy"],
                "total_r": m["total_r"],
                "profit_factor": m["profit_factor"],
            })

    results_df = pd.DataFrame(results)

    # Print comparison table for recommended filter
    rec_key = "streak_10_no_30m"
    print(f"\n    Year-by-year: baseline vs '{FILTERS[rec_key]['label']}'")
    print(f"    {'Year':<6s} {'Baseline WR':>12s} {'Baseline E':>11s} "
          f"{'Filtered WR':>12s} {'Filtered E':>11s} {'Δ Exp':>8s} {'ΔR':>10s}")
    print("    " + "-" * 75)
    for year in years:
        base_row = results_df[
            (results_df["year"] == year) & (results_df["filter"] == "baseline")
        ].iloc[0]
        filt_row = results_df[
            (results_df["year"] == year) & (results_df["filter"] == rec_key)
        ].iloc[0]
        delta_exp = filt_row["expectancy"] - base_row["expectancy"]
        delta_r = filt_row["total_r"] - base_row["total_r"]
        print(
            f"    {year:<6d} "
            f"{base_row['win_rate']:>11.4f} "
            f"{base_row['expectancy']:>+10.4f}R "
            f"{filt_row['win_rate']:>11.4f} "
            f"{filt_row['expectancy']:>+10.4f}R "
            f"{delta_exp:>+7.4f} "
            f"{delta_r:>+9.0f}R"
        )

    return {"data": results_df}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_phase4(df: pd.DataFrame) -> dict:
    """Run all Phase 4 walk-forward validation analyses.

    Parameters
    ----------
    df : pd.DataFrame
        Prepared signals DataFrame from data_loader.

    Returns
    -------
    dict
        Results keyed by validation method.
    """
    print("=" * 60)
    print("PHASE 4: Walk-Forward Validation of Signal Filters")
    print("=" * 60)
    print(f"  Total signals: {len(df):,}")
    print(f"  Years: {sorted(df['year'].unique())}")
    print(f"  Filters to validate: {len(FILTERS)}")
    print()

    # Quick preview of what each filter does
    print("  Filter preview (full dataset):")
    for fkey, fdef in FILTERS.items():
        filtered = _apply_filters(df, **fdef["params"])
        m = _metrics(filtered)
        excluded_pct = (1 - len(filtered) / len(df)) * 100
        print(
            f"    {fdef['label']:<40s} "
            f"n={m['n']:>7,} "
            f"(-{excluded_pct:4.1f}%)  "
            f"WR={m['win_rate']:.4f}  "
            f"E={m['expectancy']:+.4f}R  "
            f"TotalR={m['total_r']:+.0f}"
        )
    print()

    results = {}

    results["fixed_split"] = _fixed_split_validation(df)
    print()

    results["walkforward"] = _rolling_walkforward(df)
    print()

    results["loyo"] = _leave_one_year_out(df)
    print()

    results["significance"] = _filter_significance(df)
    print()

    results["yearly_detail"] = _yearly_filter_detail(df)
    print()

    # Final recommendation
    print("=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)

    # Evaluate the recommended filter
    rec_key = "streak_10_no_30m"
    rec_filtered = _apply_filters(df, **FILTERS[rec_key]["params"])
    rec_m = _metrics(rec_filtered)
    base_m = _metrics(df)

    print(f"  Recommended: {FILTERS[rec_key]['label']}")
    print(f"  Signals: {rec_m['n']:,} (excluded {len(df)-rec_m['n']:,}, "
          f"{(1-rec_m['n']/len(df))*100:.1f}%)")
    print(f"  Win rate: {base_m['win_rate']:.4f} → {rec_m['win_rate']:.4f} "
          f"(+{(rec_m['win_rate']-base_m['win_rate'])*100:.2f}%)")
    print(f"  Expectancy: {base_m['expectancy']:+.4f}R → {rec_m['expectancy']:+.4f}R "
          f"(+{rec_m['expectancy']-base_m['expectancy']:+.4f}R)")
    print(f"  Total R: {base_m['total_r']:+.0f} → {rec_m['total_r']:+.0f}")
    print(f"  Profit Factor: {base_m['profit_factor']:.3f} → {rec_m['profit_factor']:.3f}")

    # Check walk-forward
    wf = results["walkforward"].get("summary")
    if isinstance(wf, pd.DataFrame) and not wf.empty:
        rec_wf = wf[wf["filter"] == rec_key]
        if not rec_wf.empty:
            row = rec_wf.iloc[0]
            print(f"\n  Walk-forward validation:")
            print(f"    Avg test expectancy: {row['avg_test_exp']:+.4f}R")
            print(f"    All test windows positive: {row['all_positive']}")
            print(f"    Min test expectancy: {row['min_test_exp']:+.4f}R")

    # Check LOYO
    loyo = results["loyo"].get("summary")
    if isinstance(loyo, pd.DataFrame) and not loyo.empty:
        rec_loyo = loyo[loyo["filter"] == rec_key]
        if not rec_loyo.empty:
            row = rec_loyo.iloc[0]
            print(f"\n  Leave-one-year-out:")
            print(f"    Avg test expectancy: {row['avg_test_exp']:+.4f}R")
            print(f"    Positive years: {int(row['years_positive'])}/{int(row['years_total'])}")

    print("\n" + "=" * 60)
    print("Phase 4 complete.")
    print("=" * 60)

    return results
