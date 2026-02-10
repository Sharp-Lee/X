"""Phase 2: Conditional Analysis for backtest signals.

Analyzes win rate as a function of various conditioning variables:
ATR volatility regime, temporal patterns, streak values, direction
by market regime, yearly regime, and signal density effects.

Entry point: run_phase2(df) -> dict
"""

from __future__ import annotations

import warnings
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from backtest.analysis.data_loader import BREAKEVEN_WR, TP_R, SL_R

warnings.filterwarnings("ignore", category=FutureWarning)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _expectancy(win_rate: float) -> float:
    """Expected R per trade given a win rate (as fraction)."""
    return win_rate * TP_R - (1 - win_rate) * SL_R


def _profit_factor(wins: int, losses: int) -> float:
    """Gross profit / gross loss."""
    gross_profit = wins * TP_R
    gross_loss = losses * SL_R
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _wilson_ci_lower(wins: int, n: int, z: float = 1.96) -> float:
    """Wilson score confidence interval lower bound."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z**2 / n
    centre = p + z**2 / (2 * n)
    spread = z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n)
    return (centre - spread) / denom


def _chi2_test(observed_wins: np.ndarray, observed_n: np.ndarray) -> tuple[float, float]:
    """Chi-squared test for independence of win rate across groups.

    Returns (chi2_statistic, p_value). If the contingency table is
    degenerate, returns (nan, nan).
    """
    observed_losses = observed_n - observed_wins
    contingency = np.array([observed_wins, observed_losses])
    # Remove columns with zero total
    mask = contingency.sum(axis=0) > 0
    contingency = contingency[:, mask]
    if contingency.shape[1] < 2:
        return (np.nan, np.nan)
    try:
        chi2, p, _, _ = scipy_stats.chi2_contingency(contingency)
        return (chi2, p)
    except ValueError:
        return (np.nan, np.nan)


def _cochran_armitage_trend(wins: np.ndarray, n: np.ndarray, scores: np.ndarray | None = None) -> float:
    """Cochran-Armitage test for trend in proportions.

    Returns the two-sided p-value. Uses integer scores 1..K by default.

    Z = sum(w_i * (p_i - p_bar) * n_i) / sqrt(p_bar*(1-p_bar) * (sum(w_i^2 * n_i) - (sum(w_i * n_i))^2 / N))
    """
    K = len(wins)
    if K < 2:
        return np.nan

    N = n.sum()
    if N == 0:
        return np.nan

    p_bar = wins.sum() / N

    if p_bar == 0 or p_bar == 1:
        return np.nan

    if scores is None:
        scores = np.arange(1, K + 1, dtype=float)

    # Numerator
    p_i = wins / np.where(n > 0, n, 1)
    numerator = np.sum(scores * (p_i - p_bar) * n)

    # Denominator
    sum_w2n = np.sum(scores**2 * n)
    sum_wn = np.sum(scores * n)
    var_term = sum_w2n - sum_wn**2 / N
    denominator = np.sqrt(p_bar * (1 - p_bar) * var_term)

    if denominator == 0:
        return np.nan

    z = numerator / denominator
    p_value = 2 * scipy_stats.norm.sf(np.abs(z))
    return p_value


def _safe_groupby_winrate(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """Group by a column and compute win rate / expectancy."""
    agg = df.groupby(group_col).agg(
        n=("win", "count"),
        wins=("win", "sum"),
    ).reset_index()
    agg["win_rate"] = agg["wins"] / agg["n"]
    agg["expectancy"] = agg["win_rate"].apply(_expectancy)
    return agg


# ---------------------------------------------------------------------------
# 1. ATR Volatility Regime Analysis
# ---------------------------------------------------------------------------

def _atr_regime_analysis(df: pd.DataFrame) -> dict[str, Any]:
    print("  [Phase 2] ATR volatility regime analysis...")

    # Compute ATR quintiles WITHIN each symbol x timeframe group
    df = df.copy()
    df["atr_quintile"] = np.nan

    for (sym, tf), grp in df.groupby(["symbol", "timeframe"]):
        if len(grp) < 5:
            # Not enough data for quintiles -- assign all to quintile 3
            df.loc[grp.index, "atr_quintile"] = 3
            continue
        try:
            labels = pd.qcut(grp["atr_at_signal"], q=5, labels=[1, 2, 3, 4, 5], duplicates="drop")
            df.loc[grp.index, "atr_quintile"] = labels.astype(float)
        except ValueError:
            # Constant ATR or too many ties
            df.loc[grp.index, "atr_quintile"] = 3

    df["atr_quintile"] = df["atr_quintile"].astype(int)

    # Aggregate across all symbols
    quintile_data = _safe_groupby_winrate(df, "atr_quintile")
    quintile_data.rename(columns={"atr_quintile": "quintile"}, inplace=True)

    # Chi-squared test
    chi2_stat, chi2_p = _chi2_test(
        quintile_data["wins"].values,
        quintile_data["n"].values,
    )

    # Cochran-Armitage trend test
    trend_p = _cochran_armitage_trend(
        quintile_data["wins"].values,
        quintile_data["n"].values,
        scores=quintile_data["quintile"].values.astype(float),
    )

    quintile_data["chi2_p"] = chi2_p

    return {
        "quintile_data": quintile_data,
        "chi2_stat": chi2_stat,
        "chi2_p": chi2_p,
        "trend_p": trend_p,
    }


# ---------------------------------------------------------------------------
# 2. Temporal Patterns
# ---------------------------------------------------------------------------

def _assign_session(hour: int) -> str:
    if 0 <= hour <= 7:
        return "Asia (0-7)"
    elif 8 <= hour <= 12:
        return "Europe (8-12)"
    elif 13 <= hour <= 20:
        return "US (13-20)"
    else:
        return "Off-hours (21-23)"


_SESSION_ORDER = ["Asia (0-7)", "Europe (8-12)", "US (13-20)", "Off-hours (21-23)"]
_DAY_NAMES = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
_MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def _temporal_analysis(df: pd.DataFrame) -> dict[str, Any]:
    print("  [Phase 2] Temporal pattern analysis...")

    # --- Hourly ---
    hourly = _safe_groupby_winrate(df, "hour_utc")
    hourly.rename(columns={"hour_utc": "hour"}, inplace=True)
    hourly = hourly.sort_values("hour").reset_index(drop=True)

    # --- Daily ---
    daily = _safe_groupby_winrate(df, "day_of_week")
    daily.rename(columns={"day_of_week": "day"}, inplace=True)
    daily["day_name"] = daily["day"].map(_DAY_NAMES)
    daily = daily.sort_values("day").reset_index(drop=True)

    # --- Monthly ---
    monthly = _safe_groupby_winrate(df, "month")
    monthly["month_name"] = monthly["month"].map(_MONTH_NAMES)
    monthly = monthly.sort_values("month").reset_index(drop=True)

    # --- Session ---
    df_tmp = df.copy()
    df_tmp["session"] = df_tmp["hour_utc"].apply(_assign_session)
    session = _safe_groupby_winrate(df_tmp, "session")
    # Order sessions
    session["_order"] = session["session"].apply(lambda s: _SESSION_ORDER.index(s) if s in _SESSION_ORDER else 99)
    session = session.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)

    # Chi-squared tests
    h_chi2, hourly_chi2_p = _chi2_test(hourly["wins"].values, hourly["n"].values)
    d_chi2, daily_chi2_p = _chi2_test(daily["wins"].values, daily["n"].values)
    m_chi2, monthly_chi2_p = _chi2_test(monthly["wins"].values, monthly["n"].values)

    # Best/worst slots
    best_hour = hourly.loc[hourly["win_rate"].idxmax()]
    worst_hour = hourly.loc[hourly["win_rate"].idxmin()]
    best_day = daily.loc[daily["win_rate"].idxmax()]
    worst_day = daily.loc[daily["win_rate"].idxmin()]
    best_session = session.loc[session["win_rate"].idxmax()]
    worst_session = session.loc[session["win_rate"].idxmin()]

    print(f"    Best hour: {int(best_hour['hour'])}:00 UTC  WR={best_hour['win_rate']:.4f}  (n={int(best_hour['n'])})")
    print(f"    Worst hour: {int(worst_hour['hour'])}:00 UTC  WR={worst_hour['win_rate']:.4f}  (n={int(worst_hour['n'])})")
    print(f"    Best day: {best_day['day_name']}  WR={best_day['win_rate']:.4f}")
    print(f"    Worst day: {worst_day['day_name']}  WR={worst_day['win_rate']:.4f}")
    print(f"    Best session: {best_session['session']}  WR={best_session['win_rate']:.4f}")
    print(f"    Worst session: {worst_session['session']}  WR={worst_session['win_rate']:.4f}")

    return {
        "hourly": hourly,
        "daily": daily,
        "monthly": monthly,
        "session": session,
        "hourly_chi2_p": hourly_chi2_p,
        "daily_chi2_p": daily_chi2_p,
        "monthly_chi2_p": monthly_chi2_p,
    }


# ---------------------------------------------------------------------------
# 3. Streak Analysis
# ---------------------------------------------------------------------------

_STREAK_BUCKETS = [
    ("<=−3", lambda s: s <= -3),
    ("-2 to 0", lambda s: -2 <= s <= 0),
    ("1-3", lambda s: 1 <= s <= 3),
    ("4-6", lambda s: 4 <= s <= 6),
    ("7-10", lambda s: 7 <= s <= 10),
    ("11-20", lambda s: 11 <= s <= 20),
    ("21+", lambda s: s >= 21),
]


def _streak_analysis(df: pd.DataFrame) -> dict[str, Any]:
    print("  [Phase 2] Streak analysis...")

    # --- By individual streak value ---
    by_value = _safe_groupby_winrate(df, "streak_at_signal")
    by_value.rename(columns={"streak_at_signal": "streak"}, inplace=True)
    by_value = by_value.sort_values("streak").reset_index(drop=True)

    # Wilson CI lower bound
    by_value["ci_lower"] = by_value.apply(
        lambda row: _wilson_ci_lower(int(row["wins"]), int(row["n"])), axis=1
    )

    # --- By streak buckets ---
    bucket_rows = []
    for label, predicate in _STREAK_BUCKETS:
        mask = df["streak_at_signal"].apply(predicate)
        sub = df[mask]
        if len(sub) == 0:
            bucket_rows.append({"bucket": label, "n": 0, "wins": 0, "win_rate": np.nan, "expectancy": np.nan})
            continue
        n = len(sub)
        w = int(sub["win"].sum())
        wr = w / n
        bucket_rows.append({
            "bucket": label,
            "n": n,
            "wins": w,
            "win_rate": wr,
            "expectancy": _expectancy(wr),
        })
    by_bucket = pd.DataFrame(bucket_rows)

    # --- Chi-squared test on buckets ---
    valid_buckets = by_bucket[by_bucket["n"] > 0]
    chi2_stat, chi2_p = _chi2_test(
        valid_buckets["wins"].values.astype(int),
        valid_buckets["n"].values.astype(int),
    )

    # --- Logistic regression: outcome ~ streak + streak^2 ---
    logistic_streak_coeff = np.nan
    logistic_streak_p = np.nan
    logistic_streak2_coeff = np.nan
    logistic_streak2_p = np.nan

    try:
        import statsmodels.api as sm

        sub = df[["win", "streak_at_signal"]].dropna()
        if len(sub) > 10:
            X = sub[["streak_at_signal"]].copy()
            X["streak2"] = X["streak_at_signal"] ** 2
            X = sm.add_constant(X)
            y = sub["win"]

            model = sm.Logit(y, X)
            result = model.fit(disp=0, maxiter=100)

            logistic_streak_coeff = result.params.get("streak_at_signal", np.nan)
            logistic_streak_p = result.pvalues.get("streak_at_signal", np.nan)
            logistic_streak2_coeff = result.params.get("streak2", np.nan)
            logistic_streak2_p = result.pvalues.get("streak2", np.nan)

            print(f"    Logistic: streak coeff={logistic_streak_coeff:.6f} (p={logistic_streak_p:.4f})")
            print(f"    Logistic: streak^2 coeff={logistic_streak2_coeff:.6f} (p={logistic_streak2_p:.4f})")
    except Exception as e:
        print(f"    Logistic regression failed: {e}")

    return {
        "by_value": by_value,
        "by_bucket": by_bucket,
        "chi2_p": chi2_p,
        "logistic_streak_coeff": logistic_streak_coeff,
        "logistic_streak_p": logistic_streak_p,
        "logistic_streak2_coeff": logistic_streak2_coeff,
        "logistic_streak2_p": logistic_streak2_p,
    }


# ---------------------------------------------------------------------------
# 4. Direction Bias by Market Regime
# ---------------------------------------------------------------------------

_REGIME_BINS = [-np.inf, -0.10, -0.02, 0.02, 0.10, np.inf]
_REGIME_LABELS = ["strong_bear", "mild_bear", "sideways", "mild_bull", "strong_bull"]
_REGIME_ORDER = {label: i for i, label in enumerate(_REGIME_LABELS)}


def _direction_regime_analysis(df: pd.DataFrame) -> dict[str, Any]:
    print("  [Phase 2] Direction bias by market regime...")

    df = df.copy()

    # Compute 30-day rolling return per symbol using entry_price
    # Sort by symbol + signal_time, then use shift within group
    df = df.sort_values(["symbol", "signal_time"]).reset_index(drop=True)

    # For each signal, compute the rolling return as the % change in entry_price
    # over the trailing 30 calendar days within the same symbol.
    df["regime"] = pd.Series([np.nan] * len(df), dtype=object)

    for sym, grp in df.groupby("symbol"):
        if len(grp) < 2:
            df.loc[grp.index, "regime"] = "sideways"
            continue

        grp = grp.sort_values("signal_time")
        idx = grp.index

        # For each signal, find the entry_price from ~30 days ago
        times = grp["signal_time"].values
        prices = grp["entry_price"].values
        rolling_ret = np.full(len(grp), np.nan)

        for i in range(len(grp)):
            current_time = times[i]
            lookback = current_time - np.timedelta64(30, "D")
            # Find the most recent signal at or before the lookback point
            past_mask = times[:i + 1] <= lookback
            if past_mask.any():
                past_idx = np.where(past_mask)[0][-1]
                rolling_ret[i] = (prices[i] - prices[past_idx]) / prices[past_idx]
            elif i > 0:
                # Fallback: use the earliest available price
                rolling_ret[i] = (prices[i] - prices[0]) / prices[0]
            else:
                rolling_ret[i] = 0.0

        regimes = pd.cut(rolling_ret, bins=_REGIME_BINS, labels=_REGIME_LABELS, right=True)
        df.loc[idx, "regime"] = np.asarray(regimes)

    # Drop rows where regime is NaN (shouldn't happen but safety)
    df_valid = df.dropna(subset=["regime"]).copy()

    # Normalize direction to LONG/SHORT strings
    if df_valid["direction"].dtype in [np.int64, np.int32, int]:
        df_valid["dir_label"] = df_valid["direction"].map({1: "LONG", -1: "SHORT"})
    else:
        df_valid["dir_label"] = df_valid["direction"].astype(str).str.upper()

    # Win rate by regime x direction
    regime_dir = df_valid.groupby(["regime", "dir_label"]).agg(
        n=("win", "count"),
        wins=("win", "sum"),
    ).reset_index()
    regime_dir["win_rate"] = regime_dir["wins"] / regime_dir["n"]
    regime_dir["expectancy"] = regime_dir["win_rate"].apply(_expectancy)
    regime_dir.rename(columns={"dir_label": "direction"}, inplace=True)

    # Fisher exact tests: LONG vs SHORT within each regime
    fisher_rows = []
    for regime in _REGIME_LABELS:
        r_data = regime_dir[regime_dir["regime"] == regime]
        long_data = r_data[r_data["direction"] == "LONG"]
        short_data = r_data[r_data["direction"] == "SHORT"]

        if long_data.empty or short_data.empty:
            fisher_rows.append({
                "regime": regime,
                "long_wr": long_data["win_rate"].values[0] if not long_data.empty else np.nan,
                "short_wr": short_data["win_rate"].values[0] if not short_data.empty else np.nan,
                "long_n": int(long_data["n"].values[0]) if not long_data.empty else 0,
                "short_n": int(short_data["n"].values[0]) if not short_data.empty else 0,
                "fisher_p": np.nan,
            })
            continue

        lw = int(long_data["wins"].values[0])
        ln = int(long_data["n"].values[0])
        ll = ln - lw  # long losses
        sw = int(short_data["wins"].values[0])
        sn = int(short_data["n"].values[0])
        sl_count = sn - sw  # short losses

        # 2x2 contingency: [[long_wins, long_losses], [short_wins, short_losses]]
        _, fisher_p = scipy_stats.fisher_exact([[lw, ll], [sw, sl_count]])

        fisher_rows.append({
            "regime": regime,
            "long_wr": lw / ln,
            "short_wr": sw / sn,
            "long_n": ln,
            "short_n": sn,
            "fisher_p": fisher_p,
        })

    fisher_tests = pd.DataFrame(fisher_rows)
    # Sort by regime order
    fisher_tests["_order"] = fisher_tests["regime"].map(_REGIME_ORDER)
    fisher_tests = fisher_tests.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)

    return {
        "data": regime_dir,
        "fisher_tests": fisher_tests,
    }


# ---------------------------------------------------------------------------
# 5. Yearly Regime Analysis
# ---------------------------------------------------------------------------

def _yearly_analysis(df: pd.DataFrame) -> dict[str, Any]:
    print("  [Phase 2] Yearly / quarterly analysis...")

    # --- By year ---
    by_year = df.groupby("year").agg(
        n=("win", "count"),
        wins=("win", "sum"),
    ).reset_index()
    by_year["losses"] = by_year["n"] - by_year["wins"]
    by_year["win_rate"] = by_year["wins"] / by_year["n"]
    by_year["expectancy"] = by_year["win_rate"].apply(_expectancy)
    by_year["pf"] = by_year.apply(
        lambda r: _profit_factor(int(r["wins"]), int(r["losses"])), axis=1
    )
    by_year = by_year.sort_values("year").reset_index(drop=True)

    # --- By quarter ---
    by_quarter = df.groupby("quarter").agg(
        n=("win", "count"),
        wins=("win", "sum"),
    ).reset_index()
    by_quarter["losses"] = by_quarter["n"] - by_quarter["wins"]
    by_quarter["win_rate"] = by_quarter["wins"] / by_quarter["n"]
    by_quarter["expectancy"] = by_quarter["win_rate"].apply(_expectancy)
    by_quarter = by_quarter.sort_values("quarter").reset_index(drop=True)

    # Worst / best quarter
    worst_quarter = by_quarter.loc[by_quarter["expectancy"].idxmin(), "quarter"] if len(by_quarter) > 0 else "N/A"
    best_quarter = by_quarter.loc[by_quarter["expectancy"].idxmax(), "quarter"] if len(by_quarter) > 0 else "N/A"

    # Worst year
    worst_year_row = by_year.loc[by_year["expectancy"].idxmin()] if len(by_year) > 0 else None

    print(f"    Best quarter: {best_quarter}")
    print(f"    Worst quarter: {worst_quarter}")
    if worst_year_row is not None:
        print(f"    Worst year: {int(worst_year_row['year'])} (E[R]={worst_year_row['expectancy']:.4f})")

    return {
        "by_year": by_year,
        "by_quarter": by_quarter,
        "worst_quarter": worst_quarter,
        "best_quarter": best_quarter,
    }


# ---------------------------------------------------------------------------
# 6. Signal Density Effects
# ---------------------------------------------------------------------------

_DENSITY_BINS = [0, 1, 3, 5, 10, np.inf]
_DENSITY_LABELS = ["1", "2-3", "4-5", "6-10", "10+"]


def _density_analysis(df: pd.DataFrame) -> dict[str, Any]:
    print("  [Phase 2] Signal density analysis...")

    df = df.copy()

    # Compute the hour bucket for each signal
    df["hour_bucket"] = df["signal_time"].dt.floor("h")

    # Count signals per (symbol, timeframe, hour_bucket)
    density = df.groupby(["symbol", "timeframe", "hour_bucket"]).size().reset_index(name="density")

    # Merge density back onto original df
    df = df.merge(density, on=["symbol", "timeframe", "hour_bucket"], how="left")

    # Bucket densities
    df["density_bucket"] = pd.cut(
        df["density"],
        bins=_DENSITY_BINS,
        labels=_DENSITY_LABELS,
        right=True,
        include_lowest=True,
    )

    # Win rate by density bucket
    bucket_data = df.groupby("density_bucket", observed=False).agg(
        n=("win", "count"),
        wins=("win", "sum"),
    ).reset_index()
    bucket_data["win_rate"] = np.where(bucket_data["n"] > 0, bucket_data["wins"] / bucket_data["n"], np.nan)
    bucket_data["expectancy"] = bucket_data["win_rate"].apply(lambda wr: _expectancy(wr) if pd.notna(wr) else np.nan)

    # Spearman correlation between density (raw) and outcome
    valid = df.dropna(subset=["density", "win"])
    if len(valid) > 2 and valid["density"].nunique() > 1:
        spearman_r, spearman_p = scipy_stats.spearmanr(valid["density"], valid["win"])
    else:
        spearman_r, spearman_p = np.nan, np.nan

    return {
        "data": bucket_data,
        "spearman_r": spearman_r,
        "spearman_p": spearman_p,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_phase2(df: pd.DataFrame) -> dict:
    """Run all Phase 2 conditional analyses.

    Parameters
    ----------
    df : pd.DataFrame
        Prepared signals DataFrame with columns: win, trade_r, symbol,
        timeframe, direction, signal_time, streak_at_signal, atr_at_signal,
        mae_ratio, mfe_ratio, hour_utc, day_of_week, month, year, quarter,
        entry_price, duration_min.

    Returns
    -------
    dict
        Results keyed by analysis section.
    """
    print(f"[Phase 2] Starting conditional analysis on {len(df)} signals...")
    print(f"  Overall WR={df['win'].mean():.4f}  Breakeven={BREAKEVEN_WR:.4f}")

    results = {}

    results["atr_regime"] = _atr_regime_analysis(df)
    results["temporal"] = _temporal_analysis(df)
    results["streak"] = _streak_analysis(df)
    results["direction_regime"] = _direction_regime_analysis(df)
    results["yearly"] = _yearly_analysis(df)
    results["density"] = _density_analysis(df)

    print("[Phase 2] Complete.")
    return results
