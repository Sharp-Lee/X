"""Report formatting for backtest results.

Outputs results to console (formatted tables) and JSON files.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from backtest.stats import BREAKEVEN_WIN_RATE, BacktestResult


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class ReportFormatter:
    """Format backtest results for display and export."""

    @staticmethod
    def print_console(result: BacktestResult) -> None:
        """Print formatted report to console."""
        be = BREAKEVEN_WIN_RATE

        print("\n" + "=" * 70)
        print("  BACKTEST RESULTS — MSR Retest Capture")
        print("=" * 70)
        print(f"  Period: {result.start_date:%Y-%m-%d} → {result.end_date:%Y-%m-%d}")
        print(f"  Symbols: {', '.join(result.symbols)}")
        print(f"  Timeframes: {', '.join(result.timeframes)}")

        # Overall
        print("\n" + "-" * 70)
        print("  OVERALL")
        print("-" * 70)
        print(f"  Total signals:  {result.total_signals}")
        print(f"  Wins (TP):      {result.wins}")
        print(f"  Losses (SL):    {result.losses}")
        print(f"  Active:         {result.active}")
        above = "ABOVE" if result.win_rate >= be else "BELOW"
        print(f"  Win rate:       {result.win_rate:.1f}% ({above} {be:.1f}% breakeven)")
        print(f"  Expectancy:     {result.expectancy_r:+.2f}R per trade")
        print(f"  Total R:        {result.total_r:+.1f}R")
        print(f"  Profit factor:  {result.profit_factor:.2f}")

        # By Symbol
        if result.by_symbol:
            print("\n" + "-" * 70)
            print("  BY SYMBOL")
            print("-" * 70)
            print(f"  {'Symbol':<12} {'Total':>6} {'Wins':>6} {'Losses':>6} {'Win%':>8} {'Status':>8}")
            for s in result.by_symbol:
                status = "OK" if s.win_rate >= be else "LOW"
                print(f"  {s.symbol:<12} {s.total:>6} {s.wins:>6} {s.losses:>6} {s.win_rate:>7.1f}% {status:>8}")

        # By Timeframe
        if result.by_timeframe:
            print("\n" + "-" * 70)
            print("  BY TIMEFRAME")
            print("-" * 70)
            print(f"  {'Timeframe':<12} {'Total':>6} {'Wins':>6} {'Losses':>6} {'Win%':>8} {'Status':>8}")
            for s in result.by_timeframe:
                status = "OK" if s.win_rate >= be else "LOW"
                print(f"  {s.timeframe:<12} {s.total:>6} {s.wins:>6} {s.losses:>6} {s.win_rate:>7.1f}% {status:>8}")

        # By Direction
        if result.by_direction:
            print("\n" + "-" * 70)
            print("  BY DIRECTION")
            print("-" * 70)
            print(f"  {'Direction':<12} {'Total':>6} {'Wins':>6} {'Losses':>6} {'Win%':>8}")
            for s in result.by_direction:
                print(f"  {s.direction:<12} {s.total:>6} {s.wins:>6} {s.losses:>6} {s.win_rate:>7.1f}%")

        # MAE/MFE
        if result.mae_mfe:
            print("\n" + "-" * 70)
            print("  MAE/MFE DISTRIBUTION")
            print("-" * 70)
            for cat in ["tp", "sl"]:
                if cat not in result.mae_mfe:
                    continue
                m = result.mae_mfe[cat]
                label = "TP Signals" if cat == "tp" else "SL Signals"
                print(f"\n  {label} (n={m.count}):")
                print(f"    MAE: avg={m.avg_mae:.3f}  p25={m.mae_p25:.3f}  p50={m.mae_p50:.3f}  p75={m.mae_p75:.3f}  p90={m.mae_p90:.3f}")
                print(f"    MFE: avg={m.avg_mfe:.3f}  p25={m.mfe_p25:.3f}  p50={m.mfe_p50:.3f}  p75={m.mfe_p75:.3f}  p90={m.mfe_p90:.3f}")

        # Daily P&L (last 10 days)
        if result.daily_pnl:
            print("\n" + "-" * 70)
            print("  DAILY P&L (last 10 days)")
            print("-" * 70)
            print(f"  {'Date':<12} {'Wins':>6} {'Losses':>6} {'Daily R':>9} {'Cum R':>9}")
            for d in result.daily_pnl[-10:]:
                print(f"  {d.date:<12} {d.wins:>6} {d.losses:>6} {d.daily_r:>+8.1f}R {d.cumulative_r:>+8.1f}R")

        print("\n" + "=" * 70)

    @staticmethod
    def to_dict(result: BacktestResult) -> dict:
        """Convert results to JSON-serializable dict."""
        return {
            "metadata": {
                "start_date": result.start_date.isoformat(),
                "end_date": result.end_date.isoformat(),
                "symbols": result.symbols,
                "timeframes": result.timeframes,
            },
            "overall": {
                "total_signals": result.total_signals,
                "wins": result.wins,
                "losses": result.losses,
                "active": result.active,
                "win_rate": round(result.win_rate, 2),
                "expectancy_r": round(result.expectancy_r, 4),
                "total_r": round(result.total_r, 2),
                "profit_factor": round(result.profit_factor, 2),
            },
            "by_symbol": [
                {
                    "symbol": s.symbol,
                    "total": s.total,
                    "wins": s.wins,
                    "losses": s.losses,
                    "win_rate": round(s.win_rate, 2),
                }
                for s in result.by_symbol
            ],
            "by_timeframe": [
                {
                    "timeframe": s.timeframe,
                    "total": s.total,
                    "wins": s.wins,
                    "losses": s.losses,
                    "win_rate": round(s.win_rate, 2),
                }
                for s in result.by_timeframe
            ],
            "by_direction": [
                {
                    "direction": s.direction,
                    "total": s.total,
                    "wins": s.wins,
                    "losses": s.losses,
                    "win_rate": round(s.win_rate, 2),
                }
                for s in result.by_direction
            ],
            "daily_pnl": [
                {
                    "date": d.date,
                    "wins": d.wins,
                    "losses": d.losses,
                    "daily_r": d.daily_r,
                    "cumulative_r": d.cumulative_r,
                }
                for d in result.daily_pnl
            ],
            "mae_mfe": {
                cat: {
                    "count": m.count,
                    "avg_mae": m.avg_mae,
                    "avg_mfe": m.avg_mfe,
                    "mae_p25": m.mae_p25,
                    "mae_p50": m.mae_p50,
                    "mae_p75": m.mae_p75,
                    "mae_p90": m.mae_p90,
                    "mfe_p25": m.mfe_p25,
                    "mfe_p50": m.mfe_p50,
                    "mfe_p75": m.mfe_p75,
                    "mfe_p90": m.mfe_p90,
                }
                for cat, m in result.mae_mfe.items()
            },
            "signals": [
                {
                    "id": s.id,
                    "symbol": s.symbol,
                    "timeframe": s.timeframe,
                    "signal_time": s.signal_time.isoformat(),
                    "direction": "LONG" if s.direction == 1 else "SHORT",
                    "entry_price": float(s.entry_price),
                    "tp_price": float(s.tp_price),
                    "sl_price": float(s.sl_price),
                    "outcome": s.outcome.value if hasattr(s.outcome, "value") else str(s.outcome),
                    "outcome_time": s.outcome_time.isoformat() if s.outcome_time else None,
                    "mae_ratio": float(s.mae_ratio),
                    "mfe_ratio": float(s.mfe_ratio),
                }
                for s in result.signals
            ],
        }

    @staticmethod
    def save_json(result: BacktestResult, filepath: str) -> None:
        """Save results to JSON file."""
        data = ReportFormatter.to_dict(result)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, cls=DecimalEncoder)
        print(f"\nResults saved to {filepath}")
