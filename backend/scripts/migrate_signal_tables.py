"""Migrate signal tables to per-strategy naming.

Renames:
  signals          -> msr_signals
  backtest_signals -> backtest_msr_signals

Adds a `strategy` column (VARCHAR(50), default 'msr_retest_capture') to each.

Idempotent: safe to run multiple times.

Usage:
    python -m scripts.migrate_signal_tables
"""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg


async def migrate(database_url: str) -> None:
    conn = await asyncpg.connect(database_url)
    try:
        # ── Live signals table ────────────────────────────────────
        # Check if old table exists and new one doesn't
        old_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'signals')"
        )
        new_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'msr_signals')"
        )

        if old_exists and not new_exists:
            print("Renaming 'signals' -> 'msr_signals' ...")
            await conn.execute("ALTER TABLE signals RENAME TO msr_signals")

            # Rename indexes to match new table name
            for old_idx, new_idx in [
                ("idx_signals_symbol_time", "idx_msr_signals_symbol_time"),
                ("idx_signals_outcome", "idx_msr_signals_outcome"),
                ("idx_signals_symbol_tf_outcome", "idx_msr_signals_symbol_tf_outcome"),
            ]:
                idx_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", old_idx
                )
                if idx_exists:
                    await conn.execute(f"ALTER INDEX {old_idx} RENAME TO {new_idx}")
                    print(f"  Renamed index {old_idx} -> {new_idx}")
        elif new_exists:
            print("Table 'msr_signals' already exists, skipping rename.")
        else:
            print("Table 'signals' does not exist, nothing to rename for live signals.")

        # Add strategy column if missing
        if new_exists or old_exists:
            table_name = "msr_signals"
            col_exists = await conn.fetchval(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.columns"
                "  WHERE table_name = $1 AND column_name = 'strategy'"
                ")",
                table_name,
            )
            if not col_exists:
                print(f"Adding 'strategy' column to '{table_name}' ...")
                await conn.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN strategy VARCHAR(50) "
                    f"NOT NULL DEFAULT 'msr_retest_capture'"
                )
            else:
                print(f"Column 'strategy' already exists in '{table_name}'.")

        # ── Backtest signals table ────────────────────────────────
        bt_old_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'backtest_signals')"
        )
        bt_new_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'backtest_msr_signals')"
        )

        if bt_old_exists and not bt_new_exists:
            print("Renaming 'backtest_signals' -> 'backtest_msr_signals' ...")
            await conn.execute("ALTER TABLE backtest_signals RENAME TO backtest_msr_signals")

            for old_idx, new_idx in [
                ("idx_bt_signals_run_outcome", "idx_bt_msr_signals_run_outcome"),
                ("idx_bt_signals_run_symbol_tf", "idx_bt_msr_signals_run_symbol_tf"),
            ]:
                idx_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", old_idx
                )
                if idx_exists:
                    await conn.execute(f"ALTER INDEX {old_idx} RENAME TO {new_idx}")
                    print(f"  Renamed index {old_idx} -> {new_idx}")
        elif bt_new_exists:
            print("Table 'backtest_msr_signals' already exists, skipping rename.")
        else:
            print("Table 'backtest_signals' does not exist, nothing to rename for backtest signals.")

        # Add strategy column if missing
        if bt_new_exists or bt_old_exists:
            bt_table_name = "backtest_msr_signals"
            bt_col_exists = await conn.fetchval(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.columns"
                "  WHERE table_name = $1 AND column_name = 'strategy'"
                ")",
                bt_table_name,
            )
            if not bt_col_exists:
                print(f"Adding 'strategy' column to '{bt_table_name}' ...")
                await conn.execute(
                    f"ALTER TABLE {bt_table_name} ADD COLUMN strategy VARCHAR(50) "
                    f"NOT NULL DEFAULT 'msr_retest_capture'"
                )
            else:
                print(f"Column 'strategy' already exists in '{bt_table_name}'.")

        print("\nMigration complete.")

    finally:
        await conn.close()


def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        # Try to load from app config
        try:
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from app.config import get_settings
            database_url = get_settings().database_url
        except Exception:
            print("ERROR: Set DATABASE_URL environment variable or ensure app.config is importable.")
            sys.exit(1)

    asyncio.run(migrate(database_url))


if __name__ == "__main__":
    main()
