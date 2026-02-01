"""Fast data import using PostgreSQL COPY and connection pooling.

Optimizations:
1. Connection pool (avoid creating new connections)
2. COPY FROM stdin with CSV format (fastest bulk import)
3. Batch processing with streaming
"""

import asyncio
import io
import logging
from datetime import datetime
from decimal import Decimal
from typing import Iterator

import asyncpg

from app.config import get_settings
from app.models import AggTrade

logger = logging.getLogger(__name__)

# Global connection pool
_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """Get or create connection pool."""
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
            command_timeout=300,
        )
    return _pool


async def close_pool() -> None:
    """Close connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def copy_aggtrades_fast(trades: list[AggTrade]) -> int:
    """Import trades using COPY with connection pool.

    Uses COPY FROM stdin with CSV format for maximum speed.
    """
    if not trades:
        return 0

    pool = await get_pool()

    # Build CSV data in memory
    csv_buffer = io.StringIO()
    for t in trades:
        # Format: symbol,timestamp,agg_trade_id,price,quantity,is_buyer_maker
        csv_buffer.write(
            f"{t.symbol},{t.timestamp.isoformat()},{t.agg_trade_id},"
            f"{t.price},{t.quantity},{t.is_buyer_maker}\n"
        )

    csv_data = csv_buffer.getvalue()
    csv_buffer.close()

    async with pool.acquire() as conn:
        # Use COPY FROM stdin
        result = await conn.copy_to_table(
            "aggtrades",
            source=io.StringIO(csv_data),
            columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
            format="csv",
        )
        return int(result.split()[1])


async def copy_aggtrades_stream(
    trade_iter: Iterator[AggTrade],
    batch_size: int = 50000,
) -> int:
    """Stream import trades with batching.

    Processes trades in batches without loading all into memory.
    """
    pool = await get_pool()
    total = 0
    batch: list[AggTrade] = []

    for trade in trade_iter:
        batch.append(trade)
        if len(batch) >= batch_size:
            count = await _copy_batch_pooled(pool, batch)
            total += count
            batch = []

    if batch:
        count = await _copy_batch_pooled(pool, batch)
        total += count

    return total


async def _copy_batch_pooled(pool: asyncpg.Pool, trades: list[AggTrade]) -> int:
    """Copy a batch using pooled connection."""
    # Build CSV data
    lines = []
    for t in trades:
        lines.append(
            f"{t.symbol}\t{t.timestamp.isoformat()}\t{t.agg_trade_id}\t"
            f"{t.price}\t{t.quantity}\t{t.is_buyer_maker}"
        )
    data = "\n".join(lines)

    async with pool.acquire() as conn:
        result = await conn.copy_to_table(
            "aggtrades",
            source=io.StringIO(data),
            columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
            format="text",
        )
        return int(result.split()[1])


async def copy_from_csv_file(filepath: str, symbol: str) -> int:
    """Import directly from CSV file (fastest method).

    Expected CSV format (no header):
    agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
    """
    import csv
    from datetime import timezone

    pool = await get_pool()
    total = 0
    batch_size = 100000  # Larger batches for file import

    with open(filepath, "r") as f:
        reader = csv.reader(f)
        batch = []

        for row in reader:
            if not row or row[0] == "agg_trade_id" or not row[0].isdigit():
                continue

            # Build tuple for COPY
            batch.append((
                symbol,
                datetime.fromtimestamp(int(row[5]) / 1000, tz=timezone.utc),
                int(row[0]),
                Decimal(row[1]),
                Decimal(row[2]),
                row[6].lower() == "true",
            ))

            if len(batch) >= batch_size:
                async with pool.acquire() as conn:
                    result = await conn.copy_records_to_table(
                        "aggtrades",
                        records=batch,
                        columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
                    )
                    total += int(result.split()[1])
                batch = []

        if batch:
            async with pool.acquire() as conn:
                result = await conn.copy_records_to_table(
                    "aggtrades",
                    records=batch,
                    columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
                )
                total += int(result.split()[1])

    return total


async def import_with_disabled_indexes(
    trades: list[AggTrade],
    batch_size: int = 100000,
) -> int:
    """Import with indexes disabled temporarily (faster for large imports).

    WARNING: Only use for bulk imports, not concurrent with other operations.
    """
    pool = await get_pool()

    async with pool.acquire() as conn:
        # Disable indexes (PostgreSQL specific)
        # Note: For TimescaleDB hypertables, this may not work the same way
        # await conn.execute("ALTER TABLE aggtrades DISABLE TRIGGER ALL")

        total = 0
        for i in range(0, len(trades), batch_size):
            batch = trades[i:i + batch_size]
            records = [
                (
                    t.symbol,
                    t.timestamp,
                    t.agg_trade_id,
                    float(t.price),
                    float(t.quantity),
                    t.is_buyer_maker,
                )
                for t in batch
            ]
            result = await conn.copy_records_to_table(
                "aggtrades",
                records=records,
                columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
            )
            total += int(result.split()[1])

        # Re-enable indexes
        # await conn.execute("ALTER TABLE aggtrades ENABLE TRIGGER ALL")

    return total
