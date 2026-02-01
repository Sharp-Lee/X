"""Historical AggTrade data downloader from Binance Data Vision.

Optimized for large datasets using:
- Streaming CSV parsing (memory efficient)
- Parallel downloads, sequential DB writes (avoid lock contention)
- Batched database writes
"""

import asyncio
import csv
import io
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import httpx

from app.models import AggTrade
from app.storage import AggTradeRepository
from app.storage.fast_import import get_pool, close_pool

logger = logging.getLogger(__name__)

MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/aggTrades"
DAILY_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"


class AggTradeDownloader:
    """Download and import historical aggTrade data from Binance.

    Optimization strategy:
    - Download all files in parallel (network bound)
    - Process and save to DB sequentially (avoid lock contention)
    - Use COPY command for 5-10x faster imports (when use_copy=True)
    """

    def __init__(
        self,
        data_dir: Path | None = None,
        batch_size: int = 50000,  # Larger batches for COPY (50K optimal)
        max_concurrent_downloads: int = 10,
        use_copy: bool = True,  # Use COPY command for faster imports
    ):
        self.data_dir = data_dir or Path("/tmp/binance_data")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.repo = AggTradeRepository()
        self.batch_size = batch_size
        self.max_concurrent_downloads = max_concurrent_downloads
        self.use_copy = use_copy
        self._client: httpx.AsyncClient | None = None
        self._pool = None  # asyncpg connection pool

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=300.0,
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _download_file(self, url: str, semaphore: asyncio.Semaphore) -> bytes | None:
        """Download file with concurrency control."""
        async with semaphore:
            client = await self._get_client()
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.content
                elif response.status_code == 404:
                    return None
                else:
                    logger.warning(f"Failed: {url} ({response.status_code})")
                    return None
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")
                return None

    def _iter_csv_rows(self, content: bytes, symbol: str) -> Iterator[AggTrade]:
        """Stream parse CSV content."""
        text = content.decode("utf-8")
        reader = csv.reader(io.StringIO(text))

        for row in reader:
            if not row or row[0] == "agg_trade_id" or not row[0].isdigit():
                continue
            try:
                yield AggTrade(
                    symbol=symbol,
                    agg_trade_id=int(row[0]),
                    price=Decimal(row[1]),
                    quantity=Decimal(row[2]),
                    timestamp=datetime.fromtimestamp(int(row[5]) / 1000, tz=timezone.utc),
                    is_buyer_maker=row[6].lower() == "true",
                )
            except Exception:
                continue

    async def _process_and_save(self, content: bytes, symbol: str, label: str) -> int:
        """Process ZIP and save to database.

        Uses COPY command with connection pool for maximum speed.
        Handles duplicates by using INSERT ON CONFLICT DO NOTHING.
        """
        total = 0
        batch: list[AggTrade] = []

        # Get connection pool
        pool = await get_pool()

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if not name.endswith(".csv"):
                        continue

                    csv_content = zf.read(name)
                    for trade in self._iter_csv_rows(csv_content, symbol):
                        batch.append(trade)
                        if len(batch) >= self.batch_size:
                            count = await self._save_batch_safe(pool, batch)
                            total += count
                            batch = []

                    if batch:
                        count = await self._save_batch_safe(pool, batch)
                        total += count
                        batch = []
                    break

        except Exception as e:
            logger.error(f"Error processing {label}: {e}")

        return total

    async def _copy_batch_fast(self, pool, trades: list[AggTrade]) -> int:
        """Copy batch using pooled connection (no duplicate handling)."""
        records = [
            (
                t.symbol,
                t.timestamp,
                t.agg_trade_id,
                float(t.price),
                float(t.quantity),
                t.is_buyer_maker,
            )
            for t in trades
        ]

        async with pool.acquire() as conn:
            result = await conn.copy_records_to_table(
                "aggtrades",
                records=records,
                columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
            )
            return int(result.split()[1])

    async def _save_batch_safe(self, pool, trades: list[AggTrade]) -> int:
        """Save batch with duplicate handling.

        Strategy:
        1. Try COPY first (fastest)
        2. If UniqueViolation, fall back to batch INSERT ON CONFLICT DO NOTHING
        """
        import asyncpg

        records = [
            (
                t.symbol,
                t.timestamp,
                t.agg_trade_id,
                float(t.price),
                float(t.quantity),
                t.is_buyer_maker,
            )
            for t in trades
        ]

        async with pool.acquire() as conn:
            try:
                # Try fast COPY first
                result = await conn.copy_records_to_table(
                    "aggtrades",
                    records=records,
                    columns=["symbol", "timestamp", "agg_trade_id", "price", "quantity", "is_buyer_maker"],
                )
                return int(result.split()[1])
            except asyncpg.UniqueViolationError:
                # Fall back to batch INSERT ON CONFLICT DO NOTHING
                logger.info("Duplicate detected, using INSERT ON CONFLICT (slower)")

                # Use executemany for better performance
                result = await conn.executemany(
                    """
                    INSERT INTO aggtrades (symbol, timestamp, agg_trade_id, price, quantity, is_buyer_maker)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (symbol, timestamp, agg_trade_id) DO NOTHING
                    """,
                    records,
                )
                # executemany doesn't return count, so we return 0 to indicate duplicates were skipped
                return 0

    async def sync_recent(self, symbol: str, days: int = 7) -> int:
        """
        Download recent days with parallel download, sequential save.

        Strategy:
        1. Download all ZIP files in parallel
        2. Save to database sequentially (avoid lock contention)
        """
        end_date = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        # Build download tasks
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

        logger.info(f"[{symbol}] 下载 {len(dates)} 天数据 (并行下载, 串行写入)...")

        # Phase 1: Parallel download all files
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        download_tasks = []

        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            url = f"{DAILY_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
            download_tasks.append((date_str, self._download_file(url, semaphore)))

        logger.info(f"[{symbol}] 阶段1: 并行下载...")
        results = await asyncio.gather(*[t[1] for t in download_tasks])

        # Pair results with dates
        downloads = []
        for i, (date_str, _) in enumerate(download_tasks):
            content = results[i]
            if content:
                downloads.append((date_str, content))
                logger.info(f"[{symbol}] 下载完成: {date_str} ({len(content) / 1024 / 1024:.1f} MB)")
            else:
                logger.warning(f"[{symbol}] 下载失败: {date_str}")

        # Phase 2: Sequential save to database
        logger.info(f"[{symbol}] 阶段2: 串行写入数据库...")
        total = 0

        for date_str, content in downloads:
            count = await self._process_and_save(content, symbol, date_str)
            total += count
            logger.info(f"[{symbol}] 写入完成: {date_str} ({count:,} 条)")

        logger.info(f"[{symbol}] 完成: {total:,} 条")
        return total

    async def sync_historical(
        self,
        symbol: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        """Sync historical data (monthly + daily)."""
        if start_date is None:
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)

        total = 0
        current = start_date.replace(day=1)

        while current < end_date:
            year, month = current.year, current.month
            month_end = (current + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            if month_end < end_date:
                # Full month - download monthly file
                url = f"{MONTHLY_URL}/{symbol}/{symbol}-aggTrades-{year}-{month:02d}.zip"
                print(f"  [{symbol}] {year}-{month:02d}...", end=" ", flush=True)
                semaphore = asyncio.Semaphore(1)
                content = await self._download_file(url, semaphore)
                if content:
                    size_mb = len(content) / 1024 / 1024
                    count = await self._process_and_save(content, symbol, f"{year}-{month:02d}")
                    total += count
                    print(f"{count:>10,} 条 ({size_mb:.0f}MB)")
                else:
                    print("无数据")
            else:
                # Partial month - use daily files with parallel download
                dates = []
                day = current
                while day <= end_date and day.month == month:
                    dates.append(day)
                    day += timedelta(days=1)

                print(f"  [{symbol}] {year}-{month:02d} (日度 x{len(dates)})...", end=" ", flush=True)

                # Parallel download
                semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
                tasks = []
                for d in dates:
                    ds = d.strftime("%Y-%m-%d")
                    url = f"{DAILY_URL}/{symbol}/{symbol}-aggTrades-{ds}.zip"
                    tasks.append((ds, self._download_file(url, semaphore)))

                results = await asyncio.gather(*[t[1] for t in tasks])

                # Sequential save
                month_count = 0
                for i, (ds, _) in enumerate(tasks):
                    if results[i]:
                        count = await self._process_and_save(results[i], symbol, ds)
                        month_count += count
                        total += count

                print(f"{month_count:>10,} 条")

            current = (current + timedelta(days=32)).replace(day=1)

        print(f"  [{symbol}] 完成: {total:,} 条")
        return total


async def download_all_symbols_parallel(
    symbols: list[str],
    days: int = 7,
    max_concurrent_symbols: int = 3,
) -> dict[str, int]:
    """Download multiple symbols in parallel.

    Each symbol downloads days in parallel, and multiple symbols run concurrently.
    Uses shared connection pool for efficient database writes.
    """
    # Pre-initialize connection pool
    await get_pool()

    results = {}
    semaphore = asyncio.Semaphore(max_concurrent_symbols)

    async def download_one(symbol: str) -> tuple[str, int]:
        async with semaphore:
            downloader = AggTradeDownloader()
            try:
                count = await downloader.sync_recent(symbol, days)
                return symbol, count
            finally:
                await downloader.close()

    # Run all symbols in parallel (limited by semaphore)
    tasks = [download_one(symbol) for symbol in symbols]
    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in task_results:
        if isinstance(result, Exception):
            logger.error(f"Download failed: {result}")
        else:
            symbol, count = result
            results[symbol] = count

    return results


# Legacy compatibility
async def download_all_symbols(
    symbols: list[str],
    start_date: datetime | None = None,
    days_recent: int = 30,
) -> dict[str, int]:
    if start_date:
        downloader = AggTradeDownloader()
        results = {}
        try:
            for symbol in symbols:
                count = await downloader.sync_historical(symbol, start_date)
                results[symbol] = count
        finally:
            await downloader.close()
        return results
    else:
        return await download_all_symbols_parallel(symbols, days_recent)
