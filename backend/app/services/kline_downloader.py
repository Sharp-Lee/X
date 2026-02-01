"""Historical K-line data downloader from Binance Data Vision.

Optimized for large datasets using:
- Streaming CSV parsing (memory efficient)
- Parallel downloads, sequential DB writes (avoid lock contention)
- COPY command for fast bulk imports
"""

import asyncio
import csv
import io
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterator

import httpx
import asyncpg

from app.models import Kline
from app.storage.fast_import import get_pool

logger = logging.getLogger(__name__)

# Binance Data Vision URLs
MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
DAILY_URL = "https://data.binance.vision/data/futures/um/daily/klines"

# Supported timeframes
TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]


class KlineDownloader:
    """Download and import historical K-line data from Binance.

    Optimization strategy:
    - Download all files in parallel (network bound)
    - Process and save to DB sequentially (avoid lock contention)
    - Use COPY command for fast imports, with ON CONFLICT fallback
    """

    def __init__(
        self,
        batch_size: int = 10000,
        max_concurrent_downloads: int = 10,
    ):
        self.batch_size = batch_size
        self.max_concurrent_downloads = max_concurrent_downloads
        self._client: httpx.AsyncClient | None = None

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

    def _iter_csv_rows(self, content: bytes, symbol: str, timeframe: str) -> Iterator[Kline]:
        """Stream parse CSV content."""
        text = content.decode("utf-8")
        reader = csv.reader(io.StringIO(text))

        for row in reader:
            if not row or row[0] == "open_time" or not row[0].isdigit():
                continue
            try:
                # CSV format: open_time,open,high,low,close,volume,...
                yield Kline(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc),
                    open=Decimal(row[1]),
                    high=Decimal(row[2]),
                    low=Decimal(row[3]),
                    close=Decimal(row[4]),
                    volume=Decimal(row[5]),
                    is_closed=True,
                )
            except Exception:
                continue

    async def _process_and_save(
        self, content: bytes, symbol: str, timeframe: str, label: str
    ) -> int:
        """Process ZIP and save to database."""
        total = 0
        batch: list[Kline] = []

        pool = await get_pool()

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if not name.endswith(".csv"):
                        continue

                    csv_content = zf.read(name)
                    for kline in self._iter_csv_rows(csv_content, symbol, timeframe):
                        batch.append(kline)
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

    async def _save_batch_safe(self, pool, klines: list[Kline]) -> int:
        """Save batch with duplicate handling.

        Uses INSERT ON CONFLICT DO UPDATE (upsert) for K-lines.
        """
        records = [
            (
                k.symbol,
                k.timeframe,
                k.timestamp,
                float(k.open),
                float(k.high),
                float(k.low),
                float(k.close),
                float(k.volume),
            )
            for k in klines
        ]

        async with pool.acquire() as conn:
            # Use executemany with ON CONFLICT DO UPDATE
            await conn.executemany(
                """
                INSERT INTO klines (symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                records,
            )
            return len(records)

    async def sync_recent(
        self,
        symbol: str,
        timeframe: str = "1m",
        days: int = 7,
    ) -> int:
        """Download recent days of K-line data.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: K-line interval (e.g., "1m", "5m", "1h")
            days: Number of days to download

        Returns:
            Number of K-lines imported
        """
        end_date = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        # Build download tasks
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)

        logger.info(f"[{symbol}/{timeframe}] 下载 {len(dates)} 天数据...")

        # Phase 1: Parallel download
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        download_tasks = []

        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            url = f"{DAILY_URL}/{symbol}/{timeframe}/{symbol}-{timeframe}-{date_str}.zip"
            download_tasks.append((date_str, self._download_file(url, semaphore)))

        logger.info(f"[{symbol}/{timeframe}] 阶段1: 并行下载...")
        results = await asyncio.gather(*[t[1] for t in download_tasks])

        # Pair results with dates
        downloads = []
        for i, (date_str, _) in enumerate(download_tasks):
            content = results[i]
            if content:
                downloads.append((date_str, content))
                logger.info(f"[{symbol}/{timeframe}] 下载完成: {date_str} ({len(content) / 1024:.1f} KB)")
            else:
                logger.warning(f"[{symbol}/{timeframe}] 下载失败: {date_str}")

        # Phase 2: Sequential save
        logger.info(f"[{symbol}/{timeframe}] 阶段2: 写入数据库...")
        total = 0

        for date_str, content in downloads:
            count = await self._process_and_save(content, symbol, timeframe, date_str)
            total += count
            logger.info(f"[{symbol}/{timeframe}] 写入完成: {date_str} ({count:,} 条)")

        logger.info(f"[{symbol}/{timeframe}] 完成: {total:,} 条")
        return total

    async def sync_historical(
        self,
        symbol: str,
        timeframe: str = "1m",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        """Sync historical data (monthly + daily).

        Downloads monthly files for complete months, daily files for partial months.
        """
        if start_date is None:
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)

        total = 0
        current = start_date.replace(day=1)
        semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

        while current < end_date:
            year, month = current.year, current.month
            month_end = (current + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            if month_end < end_date:
                # Full month - download monthly file
                url = f"{MONTHLY_URL}/{symbol}/{timeframe}/{symbol}-{timeframe}-{year}-{month:02d}.zip"
                logger.info(f"[{symbol}/{timeframe}] 下载 {year}-{month:02d}...")
                content = await self._download_file(url, semaphore)
                if content:
                    count = await self._process_and_save(
                        content, symbol, timeframe, f"{year}-{month:02d}"
                    )
                    total += count
                    logger.info(f"[{symbol}/{timeframe}] {year}-{month:02d} 完成: {count:,} 条")
            else:
                # Partial month - use daily files
                dates = []
                day = current
                while day <= end_date and day.month == month:
                    dates.append(day)
                    day += timedelta(days=1)

                # Parallel download
                tasks = []
                for d in dates:
                    ds = d.strftime("%Y-%m-%d")
                    url = f"{DAILY_URL}/{symbol}/{timeframe}/{symbol}-{timeframe}-{ds}.zip"
                    tasks.append((ds, self._download_file(url, semaphore)))

                results = await asyncio.gather(*[t[1] for t in tasks])

                # Sequential save
                for i, (ds, _) in enumerate(tasks):
                    if results[i]:
                        count = await self._process_and_save(
                            results[i], symbol, timeframe, ds
                        )
                        total += count

            current = (current + timedelta(days=32)).replace(day=1)

        logger.info(f"[{symbol}/{timeframe}] 历史同步完成: {total:,} 条")
        return total


async def download_klines_parallel(
    symbols: list[str],
    timeframes: list[str] | None = None,
    days: int = 7,
    max_concurrent: int = 5,
) -> dict[str, dict[str, int]]:
    """Download K-lines for multiple symbols and timeframes in parallel.

    Args:
        symbols: List of trading pairs
        timeframes: List of intervals (default: ["1m"])
        days: Number of days to download
        max_concurrent: Max concurrent download tasks

    Returns:
        Nested dict: {symbol: {timeframe: count}}
    """
    if timeframes is None:
        timeframes = ["1m"]

    # Pre-initialize connection pool
    await get_pool()

    results: dict[str, dict[str, int]] = {}
    semaphore = asyncio.Semaphore(max_concurrent)

    async def download_one(symbol: str, timeframe: str) -> tuple[str, str, int]:
        async with semaphore:
            downloader = KlineDownloader()
            try:
                count = await downloader.sync_recent(symbol, timeframe, days)
                return symbol, timeframe, count
            finally:
                await downloader.close()

    # Build all tasks
    tasks = []
    for symbol in symbols:
        for timeframe in timeframes:
            tasks.append(download_one(symbol, timeframe))

    # Run all in parallel (limited by semaphore)
    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in task_results:
        if isinstance(result, Exception):
            logger.error(f"Download failed: {result}")
        else:
            symbol, timeframe, count = result
            if symbol not in results:
                results[symbol] = {}
            results[symbol][timeframe] = count

    return results
