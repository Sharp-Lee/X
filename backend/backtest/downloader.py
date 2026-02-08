"""Historical kline downloader — independent of app/.

Downloads 1m klines from data.binance.vision and writes directly
to PostgreSQL via asyncpg. No SQLAlchemy, no app/ imports.

Optimization strategy:
- Download all files in parallel (network-bound)
- Process and save to DB sequentially (avoid lock contention)
- executemany with ON CONFLICT upsert for idempotency
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterator

import asyncpg
import httpx

from core.models.kline import Kline

logger = logging.getLogger(__name__)

MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
DAILY_URL = "https://data.binance.vision/data/futures/um/daily/klines"


class KlineDownloader:
    """Download and import klines from Binance Data Vision.

    Writes directly to PostgreSQL via asyncpg — no app/ dependency.
    """

    def __init__(
        self,
        database_url: str,
        batch_size: int = 10_000,
        max_concurrent_downloads: int = 10,
    ):
        self._database_url = database_url
        self._batch_size = batch_size
        self._max_concurrent = max_concurrent_downloads
        self._client: httpx.AsyncClient | None = None
        self._pool: asyncpg.Pool | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=300.0,
                limits=httpx.Limits(
                    max_connections=20, max_keepalive_connections=10
                ),
            )
        return self._client

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._database_url,
                min_size=2,
                max_size=5,
                command_timeout=300,
            )
        return self._pool

    async def close(self) -> None:
        """Release all resources."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        if self._pool:
            await self._pool.close()
            self._pool = None

    # ── Download ────────────────────────────────────────────────

    async def _download_file(
        self, url: str, semaphore: asyncio.Semaphore
    ) -> bytes | None:
        async with semaphore:
            client = await self._get_client()
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code != 404:
                    logger.warning(f"HTTP {resp.status_code}: {url}")
                return None
            except Exception as e:
                logger.error(f"Download error {url}: {e}")
                return None

    # ── CSV parsing ─────────────────────────────────────────────

    @staticmethod
    def _iter_csv_rows(
        content: bytes, symbol: str, timeframe: str
    ) -> Iterator[Kline]:
        """Stream-parse CSV from ZIP content."""
        text = content.decode("utf-8")
        for row in csv.reader(io.StringIO(text)):
            if not row or not row[0].isdigit():
                continue
            try:
                yield Kline(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.fromtimestamp(
                        int(row[0]) / 1000, tz=timezone.utc
                    ),
                    open=Decimal(row[1]),
                    high=Decimal(row[2]),
                    low=Decimal(row[3]),
                    close=Decimal(row[4]),
                    volume=Decimal(row[5]),
                )
            except Exception:
                continue

    # ── Database write ──────────────────────────────────────────

    async def _save_batch(self, pool: asyncpg.Pool, klines: list[Kline]) -> int:
        """Batch upsert klines via asyncpg executemany."""
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
            await conn.executemany(
                """INSERT INTO klines
                       (symbol, timeframe, timestamp, open, high, low, close, volume)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
                       open=EXCLUDED.open, high=EXCLUDED.high,
                       low=EXCLUDED.low, close=EXCLUDED.close,
                       volume=EXCLUDED.volume""",
                records,
            )
        return len(records)

    async def _process_zip_and_save(
        self, content: bytes, symbol: str, timeframe: str, label: str
    ) -> int:
        """Extract ZIP → parse CSV → batch upsert to DB."""
        pool = await self._get_pool()
        total = 0
        batch: list[Kline] = []

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if not name.endswith(".csv"):
                        continue
                    for kline in self._iter_csv_rows(
                        zf.read(name), symbol, timeframe
                    ):
                        batch.append(kline)
                        if len(batch) >= self._batch_size:
                            total += await self._save_batch(pool, batch)
                            batch = []
                    if batch:
                        total += await self._save_batch(pool, batch)
                        batch = []
                    break  # only first CSV in ZIP
        except Exception as e:
            logger.error(f"Error processing {label}: {e}")

        return total

    # ── Public API ──────────────────────────────────────────────

    async def sync_historical(
        self,
        symbol: str,
        timeframe: str = "1m",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        """Download historical klines (monthly + daily files).

        Returns total number of klines imported.
        """
        if start_date is None:
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)

        total = 0
        current = start_date.replace(day=1)
        semaphore = asyncio.Semaphore(self._max_concurrent)

        while current < end_date:
            year, month = current.year, current.month
            month_end = (current + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            if month_end < end_date:
                # Full month → monthly file
                url = (
                    f"{MONTHLY_URL}/{symbol}/{timeframe}/"
                    f"{symbol}-{timeframe}-{year}-{month:02d}.zip"
                )
                logger.info(f"[{symbol}/{timeframe}] Downloading {year}-{month:02d}...")
                content = await self._download_file(url, semaphore)
                if content:
                    count = await self._process_zip_and_save(
                        content, symbol, timeframe, f"{year}-{month:02d}"
                    )
                    total += count
                    logger.info(
                        f"[{symbol}/{timeframe}] {year}-{month:02d}: {count:,} klines"
                    )
            else:
                # Partial month → daily files
                dates = []
                day = current
                while day <= end_date and day.month == month:
                    dates.append(day)
                    day += timedelta(days=1)

                tasks = []
                for d in dates:
                    ds = d.strftime("%Y-%m-%d")
                    url = (
                        f"{DAILY_URL}/{symbol}/{timeframe}/"
                        f"{symbol}-{timeframe}-{ds}.zip"
                    )
                    tasks.append((ds, self._download_file(url, semaphore)))

                results = await asyncio.gather(*[t[1] for t in tasks])

                for i, (ds, _) in enumerate(tasks):
                    if results[i]:
                        count = await self._process_zip_and_save(
                            results[i], symbol, timeframe, ds
                        )
                        total += count

            current = (current + timedelta(days=32)).replace(day=1)

        logger.info(f"[{symbol}/{timeframe}] Historical sync done: {total:,} klines")
        return total
