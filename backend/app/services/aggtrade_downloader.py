"""Historical AggTrade data downloader from Binance Data Vision."""

import asyncio
import csv
import gzip
import io
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import AsyncIterator

import httpx

from app.models import AggTrade
from app.storage import AggTradeRepository

logger = logging.getLogger(__name__)

# Binance Data Vision base URLs
MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/aggTrades"
DAILY_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"


class AggTradeDownloader:
    """Download and import historical aggTrade data from Binance."""

    def __init__(self, data_dir: Path | None = None):
        self.data_dir = data_dir or Path("/tmp/binance_data")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.repo = AggTradeRepository()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=300.0)
        return self._client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def download_file(self, url: str) -> bytes | None:
        """Download a file from URL."""
        client = await self._get_client()
        try:
            response = await client.get(url)
            if response.status_code == 200:
                return response.content
            elif response.status_code == 404:
                logger.debug(f"File not found: {url}")
                return None
            else:
                logger.warning(f"Failed to download {url}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    def parse_aggtrades_csv(
        self, content: bytes, symbol: str, is_gzipped: bool = False
    ) -> list[AggTrade]:
        """Parse aggTrade CSV content."""
        trades = []

        try:
            if is_gzipped:
                content = gzip.decompress(content)

            text = content.decode("utf-8")
            reader = csv.reader(io.StringIO(text))

            for row in reader:
                # Skip header if present
                if row[0] == "agg_trade_id" or not row[0].isdigit():
                    continue

                # CSV format: agg_trade_id, price, quantity, first_trade_id,
                #             last_trade_id, transact_time, is_buyer_maker
                trade = AggTrade(
                    symbol=symbol,
                    agg_trade_id=int(row[0]),
                    price=Decimal(row[1]),
                    quantity=Decimal(row[2]),
                    timestamp=datetime.fromtimestamp(
                        int(row[5]) / 1000, tz=timezone.utc
                    ),
                    is_buyer_maker=row[6].lower() == "true",
                )
                trades.append(trade)

        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")

        return trades

    async def download_monthly(
        self, symbol: str, year: int, month: int
    ) -> list[AggTrade]:
        """Download monthly aggTrade data."""
        filename = f"{symbol}-aggTrades-{year}-{month:02d}.zip"
        url = f"{MONTHLY_URL}/{symbol}/{filename}"

        logger.info(f"Downloading {filename}")
        content = await self.download_file(url)

        if content is None:
            return []

        # Extract CSV from ZIP
        trades = []
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if name.endswith(".csv"):
                        csv_content = zf.read(name)
                        trades = self.parse_aggtrades_csv(csv_content, symbol)
                        break
        except Exception as e:
            logger.error(f"Error extracting ZIP: {e}")

        logger.info(f"Parsed {len(trades)} trades from {filename}")
        return trades

    async def download_daily(
        self, symbol: str, date: datetime
    ) -> list[AggTrade]:
        """Download daily aggTrade data."""
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{symbol}-aggTrades-{date_str}.zip"
        url = f"{DAILY_URL}/{symbol}/{filename}"

        logger.info(f"Downloading {filename}")
        content = await self.download_file(url)

        if content is None:
            return []

        # Extract CSV from ZIP
        trades = []
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if name.endswith(".csv"):
                        csv_content = zf.read(name)
                        trades = self.parse_aggtrades_csv(csv_content, symbol)
                        break
        except Exception as e:
            logger.error(f"Error extracting ZIP: {e}")

        logger.info(f"Parsed {len(trades)} trades from {filename}")
        return trades

    async def sync_historical(
        self,
        symbol: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        batch_size: int = 50000,
    ) -> int:
        """
        Sync historical aggTrade data for a symbol.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            start_date: Start date (defaults to 2020-01-01)
            end_date: End date (defaults to yesterday)
            batch_size: Number of trades to save per batch

        Returns:
            Total number of trades imported
        """
        if start_date is None:
            start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)

        total_imported = 0
        current_date = start_date.replace(day=1)

        # Download monthly data
        while current_date < end_date:
            year = current_date.year
            month = current_date.month

            # Check if this month is complete (we're past it)
            month_end = (current_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            if month_end < end_date:
                # Download full month
                trades = await self.download_monthly(symbol, year, month)
                if trades:
                    # Save in batches
                    for i in range(0, len(trades), batch_size):
                        batch = trades[i : i + batch_size]
                        await self.repo.save_batch(batch)
                        total_imported += len(batch)
                        logger.info(
                            f"Saved batch of {len(batch)} trades "
                            f"(total: {total_imported})"
                        )
            else:
                # Download daily data for partial month
                day = current_date
                while day <= end_date and day.month == month:
                    trades = await self.download_daily(symbol, day)
                    if trades:
                        await self.repo.save_batch(trades)
                        total_imported += len(trades)
                    day += timedelta(days=1)

            # Move to next month
            current_date = (current_date + timedelta(days=32)).replace(day=1)

        logger.info(f"Total imported for {symbol}: {total_imported} trades")
        return total_imported

    async def sync_recent(
        self, symbol: str, days: int = 7
    ) -> int:
        """
        Sync recent aggTrade data (last N days).

        Args:
            symbol: Trading pair
            days: Number of days to sync

        Returns:
            Number of trades imported
        """
        end_date = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = end_date - timedelta(days=days)

        total = 0
        current = start_date

        while current <= end_date:
            trades = await self.download_daily(symbol, current)
            if trades:
                await self.repo.save_batch(trades)
                total += len(trades)
            current += timedelta(days=1)

        return total


async def download_all_symbols(
    symbols: list[str],
    start_date: datetime | None = None,
    days_recent: int = 30,
) -> dict[str, int]:
    """
    Download aggTrade data for multiple symbols.

    Args:
        symbols: List of trading pairs
        start_date: Start date for full historical sync (None = recent only)
        days_recent: Days of recent data if start_date is None

    Returns:
        Dict of symbol -> trades imported
    """
    downloader = AggTradeDownloader()
    results = {}

    try:
        for symbol in symbols:
            if start_date:
                count = await downloader.sync_historical(symbol, start_date)
            else:
                count = await downloader.sync_recent(symbol, days_recent)
            results[symbol] = count
    finally:
        await downloader.close()

    return results
