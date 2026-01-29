"""Binance REST API client for fetching historical data."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx

from app.models import Kline


class RateLimiter:
    """Simple rate limiter for API calls."""

    def __init__(self, calls_per_minute: int = 1200):
        self.calls_per_minute = calls_per_minute
        self.interval = 60.0 / calls_per_minute
        self.last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait if necessary to respect rate limit."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait_time = self.last_call + self.interval - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self.last_call = asyncio.get_event_loop().time()


class BinanceRestClient:
    """Binance Futures REST API client."""

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        self.rate_limiter = RateLimiter()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            headers = {}
            if self.api_key:
                headers["X-MBX-APIKEY"] = self.api_key
            self._client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                headers=headers,
                timeout=30.0,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _request(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Make an API request with rate limiting."""
        await self.rate_limiter.acquire()
        client = await self._get_client()
        response = await client.request(method, endpoint, params=params)
        response.raise_for_status()
        return response.json()

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 1500,
    ) -> list[Kline]:
        """
        Fetch K-line data from Binance.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            interval: K-line interval (e.g., "5m", "1h")
            start_time: Start time (inclusive)
            end_time: End time (inclusive)
            limit: Maximum number of K-lines (max 1500)

        Returns:
            List of Kline objects
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": min(limit, 1500),
        }

        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        data = await self._request("GET", "/fapi/v1/klines", params)

        klines = []
        for item in data:
            klines.append(
                Kline(
                    symbol=symbol,
                    timeframe=interval,
                    timestamp=datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc),
                    open=Decimal(str(item[1])),
                    high=Decimal(str(item[2])),
                    low=Decimal(str(item[3])),
                    close=Decimal(str(item[4])),
                    volume=Decimal(str(item[5])),
                    is_closed=True,  # Historical klines are always closed
                )
            )

        return klines

    async def get_all_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime | None = None,
    ) -> list[Kline]:
        """
        Fetch all K-lines in a time range, handling pagination.

        Args:
            symbol: Trading pair
            interval: K-line interval
            start_time: Start time
            end_time: End time (defaults to now)

        Returns:
            List of all Kline objects in the range
        """
        if end_time is None:
            end_time = datetime.now(timezone.utc)

        all_klines: list[Kline] = []
        current_start = start_time

        while current_start < end_time:
            klines = await self.get_klines(
                symbol=symbol,
                interval=interval,
                start_time=current_start,
                end_time=end_time,
                limit=1500,
            )

            if not klines:
                break

            all_klines.extend(klines)

            # Move start time to after the last kline
            last_timestamp = klines[-1].timestamp
            if last_timestamp >= end_time:
                break

            # Add a small buffer to avoid duplicate
            current_start = datetime.fromtimestamp(
                last_timestamp.timestamp() + 1, tz=timezone.utc
            )

            # Avoid infinite loop
            if len(klines) < 2:
                break

        return all_klines

    async def get_exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        """Get exchange information for symbols."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._request("GET", "/fapi/v1/exchangeInfo", params)

    async def get_server_time(self) -> datetime:
        """Get Binance server time."""
        data = await self._request("GET", "/fapi/v1/time")
        return datetime.fromtimestamp(data["serverTime"] / 1000, tz=timezone.utc)
