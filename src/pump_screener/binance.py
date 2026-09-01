from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator

import aiohttp
import websockets

from .config import BinanceConfig
from .models import KlineUpdate, TickerUpdate


LOGGER = logging.getLogger(__name__)


class BinanceClient:
    def __init__(self, config: BinanceConfig) -> None:
        self.config = config
        self._historical_request_lock = asyncio.Lock()
        self._next_historical_request_time = 0.0

    async def symbols(self) -> list[str]:
        url = f"{self.config.rest_base_url}/fapi/v1/exchangeInfo"
        timeout = aiohttp.ClientTimeout(total=20)
        # The threaded resolver follows Windows/VPN DNS reliably. aiohttp's
        # optional aiodns resolver can time out on otherwise healthy VPN links.
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    payload = await response.json()
            except Exception as exc:
                raise ConnectionError(
                    f"Could not reach Binance Futures REST at {url}. "
                    "Check whether the domain is available on this network or change "
                    "binance.rest_base_url in config.toml."
                ) from exc

        return sorted(
            item["symbol"]
            for item in payload.get("symbols", [])
            if item.get("contractType") == "PERPETUAL"
            and item.get("status") == "TRADING"
            and item.get("quoteAsset") == self.config.quote_asset
            and item.get("symbol") not in self.config.excluded_symbols
        )

    async def updates(self, symbols: list[str]) -> AsyncIterator[TickerUpdate | KlineUpdate]:
        symbol_set = set(symbols)
        streams = ["!ticker@arr", *(f"{symbol.lower()}@kline_1m" for symbol in symbols)]
        async with websockets.connect(
            self.config.websocket_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=8 * 1024 * 1024,
        ) as websocket:
            for start in range(0, len(streams), 100):
                await websocket.send(
                    json.dumps(
                        {
                            "method": "SUBSCRIBE",
                            "params": streams[start : start + 100],
                            "id": start // 100 + 1,
                        }
                    )
                )
                await asyncio.sleep(0.25)

            LOGGER.info("Subscribed to %d Binance streams", len(streams))
            async for raw_message in websocket:
                payload = json.loads(raw_message)
                if "result" in payload:
                    continue
                data = payload.get("data", payload)
                if isinstance(data, list):
                    for item in data:
                        update = _parse_ticker(item)
                        if update is not None and update.symbol in symbol_set:
                            yield update
                elif (
                    data.get("e") == "kline"
                    and data.get("s") in symbol_set
                    and data.get("k", {}).get("i") == "1m"
                    and data.get("st", 1) == 1
                ):
                    update = _parse_kline(data)
                    if update is not None:
                        yield update

    async def historical_klines(
        self, symbols: list[str], limit: int
    ) -> dict[str, list[KlineUpdate]]:
        """Fetch one bounded startup window; live updates remain WebSocket-only."""
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(
            resolver=aiohttp.ThreadedResolver(), limit=12
        )
        semaphore = asyncio.Semaphore(12)
        now_ms = int(time.time() * 1000)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async def fetch(symbol: str) -> tuple[str, list[KlineUpdate]]:
                url = f"{self.config.rest_base_url}/fapi/v1/klines"
                try:
                    async with semaphore:
                        async with session.get(
                            url,
                            params={"symbol": symbol, "interval": "1m", "limit": limit},
                        ) as response:
                            response.raise_for_status()
                            rows = await response.json()
                except Exception as exc:
                    LOGGER.warning("Could not warm up %s: %s", symbol, exc)
                    return symbol, []

                candles = [
                    _parse_rest_kline(symbol, row)
                    for row in rows
                    if int(row[6]) < now_ms
                ]
                return symbol, [item for item in candles if item is not None]

            results = await asyncio.gather(*(fetch(symbol) for symbol in symbols))
        return dict(results)

    async def historical_klines_between(
        self,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        interval: str = "5m",
    ) -> list[KlineUpdate]:
        """Fetch a closed historical range without retaining it on disk."""
        if end_time_ms <= start_time_ms:
            return []
        interval_ms = _interval_milliseconds(interval)
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        cursor = max(0, start_time_ms)
        candles: list[KlineUpdate] = []
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            while cursor <= end_time_ms:
                url = f"{self.config.rest_base_url}/fapi/v1/klines"
                rows = await self._historical_json(
                    session,
                    url,
                    {
                        "symbol": symbol,
                        "interval": interval,
                        "startTime": cursor,
                        "endTime": end_time_ms,
                        "limit": 1500,
                    },
                )
                if not rows:
                    break
                parsed = [
                    item
                    for item in (_parse_rest_kline(symbol, row) for row in rows)
                    if item is not None and item.close_time_ms <= end_time_ms
                ]
                candles.extend(parsed)
                last_open_time = int(rows[-1][0])
                next_cursor = last_open_time + interval_ms
                if next_cursor <= cursor or len(rows) < 1500:
                    break
                cursor = next_cursor
                await asyncio.sleep(0.05)
        return candles

    async def _historical_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict[str, str | int],
    ) -> list:
        """Serialize catch-up requests and honor Binance rate-limit backoff."""
        for attempt in range(6):
            async with self._historical_request_lock:
                loop = asyncio.get_running_loop()
                delay = self._next_historical_request_time - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                async with session.get(url, params=params) as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get("Retry-After", "1"))
                        self._next_historical_request_time = (
                            loop.time() + max(retry_after, 1.0)
                        )
                        if attempt == 5:
                            response.raise_for_status()
                        continue
                    response.raise_for_status()
                    payload = await response.json()
                    # Five requests per second is deliberately cautious because
                    # a VPN exit IP may be shared with other Binance clients.
                    self._next_historical_request_time = loop.time() + 0.2
                    return payload
        raise RuntimeError("Historical Binance request retries exhausted")


def _parse_ticker(item: dict) -> TickerUpdate | None:
    if item.get("e") != "24hrTicker":
        return None
    try:
        return TickerUpdate(
            symbol=item["s"],
            event_time_ms=int(item["E"]),
            price=float(item["c"]),
            return_24h_pct=float(item["P"]),
            quote_volume_24h=float(item["q"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _parse_kline(data: dict) -> KlineUpdate | None:
    try:
        item = data["k"]
        return KlineUpdate(
            symbol=data["s"],
            event_time_ms=int(data["E"]),
            open_time_ms=int(item["t"]),
            close_time_ms=int(item["T"]),
            open_price=float(item["o"]),
            high_price=float(item["h"]),
            low_price=float(item["l"]),
            close_price=float(item["c"]),
            base_volume=float(item["v"]),
            quote_volume=float(item["q"]),
            taker_buy_quote_volume=float(item["Q"]),
            is_closed=bool(item["x"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _parse_rest_kline(symbol: str, item: list) -> KlineUpdate | None:
    try:
        return KlineUpdate(
            symbol=symbol,
            event_time_ms=int(item[6]),
            open_time_ms=int(item[0]),
            close_time_ms=int(item[6]),
            open_price=float(item[1]),
            high_price=float(item[2]),
            low_price=float(item[3]),
            close_price=float(item[4]),
            base_volume=float(item[5]),
            quote_volume=float(item[7]),
            taker_buy_quote_volume=float(item[10]),
            is_closed=True,
        )
    except (IndexError, TypeError, ValueError):
        return None


def _interval_milliseconds(interval: str) -> int:
    values = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
    }
    try:
        return values[interval]
    except KeyError as exc:
        raise ValueError(f"Unsupported historical interval: {interval}") from exc
