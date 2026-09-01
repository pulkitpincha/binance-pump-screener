from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import sqlite3
import time

from aiohttp import web
import websockets

from .config import AppConfig


LOGGER = logging.getLogger(__name__)


class PositionDashboard:
    """Read-only decision dashboard with an independent Binance live-price feed."""

    def __init__(self, config: AppConfig, host: str, port: int) -> None:
        self.config = config
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}/"
        database_uri = f"file:{config.storage.database_path.as_posix()}?mode=ro"
        self.connection = sqlite3.connect(
            database_uri,
            uri=True,
            timeout=5,
            check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA query_only = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.prices: dict[str, float] = {}
        self.price_update_ms: int | None = None
        self._runner: web.AppRunner | None = None
        self._price_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/", self._index)
        app.router.add_get("/api/positions", self._positions)
        app.router.add_get("/api/health", self._health)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        self._price_task = asyncio.create_task(self._price_feed())
        LOGGER.info("Position dashboard ready at %s", self.url)

    async def stop(self) -> None:
        if self._price_task is not None:
            self._price_task.cancel()
            try:
                await self._price_task
            except asyncio.CancelledError:
                pass
            self._price_task = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        self.connection.close()

    async def _index(self, request: web.Request) -> web.StreamResponse:
        del request
        return web.FileResponse(Path(__file__).with_name("positions.html"))

    async def _positions(self, request: web.Request) -> web.Response:
        decision = request.query.get("decision", "").upper()
        if decision and decision not in {"TRADE", "IGNORE"}:
            return web.json_response(
                {"error": "decision must be TRADE or IGNORE"}, status=400
            )

        sql = """
            SELECT
                id, symbol, screener_type, entry_type, signal_time_ms,
                entry_price, return_5m_pct, return_24h_pct, rvol,
                max_price, min_price, max_price_time_ms, min_price_time_ms,
                last_price, last_seen_ms, status, stop_reason,
                review_status, decision_time_ms, decision_price
            FROM events
            WHERE review_status IN ('TRADE', 'IGNORE')
        """
        parameters: tuple[str, ...] = ()
        if decision:
            sql += " AND review_status = ?"
            parameters = (decision,)
        sql += " ORDER BY decision_time_ms DESC"

        rows = self.connection.execute(sql, parameters).fetchall()
        payload = [self._serialize(row) for row in rows]
        return web.json_response(
            {
                "positions": payload,
                "price_update_ms": self.price_update_ms,
                "server_time_ms": int(time.time() * 1000),
            }
        )

    async def _health(self, request: web.Request) -> web.Response:
        del request
        now_ms = int(time.time() * 1000)
        feed_age_ms = (
            now_ms - self.price_update_ms if self.price_update_ms is not None else None
        )
        return web.json_response(
            {
                "ok": True,
                "live_price_symbols": len(self.prices),
                "price_feed_age_ms": feed_age_ms,
            }
        )

    def _serialize(self, row: sqlite3.Row) -> dict:
        item = dict(row)
        live_price = self.prices.get(row["symbol"])
        source = "BINANCE_LIVE" if live_price is not None else "SCREENER_SNAPSHOT"
        if live_price is None:
            live_price = row["last_price"]

        item["live_price"] = live_price
        item["price_source"] = source
        item["signal_return_pct"] = (
            (live_price / row["entry_price"] - 1.0) * 100.0
        )
        decision_price = row["decision_price"]
        item["decision_return_pct"] = (
            (live_price / decision_price - 1.0) * 100.0
            if decision_price
            else None
        )
        item["research_max_upside_pct"] = max(
            (row["max_price"] / row["entry_price"] - 1.0) * 100.0,
            0.0,
        )
        item["research_max_drawdown_pct"] = max(
            (1.0 - row["min_price"] / row["entry_price"]) * 100.0,
            0.0,
        )
        item["decision_delay_seconds"] = (
            (row["decision_time_ms"] - row["signal_time_ms"]) / 1000.0
            if row["decision_time_ms"] is not None
            else None
        )
        item["tradingview_symbol"] = f"BINANCE:{row['symbol']}.P"
        return item

    async def _price_feed(self) -> None:
        reconnect_delay = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.config.binance.websocket_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=8 * 1024 * 1024,
                ) as websocket:
                    await websocket.send(
                        json.dumps(
                            {
                                "method": "SUBSCRIBE",
                                "params": ["!ticker@arr"],
                                "id": 1,
                            }
                        )
                    )
                    reconnect_delay = 1.0
                    async for raw_message in websocket:
                        payload = json.loads(raw_message)
                        data = payload.get("data", payload)
                        if not isinstance(data, list):
                            continue
                        updated = False
                        for ticker in data:
                            try:
                                self.prices[ticker["s"]] = float(ticker["c"])
                            except (KeyError, TypeError, ValueError):
                                continue
                            updated = True
                        if updated:
                            self.price_update_ms = int(time.time() * 1000)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Dashboard price feed disconnected: %s", exc)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2.0, 30.0)


async def run_positions_dashboard(
    config: AppConfig,
    host: str = "127.0.0.1",
    port: int = 8766,
) -> None:
    dashboard = PositionDashboard(config, host, port)
    await dashboard.start()
    try:
        await asyncio.Event().wait()
    finally:
        await dashboard.stop()
