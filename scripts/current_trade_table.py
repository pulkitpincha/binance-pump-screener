from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import time

import aiohttp

from pump_screener.binance import BinanceClient
from pump_screener.config import load_config


IST = timezone(timedelta(hours=5, minutes=30))
ONE_MINUTE_MS = 60_000
FINAL_HORIZON_MS = 10_080 * 60_000


def effective_close(event: dict) -> int:
    if event["status"] == "stopped" and event["completed_time_ms"]:
        return event["completed_time_ms"]
    return event["signal_time_ms"] + FINAL_HORIZON_MS


def holding_label(total_seconds: int) -> str:
    days, remainder = divmod(total_seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m"
    return f"{hours}h {minutes:02d}m"


async def main() -> None:
    config = load_config(Path("config.toml"))
    connection = sqlite3.connect(config.storage.database_path)
    connection.row_factory = sqlite3.Row
    raw = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM events ORDER BY signal_time_ms, id"
        )
    ]
    connection.close()

    accepted: list[dict] = []
    rejected: list[dict] = []
    latest: dict[tuple[str, str, str], dict] = {}
    for event in raw:
        key = (event["symbol"], event["screener_type"], event["entry_type"])
        prior = latest.get(key)
        if prior is not None and event["signal_time_ms"] < effective_close(prior):
            rejected.append(event)
        else:
            accepted.append(event)
            latest[key] = event

    now_ms = int(time.time() * 1000)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for event in accepted:
        grouped[event["symbol"]].append(event)

    client = BinanceClient(config.binance)
    histories: dict[str, list] = {}

    async def fetch_symbol(symbol: str, events: list[dict]) -> None:
        earliest = min(event["signal_time_ms"] for event in events)
        start_ms = (earliest // ONE_MINUTE_MS) * ONE_MINUTE_MS
        histories[symbol] = await client.historical_klines_between(
            symbol, start_ms, now_ms, "1m"
        )

    results = await asyncio.gather(
        *(fetch_symbol(symbol, events) for symbol, events in grouped.items()),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, Exception)]
    if failures:
        raise RuntimeError(
            f"Historical failures={len(failures)} first={failures[0]}"
        )

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        url = f"{config.binance.rest_base_url}/fapi/v1/ticker/price"
        async with session.get(url) as response:
            response.raise_for_status()
            ticker_payload = await response.json()
    current_prices = {
        item["symbol"]: float(item["price"]) for item in ticker_payload
    }

    def performance_row(
        event: dict,
        entry_price: float,
        entry_time_ms: int,
        include_research_extrema: bool,
    ) -> list[str]:
        current_price = current_prices[event["symbol"]]
        stop_price = entry_price * 0.90
        first_full_candle = (
            (entry_time_ms + ONE_MINUTE_MS - 1) // ONE_MINUTE_MS
        ) * ONE_MINUTE_MS
        candles = [
            candle
            for candle in histories[event["symbol"]]
            if candle.open_time_ms >= first_full_candle
        ]
        maximum = entry_price
        minimum = entry_price
        stop_time_ms: int | None = None

        # The live tracker can safely fill the otherwise unresolved partial
        # signal minute because its extrema carry actual event timestamps.
        if include_research_extrema:
            max_time_ms = int(event["max_price_time_ms"])
            min_time_ms = int(event["min_price_time_ms"])
            partial_stop = (
                min_time_ms < first_full_candle
                and float(event["min_price"]) <= stop_price
            )
            if partial_stop:
                stop_time_ms = int(event["completed_time_ms"] or min_time_ms)
                if max_time_ms <= stop_time_ms:
                    maximum = max(maximum, float(event["max_price"]))
                minimum = stop_price
            else:
                if max_time_ms < first_full_candle:
                    maximum = max(maximum, float(event["max_price"]))
                if min_time_ms < first_full_candle:
                    minimum = min(minimum, float(event["min_price"]))

        if stop_time_ms is None:
            for candle in candles:
                # If the stop and high share a one-minute candle, assume the
                # stop occurred first because OHLC cannot prove otherwise.
                if candle.low_price <= stop_price:
                    stop_time_ms = candle.close_time_ms
                    minimum = stop_price
                    break
                maximum = max(maximum, candle.high_price)
                minimum = min(minimum, candle.low_price)

        if stop_time_ms is None and current_price <= stop_price:
            stop_time_ms = now_ms
            minimum = stop_price

        max_upside = max((maximum / entry_price - 1.0) * 100.0, 0.0)
        max_drawdown = (
            10.0
            if stop_time_ms is not None
            else max((1.0 - minimum / entry_price) * 100.0, 0.0)
        )
        current_return = (current_price / entry_price - 1.0) * 100.0
        holding_end_ms = stop_time_ms if stop_time_ms is not None else now_ms
        held_seconds = max(0, (holding_end_ms - entry_time_ms) // 1000)
        return [
            event["symbol"],
            f"{max_drawdown:.2f}%",
            f"{max_upside:.2f}%",
            f"{current_return:+.2f}%",
            holding_label(held_seconds),
            "YES" if stop_time_ms is not None else "NO",
            datetime.fromtimestamp(entry_time_ms / 1000, IST).strftime(
                "%d %b %Y %H:%M:%S"
            ),
        ]

    table_rows = [
        performance_row(
            event,
            float(event["entry_price"]),
            int(event["signal_time_ms"]),
            True,
        )
        for event in accepted
    ]

    selected_rows: dict[str, list[list[str]]] = {"TRADE": [], "IGNORE": []}
    for decision in selected_rows:
        selected = [
            event
            for event in accepted
            if event["review_status"] == decision
            and event["decision_time_ms"] is not None
            and event["decision_price"] is not None
        ]
        selected_rows[decision] = [
            performance_row(
                event,
                float(event["decision_price"]),
                int(event["decision_time_ms"]),
                False,
            )
            for event in selected
        ]

    output = Path("outputs/all_trades_current_2026-08-28.md")
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"Updated: {datetime.fromtimestamp(now_ms / 1000, IST).strftime('%d %b %Y %H:%M:%S IST')}",
        "",
        f"## All screener entries ({len(table_rows)})",
        "",
        "| Ticker | Max drawdown | Max upside | Current | Holding | SL hit | Trade taken (IST) |",
        "|---|---:|---:|---:|---:|:---:|---|",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in table_rows)
    for decision, rows in selected_rows.items():
        lines.extend(
            [
                "",
                f"## Marked {decision} ({len(rows)})",
                "",
                "Performance below is measured from your decision price and time.",
                "",
                "| Ticker | Max drawdown | Max upside | Current | Holding | SL hit | Decision taken (IST) |",
                "|---|---:|---:|---:|---:|:---:|---|",
            ]
        )
        lines.extend("| " + " | ".join(row) + " |" for row in rows)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        f"OUTPUT={output.resolve()} VALID={len(accepted)} "
        f"DUPLICATES_OMITTED={len(rejected)} "
        f"TRADE={len(selected_rows['TRADE'])} IGNORE={len(selected_rows['IGNORE'])}"
    )


if __name__ == "__main__":
    asyncio.run(main())
