from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time

from pump_screener.binance import BinanceClient
from pump_screener.config import load_config


IST = timezone(timedelta(hours=5, minutes=30))
ONE_MINUTE_MS = 60_000
FINAL_HORIZON_MS = 10_080 * 60_000
INITIAL_MARGIN = 10.0
LEVERAGE = 10.0
TARGET_UPSIDE = 0.03
HARD_STOP_DOWNSIDE = 0.10
TRAILING_DOWNSIDE = 0.10
POSITION_NOTIONAL = INITIAL_MARGIN * LEVERAGE
PARTIAL_CLOSE_FRACTION = 1.0 / (1.0 + LEVERAGE * TARGET_UPSIDE)
RUNNER_FRACTION = 1.0 - PARTIAL_CLOSE_FRACTION


def effective_close(event: dict) -> int:
    if event["status"] == "stopped" and event["completed_time_ms"]:
        return int(event["completed_time_ms"])
    return int(event["signal_time_ms"]) + FINAL_HORIZON_MS


def deduplicate(raw: list[dict]) -> tuple[list[dict], list[dict]]:
    accepted: list[dict] = []
    rejected: list[dict] = []
    latest: dict[tuple[str, str, str], dict] = {}
    for event in raw:
        key = (event["symbol"], event["screener_type"], event["entry_type"])
        prior = latest.get(key)
        if prior is not None and int(event["signal_time_ms"]) < effective_close(prior):
            rejected.append(event)
        else:
            accepted.append(event)
            latest[key] = event
    return accepted, rejected


def method_name(event: dict) -> str:
    if event["screener_type"] == "SPIKE_RVOL":
        return "SPIKE_RVOL"
    return str(event["entry_type"])


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * p
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def summarize(rows: list[dict]) -> dict:
    pnls = [row["pnl_usd"] for row in rows]
    positive = [value for value in pnls if value > 0]
    negative = [value for value in pnls if value < 0]
    total_margin = len(rows) * INITIAL_MARGIN
    total_pnl = sum(pnls)
    return {
        "entries": len(rows),
        "symbols": len({row["symbol"] for row in rows}),
        "initial_margin_usd": total_margin,
        "ending_equity_usd": total_margin + total_pnl,
        "total_pnl_usd": total_pnl,
        "roi_pct": total_pnl / total_margin * 100.0 if total_margin else 0.0,
        "expectancy_usd_per_trade": total_pnl / len(rows) if rows else 0.0,
        "median_pnl_usd": percentile(pnls, 0.5),
        "profitable": len(positive),
        "losing": len(negative),
        "flat": len(rows) - len(positive) - len(negative),
        "profitable_rate_pct": len(positive) / len(rows) * 100.0 if rows else 0.0,
        "profit_factor": sum(positive) / abs(sum(negative)) if negative else None,
        "target_triggered": sum(row["target_triggered"] for row in rows),
        "hard_stops": sum(row["outcome"] == "HARD_STOP" for row in rows),
        "trailing_stops": sum(row["outcome"] == "TRAIL_STOP" for row in rows),
        "open_runners": sum(row["outcome"] == "OPEN_RUNNER" for row in rows),
        "open_untriggered": sum(row["outcome"] == "OPEN_UNTRIGGERED" for row in rows),
        "best_pnl_usd": max(pnls) if pnls else 0.0,
        "worst_pnl_usd": min(pnls) if pnls else 0.0,
    }


async def main() -> None:
    config = load_config(Path("config.toml"))

    # Use the most recent fully closed one-minute candle as a common cutoff.
    cutoff_ms = (int(time.time() * 1000) // ONE_MINUTE_MS) * ONE_MINUTE_MS - 1

    connection = sqlite3.connect(config.storage.database_path)
    connection.row_factory = sqlite3.Row
    raw = [
        dict(row)
        for row in connection.execute("SELECT * FROM events ORDER BY signal_time_ms, id")
        if int(row["signal_time_ms"]) <= cutoff_ms
    ]
    connection.close()
    accepted, rejected = deduplicate(raw)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for event in accepted:
        grouped[event["symbol"]].append(event)

    client = BinanceClient(config.binance)
    histories: dict[str, list] = {}

    async def fetch_symbol(symbol: str, events: list[dict]) -> None:
        earliest = min(int(event["signal_time_ms"]) for event in events)
        start_ms = (earliest // ONE_MINUTE_MS) * ONE_MINUTE_MS
        histories[symbol] = await client.historical_klines_between(
            symbol, start_ms, cutoff_ms, "1m"
        )

    results = await asyncio.gather(
        *(fetch_symbol(symbol, events) for symbol, events in grouped.items()),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, Exception)]
    if failures:
        raise RuntimeError(f"Historical failures={len(failures)} first={failures[0]}")

    simulations: list[dict] = []
    for event in accepted:
        entry = float(event["entry_price"])
        entry_time_ms = int(event["signal_time_ms"])
        first_full_minute = (
            (entry_time_ms + ONE_MINUTE_MS - 1) // ONE_MINUTE_MS
        ) * ONE_MINUTE_MS
        target_price = entry * (1.0 + TARGET_UPSIDE)
        hard_stop_price = entry * (1.0 - HARD_STOP_DOWNSIDE)
        triggered = False
        peak = entry
        outcome = "OPEN_UNTRIGGERED"
        exit_price: float | None = None
        exit_time_ms: int | None = None

        # Resolve the partial signal minute from live-tracker extrema when their
        # timestamps prove that they belong to that minute. Low-first ordering is
        # deliberately conservative for a long trade.
        partial_low = (
            float(event["min_price"])
            if int(event["min_price_time_ms"]) < first_full_minute
            else entry
        )
        partial_high = (
            float(event["max_price"])
            if int(event["max_price_time_ms"]) < first_full_minute
            else entry
        )
        if partial_low <= hard_stop_price:
            outcome = "HARD_STOP"
            exit_price = hard_stop_price
            exit_time_ms = int(event["min_price_time_ms"])
        elif partial_high >= target_price:
            triggered = True
            peak = max(target_price, partial_high)
            outcome = "OPEN_RUNNER"

        relevant = [
            candle
            for candle in histories[event["symbol"]]
            if candle.open_time_ms >= first_full_minute
            and candle.close_time_ms <= cutoff_ms
        ]
        last_price = relevant[-1].close_price if relevant else float(event["last_price"])

        if exit_price is None:
            for candle in relevant:
                if not triggered:
                    # Low-first ordering: if target and hard stop share a candle,
                    # the hard stop wins.
                    if candle.low_price <= hard_stop_price:
                        outcome = "HARD_STOP"
                        exit_price = hard_stop_price
                        exit_time_ms = candle.close_time_ms
                        break
                    if candle.high_price >= target_price:
                        triggered = True
                        peak = max(target_price, candle.high_price)
                        outcome = "OPEN_RUNNER"
                    continue

                # Use the trail established by prior candles, then update the peak.
                # This avoids crediting an unknowable high-before-low sequence.
                trailing_price = peak * (1.0 - TRAILING_DOWNSIDE)
                if candle.low_price <= trailing_price:
                    outcome = "TRAIL_STOP"
                    exit_price = trailing_price
                    exit_time_ms = candle.close_time_ms
                    break
                peak = max(peak, candle.high_price)

        if outcome == "HARD_STOP":
            pnl = -INITIAL_MARGIN
        elif triggered:
            runner_exit = exit_price if exit_price is not None else last_price
            partial_realized_pnl = (
                POSITION_NOTIONAL * PARTIAL_CLOSE_FRACTION * TARGET_UPSIDE
            )
            runner_pnl = (
                POSITION_NOTIONAL
                * RUNNER_FRACTION
                * (runner_exit / entry - 1.0)
            )
            pnl = partial_realized_pnl + runner_pnl
        else:
            pnl = POSITION_NOTIONAL * (last_price / entry - 1.0)

        simulations.append(
            {
                "event_id": event["id"],
                "symbol": event["symbol"],
                "method": method_name(event),
                "entry_time_ist": datetime.fromtimestamp(
                    entry_time_ms / 1000, IST
                ).isoformat(),
                "entry_price": entry,
                "target_triggered": triggered,
                "peak_price": peak,
                "outcome": outcome,
                "exit_time_ist": (
                    datetime.fromtimestamp(exit_time_ms / 1000, IST).isoformat()
                    if exit_time_ms is not None
                    else None
                ),
                "exit_or_mark_price": exit_price if exit_price is not None else last_price,
                "pnl_usd": pnl,
                "return_on_margin_pct": pnl / INITIAL_MARGIN * 100.0,
            }
        )

    by_method = {
        method: summarize([row for row in simulations if row["method"] == method])
        for method in ("SPIKE_RVOL", "INITIAL_EXPANSION", "RE_EXPANSION")
    }
    output = {
        "as_of_ist": datetime.fromtimestamp(cutoff_ms / 1000, IST).isoformat(),
        "assumptions": {
            "initial_margin_usd": INITIAL_MARGIN,
            "leverage": LEVERAGE,
            "position_notional_usd": POSITION_NOTIONAL,
            "initial_hard_stop_underlying_pct": HARD_STOP_DOWNSIDE * 100.0,
            "partial_trigger_underlying_pct": TARGET_UPSIDE * 100.0,
            "partial_close_fraction_pct": PARTIAL_CLOSE_FRACTION * 100.0,
            "runner_fraction_pct": RUNNER_FRACTION * 100.0,
            "cash_recovered_at_trigger_usd": INITIAL_MARGIN,
            "runner_trail_below_peak_pct": TRAILING_DOWNSIDE * 100.0,
            "fees_funding_slippage_included": False,
            "unresolved_positions": "marked to final one-minute close",
            "within_candle_ordering": "low before high",
        },
        "accepted": len(accepted),
        "duplicates_omitted": len(rejected),
        "overall": summarize(simulations),
        "by_method": by_method,
        "details": simulations,
    }
    output_path = Path(
        f"outputs/initials_3pct_trailing_10pct_{datetime.fromtimestamp(cutoff_ms / 1000, IST):%Y-%m-%d}.json"
    )
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {key: value for key, value in output.items() if key != "details"},
            indent=2,
        )
    )
    print(f"OUTPUT={output_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
