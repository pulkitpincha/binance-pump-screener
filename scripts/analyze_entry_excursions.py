from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

from pump_screener.binance import BinanceClient
from pump_screener.config import load_config


IST = timezone(timedelta(hours=5, minutes=30))
ONE_MINUTE_MS = 60_000
FINAL_HORIZON_MS = 10_080 * 60_000
STOP_PCT = 10.0


def effective_close(event: dict) -> int:
    if event["status"] == "stopped" and event["completed_time_ms"]:
        return int(event["completed_time_ms"])
    return int(event["signal_time_ms"]) + FINAL_HORIZON_MS


def accepted_events(raw: list[dict]) -> tuple[list[dict], list[dict]]:
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
    upsides = [row["pre_stop_max_upside_pct"] for row in rows]
    drawdowns = [row["capped_drawdown_pct"] for row in rows]
    stopped = [row for row in rows if row["sl_hit"]]
    total_upside = sum(upsides)
    total_drawdown = sum(drawdowns)
    return {
        "entries": len(rows),
        "symbols": len({row["symbol"] for row in rows}),
        "sl_hits": len(stopped),
        "sl_rate_pct": len(stopped) / len(rows) * 100.0 if rows else 0.0,
        "total_pre_stop_upside_pct_points": total_upside,
        "total_capped_drawdown_pct_points": total_drawdown,
        "upside_to_drawdown_ratio": total_upside / total_drawdown if total_drawdown else None,
        "average_upside_pct": sum(upsides) / len(upsides) if upsides else 0.0,
        "median_upside_pct": percentile(upsides, 0.5),
        "p75_upside_pct": percentile(upsides, 0.75),
        "average_capped_drawdown_pct": sum(drawdowns) / len(drawdowns) if drawdowns else 0.0,
        "median_capped_drawdown_pct": percentile(drawdowns, 0.5),
        "reached_1pct_upside": sum(value >= 1.0 for value in upsides),
        "reached_3pct_upside": sum(value >= 3.0 for value in upsides),
        "reached_5pct_upside": sum(value >= 5.0 for value in upsides),
        "reached_10pct_upside": sum(value >= 10.0 for value in upsides),
        "reached_20pct_upside": sum(value >= 20.0 for value in upsides),
        "upside_exceeded_drawdown": sum(
            row["pre_stop_max_upside_pct"] > row["capped_drawdown_pct"]
            for row in rows
        ),
        "median_stop_minutes": percentile(
            [row["stop_minutes"] for row in stopped if row["stop_minutes"] is not None],
            0.5,
        ),
    }


async def main() -> None:
    config = load_config(Path("config.toml"))
    report_path = Path("outputs/all_trades_current_2026-08-28.md")
    header = report_path.read_text(encoding="utf-8").splitlines()[0]
    cutoff_text = header.removeprefix("Updated: ").removesuffix(" IST")
    cutoff = datetime.strptime(cutoff_text, "%d %b %Y %H:%M:%S").replace(tzinfo=IST)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    connection = sqlite3.connect(config.storage.database_path)
    connection.row_factory = sqlite3.Row
    raw = [
        dict(row)
        for row in connection.execute("SELECT * FROM events ORDER BY signal_time_ms, id")
        if int(row["signal_time_ms"]) <= cutoff_ms
    ]
    connection.close()
    accepted, rejected = accepted_events(raw)

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

    def excursion(
        event: dict,
        entry: float,
        entry_time_ms: int,
        include_signal_partial_minute: bool,
    ) -> tuple[float, float, int | None]:
        stop_price = entry * (1.0 - STOP_PCT / 100.0)
        first_full_minute = (
            (entry_time_ms + ONE_MINUTE_MS - 1) // ONE_MINUTE_MS
        ) * ONE_MINUTE_MS
        maximum = entry
        minimum = entry
        stop_time_ms: int | None = None

        if include_signal_partial_minute:
            max_time_ms = int(event["max_price_time_ms"])
            min_time_ms = int(event["min_price_time_ms"])
            if min_time_ms < first_full_minute and float(event["min_price"]) <= stop_price:
                stop_time_ms = int(event["completed_time_ms"] or min_time_ms)
                if max_time_ms <= stop_time_ms:
                    maximum = max(maximum, float(event["max_price"]))
                minimum = stop_price
            else:
                if max_time_ms < first_full_minute:
                    maximum = max(maximum, float(event["max_price"]))
                if min_time_ms < first_full_minute:
                    minimum = min(minimum, float(event["min_price"]))

        if stop_time_ms is None:
            for candle in histories[event["symbol"]]:
                if candle.open_time_ms < first_full_minute:
                    continue
                if candle.close_time_ms > cutoff_ms:
                    break
                # If low and high share a minute, assume the stop occurred first.
                if candle.low_price <= stop_price:
                    stop_time_ms = candle.close_time_ms
                    minimum = stop_price
                    break
                maximum = max(maximum, candle.high_price)
                minimum = min(minimum, candle.low_price)

        upside = max((maximum / entry - 1.0) * 100.0, 0.0)
        drawdown = (
            STOP_PCT
            if stop_time_ms is not None
            else max((1.0 - minimum / entry) * 100.0, 0.0)
        )
        return upside, drawdown, stop_time_ms

    details: list[dict] = []
    for event in accepted:
        entry = float(event["entry_price"])
        signal_ms = int(event["signal_time_ms"])
        upside, drawdown, stop_time_ms = excursion(event, entry, signal_ms, True)
        sl_hit = stop_time_ms is not None
        method = (
            "SPIKE_RVOL"
            if event["screener_type"] == "SPIKE_RVOL"
            else str(event["entry_type"])
        )
        details.append(
            {
                "event_id": event["id"],
                "symbol": event["symbol"],
                "method": method,
                "screener_type": event["screener_type"],
                "entry_type": event["entry_type"],
                "signal_time_ms": signal_ms,
                "entry_time_ist": datetime.fromtimestamp(signal_ms / 1000, IST).isoformat(),
                "return_5m_pct": float(event["return_5m_pct"]),
                "return_24h_pct": float(event["return_24h_pct"]),
                "rvol": float(event["rvol"]),
                "pre_stop_max_upside_pct": upside,
                "capped_drawdown_pct": drawdown,
                "sl_hit": sl_hit,
                "stop_time_ist": (
                    datetime.fromtimestamp(stop_time_ms / 1000, IST).isoformat()
                    if stop_time_ms is not None
                    else None
                ),
                "stop_minutes": (
                    (stop_time_ms - signal_ms) / ONE_MINUTE_MS
                    if stop_time_ms is not None
                    else None
                ),
            }
        )

    decision_details: list[dict] = []
    for event in accepted:
        if (
            event["review_status"] not in {"TRADE", "IGNORE"}
            or event["decision_time_ms"] is None
            or event["decision_price"] is None
        ):
            continue
        decision_ms = int(event["decision_time_ms"])
        entry = float(event["decision_price"])
        upside, drawdown, stop_time_ms = excursion(event, entry, decision_ms, False)
        method = (
            "SPIKE_RVOL"
            if event["screener_type"] == "SPIKE_RVOL"
            else str(event["entry_type"])
        )
        decision_details.append(
            {
                "event_id": event["id"],
                "symbol": event["symbol"],
                "method": method,
                "review_status": event["review_status"],
                "signal_time_ms": int(event["signal_time_ms"]),
                "decision_time_ms": decision_ms,
                "entry_time_ist": datetime.fromtimestamp(
                    decision_ms / 1000, IST
                ).isoformat(),
                "pre_stop_max_upside_pct": upside,
                "capped_drawdown_pct": drawdown,
                "sl_hit": stop_time_ms is not None,
                "stop_time_ist": (
                    datetime.fromtimestamp(stop_time_ms / 1000, IST).isoformat()
                    if stop_time_ms is not None
                    else None
                ),
                "stop_minutes": (
                    (stop_time_ms - decision_ms) / ONE_MINUTE_MS
                    if stop_time_ms is not None
                    else None
                ),
            }
        )

    by_method: dict[str, dict] = {}
    for method in ("SPIKE_RVOL", "INITIAL_EXPANSION", "RE_EXPANSION"):
        by_method[method] = summarize([row for row in details if row["method"] == method])

    by_decision = {
        decision: summarize(
            [row for row in decision_details if row["review_status"] == decision]
        )
        for decision in ("TRADE", "IGNORE")
    }
    by_decision_method = {
        decision: {
            method: summarize(
                [
                    row
                    for row in decision_details
                    if row["review_status"] == decision and row["method"] == method
                ]
            )
            for method in ("SPIKE_RVOL", "INITIAL_EXPANSION", "RE_EXPANSION")
        }
        for decision in ("TRADE", "IGNORE")
    }

    # Quantify method overlap: another method firing on the same coin within 5m.
    overlapping_ids: set[str] = set()
    for symbol, symbol_rows in {
        symbol: sorted(
            (row for row in details if row["symbol"] == symbol),
            key=lambda row: row["signal_time_ms"],
        )
        for symbol in {row["symbol"] for row in details}
    }.items():
        for index, left in enumerate(symbol_rows):
            for right in symbol_rows[index + 1:]:
                delta = right["signal_time_ms"] - left["signal_time_ms"]
                if delta > 5 * ONE_MINUTE_MS:
                    break
                if left["method"] != right["method"]:
                    overlapping_ids.update((left["event_id"], right["event_id"]))

    twenty_four_hour_buckets: dict[str, dict] = {}
    bucket_rules = (
        ("below_0", lambda value: value < 0),
        ("0_to_20", lambda value: 0 <= value < 20),
        ("20_to_35", lambda value: 20 <= value <= 35),
        ("above_35", lambda value: value > 35),
    )
    for label, rule in bucket_rules:
        twenty_four_hour_buckets[label] = summarize(
            [row for row in details if rule(row["return_24h_pct"])]
        )

    output = {
        "as_of_ist": cutoff.isoformat(),
        "accepted": len(accepted),
        "duplicates_omitted": len(rejected),
        "overall": summarize(details),
        "by_method": by_method,
        "by_decision": by_decision,
        "by_decision_method": by_decision_method,
        "twenty_four_hour_buckets": twenty_four_hour_buckets,
        "cross_method_entries_within_5m": len(overlapping_ids),
        "review_status_counts": dict(Counter(event["review_status"] for event in accepted)),
        "details": details,
        "decision_details": decision_details,
    }
    output_path = Path(f"outputs/entry_excursion_analysis_{cutoff:%Y-%m-%d}.json")
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                key: value
                for key, value in output.items()
                if key not in {"details", "decision_details"}
            },
            indent=2,
        )
    )
    print(f"OUTPUT={output_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
