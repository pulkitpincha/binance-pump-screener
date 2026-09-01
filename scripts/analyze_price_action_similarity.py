from __future__ import annotations

import asyncio
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime
import json
import math
from pathlib import Path
import sqlite3
import statistics

from pump_screener.binance import BinanceClient
from pump_screener.config import load_config


MINUTE_MS = 60_000
LOOKBACK_MINUTES = 24 * 60


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def pct_change(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old else 0.0


def range_pct(candles: list) -> float | None:
    if not candles:
        return None
    low = min(item.low_price for item in candles)
    high = max(item.high_price for item in candles)
    return pct_change(high, low) if low else None


def sum_volume(candles: list) -> float:
    return sum(item.quote_volume for item in candles)


def taker_ratio(candles: list) -> float | None:
    total = sum_volume(candles)
    if total <= 0:
        return None
    return sum(item.taker_buy_quote_volume for item in candles) / total


def cliff_delta(left: list[float], right: list[float]) -> float | None:
    if not left or not right:
        return None
    greater = 0
    lower = 0
    for first in left:
        for second in right:
            greater += first > second
            lower += first < second
    return (greater - lower) / (len(left) * len(right))


def rolling_move(
    candles: list,
    entry_price: float,
    signal_time_ms: int,
    horizon_minutes: int,
    search_minutes: int,
) -> tuple[float | None, float | None, float | None, float | None]:
    points = [
        (item.close_time_ms, item.close_price)
        for item in candles
        if item.close_time_ms >= signal_time_ms - search_minutes * MINUTE_MS
    ]
    points.append((signal_time_ms, entry_price))
    if len(points) <= horizon_minutes:
        return None, None, None, None
    best: tuple[float, int] | None = None
    worst: tuple[float, int] | None = None
    for index in range(horizon_minutes, len(points)):
        move = pct_change(points[index][1], points[index - horizon_minutes][1])
        if best is None or move > best[0]:
            best = (move, points[index][0])
        if worst is None or move < worst[0]:
            worst = (move, points[index][0])
    return (
        best[0] if best else None,
        (signal_time_ms - best[1]) / MINUTE_MS if best else None,
        worst[0] if worst else None,
        (signal_time_ms - worst[1]) / MINUTE_MS if worst else None,
    )


def swing_highs(candles: list, radius: int = 2) -> list[float]:
    values: list[float] = []
    for index in range(radius, len(candles) - radius):
        candidate = candles[index].high_price
        neighbors = candles[index - radius : index + radius + 1]
        if candidate >= max(item.high_price for item in neighbors):
            values.append(candidate)
    return values


def max_equal_high_touches(prices: list[float], tolerance_pct: float = 0.15) -> int:
    if not prices:
        return 0
    ordered = sorted(prices)
    maximum = 1
    left = 0
    for right, value in enumerate(ordered):
        while left < right and pct_change(value, ordered[left]) > tolerance_pct:
            left += 1
        maximum = max(maximum, right - left + 1)
    return maximum


def event_features(event: dict, candles: list) -> dict:
    signal_ms = int(event["signal_time_ms"])
    entry = float(event["entry_price"])
    visible = [item for item in candles if item.close_time_ms <= signal_ms]
    prior = visible
    close_times = [item.close_time_ms for item in visible]

    def prior_close(minutes: int) -> float | None:
        index = bisect_right(close_times, signal_ms - minutes * MINUTE_MS) - 1
        return visible[index].close_price if index >= 0 else None

    def window(minutes: int, offset: int = 0) -> list:
        end = signal_ms - offset * MINUTE_MS
        start = end - minutes * MINUTE_MS
        return [item for item in prior if start < item.close_time_ms <= end]

    features: dict[str, float | int | bool | None] = {}
    for minutes in (15, 30, 60, 240, 720, 1440):
        old = prior_close(minutes)
        features[f"return_{minutes}m_pct"] = pct_change(entry, old) if old else None

    for minutes in (15, 60, 240, 1440):
        items = window(minutes)
        if items:
            high = max(item.high_price for item in items)
            low = min(item.low_price for item in items)
            features[f"below_{minutes}m_high_pct"] = max(
                (high - entry) / high * 100.0, 0.0
            )
            features[f"above_{minutes}m_low_pct"] = max(
                (entry - low) / low * 100.0, 0.0
            )
            features[f"position_in_{minutes}m_range"] = (
                (entry - low) / (high - low) if high > low else 0.5
            )
            features[f"range_{minutes}m_pct"] = pct_change(high, low)
            features[f"breakout_{minutes}m_high"] = entry >= high
        else:
            for name in ("below", "above", "position_in", "range"):
                suffix = "_pct" if name != "position_in" else ""
                features[f"{name}_{minutes}m_{'high' if name == 'below' else 'low'}{suffix}"] = None
            features[f"breakout_{minutes}m_high"] = None

    last_15 = window(15)
    preceding_60 = window(60, 15)
    range_15 = range_pct(last_15)
    prior_range_60 = range_pct(preceding_60)
    features["compression_15m_vs_prior60m"] = (
        range_15 / prior_range_60
        if range_15 is not None and prior_range_60 and prior_range_60 > 0
        else None
    )
    one_minute_returns = [
        abs(pct_change(item.close_price, item.open_price)) for item in window(60)
    ]
    features["median_abs_1m_return_60m_pct"] = median(one_minute_returns)

    recent_5 = window(5)
    recent_15 = window(15)
    baseline = window(240, 5)
    baseline_blocks = [
        sum_volume(baseline[index : index + 5])
        for index in range(0, len(baseline) - 4, 5)
    ]
    baseline_median = median(baseline_blocks)
    features["volume_5m_vs_prior4h_median"] = (
        sum_volume(recent_5) / baseline_median
        if baseline_median and baseline_median > 0
        else None
    )
    features["taker_buy_ratio_5m"] = taker_ratio(recent_5)
    features["taker_buy_ratio_15m"] = taker_ratio(recent_15)

    latest = visible[-1] if visible else None
    if latest:
        candle_range = latest.high_price - latest.low_price
        features["signal_candle_body_pct"] = abs(
            pct_change(latest.close_price, latest.open_price)
        )
        features["signal_candle_close_location"] = (
            (latest.close_price - latest.low_price) / candle_range
            if candle_range > 0
            else 0.5
        )
        features["signal_candle_upper_wick_share"] = (
            (latest.high_price - max(latest.open_price, latest.close_price))
            / candle_range
            if candle_range > 0
            else 0.0
        )
        features["signal_candle_lower_wick_share"] = (
            (min(latest.open_price, latest.close_price) - latest.low_price)
            / candle_range
            if candle_range > 0
            else 0.0
        )

    pump_15, pump_age, dump_15, dump_age = rolling_move(
        visible, entry, signal_ms, 15, 240
    )
    features["max_15m_pump_prior4h_pct"] = pump_15
    features["minutes_since_max_15m_pump"] = pump_age
    features["max_15m_dump_prior4h_pct"] = dump_15
    features["minutes_since_max_15m_dump"] = dump_age

    prior_24h = window(1440)
    highs = swing_highs(prior_24h)
    overhead = [price for price in highs if price > entry]
    distances = sorted(pct_change(price, entry) for price in overhead)
    features["nearest_swing_high_above_pct"] = distances[0] if distances else None
    for threshold in (0.5, 1.0, 2.0):
        key = str(threshold).replace(".", "_")
        candidates = [price for price in overhead if pct_change(price, entry) <= threshold]
        features[f"swing_highs_above_within_{key}pct"] = len(candidates)
        features[f"overhead_equal_high_touches_within_{key}pct"] = (
            max_equal_high_touches(candidates)
        )
    features["no_prior_swing_high_above_24h"] = not overhead

    features["log10_quote_volume_24h"] = (
        math.log10(float(event["quote_volume_24h"]))
        if float(event["quote_volume_24h"]) > 0
        else None
    )
    if event.get("avwap_at_entry"):
        features["entry_above_avwap_pct"] = pct_change(
            entry, float(event["avwap_at_entry"])
        )
    else:
        features["entry_above_avwap_pct"] = None
    if event.get("upper_band_at_entry"):
        features["entry_above_upper_band_pct"] = pct_change(
            entry, float(event["upper_band_at_entry"])
        )
    else:
        features["entry_above_upper_band_pct"] = None
    if event.get("avwap_anchor_time_ms"):
        features["avwap_anchor_age_minutes"] = (
            signal_ms - int(event["avwap_anchor_time_ms"])
        ) / MINUTE_MS
    else:
        features["avwap_anchor_age_minutes"] = None
    return features


def numeric_comparisons(top: list[dict], rest: list[dict]) -> list[dict]:
    keys = sorted(top[0]["features"])
    rows: list[dict] = []
    for key in keys:
        left = [
            float(item["features"][key])
            for item in top
            if isinstance(item["features"].get(key), (int, float))
            and not isinstance(item["features"].get(key), bool)
        ]
        right = [
            float(item["features"][key])
            for item in rest
            if isinstance(item["features"].get(key), (int, float))
            and not isinstance(item["features"].get(key), bool)
        ]
        if len(left) < 5 or len(right) < 5:
            continue
        rows.append(
            {
                "feature": key,
                "top_median": median(left),
                "rest_median": median(right),
                "cliff_delta": cliff_delta(left, right),
                "top_n": len(left),
                "rest_n": len(right),
            }
        )
    return sorted(rows, key=lambda item: abs(item["cliff_delta"] or 0), reverse=True)


def categorical_comparisons(top: list[dict], rest: list[dict]) -> list[dict]:
    rules = {
        "15m pump >= 3% in prior 4h": lambda f: (f.get("max_15m_pump_prior4h_pct") or -999) >= 3,
        "15m pump >= 5% in prior 4h": lambda f: (f.get("max_15m_pump_prior4h_pct") or -999) >= 5,
        "15m dump <= -3% in prior 4h": lambda f: (f.get("max_15m_dump_prior4h_pct") or 999) <= -3,
        "within 0.5% of 4h high": lambda f: (f.get("below_240m_high_pct") or 0) <= 0.5,
        "within 1% of 4h high": lambda f: (f.get("below_240m_high_pct") or 0) <= 1,
        "within 2% of 24h high": lambda f: (f.get("below_1440m_high_pct") or 0) <= 2,
        "swing high within 0.5% above": lambda f: (f.get("swing_highs_above_within_0_5pct") or 0) > 0,
        "swing high within 1% above": lambda f: (f.get("swing_highs_above_within_1_0pct") or 0) > 0,
        "swing high within 2% above": lambda f: (f.get("swing_highs_above_within_2_0pct") or 0) > 0,
        "2+ equal-high touches within 1% above": lambda f: (f.get("overhead_equal_high_touches_within_1_0pct") or 0) >= 2,
        "no prior 24h swing high above": lambda f: bool(f.get("no_prior_swing_high_above_24h")),
        "5m volume >= 2x prior median": lambda f: (f.get("volume_5m_vs_prior4h_median") or 0) >= 2,
        "5m volume >= 3x prior median": lambda f: (f.get("volume_5m_vs_prior4h_median") or 0) >= 3,
        "5m taker-buy ratio >= 55%": lambda f: (f.get("taker_buy_ratio_5m") or 0) >= 0.55,
        "5m taker-buy ratio >= 60%": lambda f: (f.get("taker_buy_ratio_5m") or 0) >= 0.60,
        "15m range compressed vs prior hour": lambda f: (f.get("compression_15m_vs_prior60m") or 999) <= 0.5,
    }
    comparisons = []
    for label, rule in rules.items():
        top_hits = sum(rule(item["features"]) for item in top)
        rest_hits = sum(rule(item["features"]) for item in rest)
        top_rate = top_hits / len(top) * 100.0
        rest_rate = rest_hits / len(rest) * 100.0
        comparisons.append(
            {
                "feature": label,
                "top_hits": top_hits,
                "top_rate_pct": top_rate,
                "rest_hits": rest_hits,
                "rest_rate_pct": rest_rate,
                "rate_difference_pct_points": top_rate - rest_rate,
            }
        )
    return sorted(
        comparisons,
        key=lambda item: abs(item["rate_difference_pct_points"]),
        reverse=True,
    )


async def main() -> None:
    config = load_config(Path("config.toml"))
    analysis_path = Path("outputs/entry_excursion_analysis_2026-08-29.json")
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    accepted = analysis["details"]
    ids = {item["event_id"] for item in accepted}

    connection = sqlite3.connect(config.storage.database_path)
    connection.row_factory = sqlite3.Row
    raw = {
        row["id"]: dict(row)
        for row in connection.execute("SELECT * FROM events")
        if row["id"] in ids
    }
    connection.close()

    grouped: dict[str, list[dict]] = defaultdict(list)
    outcomes = {item["event_id"]: item for item in accepted}
    for event_id in ids:
        event = raw[event_id]
        event.update(outcomes[event_id])
        grouped[event["symbol"]].append(event)

    client = BinanceClient(config.binance)
    histories: dict[str, list] = {}

    async def fetch(symbol: str, events: list[dict]) -> None:
        start_ms = min(int(item["signal_time_ms"]) for item in events)
        start_ms -= LOOKBACK_MINUTES * MINUTE_MS
        end_ms = max(int(item["signal_time_ms"]) for item in events)
        histories[symbol] = await client.historical_klines_between(
            symbol, start_ms, end_ms, "1m"
        )

    results = await asyncio.gather(
        *(fetch(symbol, events) for symbol, events in grouped.items()),
        return_exceptions=True,
    )
    failures = [item for item in results if isinstance(item, Exception)]
    if failures:
        raise RuntimeError(f"Historical failures={len(failures)} first={failures[0]}")

    rows: list[dict] = []
    for symbol, events in grouped.items():
        for event in events:
            rows.append(
                {
                    "event_id": event["id"],
                    "symbol": symbol,
                    "method": event["method"],
                    "signal_time_ms": event["signal_time_ms"],
                    "entry_time_ist": event["entry_time_ist"],
                    "pre_stop_max_upside_pct": event["pre_stop_max_upside_pct"],
                    "capped_drawdown_pct": event["capped_drawdown_pct"],
                    "sl_hit": event["sl_hit"],
                    "features": event_features(event, histories[symbol]),
                }
            )

    rows.sort(key=lambda item: item["pre_stop_max_upside_pct"], reverse=True)
    top_count = math.ceil(len(rows) * 0.25)
    top = rows[:top_count]
    rest = rows[top_count:]
    output = {
        "as_of_ist": analysis["as_of_ist"],
        "entries": len(rows),
        "top_count": top_count,
        "top_cutoff_upside_pct": top[-1]["pre_stop_max_upside_pct"],
        "numeric_comparisons": numeric_comparisons(top, rest),
        "categorical_comparisons": categorical_comparisons(top, rest),
        "rows": rows,
    }
    date = datetime.fromisoformat(analysis["as_of_ist"]).date()
    output_path = Path(f"outputs/price_action_similarity_{date}.json")
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "entries": output["entries"],
                "top_count": output["top_count"],
                "top_cutoff_upside_pct": output["top_cutoff_upside_pct"],
                "strongest_numeric": output["numeric_comparisons"][:15],
                "strongest_categorical": output["categorical_comparisons"],
                "output": str(output_path.resolve()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
