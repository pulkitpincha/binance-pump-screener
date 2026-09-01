from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
import logging
import time

from .binance import BinanceClient
from .models import ActiveEvent, KlineUpdate
from .storage import Store


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackfillSummary:
    events_checked: int = 0
    events_updated: int = 0
    events_stopped: int = 0
    symbols_failed: int = 0


async def backfill_missing_history(
    store: Store,
    client: BinanceClient,
    horizons_minutes: tuple[int, ...],
    live_monitoring_minutes: int,
    max_drawdown_pct: float,
    available_symbols: set[str] | None = None,
    now_ms: int | None = None,
) -> BackfillSummary:
    """Catch up compact paper outcomes; downloaded candles remain transient."""
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    final_horizon = max(horizons_minutes)
    events = store.load_backfill_events(final_horizon)
    if available_symbols is not None:
        events = [event for event in events if event.symbol in available_symbols]
    if not events:
        return BackfillSummary()

    grouped: dict[str, list[ActiveEvent]] = defaultdict(list)
    for event in events:
        due_ms = event.signal_time_ms + final_horizon * 60_000
        if event.last_seen_ms < min(now_ms, due_ms):
            # Until the missing range is successfully reconstructed, keep this
            # research position logically open but out of the live tick tracker.
            store.defer_event(event, event.last_seen_ms)
            grouped[event.symbol].append(event)
    store.commit()

    semaphore = asyncio.Semaphore(5)

    async def process_symbol(symbol: str, symbol_events: list[ActiveEvent]) -> tuple[int, int, bool]:
        start_ms = min(event.last_seen_ms for event in symbol_events) - 300_000
        end_ms = min(
            now_ms,
            max(
                event.signal_time_ms + final_horizon * 60_000 + 300_000
                for event in symbol_events
            ),
        )
        try:
            async with semaphore:
                five_minute = await client.historical_klines_between(
                    symbol, start_ms, end_ms, "5m"
                )
        except Exception as exc:
            LOGGER.warning("Historical backfill failed for %s: %s", symbol, exc)
            return 0, 0, True

        one_minute_cache: dict[int, list[KlineUpdate]] = {}
        updated = 0
        stopped = 0
        for event in symbol_events:
            previous_last_seen_ms = event.last_seen_ms
            event_end_ms = min(
                now_ms, event.signal_time_ms + final_horizon * 60_000
            )
            relevant = [
                candle
                for candle in five_minute
                if candle.close_time_ms > event.last_seen_ms
                and candle.open_time_ms <= event_end_ms
            ]
            expanded: list[KlineUpdate] = []
            stop_price = event.entry_price * (1.0 - max_drawdown_pct / 100.0)
            for candle in relevant:
                partial = candle.open_time_ms < event.last_seen_ms < candle.close_time_ms
                touches_stop = candle.low_price <= stop_price
                crosses_missing_horizon = any(
                    horizon not in event.completed_horizons
                    and candle.open_time_ms
                    < event.signal_time_ms + horizon * 60_000
                    <= candle.close_time_ms
                    for horizon in horizons_minutes
                )
                if partial or touches_stop or crosses_missing_horizon:
                    cache_key = candle.open_time_ms
                    minute_rows = one_minute_cache.get(cache_key)
                    if minute_rows is None:
                        try:
                            async with semaphore:
                                minute_rows = await client.historical_klines_between(
                                    symbol,
                                    candle.open_time_ms,
                                    candle.close_time_ms,
                                    "1m",
                                )
                        except Exception as exc:
                            LOGGER.warning(
                                "One-minute refinement failed for %s at %d: %s",
                                symbol,
                                candle.open_time_ms,
                                exc,
                            )
                            minute_rows = []
                        one_minute_cache[cache_key] = minute_rows
                    if minute_rows:
                        expanded.extend(minute_rows)
                    else:
                        expanded.append(candle)
                    # Once the first possible stop candle is included, later
                    # candles cannot affect this position and need no requests.
                    if touches_stop:
                        break
                    continue
                expanded.append(candle)

            was_stopped = apply_historical_candles(
                store,
                event,
                sorted(expanded, key=lambda item: item.close_time_ms),
                horizons_minutes,
                live_monitoring_minutes,
                max_drawdown_pct,
                event_end_ms,
            )
            updated += int(event.last_seen_ms > previous_last_seen_ms)
            stopped += int(was_stopped)
        store.commit()
        return updated, stopped, False

    results = await asyncio.gather(
        *(process_symbol(symbol, items) for symbol, items in grouped.items())
    )
    updated = sum(item[0] for item in results)
    stopped = sum(item[1] for item in results)
    failures = sum(item[2] for item in results)
    return BackfillSummary(len(events), updated, stopped, failures)


def apply_historical_candles(
    store: Store,
    event: ActiveEvent,
    candles: list[KlineUpdate],
    horizons_minutes: tuple[int, ...],
    live_monitoring_minutes: int,
    max_drawdown_pct: float,
    end_time_ms: int,
) -> bool:
    """Apply OHLC conservatively and persist summaries, never candle rows."""
    stop_price = event.entry_price * (1.0 - max_drawdown_pct / 100.0)
    stopped = False
    for candle in candles:
        if candle.close_time_ms <= event.last_seen_ms:
            continue
        if candle.open_time_ms > end_time_ms:
            break

        # If low and high occur in the same unresolved candle, assume the stop
        # happens first. This avoids crediting upside that may have occurred later.
        if candle.low_price <= stop_price:
            if candle.low_price < event.min_price:
                event.min_price = candle.low_price
                event.min_price_time_ms = candle.close_time_ms
            event.last_price = candle.low_price
            event.last_seen_ms = candle.close_time_ms
            store.update_event(event)
            elapsed_ms = event.last_seen_ms - event.signal_time_ms
            for horizon in horizons_minutes:
                if horizon not in event.completed_horizons and elapsed_ms >= horizon * 60_000:
                    store.save_outcome(event, horizon, event.last_seen_ms)
                    event.completed_horizons.add(horizon)
            store.stop_event(event, candle.close_time_ms, "MAX_DRAWDOWN")
            stopped = True
            break

        if candle.high_price > event.max_price:
            event.max_price = candle.high_price
            event.max_price_time_ms = candle.close_time_ms
        if candle.low_price < event.min_price:
            event.min_price = candle.low_price
            event.min_price_time_ms = candle.close_time_ms
        event.last_price = candle.close_price
        event.last_seen_ms = candle.close_time_ms

        elapsed_ms = event.last_seen_ms - event.signal_time_ms
        for horizon in horizons_minutes:
            if horizon not in event.completed_horizons and elapsed_ms >= horizon * 60_000:
                store.save_outcome(event, horizon, event.last_seen_ms)
                event.completed_horizons.add(horizon)
        store.update_event(event)

    if stopped:
        return True

    if event.last_seen_ms - event.signal_time_ms >= max(horizons_minutes) * 60_000:
        store.complete_event(event, event.last_seen_ms)
    elif event.last_seen_ms - event.signal_time_ms >= live_monitoring_minutes * 60_000:
        store.defer_event(event, event.last_seen_ms)
    elif event.status != "active":
        event.status = "active"
        store.connection.execute(
            "UPDATE events SET status = 'active', completed_time_ms = NULL WHERE id = ?",
            (event.event_id,),
        )
    store.commit()
    return False
