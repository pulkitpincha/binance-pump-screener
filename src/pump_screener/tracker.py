from __future__ import annotations

from collections import defaultdict

from .models import ActiveEvent, Signal
from .storage import Store


class PaperTradeTracker:
    def __init__(
        self,
        store: Store,
        horizons_minutes: tuple[int, ...],
        max_drawdown_pct: float = 10.0,
        live_monitoring_minutes: int | None = None,
    ) -> None:
        self.store = store
        self.horizons_minutes = horizons_minutes
        self.max_drawdown_pct = max_drawdown_pct
        self.live_monitoring_minutes = (
            live_monitoring_minutes
            if live_monitoring_minutes is not None
            else max(horizons_minutes)
        )
        self._events: dict[str, list[ActiveEvent]] = defaultdict(list)
        for event in store.load_active_events():
            self._events[event.symbol].append(event)

    @property
    def active_count(self) -> int:
        return sum(len(events) for events in self._events.values())

    def has_open(self, symbol: str, screener_type: str, entry_type: str) -> bool:
        return self.store.has_open_event(symbol, screener_type, entry_type)

    def add(self, signal: Signal) -> ActiveEvent | None:
        if self.has_open(signal.symbol, signal.screener_type, signal.entry_type):
            return None
        event = self.store.create_event(signal)
        self._events[event.symbol].append(event)
        return event

    def on_price(
        self, symbol: str, timestamp_ms: int, price: float
    ) -> tuple[list[tuple[ActiveEvent, int]], list[ActiveEvent]]:
        if price <= 0:
            return [], []
        events = self._events.get(symbol)
        if not events:
            return [], []

        completed_outcomes: list[tuple[ActiveEvent, int]] = []
        stopped_events: list[ActiveEvent] = []
        still_active: list[ActiveEvent] = []
        for event in events:
            if timestamp_ms < event.signal_time_ms:
                still_active.append(event)
                continue

            event.last_price = price
            event.last_seen_ms = timestamp_ms
            if price > event.max_price:
                event.max_price = price
                event.max_price_time_ms = timestamp_ms
            if price < event.min_price:
                event.min_price = price
                event.min_price_time_ms = timestamp_ms

            elapsed_ms = timestamp_ms - event.signal_time_ms
            for horizon in self.horizons_minutes:
                if horizon not in event.completed_horizons and elapsed_ms >= horizon * 60_000:
                    self.store.save_outcome(event, horizon, timestamp_ms)
                    event.completed_horizons.add(horizon)
                    completed_outcomes.append((event, horizon))

            self.store.update_event(event)
            drawdown_pct = max(
                (1.0 - event.min_price / event.entry_price) * 100.0,
                0.0,
            )
            drawdown_price = event.entry_price * (1.0 - self.max_drawdown_pct / 100.0)
            if event.min_price <= drawdown_price:
                self.store.stop_event(event, timestamp_ms, "MAX_DRAWDOWN")
                stopped_events.append(event)
            elif elapsed_ms >= self.live_monitoring_minutes * 60_000:
                if set(self.horizons_minutes).issubset(event.completed_horizons):
                    self.store.complete_event(event, timestamp_ms)
                else:
                    self.store.defer_event(event, timestamp_ms)
            elif set(self.horizons_minutes).issubset(event.completed_horizons):
                self.store.complete_event(event, timestamp_ms)
            else:
                still_active.append(event)

        if still_active:
            self._events[symbol] = still_active
        else:
            self._events.pop(symbol, None)
        self.store.commit()
        return completed_outcomes, stopped_events
