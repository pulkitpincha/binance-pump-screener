from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TickerUpdate:
    symbol: str
    event_time_ms: int
    price: float
    return_24h_pct: float
    quote_volume_24h: float


@dataclass(frozen=True)
class KlineUpdate:
    symbol: str
    event_time_ms: int
    open_time_ms: int
    close_time_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    base_volume: float
    quote_volume: float
    taker_buy_quote_volume: float
    is_closed: bool


@dataclass(frozen=True)
class Signal:
    symbol: str
    signal_time_ms: int
    entry_price: float
    return_5m_pct: float
    return_24h_pct: float
    rvol: float
    quote_volume_5m: float
    quote_volume_24h: float
    screener_type: str = "SPIKE_RVOL"
    entry_type: str = "SPIKE_RVOL"
    avwap_anchor_time_ms: int | None = None
    avwap_at_entry: float | None = None
    upper_band_at_entry: float | None = None


@dataclass
class ActiveEvent:
    event_id: str
    symbol: str
    screener_type: str
    entry_type: str
    signal_time_ms: int
    entry_price: float
    max_price: float
    min_price: float
    max_price_time_ms: int
    min_price_time_ms: int
    last_price: float
    last_seen_ms: int
    completed_horizons: set[int]
    status: str = "active"
