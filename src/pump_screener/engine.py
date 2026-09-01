from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass

from .config import SignalConfig
from .models import KlineUpdate, Signal, TickerUpdate


FIVE_MINUTES_MS = 5 * 60 * 1000
FIVE_MINUTE_WINDOWS_PER_DAY = 24 * 60 // 5


@dataclass
class VolumeState:
    quote_volume_5m: float
    event_time_ms: int
    bucket: int
    minute_volumes: dict[int, float]


class SignalEngine:
    """Evaluates the user's three-condition signal on Binance stream updates."""

    def __init__(self, config: SignalConfig) -> None:
        self.config = config
        self._prices: dict[str, deque[tuple[int, float]]] = defaultdict(deque)
        self._volumes: dict[str, VolumeState] = {}
        self._matching: dict[str, bool] = defaultdict(bool)

    def on_kline(self, update: KlineUpdate) -> None:
        bucket = update.open_time_ms // FIVE_MINUTES_MS
        state = self._volumes.get(update.symbol)
        if state is None or state.bucket != bucket:
            state = VolumeState(0.0, update.event_time_ms, bucket, {})
            self._volumes[update.symbol] = state
        state.minute_volumes[update.open_time_ms] = update.quote_volume
        state.quote_volume_5m = sum(state.minute_volumes.values())
        state.event_time_ms = update.event_time_ms

    def rvol_for(self, update: TickerUpdate) -> tuple[float, float]:
        volume = self._volumes.get(update.symbol)
        if volume is None or update.quote_volume_24h <= 0:
            return 0.0, 0.0
        average_5m_volume = update.quote_volume_24h / FIVE_MINUTE_WINDOWS_PER_DAY
        rvol = volume.quote_volume_5m / average_5m_volume if average_5m_volume > 0 else 0.0
        return rvol, volume.quote_volume_5m

    def on_ticker(self, update: TickerUpdate) -> Signal | None:
        prices = self._prices[update.symbol]
        prices.append((update.event_time_ms, update.price))

        target_ms = update.event_time_ms - FIVE_MINUTES_MS
        while len(prices) > 1 and prices[1][0] <= target_ms:
            prices.popleft()

        # A full rolling window is required. This prevents false alerts during
        # the first five minutes after a fresh process start.
        if not prices or prices[0][0] > target_ms:
            self._matching[update.symbol] = False
            return None

        reference_price = prices[0][1]
        if reference_price <= 0 or update.price <= 0:
            return None

        return_5m_pct = (update.price / reference_price - 1.0) * 100.0
        volume = self._volumes.get(update.symbol)
        if volume is None or update.quote_volume_24h <= 0:
            self._matching[update.symbol] = False
            return None

        rvol, quote_volume_5m = self.rvol_for(update)
        matches = (
            return_5m_pct > self.config.move_5m_min_pct
            and abs(update.return_24h_pct) < self.config.move_24h_max_pct
            and rvol >= self.config.rvol_min
        )
        was_matching = self._matching[update.symbol]
        self._matching[update.symbol] = matches
        if not matches or was_matching:
            return None

        return Signal(
            symbol=update.symbol,
            signal_time_ms=update.event_time_ms,
            entry_price=update.price,
            return_5m_pct=return_5m_pct,
            return_24h_pct=update.return_24h_pct,
            rvol=rvol,
            quote_volume_5m=quote_volume_5m,
            quote_volume_24h=update.quote_volume_24h,
            screener_type="SPIKE_RVOL",
            entry_type="SPIKE_RVOL",
        )
