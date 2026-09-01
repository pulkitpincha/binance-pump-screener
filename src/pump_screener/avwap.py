from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from math import sqrt
from statistics import median

from .config import AvwapConfig
from .models import KlineUpdate, Signal, TickerUpdate


@dataclass(frozen=True)
class AvwapBands:
    mid: float
    upper: float
    lower: float


@dataclass
class AvwapRegime:
    anchor_time_ms: int
    pullback_seen: bool = False
    degraded_below_mid: bool = False


class AvwapTrendEngine:
    """Causal AVWAP trend detector evaluated on completed one-minute candles."""

    def __init__(self, config: AvwapConfig) -> None:
        self.config = config
        self._history: dict[str, deque[KlineUpdate]] = defaultdict(
            # Six hours covers the four-hour research horizon while keeping a
            # frozen anchor available for later re-expansion signals.
            lambda: deque(maxlen=max(config.history_minutes, 360))
        )
        self._regimes: dict[str, AvwapRegime] = {}
        self._matching: dict[tuple[str, str], bool] = defaultdict(bool)

    def warmup(self, candles: dict[str, list[KlineUpdate]]) -> None:
        for symbol, items in candles.items():
            for candle in sorted(items, key=lambda item: item.open_time_ms):
                self._process(candle, None, 0.0, 0.0, emit=False)

    def on_kline(
        self,
        candle: KlineUpdate,
        ticker: TickerUpdate | None,
        rvol: float,
        quote_volume_5m: float,
    ) -> Signal | None:
        if not self.config.enabled or not candle.is_closed:
            return None
        return self._process(candle, ticker, rvol, quote_volume_5m, emit=True)

    def _process(
        self,
        candle: KlineUpdate,
        ticker: TickerUpdate | None,
        rvol: float,
        quote_volume_5m: float,
        *,
        emit: bool,
    ) -> Signal | None:
        history = self._history[candle.symbol]
        if history and history[-1].open_time_ms == candle.open_time_ms:
            history[-1] = candle
        else:
            history.append(candle)

        initial = (
            self._initial_expansion(candle.symbol)
            if candle.symbol not in self._regimes
            else None
        )
        initial_edge = self._edge(candle.symbol, "INITIAL_EXPANSION", initial is not None)
        if initial is not None:
            anchor_time_ms, bands = initial
            self._regimes[candle.symbol] = AvwapRegime(anchor_time_ms=anchor_time_ms)
            if emit and initial_edge and ticker is not None:
                return self._signal(
                    candle,
                    ticker,
                    rvol,
                    quote_volume_5m,
                    "INITIAL_EXPANSION",
                    anchor_time_ms,
                    bands,
                )

        regime = self._regimes.get(candle.symbol)
        if regime is None:
            self._edge(candle.symbol, "RE_EXPANSION", False)
            return None

        anchored = self._anchored_candles(candle.symbol, regime.anchor_time_ms)
        if not anchored or anchored[0].open_time_ms != regime.anchor_time_ms:
            self._regimes.pop(candle.symbol, None)
            self._edge(candle.symbol, "RE_EXPANSION", False)
            return None
        bands = _bands(anchored, self.config.band_stddev)
        if bands is None:
            return None

        if candle.low_price <= bands.mid * (1.0 + self.config.mid_touch_tolerance_pct / 100.0):
            regime.pullback_seen = True

        if self._is_completed_five_minute_close(anchored):
            if candle.close_price < bands.lower:
                self._regimes.pop(candle.symbol, None)
                self._edge(candle.symbol, "RE_EXPANSION", False)
                return None
            if candle.close_price < bands.mid:
                regime.degraded_below_mid = True
                regime.pullback_seen = True

        reexpansion = self._is_reexpansion(candle.symbol, regime, bands)
        reexpansion_edge = self._edge(candle.symbol, "RE_EXPANSION", reexpansion)
        if emit and reexpansion and reexpansion_edge and ticker is not None:
            regime.pullback_seen = False
            regime.degraded_below_mid = False
            return self._signal(
                candle,
                ticker,
                rvol,
                quote_volume_5m,
                "RE_EXPANSION",
                regime.anchor_time_ms,
                bands,
            )
        return None

    def _initial_expansion(self, symbol: str) -> tuple[int, AvwapBands] | None:
        candles = list(self._history[symbol])
        required = max(
            self.config.anchor_lookback_minutes,
            self.config.breakout_lookback_minutes,
            self.config.volume_lookback_minutes,
        )
        if len(candles) < required + 1:
            return None

        current = candles[-1]
        prior = candles[:-1]
        anchor_window = prior[-self.config.anchor_lookback_minutes :]
        anchor = min(anchor_window, key=lambda item: (item.low_price, item.open_time_ms))
        anchor_index = next(
            index for index, item in enumerate(candles) if item.open_time_ms == anchor.open_time_ms
        )
        anchored = candles[anchor_index:]
        bands = _bands(anchored, self.config.band_stddev)
        previous_bands = _bands(anchored[:-1], self.config.band_stddev)
        if bands is None or previous_bands is None:
            return None

        base_low = min(item.low_price for item in anchor_window)
        base_high = max(item.high_price for item in anchor_window)
        base_range_pct = (base_high / base_low - 1.0) * 100.0 if base_low > 0 else float("inf")
        prior_high = max(
            item.high_price for item in prior[-self.config.breakout_lookback_minutes :]
        )
        body_pct = (
            (current.close_price / current.open_price - 1.0) * 100.0
            if current.open_price > 0
            else 0.0
        )
        prior_volumes = [
            item.quote_volume for item in prior[-self.config.volume_lookback_minutes :]
            if item.quote_volume > 0
        ]
        typical_volume = median(prior_volumes) if prior_volumes else 0.0
        volume_ratio = current.quote_volume / typical_volume if typical_volume > 0 else 0.0
        taker_buy_ratio = (
            current.taker_buy_quote_volume / current.quote_volume
            if current.quote_volume > 0
            else 0.0
        )

        matches = (
            current.close_price > current.open_price
            and body_pct >= self.config.initial_body_min_pct
            and volume_ratio >= self.config.initial_volume_ratio_min
            and taker_buy_ratio >= self.config.initial_taker_buy_ratio_min
            and base_range_pct <= self.config.base_range_max_pct
            and current.close_price > prior_high
            and current.close_price > bands.upper
            and bands.mid > previous_bands.mid
        )
        return (anchor.open_time_ms, bands) if matches else None

    def _is_reexpansion(
        self, symbol: str, regime: AvwapRegime, bands: AvwapBands
    ) -> bool:
        if not regime.pullback_seen:
            return False
        anchored = self._anchored_candles(symbol, regime.anchor_time_ms)
        lookback = self.config.reexpansion_lookback_minutes
        if len(anchored) < lookback + 2:
            return False

        current = anchored[-1]
        consolidation = anchored[-(lookback + 1) : -1]
        prior_high = max(item.high_price for item in consolidation)
        range_low = min(item.low_price for item in consolidation)
        range_high = max(item.high_price for item in consolidation)
        range_pct = (range_high / range_low - 1.0) * 100.0 if range_low > 0 else float("inf")
        recent_lows = [item.low_price for item in anchored[-3:]]
        higher_lows = recent_lows[-1] >= min(recent_lows[:-1]) and recent_lows[-1] > recent_lows[0]
        prior_slice = anchored[:-3] if len(anchored) > 3 else anchored[:-1]
        earlier_bands = _bands(prior_slice, self.config.band_stddev)
        rising_mid = earlier_bands is not None and bands.mid >= earlier_bands.mid
        reclaimed = all(item.close_price >= bands.mid for item in consolidation[-2:])

        return (
            current.close_price > current.open_price
            and current.close_price > prior_high
            and current.close_price > bands.upper
            and range_pct <= self.config.compression_range_max_pct
            and higher_lows
            and rising_mid
            and reclaimed
        )

    def _anchored_candles(self, symbol: str, anchor_time_ms: int) -> list[KlineUpdate]:
        return [
            candle
            for candle in self._history[symbol]
            if candle.open_time_ms >= anchor_time_ms
        ]

    @staticmethod
    def _is_completed_five_minute_close(candles: list[KlineUpdate]) -> bool:
        if len(candles) < 5:
            return False
        current = candles[-1]
        if (current.open_time_ms // 60_000) % 5 != 4:
            return False
        last_five = candles[-5:]
        return all(
            right.open_time_ms - left.open_time_ms == 60_000
            for left, right in zip(last_five, last_five[1:])
        )

    def _edge(self, symbol: str, entry_type: str, matches: bool) -> bool:
        key = (symbol, entry_type)
        previous = self._matching[key]
        self._matching[key] = matches
        return matches and not previous

    def _signal(
        self,
        candle: KlineUpdate,
        ticker: TickerUpdate,
        rvol: float,
        quote_volume_5m: float,
        entry_type: str,
        anchor_time_ms: int,
        bands: AvwapBands,
    ) -> Signal:
        candles = list(self._history[candle.symbol])
        reference = candles[-6].close_price if len(candles) >= 6 else candle.open_price
        return_5m_pct = (
            (candle.close_price / reference - 1.0) * 100.0 if reference > 0 else 0.0
        )
        return Signal(
            symbol=candle.symbol,
            signal_time_ms=candle.close_time_ms,
            entry_price=candle.close_price,
            return_5m_pct=return_5m_pct,
            return_24h_pct=ticker.return_24h_pct,
            rvol=rvol,
            quote_volume_5m=quote_volume_5m,
            quote_volume_24h=ticker.quote_volume_24h,
            screener_type="AVWAP_TREND",
            entry_type=entry_type,
            avwap_anchor_time_ms=anchor_time_ms,
            avwap_at_entry=bands.mid,
            upper_band_at_entry=bands.upper,
        )


def _bands(candles: list[KlineUpdate], stddev_multiplier: float) -> AvwapBands | None:
    weighted = [
        (((item.high_price + item.low_price + item.close_price) / 3.0), item.base_volume)
        for item in candles
        if item.base_volume > 0
    ]
    total_volume = sum(volume for _, volume in weighted)
    if total_volume <= 0:
        return None
    mid = sum(price * volume for price, volume in weighted) / total_volume
    variance = sum(volume * (price - mid) ** 2 for price, volume in weighted) / total_volume
    deviation = sqrt(max(variance, 0.0)) * stddev_multiplier
    return AvwapBands(mid=mid, upper=mid + deviation, lower=mid - deviation)
