from pump_screener.avwap import AvwapTrendEngine
from pump_screener.config import AvwapConfig
from pump_screener.models import KlineUpdate, TickerUpdate


def candle(
    minute: int,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float = 100.0,
) -> KlineUpdate:
    start = minute * 60_000
    return KlineUpdate(
        symbol="ALTUSDT",
        event_time_ms=start + 59_999,
        open_time_ms=start,
        close_time_ms=start + 59_999,
        open_price=open_price,
        high_price=high,
        low_price=low,
        close_price=close,
        base_volume=volume,
        quote_volume=volume * close,
        taker_buy_quote_volume=volume * close * 0.7,
        is_closed=True,
    )


def ticker(timestamp_ms: int, price: float) -> TickerUpdate:
    return TickerUpdate("ALTUSDT", timestamp_ms, price, 8.0, 1_000_000.0)


def seeded_engine() -> AvwapTrendEngine:
    engine = AvwapTrendEngine(AvwapConfig())
    history = [candle(i, 100.0, 100.1, 99.9, 100.0) for i in range(31)]
    engine.warmup({"ALTUSDT": history})
    return engine


def test_initial_expansion_fires_on_first_completed_breakout_candle() -> None:
    engine = seeded_engine()
    breakout = candle(31, 100.0, 102.2, 99.95, 102.0, volume=400.0)

    signal = engine.on_kline(breakout, ticker(breakout.close_time_ms, 102.0), 4.0, 40_000.0)

    assert signal is not None
    assert signal.screener_type == "AVWAP_TREND"
    assert signal.entry_type == "INITIAL_EXPANSION"
    assert signal.avwap_anchor_time_ms is not None
    assert signal.entry_price == 102.0


def test_mid_reclaim_alone_does_not_signal_but_structural_reexpansion_does() -> None:
    engine = seeded_engine()
    breakout = candle(31, 100.0, 102.2, 99.95, 102.0, volume=400.0)
    assert engine.on_kline(
        breakout, ticker(breakout.close_time_ms, 102.0), 4.0, 40_000.0
    ) is not None

    pullback = candle(32, 102.0, 102.1, 99.9, 100.2, volume=180.0)
    assert engine.on_kline(
        pullback, ticker(pullback.close_time_ms, 100.2), 1.5, 20_000.0
    ) is None

    consolidation = [
        candle(33, 100.2, 100.6, 100.1, 100.4),
        candle(34, 100.4, 100.8, 100.3, 100.6),
        candle(35, 100.6, 101.0, 100.5, 100.8),
        candle(36, 100.8, 101.2, 100.7, 101.0),
        candle(37, 101.0, 101.4, 100.9, 101.2),
    ]
    for item in consolidation:
        assert engine.on_kline(
            item, ticker(item.close_time_ms, item.close_price), 1.0, 10_000.0
        ) is None

    trigger = candle(38, 101.2, 103.0, 101.1, 102.8, volume=250.0)
    signal = engine.on_kline(
        trigger, ticker(trigger.close_time_ms, 102.8), 2.0, 25_000.0
    )

    assert signal is not None
    assert signal.entry_type == "RE_EXPANSION"
