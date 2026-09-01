from dataclasses import replace

import pytest

from pump_screener.config import SignalConfig
from pump_screener.engine import SignalEngine
from pump_screener.models import KlineUpdate, TickerUpdate


def ticker(timestamp_ms: int, price: float, daily: float = 10.0, volume: float = 28_800.0) -> TickerUpdate:
    return TickerUpdate("ALTUSDT", timestamp_ms, price, daily, volume)


def kline(timestamp_ms: int, quote_volume: float) -> KlineUpdate:
    return KlineUpdate(
        "ALTUSDT", timestamp_ms, timestamp_ms, timestamp_ms + 59_999,
        100.0, 100.0, 100.0, 100.0, 1.0, quote_volume, quote_volume / 2, False,
    )


def test_signal_requires_full_five_minute_history_and_all_conditions() -> None:
    engine = SignalEngine(SignalConfig(move_5m_min_pct=5.0, move_24h_max_pct=35.0, rvol_min=2.0))
    engine.on_kline(kline(300_000, 250.0))

    assert engine.on_ticker(ticker(0, 100.0)) is None
    assert engine.on_ticker(ticker(299_000, 106.0)) is None
    signal = engine.on_ticker(ticker(300_000, 106.0))

    assert signal is not None
    assert signal.symbol == "ALTUSDT"
    assert signal.return_5m_pct == pytest.approx(6.0)
    assert signal.rvol == pytest.approx(2.5)


def test_signal_is_edge_triggered_until_rule_stops_matching() -> None:
    engine = SignalEngine(SignalConfig())
    engine.on_kline(kline(300_000, 600.0))
    engine.on_ticker(ticker(0, 100.0))

    assert engine.on_ticker(ticker(300_000, 106.0)) is not None
    assert engine.on_ticker(ticker(301_000, 107.0)) is None
    assert engine.on_ticker(ticker(302_000, 104.0)) is None
    assert engine.on_ticker(ticker(303_000, 106.0)) is not None


def test_daily_cap_and_rvol_block_signal() -> None:
    engine = SignalEngine(SignalConfig())
    engine.on_ticker(ticker(0, 100.0))
    engine.on_kline(kline(300_000, 100.0))

    assert engine.on_ticker(ticker(300_000, 106.0, daily=40.0)) is None
    assert engine.on_ticker(ticker(301_000, 107.0, daily=10.0)) is None


def test_daily_cap_uses_absolute_move() -> None:
    engine = SignalEngine(SignalConfig(rvol_min=0.0))
    engine.on_ticker(ticker(0, 100.0))
    engine.on_kline(kline(300_000, 250.0))

    assert engine.on_ticker(ticker(300_000, 106.0, daily=-40.0)) is None


def test_partial_one_minute_volume_updates_replace_instead_of_double_counting() -> None:
    engine = SignalEngine(SignalConfig(rvol_min=0.0))
    first = kline(300_000, 100.0)
    engine.on_kline(first)
    engine.on_kline(replace(first, event_time_ms=300_001, quote_volume=150.0))

    rvol, quote_volume = engine.rvol_for(ticker(300_001, 100.0, volume=28_800.0))

    assert quote_volume == pytest.approx(150.0)
    assert rvol == pytest.approx(1.5)
