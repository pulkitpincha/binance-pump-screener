import asyncio
from pathlib import Path

import pytest

from pump_screener.models import Signal
from pump_screener.models import KlineUpdate
from pump_screener.backfill import apply_historical_candles, backfill_missing_history
from pump_screener.storage import Store
from pump_screener.tracker import PaperTradeTracker


def test_tracker_records_cumulative_long_excursions(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    tracker = PaperTradeTracker(store, (5, 15))
    signal = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 3.0, 300.0, 30_000.0)
    tracker.add(signal)

    tracker.on_price("ALTUSDT", 60_000, 108.0)
    tracker.on_price("ALTUSDT", 240_000, 94.0)
    tracker.on_price("ALTUSDT", 300_000, 96.0)
    tracker.on_price("ALTUSDT", 900_000, 90.01)

    rows = store.report_rows()
    assert len(rows) == 2
    five, fifteen = rows
    assert five["max_drawdown_pct"] == pytest.approx(6.0)
    assert five["max_upside_pct"] == pytest.approx(8.0)
    assert five["long_return_pct"] == pytest.approx(-4.0)
    assert fifteen["max_drawdown_pct"] == pytest.approx(9.99)
    assert fifteen["max_upside_pct"] == pytest.approx(8.0)
    assert fifteen["long_return_pct"] == pytest.approx(-9.99)
    assert five["signal_time_ms"] == 0
    assert five["entry_time_ist"] == "1970-01-01T05:30:00+05:30"
    assert five["entry_price"] == pytest.approx(100.0)
    assert fifteen["status"] == "complete"
    store.close()


def test_tracker_blocks_same_open_type_but_allows_other_screener(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    tracker = PaperTradeTracker(store, (60,))
    spike = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    avwap = Signal(
        "ALTUSDT", 1, 101.0, 2.0, 10.0, 2.0, 200.0, 30_000.0,
        screener_type="AVWAP_TREND", entry_type="INITIAL_EXPANSION",
    )

    assert tracker.add(spike) is not None
    assert tracker.add(spike) is None
    assert tracker.add(avwap) is not None
    assert tracker.active_count == 2
    store.close()


def test_tracker_stops_monitoring_at_ten_percent_drawdown(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    tracker = PaperTradeTracker(store, (5, 15), max_drawdown_pct=10.0)
    signal = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    tracker.add(signal)

    _, stopped = tracker.on_price("ALTUSDT", 120_000, 90.0)

    assert len(stopped) == 1
    assert tracker.active_count == 0
    row = store.connection.execute("SELECT status, stop_reason FROM events").fetchone()
    assert row["status"] == "stopped"
    assert row["stop_reason"] == "MAX_DRAWDOWN"
    assert tracker.add(signal) is not None
    store.close()


def test_tracker_defers_after_live_window_when_long_horizons_remain(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    tracker = PaperTradeTracker(
        store, (5, 15, 240, 720), max_drawdown_pct=10.0,
        live_monitoring_minutes=240,
    )
    signal = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    tracker.add(signal)

    tracker.on_price("ALTUSDT", 240 * 60_000, 105.0)

    row = store.connection.execute("SELECT status FROM events").fetchone()
    horizons = {
        item["horizon_minutes"]
        for item in store.connection.execute("SELECT horizon_minutes FROM outcomes")
    }
    assert row["status"] == "deferred"
    assert horizons == {5, 15, 240}
    assert tracker.active_count == 0
    store.close()


def test_historical_backfill_keeps_summaries_and_stops_conservatively(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    tracker = PaperTradeTracker(
        store, (240, 720, 1440), max_drawdown_pct=10.0,
        live_monitoring_minutes=240,
    )
    signal = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    event = tracker.add(signal)
    assert event is not None
    tracker.on_price("ALTUSDT", 240 * 60_000, 105.0)

    deferred = store.load_backfill_events(1440)[0]
    candles = [
        KlineUpdate(
            "ALTUSDT", 720 * 60_000, 715 * 60_000, 720 * 60_000,
            105.0, 110.0, 98.0, 108.0, 1.0, 100.0, 60.0, True,
        ),
        KlineUpdate(
            "ALTUSDT", 1440 * 60_000, 1435 * 60_000, 1440 * 60_000,
            108.0, 130.0, 89.0, 95.0, 1.0, 100.0, 60.0, True,
        ),
    ]
    stopped = apply_historical_candles(
        store, deferred, candles, (240, 720, 1440), 240, 10.0,
        1440 * 60_000,
    )

    row = store.connection.execute(
        "SELECT status, max_price, min_price FROM events"
    ).fetchone()
    horizons = {
        item["horizon_minutes"]
        for item in store.connection.execute("SELECT horizon_minutes FROM outcomes")
    }
    tables = {
        item["name"]
        for item in store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert stopped
    assert row["status"] == "stopped"
    assert row["max_price"] == pytest.approx(110.0)
    assert row["min_price"] == pytest.approx(89.0)
    assert horizons == {240, 720, 1440}
    assert tables == {"events", "outcomes"}
    store.close()


def test_decisions_are_optional_and_recorded_once(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    signal = Signal("ALTUSDT", 1_000, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    event = store.create_event(signal)

    assert store.review_rows(2_000) == []
    pending = store.review_rows(0)[0]
    assert pending["review_status"] == "UNREVIEWED"

    decided = store.save_decision(event.event_id, "TRADE", 2_000)
    assert decided is not None
    assert decided["review_status"] == "TRADE"
    assert decided["decision_price"] == pytest.approx(100.0)

    unchanged = store.save_decision(event.event_id, "IGNORE", 3_000)
    assert unchanged is not None
    assert unchanged["review_status"] == "TRADE"
    assert unchanged["decision_time_ms"] == 2_000

    report = store.report_rows()[0]
    assert report["decision_time_ist"] == "1970-01-01T05:30:02+05:30"
    assert report["decision_latency_seconds"] == pytest.approx(1.0)
    store.close()


def test_deferred_position_still_blocks_same_entry_type(tmp_path: Path) -> None:
    store = Store(tmp_path / "test.sqlite3")
    tracker = PaperTradeTracker(store, (240, 720), live_monitoring_minutes=240)
    signal = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    assert tracker.add(signal) is not None
    tracker.on_price("ALTUSDT", 240 * 60_000, 105.0)

    restarted = PaperTradeTracker(store, (240, 720), live_monitoring_minutes=240)
    assert restarted.active_count == 0
    assert restarted.add(signal) is None
    store.close()


def test_failed_backfill_keeps_position_deferred_and_open(tmp_path: Path) -> None:
    class FailingClient:
        async def historical_klines_between(self, *args, **kwargs):
            raise ConnectionError("offline")

    store = Store(tmp_path / "test.sqlite3")
    signal = Signal("ALTUSDT", 0, 100.0, 6.0, 10.0, 5.0, 500.0, 30_000.0)
    store.create_event(signal)

    summary = asyncio.run(
        backfill_missing_history(
            store,
            FailingClient(),
            (240, 720),
            240,
            10.0,
            {"ALTUSDT"},
            now_ms=300 * 60_000,
        )
    )

    row = store.connection.execute("SELECT status FROM events").fetchone()
    tracker = PaperTradeTracker(store, (240, 720), live_monitoring_minutes=240)
    assert summary.symbols_failed == 1
    assert row["status"] == "deferred"
    assert tracker.active_count == 0
    assert tracker.add(signal) is None
    store.close()
