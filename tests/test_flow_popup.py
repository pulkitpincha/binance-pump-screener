import pytest

from pump_screener.flow_popup import (
    evaluate_flow,
    extract_symbol_from_title,
    fair_value_pressure,
    snapshot_from_kline,
)


def test_extracts_bitunix_symbol_from_chrome_title() -> None:
    assert (
        extract_symbol_from_title(
            "0.05227 | NILUSDT Futures Exchange - Crypto Futures | Bitunix - Google Chrome"
        )
        == "NILUSDT"
    )


def test_extracts_tradingview_perpetual_suffix() -> None:
    assert extract_symbol_from_title("NILUSDT.P 0.05227 — TradingView") == "NILUSDT"


def test_ignores_titles_without_usdt_contract() -> None:
    assert extract_symbol_from_title("New Tab - Google Chrome") is None


def test_flow_evaluation_uses_price_response() -> None:
    assert evaluate_flow(0.4, 55)[0] == "Bullish initiative buying"
    assert evaluate_flow(0.4, 45)[0] == "Bullish maker-bid absorption"
    assert evaluate_flow(-0.4, 45)[0] == "Bearish initiative selling"
    assert evaluate_flow(-0.4, 55)[0] == "Bearish maker-sell absorption"


def test_fair_value_pressure_tracks_initiative_and_absorption() -> None:
    assert fair_value_pressure(0.4, 55)[0] == "FV PRESSURE ↑"
    assert fair_value_pressure(0.4, 45)[0] == "FV PRESSURE ↑"
    assert fair_value_pressure(-0.4, 45)[0] == "FV PRESSURE ↓"
    assert fair_value_pressure(-0.4, 55)[0] == "FV PRESSURE ↓"
    assert fair_value_pressure(0.0, 45)[0] == "FV PRESSURE ↑"
    assert fair_value_pressure(0.0, 55)[0] == "FV PRESSURE ↓"
    assert fair_value_pressure(0.0, 50)[0] == "FV PRESSURE →"


def test_snapshot_calculates_mirrored_flow() -> None:
    row = [0, "100", "102", "99", "101", "0", 299999, "1000", 0, 0, "450"]
    snapshot = snapshot_from_kline("TESTUSDT", "5m", row)
    assert snapshot.return_pct == pytest.approx(1.0)
    assert snapshot.taker_buy_pct == 45.0
    assert snapshot.taker_sell_pct == 55.0
