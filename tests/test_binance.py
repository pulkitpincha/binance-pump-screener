from pump_screener.binance import _parse_kline, _parse_ticker
from pump_screener.config import BinanceConfig


def test_parse_binance_futures_ticker() -> None:
    update = _parse_ticker(
        {
            "e": "24hrTicker",
            "E": 1_700_000_000_000,
            "s": "ALTUSDT",
            "c": "1.25",
            "P": "12.5",
            "q": "1234567.89",
        }
    )
    assert update is not None
    assert update.symbol == "ALTUSDT"
    assert update.price == 1.25
    assert update.return_24h_pct == 12.5
    assert update.quote_volume_24h == 1_234_567.89


def test_current_market_stream_is_the_default() -> None:
    assert BinanceConfig().websocket_url.endswith("/market/stream")


def test_parse_completed_one_minute_kline() -> None:
    update = _parse_kline(
        {
            "e": "kline",
            "E": 60_000,
            "s": "ALTUSDT",
            "k": {
                "t": 0, "T": 59_999, "i": "1m", "o": "1.0", "h": "1.2",
                "l": "0.9", "c": "1.1", "v": "100", "q": "110",
                "Q": "70", "x": True,
            },
        }
    )
    assert update is not None
    assert update.is_closed
    assert update.close_price == 1.1
    assert update.taker_buy_quote_volume == 70.0
