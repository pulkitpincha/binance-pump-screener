from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import time

from .binance import BinanceClient
from .backfill import backfill_missing_history
from .avwap import AvwapTrendEngine
from .config import AppConfig
from .engine import SignalEngine
from .models import KlineUpdate, TickerUpdate
from .storage import Store
from .tracker import PaperTradeTracker
from .review import ReviewServer


LOGGER = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30), name="IST")


def format_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=IST).isoformat(timespec="seconds")


async def run_screener(config: AppConfig, review_enabled: bool | None = None) -> None:
    session_start_ms = int(time.time() * 1000)
    store = Store(config.storage.database_path)
    engine = SignalEngine(config.signal)
    avwap_engine = AvwapTrendEngine(config.avwap)
    client = BinanceClient(config.binance)
    latest_tickers: dict[str, TickerUpdate] = {}
    should_review = (
        config.review.mode == "review" if review_enabled is None else review_enabled
    )
    review: ReviewServer | None = None

    try:
        if should_review:
            candidate = ReviewServer(store, config.review, session_start_ms)
            if await candidate.start():
                review = candidate
        else:
            LOGGER.info("Collection mode active: pop-ups and review alerts are disabled")

        symbols = await client.symbols()
        if not symbols:
            raise RuntimeError("Binance returned no matching perpetual symbols")
        backfill = await backfill_missing_history(
            store,
            client,
            config.signal.horizons_minutes,
            config.signal.live_monitoring_minutes,
            config.signal.max_drawdown_pct,
            set(symbols),
        )
        if backfill.events_checked:
            LOGGER.info(
                "Historical continuation checked %d trades; updated=%d stopped=%d failures=%d",
                backfill.events_checked,
                backfill.events_updated,
                backfill.events_stopped,
                backfill.symbols_failed,
            )
        tracker = PaperTradeTracker(
            store,
            config.signal.horizons_minutes,
            config.signal.max_drawdown_pct,
            config.signal.live_monitoring_minutes,
        )
        LOGGER.info(
            "Monitoring %d %s perpetuals",
            len(symbols),
            config.binance.quote_asset,
        )
        if config.avwap.enabled:
            LOGGER.info(
                "Loading %d completed one-minute candles per symbol for AVWAP warmup",
                config.avwap.history_minutes,
            )
            history = await client.historical_klines(symbols, config.avwap.history_minutes)
            avwap_engine.warmup(history)
            warmed = sum(bool(items) for items in history.values())
            LOGGER.info("AVWAP history ready for %d/%d symbols", warmed, len(symbols))
        if tracker.active_count:
            LOGGER.info("Resumed %d active paper trades", tracker.active_count)

        reconnect_delay = 1.0
        while True:
            try:
                async for update in client.updates(symbols):
                    reconnect_delay = 1.0
                    if isinstance(update, KlineUpdate):
                        engine.on_kline(update)
                        if update.is_closed:
                            ticker = latest_tickers.get(update.symbol)
                            rvol, quote_volume_5m = (
                                engine.rvol_for(ticker) if ticker is not None else (0.0, 0.0)
                            )
                            avwap_signal = avwap_engine.on_kline(
                                update, ticker, rvol, quote_volume_5m
                            )
                            if avwap_signal is not None:
                                event = tracker.add(avwap_signal)
                                if event is not None:
                                    LOGGER.warning(
                                        "LONG ALERT %s %s/%s at %s entry=%.10g "
                                        "5m=%.2f%% 24h=%.2f%% RVOL=%.2fx",
                                        avwap_signal.symbol,
                                        avwap_signal.screener_type,
                                        avwap_signal.entry_type,
                                        format_time(avwap_signal.signal_time_ms),
                                        avwap_signal.entry_price,
                                        avwap_signal.return_5m_pct,
                                        avwap_signal.return_24h_pct,
                                        avwap_signal.rvol,
                                    )
                                    if review is not None:
                                        review.notify(event)
                        continue

                    latest_tickers[update.symbol] = update
                    completed, stopped = tracker.on_price(
                        update.symbol, update.event_time_ms, update.price
                    )
                    for event, horizon in completed:
                        LOGGER.info(
                            "LONG OUTCOME %s %s/%s %dm entry=%.10g observed=%.10g "
                            "high=%.10g low=%.10g",
                            event.symbol,
                            event.screener_type,
                            event.entry_type,
                            horizon,
                            event.entry_price,
                            event.last_price,
                            event.max_price,
                            event.min_price,
                        )
                    for event in stopped:
                        drawdown = (1.0 - event.min_price / event.entry_price) * 100.0
                        LOGGER.warning(
                            "MONITORING STOPPED %s %s/%s drawdown=%.2f%% at %s",
                            event.symbol,
                            event.screener_type,
                            event.entry_type,
                            drawdown,
                            format_time(event.last_seen_ms),
                        )

                    signal = engine.on_ticker(update)
                    if signal is None:
                        continue
                    event = tracker.add(signal)
                    if event is not None:
                        LOGGER.warning(
                            "LONG ALERT %s %s/%s at %s entry=%.10g "
                            "5m=%.2f%% 24h=%.2f%% RVOL=%.2fx",
                            signal.symbol,
                            signal.screener_type,
                            signal.entry_type,
                            format_time(signal.signal_time_ms),
                            signal.entry_price,
                            signal.return_5m_pct,
                            signal.return_24h_pct,
                            signal.rvol,
                        )
                        if review is not None:
                            review.notify(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.error("Binance stream disconnected: %s", exc)
                LOGGER.info("Reconnecting in %.0f seconds", reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60.0)
    finally:
        if review is not None:
            await review.stop()
        store.close()
