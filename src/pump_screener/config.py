from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class SignalConfig:
    move_5m_min_pct: float = 5.0
    move_24h_max_pct: float = 35.0
    rvol_min: float = 5.0
    horizons_minutes: tuple[int, ...] = (
        5, 15, 30, 60, 240, 720, 1440, 2880, 4320, 10080
    )
    live_monitoring_minutes: int = 240
    max_drawdown_pct: float = 10.0


@dataclass(frozen=True)
class AvwapConfig:
    enabled: bool = True
    history_minutes: int = 60
    anchor_lookback_minutes: int = 30
    breakout_lookback_minutes: int = 10
    volume_lookback_minutes: int = 20
    band_stddev: float = 1.0
    initial_body_min_pct: float = 0.75
    initial_volume_ratio_min: float = 3.0
    initial_taker_buy_ratio_min: float = 0.55
    base_range_max_pct: float = 6.0
    reexpansion_lookback_minutes: int = 5
    compression_range_max_pct: float = 4.0
    mid_touch_tolerance_pct: float = 0.35


@dataclass(frozen=True)
class BinanceConfig:
    rest_base_url: str = "https://fapi.binance.com"
    websocket_url: str = "wss://fstream.binance.com/market/stream"
    quote_asset: str = "USDT"
    excluded_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class StorageConfig:
    database_path: Path = Path("data/screener.sqlite3")


@dataclass(frozen=True)
class ReviewConfig:
    mode: str = "review"
    host: str = "127.0.0.1"
    port: int = 8765


@dataclass(frozen=True)
class AppConfig:
    signal: SignalConfig = field(default_factory=SignalConfig)
    avwap: AvwapConfig = field(default_factory=AvwapConfig)
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    review: ReviewConfig = field(default_factory=ReviewConfig)


def load_config(path: Path | None) -> AppConfig:
    raw: dict = {}
    config_directory = Path.cwd()
    if path is not None:
        resolved_path = path.resolve()
        config_directory = resolved_path.parent
        with resolved_path.open("rb") as handle:
            raw = tomllib.load(handle)

    signal_raw = raw.get("signal", {})
    avwap_raw = raw.get("avwap", {})
    binance_raw = raw.get("binance", {})
    storage_raw = raw.get("storage", {})
    review_raw = raw.get("review", {})

    horizons = tuple(
        sorted(
            {
                int(value)
                for value in signal_raw.get(
                    "horizons_minutes",
                    (5, 15, 30, 60, 240, 720, 1440, 2880, 4320, 10080),
                )
            }
        )
    )
    if not horizons or horizons[0] <= 0:
        raise ValueError("signal.horizons_minutes must contain positive integers")

    signal = SignalConfig(
        move_5m_min_pct=float(signal_raw.get("move_5m_min_pct", 5.0)),
        move_24h_max_pct=float(signal_raw.get("move_24h_max_pct", 35.0)),
        rvol_min=float(signal_raw.get("rvol_min", 5.0)),
        horizons_minutes=horizons,
        live_monitoring_minutes=int(signal_raw.get("live_monitoring_minutes", 240)),
        max_drawdown_pct=float(signal_raw.get("max_drawdown_pct", 10.0)),
    )
    if signal.move_5m_min_pct <= 0:
        raise ValueError("signal.move_5m_min_pct must be positive")
    if signal.rvol_min < 0:
        raise ValueError("signal.rvol_min cannot be negative")
    if signal.max_drawdown_pct <= 0:
        raise ValueError("signal.max_drawdown_pct must be positive")
    if signal.live_monitoring_minutes <= 0:
        raise ValueError("signal.live_monitoring_minutes must be positive")
    if signal.live_monitoring_minutes not in signal.horizons_minutes:
        raise ValueError(
            "signal.horizons_minutes must include signal.live_monitoring_minutes"
        )

    avwap = AvwapConfig(
        enabled=bool(avwap_raw.get("enabled", True)),
        history_minutes=int(avwap_raw.get("history_minutes", 60)),
        anchor_lookback_minutes=int(avwap_raw.get("anchor_lookback_minutes", 30)),
        breakout_lookback_minutes=int(avwap_raw.get("breakout_lookback_minutes", 10)),
        volume_lookback_minutes=int(avwap_raw.get("volume_lookback_minutes", 20)),
        band_stddev=float(avwap_raw.get("band_stddev", 1.0)),
        initial_body_min_pct=float(avwap_raw.get("initial_body_min_pct", 0.75)),
        initial_volume_ratio_min=float(avwap_raw.get("initial_volume_ratio_min", 3.0)),
        initial_taker_buy_ratio_min=float(
            avwap_raw.get("initial_taker_buy_ratio_min", 0.55)
        ),
        base_range_max_pct=float(avwap_raw.get("base_range_max_pct", 6.0)),
        reexpansion_lookback_minutes=int(avwap_raw.get("reexpansion_lookback_minutes", 5)),
        compression_range_max_pct=float(avwap_raw.get("compression_range_max_pct", 4.0)),
        mid_touch_tolerance_pct=float(avwap_raw.get("mid_touch_tolerance_pct", 0.35)),
    )
    if min(
        avwap.history_minutes,
        avwap.anchor_lookback_minutes,
        avwap.breakout_lookback_minutes,
        avwap.volume_lookback_minutes,
        avwap.reexpansion_lookback_minutes,
    ) <= 0:
        raise ValueError("AVWAP lookback values must be positive")
    if avwap.history_minutes < avwap.anchor_lookback_minutes:
        raise ValueError("avwap.history_minutes must cover the anchor lookback")

    database_path = Path(storage_raw.get("database_path", "data/screener.sqlite3"))
    if not database_path.is_absolute():
        database_path = config_directory / database_path

    review_mode = str(review_raw.get("mode", "review")).lower()
    if review_mode not in {"review", "silent"}:
        raise ValueError("review.mode must be 'review' or 'silent'")
    review_port = int(review_raw.get("port", 8765))
    if not 1 <= review_port <= 65535:
        raise ValueError("review.port must be between 1 and 65535")

    return AppConfig(
        signal=signal,
        avwap=avwap,
        binance=BinanceConfig(
            rest_base_url=str(binance_raw.get("rest_base_url", "https://fapi.binance.com")).rstrip("/"),
            websocket_url=str(
                binance_raw.get(
                    "websocket_url", "wss://fstream.binance.com/market/stream"
                )
            ),
            quote_asset=str(binance_raw.get("quote_asset", "USDT")).upper(),
            excluded_symbols=tuple(
                str(symbol).upper()
                for symbol in binance_raw.get("excluded_symbols", ())
            ),
        ),
        storage=StorageConfig(
            database_path=database_path
        ),
        review=ReviewConfig(
            mode=review_mode,
            host=str(review_raw.get("host", "127.0.0.1")),
            port=review_port,
        ),
    )
