from __future__ import annotations

from dataclasses import dataclass
import ctypes
from ctypes import wintypes
import json
import queue
import re
import threading
import time
import tkinter as tk
from tkinter import font as tkfont
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BINANCE_FUTURES_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
POLL_SECONDS = 2.0
SYMBOL_PATTERN = re.compile(
    r"(?<![A-Z0-9])([A-Z0-9]{2,24}USDT)(?:\.P)?(?![A-Z0-9])",
    re.IGNORECASE,
)

BG = "#0a0e0c"
PANEL = "#111814"
PANEL_RAISED = "#17211b"
TEXT = "#edf5ef"
MUTED = "#8ca095"
GREEN = "#46d17d"
RED = "#ff657a"
AMBER = "#efb84f"
BORDER = "#26372d"


@dataclass(frozen=True)
class FlowSnapshot:
    symbol: str
    interval: str
    open_price: float
    last_price: float
    quote_volume: float
    taker_buy_quote_volume: float
    close_time_ms: int

    @property
    def return_pct(self) -> float:
        if self.open_price <= 0:
            return 0.0
        return (self.last_price / self.open_price - 1.0) * 100.0

    @property
    def taker_buy_pct(self) -> float:
        if self.quote_volume <= 0:
            return 50.0
        return self.taker_buy_quote_volume / self.quote_volume * 100.0

    @property
    def taker_sell_pct(self) -> float:
        return 100.0 - self.taker_buy_pct


def extract_symbol_from_title(title: str) -> str | None:
    match = SYMBOL_PATTERN.search(title.upper())
    return match.group(1).upper() if match else None


def evaluate_flow(return_pct: float, taker_buy_pct: float) -> tuple[str, str]:
    """Classify price response to aggressive flow without overstating small skews."""
    price_threshold = 0.05
    flow_upper = 51.5
    flow_lower = 48.5
    if return_pct >= price_threshold:
        if taker_buy_pct >= flow_upper:
            return "Bullish initiative buying", GREEN
        if taker_buy_pct <= flow_lower:
            return "Bullish maker-bid absorption", GREEN
        return "Bullish · balanced flow", GREEN
    if return_pct <= -price_threshold:
        if taker_buy_pct <= flow_lower:
            return "Bearish initiative selling", RED
        if taker_buy_pct >= flow_upper:
            return "Bearish maker-sell absorption", RED
        return "Bearish · balanced flow", RED
    if taker_buy_pct >= flow_upper:
        return "Buy aggression being absorbed", AMBER
    if taker_buy_pct <= flow_lower:
        return "Sell aggression being absorbed", AMBER
    return "Balanced / neutral", MUTED


def fair_value_pressure(return_pct: float, taker_buy_pct: float) -> tuple[str, str]:
    """Estimate directional pressure; this is not an equilibrium-price calculation."""
    evaluation, _color = evaluate_flow(return_pct, taker_buy_pct)
    if evaluation.startswith("Bullish") or evaluation == "Sell aggression being absorbed":
        return "FV PRESSURE ↑", GREEN
    if evaluation.startswith("Bearish") or evaluation == "Buy aggression being absorbed":
        return "FV PRESSURE ↓", RED
    return "FV PRESSURE →", MUTED


def snapshot_from_kline(symbol: str, interval: str, row: list) -> FlowSnapshot:
    return FlowSnapshot(
        symbol=symbol,
        interval=interval,
        open_price=float(row[1]),
        last_price=float(row[4]),
        quote_volume=float(row[7]),
        taker_buy_quote_volume=float(row[10]),
        close_time_ms=int(row[6]),
    )


def format_quote_volume(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.0f}"


def _window_text(handle: int) -> str:
    user32 = ctypes.windll.user32
    length = user32.GetWindowTextLengthW(handle)
    if length <= 0:
        return ""
    buffer = ctypes.create_unicode_buffer(length + 1)
    user32.GetWindowTextW(handle, buffer, length + 1)
    return buffer.value


def _window_class(handle: int) -> str:
    buffer = ctypes.create_unicode_buffer(256)
    ctypes.windll.user32.GetClassNameW(handle, buffer, len(buffer))
    return buffer.value


def active_chrome_title() -> str | None:
    """Read only Chrome window titles; never inspect browser content or URLs."""
    if not hasattr(ctypes, "windll"):
        return None
    user32 = ctypes.windll.user32
    handle = user32.GetForegroundWindow()
    if handle and _window_class(handle).startswith("Chrome_WidgetWin"):
        return _window_text(handle) or None

    # When the popup itself is foreground, a single Chrome window remains
    # unambiguous: its title still represents that window's active tab.
    candidates: list[str] = []
    callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    @callback_type
    def collect(candidate: int, _parameter: int) -> bool:
        if user32.IsWindowVisible(candidate) and _window_class(candidate).startswith(
            "Chrome_WidgetWin"
        ):
            title = _window_text(candidate)
            if title and extract_symbol_from_title(title):
                candidates.append(title)
        return True

    user32.EnumWindows(collect, 0)
    return candidates[0] if len(candidates) == 1 else None


class BinanceFlowClient:
    def fetch(self, symbol: str, interval: str) -> FlowSnapshot:
        query = urlencode({"symbol": symbol, "interval": interval, "limit": 1})
        request = Request(
            f"{BINANCE_FUTURES_KLINES_URL}?{query}",
            headers={"User-Agent": "pump-screener-flow/0.1"},
        )
        try:
            with urlopen(request, timeout=6) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            try:
                payload = json.loads(exc.read().decode("utf-8"))
                message = payload.get("msg", str(exc))
            except Exception:
                message = str(exc)
            raise ConnectionError(f"Binance rejected {symbol}: {message}") from exc
        except (URLError, TimeoutError) as exc:
            raise ConnectionError(f"Could not reach Binance Futures: {exc}") from exc
        if not isinstance(payload, list) or not payload:
            raise ConnectionError(f"No Binance Futures candle returned for {symbol}")
        return snapshot_from_kline(symbol, interval, payload[-1])


class FlowCard:
    def __init__(self, parent: tk.Widget, title: str) -> None:
        self.frame = tk.Frame(
            parent,
            bg=PANEL,
            highlightbackground=BORDER,
            highlightthickness=1,
            padx=10,
            pady=7,
        )
        self.frame.pack(fill="x", pady=(0, 7))
        header = tk.Frame(self.frame, bg=PANEL)
        header.pack(fill="x")
        tk.Label(
            header,
            text=title,
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI Semibold", 9),
        ).pack(side="left")
        self.fair_value = tk.StringVar(value="FV PRESSURE →")
        self.fair_value_label = tk.Label(
            header,
            textvariable=self.fair_value,
            bg=PANEL,
            fg=MUTED,
            font=("Segoe UI Semibold", 7),
        )
        self.fair_value_label.pack(side="left", padx=(9, 0))
        self.countdown = tk.StringVar(value="—")
        tk.Label(
            header,
            textvariable=self.countdown,
            bg=PANEL,
            fg=MUTED,
            font=("Segoe UI", 8),
        ).pack(side="right")

        summary = tk.Frame(self.frame, bg=PANEL)
        summary.pack(fill="x", pady=(4, 2))
        self.evaluation = tk.StringVar(value="Waiting for Binance…")
        self.evaluation_label = tk.Label(
            summary,
            textvariable=self.evaluation,
            bg=PANEL,
            fg=MUTED,
            anchor="w",
            font=("Segoe UI Semibold", 9),
        )
        self.evaluation_label.pack(side="left")
        self.return_value = tk.StringVar(value="—")
        self.return_label = tk.Label(
            summary,
            textvariable=self.return_value,
            bg=PANEL,
            fg=MUTED,
            font=("Consolas", 10, "bold"),
        )
        self.return_label.pack(side="right")

        self.flow_bar = tk.Canvas(
            self.frame, height=5, bg=RED, highlightthickness=0
        )
        self.flow_bar.pack(fill="x", pady=(4, 4))

        self.flow_text = tk.StringVar(value="Taker  B —  S —")
        tk.Label(
            self.frame,
            textvariable=self.flow_text,
            bg=PANEL,
            fg=TEXT,
            anchor="w",
            font=("Consolas", 8),
        ).pack(fill="x")
        self.maker_text = tk.StringVar(value="Maker  B —  S —")
        tk.Label(
            self.frame,
            textvariable=self.maker_text,
            bg=PANEL,
            fg=MUTED,
            anchor="w",
            font=("Consolas", 8),
        ).pack(fill="x", pady=(1, 0))

    def update(self, snapshot: FlowSnapshot) -> None:
        label, color = evaluate_flow(snapshot.return_pct, snapshot.taker_buy_pct)
        fair_value, fair_value_color = fair_value_pressure(
            snapshot.return_pct, snapshot.taker_buy_pct
        )
        self.evaluation.set(label)
        self.evaluation_label.configure(fg=color)
        self.fair_value.set(fair_value)
        self.fair_value_label.configure(fg=fair_value_color)
        self.return_value.set(f"{snapshot.return_pct:+.2f}%")
        self.return_label.configure(fg=GREEN if snapshot.return_pct >= 0 else RED)
        self.flow_text.set(
            f"Taker  B {snapshot.taker_buy_pct:5.1f}%   "
            f"S {snapshot.taker_sell_pct:5.1f}%"
        )
        self.maker_text.set(
            f"Maker  B {snapshot.taker_sell_pct:5.1f}%   "
            f"S {snapshot.taker_buy_pct:5.1f}%   ·   "
            f"Vol {format_quote_volume(snapshot.quote_volume)}"
        )
        remaining = max(0, (snapshot.close_time_ms - int(time.time() * 1000)) // 1000)
        self.countdown.set(f"{snapshot.last_price:.8g}  ·  {remaining // 60:02d}:{remaining % 60:02d}")
        self.frame.update_idletasks()
        width = max(1, self.flow_bar.winfo_width())
        buy_width = width * snapshot.taker_buy_pct / 100.0
        self.flow_bar.delete("all")
        self.flow_bar.create_rectangle(0, 0, buy_width, 5, fill=GREEN, outline="")
        self.flow_bar.create_rectangle(buy_width, 0, width, 5, fill=RED, outline="")


class FlowPopup:
    def __init__(self, symbol: str | None = None) -> None:
        self.fixed_symbol = symbol.upper() if symbol else None
        initial_title = active_chrome_title()
        initial_symbol = self.fixed_symbol or (
            extract_symbol_from_title(initial_title) if initial_title else None
        )
        self.root = tk.Tk()
        self.root.title("Binance Taker/Maker Flow")
        self.root.configure(bg=BG)
        self.root.geometry(self._initial_geometry(330, 285))
        self.root.minsize(310, 270)
        self.root.attributes("-topmost", True)
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        tkfont.nametofont("TkDefaultFont").configure(family="Segoe UI")
        container = tk.Frame(self.root, bg=BG, padx=10, pady=8)
        container.pack(fill="both", expand=True)
        top = tk.Frame(container, bg=BG)
        top.pack(fill="x", pady=(0, 7))
        self.symbol_text = tk.StringVar(value=initial_symbol or "FOCUS A CHROME TRADING TAB")
        tk.Label(
            top,
            textvariable=self.symbol_text,
            bg=BG,
            fg=TEXT,
            font=("Segoe UI Semibold", 15),
        ).pack(side="left")
        tk.Label(
            top,
            text="BINANCE · LIVE",
            bg=BG,
            fg=GREEN,
            font=("Segoe UI Semibold", 8),
        ).pack(side="right")
        self.source_text = tk.StringVar(
            value=("Locked symbol" if self.fixed_symbol else "Watching active Chrome tab title")
        )

        self.cards = {"5m": FlowCard(container, "5 MINUTE"), "15m": FlowCard(container, "15 MINUTE")}
        footer = tk.Frame(container, bg=BG)
        footer.pack(fill="x", pady=(1, 0))
        self.status = tk.StringVar(value="Waiting for a symbol…")
        tk.Label(
            footer,
            textvariable=self.status,
            bg=BG,
            fg=MUTED,
            anchor="w",
            font=("Segoe UI", 8),
        ).pack(side="left")
        tk.Label(
            footer,
            text="FV = flow proxy · 2s",
            bg=BG,
            fg=MUTED,
            font=("Segoe UI", 8),
        ).pack(side="right")

        self._symbol_lock = threading.Lock()
        self._symbol = initial_symbol
        self._results: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._client = BinanceFlowClient()
        self._worker = threading.Thread(target=self._poll_worker, daemon=True)
        self._worker.start()
        if initial_symbol:
            self._wake.set()
        self.root.after(250, self._tick)

    def _initial_geometry(self, width: int, height: int) -> str:
        user32 = ctypes.windll.user32
        screen_width = user32.GetSystemMetrics(0)
        return f"{width}x{height}+{max(10, screen_width - width - 24)}+48"

    def _set_symbol(self, symbol: str) -> None:
        with self._symbol_lock:
            if symbol == self._symbol:
                return
            self._symbol = symbol
        self.symbol_text.set(symbol)
        self.status.set("Switching Binance feed…")
        self._wake.set()

    def _poll_worker(self) -> None:
        while not self._stop.is_set():
            with self._symbol_lock:
                symbol = self._symbol
            if symbol:
                try:
                    snapshots = {
                        interval: self._client.fetch(symbol, interval)
                        for interval in ("5m", "15m")
                    }
                    self._results.put(("ok", symbol, snapshots))
                except Exception as exc:
                    self._results.put(("error", symbol, str(exc)))
            self._wake.wait(POLL_SECONDS)
            self._wake.clear()

    def _tick(self) -> None:
        if not self.fixed_symbol:
            title = active_chrome_title()
            detected = extract_symbol_from_title(title) if title else None
            if detected:
                self._set_symbol(detected)
                self.source_text.set(f"Chrome · {title[:58]}")
        try:
            while True:
                kind, symbol, payload = self._results.get_nowait()
                with self._symbol_lock:
                    current_symbol = self._symbol
                if symbol != current_symbol:
                    continue
                if kind == "ok":
                    for interval, snapshot in payload.items():
                        self.cards[interval].update(snapshot)
                    self.status.set(f"Live · {time.strftime('%H:%M:%S')}")
                else:
                    self.status.set(str(payload))
        except queue.Empty:
            pass
        if not self._stop.is_set():
            self.root.after(250, self._tick)

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()


def run_flow_popup(symbol: str | None = None) -> None:
    FlowPopup(symbol).run()


if __name__ == "__main__":
    run_flow_popup()
