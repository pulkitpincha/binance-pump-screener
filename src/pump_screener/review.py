from __future__ import annotations

import asyncio
import ctypes
from ctypes import wintypes
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
import webbrowser

from aiohttp import web

from .config import ReviewConfig
from .models import ActiveEvent
from .storage import Store


LOGGER = logging.getLogger(__name__)


class ReviewServer:
    """A non-blocking local review surface; scanner state never depends on it."""

    def __init__(self, store: Store, config: ReviewConfig, session_start_ms: int) -> None:
        self.store = store
        self.config = config
        self.session_start_ms = session_start_ms
        self.url = f"http://{config.host}:{config.port}/"
        self._runner: web.AppRunner | None = None
        self._window_launched = False
        self._window_lock = threading.Lock()

    async def start(self) -> bool:
        app = web.Application()
        app.router.add_get("/", self._index)
        app.router.add_get("/api/signals", self._signals)
        app.router.add_post("/api/signals/{event_id}/decision", self._decision)
        app.router.add_get("/api/health", self._health)
        self._runner = web.AppRunner(app, access_log=None)
        try:
            await self._runner.setup()
            site = web.TCPSite(self._runner, self.config.host, self.config.port)
            await site.start()
        except Exception:
            LOGGER.exception(
                "Review window could not start on %s:%d; screening will continue silently",
                self.config.host,
                self.config.port,
            )
            if self._runner is not None:
                await self._runner.cleanup()
                self._runner = None
            return False
        LOGGER.info("Signal review window ready at %s", self.url)
        return True

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    def notify(self, event: ActiveEvent) -> None:
        """Schedule UI launch/focus without awaiting a human or blocking prices."""
        del event
        asyncio.create_task(asyncio.to_thread(self._show_window))

    async def _index(self, request: web.Request) -> web.StreamResponse:
        del request
        return web.FileResponse(Path(__file__).with_name("review.html"))

    async def _signals(self, request: web.Request) -> web.Response:
        del request
        payload = []
        for row in self.store.review_rows(self.session_start_ms):
            item = dict(row)
            item["tradingview_symbol"] = f"BINANCE:{row['symbol']}.P"
            payload.append(item)
        return web.json_response(payload)

    async def _decision(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
            row = self.store.save_decision(
                request.match_info["event_id"],
                str(body.get("decision", "")),
                int(time.time() * 1000),
            )
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            return web.json_response({"error": str(exc)}, status=400)
        if row is None:
            return web.json_response({"error": "signal not found"}, status=404)
        return web.json_response(dict(row))

    async def _health(self, request: web.Request) -> web.Response:
        del request
        return web.json_response({"ok": True, "session_start_ms": self.session_start_ms})

    def _show_window(self) -> None:
        with self._window_lock:
            try:
                if not self._window_launched:
                    edge = _find_edge()
                    if edge is not None:
                        subprocess.Popen(
                            [str(edge), f"--app={self.url}", "--new-window"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    else:
                        webbrowser.open(self.url, new=1, autoraise=True)
                    self._window_launched = True

                focused = _focus_review_window(wait_seconds=3.0)
                if not focused and os.name == "nt":
                    import winsound

                    winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
            except Exception:
                self._window_launched = False
                LOGGER.exception("Could not show the review window; screening will continue")


def _find_edge() -> Path | None:
    executable = shutil.which("msedge")
    if executable:
        return Path(executable)
    candidates = [
        Path(os.environ.get("PROGRAMFILES(X86)", ""))
        / "Microsoft/Edge/Application/msedge.exe",
        Path(os.environ.get("PROGRAMFILES", ""))
        / "Microsoft/Edge/Application/msedge.exe",
        Path(os.environ.get("LOCALAPPDATA", ""))
        / "Microsoft/Edge/Application/msedge.exe",
    ]
    return next((path for path in candidates if path.is_file()), None)


def _focus_review_window(wait_seconds: float = 0.0) -> bool:
    if os.name != "nt":
        return False
    user32 = ctypes.windll.user32
    found: list[int] = []
    callback_type = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)

    def enumerate_window(hwnd: int, lparam: int) -> bool:
        del lparam
        if not user32.IsWindowVisible(hwnd):
            return True
        length = user32.GetWindowTextLengthW(hwnd)
        if not length:
            return True
        title = ctypes.create_unicode_buffer(length + 1)
        user32.GetWindowTextW(hwnd, title, length + 1)
        if "Signal Review" in title.value:
            found.append(hwnd)
            return False
        return True

    deadline = time.monotonic() + wait_seconds
    while True:
        found.clear()
        callback = callback_type(enumerate_window)
        user32.EnumWindows(callback, 0)
        if found:
            hwnd = found[0]
            user32.ShowWindow(hwnd, 9)
            user32.FlashWindow(hwnd, True)
            return bool(user32.SetForegroundWindow(hwnd))
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.15)
