from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import sys

from .app import run_screener
from .config import load_config
from .flow_popup import run_flow_popup
from .positions import run_positions_dashboard
from .storage import Store


IST = timezone(timedelta(hours=5, minutes=30), name="IST")


def _timestamp(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=IST).strftime("%Y-%m-%d %H:%M:%S")


def _report(config_path: Path | None, csv_path: Path | None) -> int:
    config = load_config(config_path)
    store = Store(config.storage.database_path)
    try:
        rows = store.report_rows()
        if csv_path is not None:
            count = store.export_csv(csv_path)
            print(f"Exported {count} rows to {csv_path}")
            return 0
        if not rows:
            print("No signal events recorded yet.")
            return 0
        print(
            "symbol     screener/entry type                  entry IST            "
            "entry price   5m%    24h%   rvol  horizon   drawdown/upside/return  status"
        )
        for row in rows:
            horizon = f"{row['horizon_minutes']}m" if row["horizon_minutes"] is not None else "pending"
            metrics = (
                f"{row['max_drawdown_pct']:.2f}% / {row['max_upside_pct']:.2f}% / "
                f"{row['long_return_pct']:.2f}%"
                if row["max_drawdown_pct"] is not None
                else "-"
            )
            print(
                f"{row['symbol']:<10} "
                f"{row['screener_type']}/{row['entry_type']:<30} "
                f"{_timestamp(row['signal_time_ms'])} "
                f"{row['entry_price']:<13.8g} {row['return_5m_pct']:>6.2f} "
                f"{row['return_24h_pct']:>7.2f} {row['rvol']:>6.2f} "
                f"{horizon:>8}   {metrics}  {row['status']}"
            )
        return 0
    finally:
        store.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Binance pump-event long-only research screener")
    parser.add_argument("--config", type=Path, help="TOML configuration file")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Run the live screener")
    mode = run.add_mutually_exclusive_group()
    mode.add_argument(
        "--review", action="store_true", help="Show the TradingView review window"
    )
    mode.add_argument(
        "--silent", action="store_true", help="Collect without pop-ups or alerts"
    )
    report = subparsers.add_parser("report", help="Print or export recorded outcomes")
    report.add_argument("--csv", type=Path, help="Export report rows to CSV")
    dashboard = subparsers.add_parser(
        "dashboard", help="Watch TRADE and IGNORE decisions in a live dashboard"
    )
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8766)
    flow = subparsers.add_parser(
        "flow", help="Show live Binance taker/maker flow for the active Chrome symbol"
    )
    flow.add_argument(
        "--symbol",
        help="Lock the popup to one Binance Futures symbol instead of following Chrome",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.command == "report":
        return _report(args.config, args.csv)
    if args.command == "flow":
        run_flow_popup(args.symbol)
        return 0
    config = load_config(args.config)
    if args.command == "dashboard":
        try:
            asyncio.run(run_positions_dashboard(config, args.host, args.port))
        except KeyboardInterrupt:
            print("Dashboard stopped.")
        return 0
    review_enabled = None
    if args.review:
        review_enabled = True
    elif args.silent:
        review_enabled = False
    try:
        asyncio.run(run_screener(config, review_enabled=review_enabled))
    except KeyboardInterrupt:
        print("Stopped.")
    except ConnectionError as exc:
        print(f"Connectivity error: {exc}")
        return 1
    return 0


def shortcut_main() -> int:
    """Memorable launcher that works without depending on the current directory."""
    arguments = sys.argv[1:]
    project_config = Path(__file__).resolve().parents[2] / "config.toml"
    base = ["--config", str(project_config)]
    if not arguments:
        return main([*base, "run", "--review"])

    command, *remainder = arguments
    command = command.lower()
    if command == "review":
        return main([*base, "run", "--review", *remainder])
    if command == "silent":
        return main([*base, "run", "--silent", *remainder])
    if command == "report":
        return main([*base, "report", *remainder])
    if command in {"dashboard", "positions"}:
        return main([*base, "dashboard", *remainder])
    if command in {"flow", "orderflow"}:
        return main([*base, "flow", *remainder])
    if command == "run":
        return main([*base, "run", *remainder])
    if command in {"-h", "--help"}:
        print("Usage: screener [review|silent|dashboard|flow|report] [options]")
        print("  screener          Start with the TradingView review window")
        print("  screener silent   Collect data without pop-ups or alerts")
        print("  screener dashboard Watch TRADE and IGNORE decisions")
        print("  screener flow      Watch Binance taker/maker flow for the active Chrome symbol")
        print("  screener report   Print recorded paper-trade results")
        return 0
    print(f"Unknown screener command: {command}")
    print("Use: screener [review|silent|dashboard|flow|report]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
