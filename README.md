# Binance Altcoin Screener

[![Tests](https://github.com/pulkitpincha/binance-pump-screener/actions/workflows/tests.yml/badge.svg)](https://github.com/pulkitpincha/binance-pump-screener/actions/workflows/tests.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A long-only research screener for Binance USD-M perpetual altcoins. It detects
price/volume expansion and anchored-VWAP trend setups, records each signal as a
paper position, and measures subsequent price excursions. It never places an
order and does not require a Binance API key.

> [!WARNING]
> This project is research software, not financial advice. Results exclude
> fees, funding, spread, and slippage. Do not use it as an automated trading
> system.

## Features

- Live Binance USD-M Futures REST and WebSocket market data.
- `SPIKE_RVOL` and causal `AVWAP_TREND` signal families.
- Local TradingView review queue with `TRADE` and `IGNORE` annotations.
- SQLite paper-position tracking through fixed horizons from 5 minutes to 7 days.
- Historical backfill after restarts and conservative drawdown handling.
- Optional Windows flow popup using Binance 5-minute and 15-minute taker volume.
- No exchange credentials, account access, or order execution.

## Requirements

- Python 3.11 or newer.
- Internet access to Binance public Futures endpoints.
- Windows 10/11 is recommended and required for window focusing, audible alerts,
  active-Chrome-title detection, and the double-clickable flow popup. Silent
  collection and reports use platform-neutral Python APIs.

## Quick start

### Windows PowerShell

```powershell
git clone https://github.com/pulkitpincha/binance-pump-screener.git
cd binance-pump-screener
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
Copy-Item config.example.toml config.toml
screener silent
```

For the interactive review window, run `screener review` instead. Stop the
screener with `Ctrl+C`.

If PowerShell blocks virtual-environment activation, either allow locally
signed scripts for your user or call `.\.venv\Scripts\python.exe` directly.

### macOS or Linux

```bash
git clone https://github.com/pulkitpincha/binance-pump-screener.git
cd binance-pump-screener
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
cp config.example.toml config.toml
screener silent
```

The Windows-specific review focus/alert and Chrome-title flow features are not
available on macOS or Linux.

## Commands

```text
screener                 Start the screener with the review window
screener review          Start with the TradingView review window
screener silent          Collect without windows, sounds, or notifications
screener dashboard       Open the live TRADE/IGNORE decisions dashboard
screener flow            Follow a Binance symbol in the active Chrome title
screener flow --symbol NILUSDT
screener report          Print stored paper-position results
screener report --csv path/to/results.csv
```

The full CLI is also available as `pump-screener`:

```powershell
pump-screener --config .\config.toml run --silent
```

## Configuration

Copy `config.example.toml` to `config.toml` before running. The local config is
intentionally ignored by Git, so personal thresholds and runtime paths are not
published accidentally.

Key settings include:

| Section | Purpose |
| --- | --- |
| `signal` | Five-minute move, 24-hour move, RVOL, horizons, and drawdown limits |
| `avwap` | Anchor lookback, expansion filters, bands, and compression thresholds |
| `binance` | Public endpoints, quote asset, and excluded contracts |
| `storage` | Local SQLite database path |
| `review` | Review mode, host, and port |

The default public endpoints are `https://fapi.binance.com` and
`wss://fstream.binance.com/market/stream`. No API key is used. If Binance is
blocked by your network or jurisdiction, the application will not connect.

## Signal model

### `SPIKE_RVOL`

A signal requires all of the following:

1. Price rises by more than the configured threshold over a rolling five-minute window.
2. The magnitude of Binance's rolling 24-hour move remains below its configured cap.
3. Five-minute relative quote volume reaches the configured minimum.

RVOL is calculated as:

```text
current 5m quote volume / (rolling 24h quote volume / 288)
```

### `AVWAP_TREND`

The AVWAP anchor is chosen causally from the lowest completed one-minute candle
in the configured pre-signal base and then frozen. VWAP and its volume-weighted
standard-deviation bands use typical price and base volume.

- `INITIAL_EXPANSION` identifies the first completed one-minute expansion from a quiet base.
- `RE_EXPANSION` identifies a later expansion after a bullish AVWAP regime pulls
  back toward the midline, recovers, and compresses.
- A midline break or reclaim alone is not an entry.

Only one open research position for an exact symbol, screener type, and entry
type is allowed at a time.

## Research tracking

Every signal is stored as a paper long with entry metadata, maximum upside,
maximum drawdown, and returns at 5m, 15m, 30m, 60m, 4h, 12h, 24h, 48h, 72h,
and 7d. Live tick monitoring ends after four hours by default; future launches
use public historical candles to fill missed intervals and continue open
positions. Tracking ends at the configured maximum drawdown or after seven days.

Runtime data is stored in `data/screener.sqlite3`. The `data/`, `backups/`, and
`outputs/` directories are ignored by Git.

## Development

Install the development dependencies and run the test suite:

```powershell
python -m pip install -e ".[dev]"
python -m pytest -q
```

The GitHub Actions workflow runs the suite on supported Python versions for
every push and pull request. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
contribution workflow and [SECURITY.md](SECURITY.md) for vulnerability reports.

## License

Released under the [MIT License](LICENSE).
