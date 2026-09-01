# Contributing

Thanks for helping improve Binance Altcoin Screener.

## Development setup

1. Fork and clone the repository.
2. Create a virtual environment with Python 3.11 or newer.
3. Install the package with `python -m pip install -e ".[dev]"`.
4. Copy `config.example.toml` to `config.toml` if you need to run the live app.
5. Run `python -m pytest -q` before submitting a change.

## Pull requests

- Keep changes focused and describe their motivation and behavior.
- Add or update tests for behavior changes.
- Do not commit databases, generated reports, credentials, or personal config.
- Preserve the core safety property: this project must not place live orders.
- Call out changes to signal semantics or stored calculations explicitly.

Bug reports should include the operating system, Python version, command used,
and a minimal reproducible example. Remove account details, paths, and trading
records before sharing logs or screenshots.
