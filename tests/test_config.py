from pathlib import Path

from pump_screener.config import load_config


def test_relative_database_path_is_resolved_from_config_location(tmp_path: Path) -> None:
    config_path = tmp_path / "settings" / "config.toml"
    config_path.parent.mkdir()
    config_path.write_text(
        """
[signal]
horizons_minutes = [5, 240, 720]
live_monitoring_minutes = 240

[storage]
database_path = "data/research.sqlite3"
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.storage.database_path == config_path.parent / "data/research.sqlite3"
    assert config.signal.horizons_minutes == (5, 240, 720)
