from __future__ import annotations

import csv
from pathlib import Path
import sqlite3
from uuid import uuid4

from .models import ActiveEvent, Signal


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    screener_type TEXT NOT NULL DEFAULT 'SPIKE_RVOL',
    entry_type TEXT NOT NULL DEFAULT 'SPIKE_RVOL',
    signal_time_ms INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    return_5m_pct REAL NOT NULL,
    return_24h_pct REAL NOT NULL,
    rvol REAL NOT NULL,
    quote_volume_5m REAL NOT NULL,
    quote_volume_24h REAL NOT NULL,
    max_price REAL NOT NULL,
    min_price REAL NOT NULL,
    max_price_time_ms INTEGER NOT NULL,
    min_price_time_ms INTEGER NOT NULL,
    last_price REAL NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    completed_time_ms INTEGER,
    stop_reason TEXT,
    avwap_anchor_time_ms INTEGER,
    avwap_at_entry REAL,
    upper_band_at_entry REAL,
    review_status TEXT NOT NULL DEFAULT 'UNREVIEWED',
    decision_time_ms INTEGER,
    decision_price REAL
);

CREATE INDEX IF NOT EXISTS idx_events_symbol_time
ON events(symbol, signal_time_ms);

CREATE TABLE IF NOT EXISTS outcomes (
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    horizon_minutes INTEGER NOT NULL,
    observed_time_ms INTEGER NOT NULL,
    exit_price REAL NOT NULL,
    high_from_entry_pct REAL NOT NULL,
    low_from_entry_pct REAL NOT NULL,
    short_mae_pct REAL NOT NULL,
    short_mfe_pct REAL NOT NULL,
    short_return_pct REAL NOT NULL,
    max_drawdown_pct REAL NOT NULL DEFAULT 0,
    max_upside_pct REAL NOT NULL DEFAULT 0,
    long_return_pct REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(event_id, horizon_minutes)
);
"""


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Add research fields to databases created by older versions."""
        outcome_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(outcomes)").fetchall()
        }
        added_columns: list[str] = []
        for column in ("max_drawdown_pct", "max_upside_pct", "long_return_pct"):
            if column not in outcome_columns:
                self.connection.execute(
                    f"ALTER TABLE outcomes ADD COLUMN {column} REAL NOT NULL DEFAULT 0"
                )
                added_columns.append(column)

        if added_columns:
            # Older releases stored the same price path from a short perspective.
            # Convert those rows so existing research remains usable.
            self.connection.execute(
                """
                UPDATE outcomes SET
                    max_drawdown_pct = short_mfe_pct,
                    max_upside_pct = short_mae_pct,
                    long_return_pct = -short_return_pct
                """
            )
            self.connection.commit()

        event_columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(events)").fetchall()
        }
        event_definitions = {
            "screener_type": "TEXT NOT NULL DEFAULT 'SPIKE_RVOL'",
            "entry_type": "TEXT NOT NULL DEFAULT 'SPIKE_RVOL'",
            "stop_reason": "TEXT",
            "avwap_anchor_time_ms": "INTEGER",
            "avwap_at_entry": "REAL",
            "upper_band_at_entry": "REAL",
            "review_status": "TEXT NOT NULL DEFAULT 'UNREVIEWED'",
            "decision_time_ms": "INTEGER",
            "decision_price": "REAL",
        }
        for column, definition in event_definitions.items():
            if column not in event_columns:
                self.connection.execute(
                    f"ALTER TABLE events ADD COLUMN {column} {definition}"
                )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def create_event(self, signal: Signal) -> ActiveEvent:
        event_id = uuid4().hex
        values = (
            event_id,
            signal.symbol,
            signal.screener_type,
            signal.entry_type,
            signal.signal_time_ms,
            signal.entry_price,
            signal.return_5m_pct,
            signal.return_24h_pct,
            signal.rvol,
            signal.quote_volume_5m,
            signal.quote_volume_24h,
            signal.entry_price,
            signal.entry_price,
            signal.signal_time_ms,
            signal.signal_time_ms,
            signal.entry_price,
            signal.signal_time_ms,
            signal.avwap_anchor_time_ms,
            signal.avwap_at_entry,
            signal.upper_band_at_entry,
        )
        self.connection.execute(
            """
            INSERT INTO events (
                id, symbol, screener_type, entry_type, signal_time_ms,
                entry_price, return_5m_pct,
                return_24h_pct, rvol, quote_volume_5m, quote_volume_24h,
                max_price, min_price, max_price_time_ms, min_price_time_ms,
                last_price, last_seen_ms, avwap_anchor_time_ms,
                avwap_at_entry, upper_band_at_entry
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        self.connection.commit()
        return ActiveEvent(
            event_id=event_id,
            symbol=signal.symbol,
            screener_type=signal.screener_type,
            entry_type=signal.entry_type,
            signal_time_ms=signal.signal_time_ms,
            entry_price=signal.entry_price,
            max_price=signal.entry_price,
            min_price=signal.entry_price,
            max_price_time_ms=signal.signal_time_ms,
            min_price_time_ms=signal.signal_time_ms,
            last_price=signal.entry_price,
            last_seen_ms=signal.signal_time_ms,
            completed_horizons=set(),
            status="active",
        )

    def load_active_events(self) -> list[ActiveEvent]:
        outcome_rows = self.connection.execute(
            "SELECT event_id, horizon_minutes FROM outcomes"
        ).fetchall()
        completed: dict[str, set[int]] = {}
        for row in outcome_rows:
            completed.setdefault(row["event_id"], set()).add(row["horizon_minutes"])

        rows = self.connection.execute(
            "SELECT * FROM events WHERE status = 'active' ORDER BY signal_time_ms"
        ).fetchall()
        return [
            ActiveEvent(
                event_id=row["id"],
                symbol=row["symbol"],
                screener_type=row["screener_type"],
                entry_type=row["entry_type"],
                signal_time_ms=row["signal_time_ms"],
                entry_price=row["entry_price"],
                max_price=row["max_price"],
                min_price=row["min_price"],
                max_price_time_ms=row["max_price_time_ms"],
                min_price_time_ms=row["min_price_time_ms"],
                last_price=row["last_price"],
                last_seen_ms=row["last_seen_ms"],
                completed_horizons=completed.get(row["id"], set()),
                status=row["status"],
            )
            for row in rows
        ]

    def has_open_event(self, symbol: str, screener_type: str, entry_type: str) -> bool:
        """Treat deferred research positions as open until stop or final horizon."""
        row = self.connection.execute(
            """
            SELECT 1 FROM events
            WHERE symbol = ? AND screener_type = ? AND entry_type = ?
              AND status IN ('active', 'deferred')
            LIMIT 1
            """,
            (symbol, screener_type, entry_type),
        ).fetchone()
        return row is not None

    def update_event(self, event: ActiveEvent) -> None:
        self.connection.execute(
            """
            UPDATE events SET
                max_price = ?, min_price = ?, max_price_time_ms = ?,
                min_price_time_ms = ?, last_price = ?, last_seen_ms = ?
            WHERE id = ?
            """,
            (
                event.max_price,
                event.min_price,
                event.max_price_time_ms,
                event.min_price_time_ms,
                event.last_price,
                event.last_seen_ms,
                event.event_id,
            ),
        )

    def save_outcome(self, event: ActiveEvent, horizon_minutes: int, observed_time_ms: int) -> None:
        high_pct = (event.max_price / event.entry_price - 1.0) * 100.0
        low_pct = (event.min_price / event.entry_price - 1.0) * 100.0
        max_upside = max(high_pct, 0.0)
        max_drawdown = max(-low_pct, 0.0)
        long_return = (event.last_price - event.entry_price) / event.entry_price * 100.0
        self.connection.execute(
            """
            INSERT OR IGNORE INTO outcomes (
                event_id, horizon_minutes, observed_time_ms, exit_price,
                high_from_entry_pct, low_from_entry_pct, short_mae_pct,
                short_mfe_pct, short_return_pct, max_drawdown_pct,
                max_upside_pct, long_return_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                horizon_minutes,
                observed_time_ms,
                event.last_price,
                high_pct,
                low_pct,
                max_upside,
                max_drawdown,
                -long_return,
                max_drawdown,
                max_upside,
                long_return,
            ),
        )

    def complete_event(self, event: ActiveEvent, completed_time_ms: int) -> None:
        event.status = "complete"
        self.connection.execute(
            "UPDATE events SET status = 'complete', completed_time_ms = ? WHERE id = ?",
            (completed_time_ms, event.event_id),
        )

    def defer_event(self, event: ActiveEvent, deferred_time_ms: int) -> None:
        event.status = "deferred"
        self.connection.execute(
            "UPDATE events SET status = 'deferred', completed_time_ms = ? WHERE id = ?",
            (deferred_time_ms, event.event_id),
        )

    def stop_event(self, event: ActiveEvent, stopped_time_ms: int, reason: str) -> None:
        event.status = "stopped"
        self.connection.execute(
            """
            UPDATE events SET status = 'stopped', completed_time_ms = ?, stop_reason = ?
            WHERE id = ?
            """,
            (stopped_time_ms, reason, event.event_id),
        )

    def load_backfill_events(self, final_horizon_minutes: int) -> list[ActiveEvent]:
        outcome_rows = self.connection.execute(
            "SELECT event_id, horizon_minutes FROM outcomes"
        ).fetchall()
        completed: dict[str, set[int]] = {}
        for row in outcome_rows:
            completed.setdefault(row["event_id"], set()).add(row["horizon_minutes"])

        rows = self.connection.execute(
            """
            SELECT * FROM events
            WHERE status IN ('active', 'deferred', 'complete')
            ORDER BY symbol, signal_time_ms
            """
        ).fetchall()
        return [
            ActiveEvent(
                event_id=row["id"],
                symbol=row["symbol"],
                screener_type=row["screener_type"],
                entry_type=row["entry_type"],
                signal_time_ms=row["signal_time_ms"],
                entry_price=row["entry_price"],
                max_price=row["max_price"],
                min_price=row["min_price"],
                max_price_time_ms=row["max_price_time_ms"],
                min_price_time_ms=row["min_price_time_ms"],
                last_price=row["last_price"],
                last_seen_ms=row["last_seen_ms"],
                completed_horizons=completed.get(row["id"], set()),
                status=row["status"],
            )
            for row in rows
            if final_horizon_minutes not in completed.get(row["id"], set())
        ]

    def review_rows(self, session_start_ms: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT
                id, symbol, screener_type, entry_type, signal_time_ms,
                entry_price, return_5m_pct, return_24h_pct, rvol,
                avwap_anchor_time_ms, avwap_at_entry, upper_band_at_entry,
                last_price, max_price, min_price, status, review_status,
                decision_time_ms, decision_price
            FROM events
            WHERE signal_time_ms >= ?
            ORDER BY signal_time_ms DESC
            LIMIT 250
            """,
            (session_start_ms,),
        ).fetchall()

    def save_decision(self, event_id: str, decision: str, decision_time_ms: int) -> sqlite3.Row | None:
        normalized = decision.upper()
        if normalized not in {"TRADE", "IGNORE"}:
            raise ValueError("decision must be TRADE or IGNORE")
        self.connection.execute(
            """
            UPDATE events
            SET review_status = ?, decision_time_ms = ?, decision_price = last_price
            WHERE id = ? AND review_status = 'UNREVIEWED'
            """,
            (normalized, decision_time_ms, event_id),
        )
        self.connection.commit()
        return self.connection.execute(
            """
            SELECT id, review_status, decision_time_ms, decision_price
            FROM events WHERE id = ?
            """,
            (event_id,),
        ).fetchone()

    def commit(self) -> None:
        self.connection.commit()

    def report_rows(self) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT
                e.id AS event_id, e.symbol, e.screener_type, e.entry_type,
                e.signal_time_ms,
                strftime(
                    '%Y-%m-%dT%H:%M:%S+05:30',
                    (e.signal_time_ms / 1000) + 19800,
                    'unixepoch'
                ) AS entry_time_ist,
                e.entry_price, e.return_5m_pct,
                e.return_24h_pct, e.rvol, e.status, e.stop_reason,
                e.review_status, e.decision_time_ms,
                CASE WHEN e.decision_time_ms IS NOT NULL THEN
                    strftime(
                        '%Y-%m-%dT%H:%M:%S+05:30',
                        (e.decision_time_ms / 1000) + 19800,
                        'unixepoch'
                    )
                END AS decision_time_ist,
                CASE WHEN e.decision_time_ms IS NOT NULL THEN
                    (e.decision_time_ms - e.signal_time_ms) / 1000.0
                END AS decision_latency_seconds,
                e.decision_price,
                e.avwap_anchor_time_ms, e.avwap_at_entry, e.upper_band_at_entry,
                e.completed_time_ms AS monitoring_stop_time_ms,
                CASE WHEN e.completed_time_ms IS NOT NULL THEN
                    strftime(
                        '%Y-%m-%dT%H:%M:%S+05:30',
                        (e.completed_time_ms / 1000) + 19800,
                        'unixepoch'
                    )
                END AS monitoring_end_time_ist,
                e.last_price AS monitoring_stop_price,
                MAX((1.0 - e.min_price / e.entry_price) * 100.0, 0.0)
                    AS total_max_drawdown_pct,
                MAX((e.max_price / e.entry_price - 1.0) * 100.0, 0.0)
                    AS total_max_upside_pct,
                o.horizon_minutes, o.observed_time_ms,
                o.exit_price AS observed_price, o.max_drawdown_pct,
                o.max_upside_pct, o.long_return_pct
            FROM events e
            LEFT JOIN outcomes o ON o.event_id = e.id
            ORDER BY e.signal_time_ms DESC, o.horizon_minutes
            """
        ).fetchall()

    def export_csv(self, destination: Path) -> int:
        rows = self.report_rows()
        destination.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(rows[0].keys()) if rows else [
            "event_id", "symbol", "screener_type", "entry_type", "signal_time_ms",
            "entry_time_ist", "entry_price", "return_5m_pct",
            "return_24h_pct", "rvol", "status", "stop_reason",
            "review_status", "decision_time_ms", "decision_time_ist",
            "decision_latency_seconds", "decision_price",
            "avwap_anchor_time_ms", "avwap_at_entry", "upper_band_at_entry",
            "monitoring_stop_time_ms", "monitoring_end_time_ist",
            "monitoring_stop_price",
            "total_max_drawdown_pct", "total_max_upside_pct", "horizon_minutes",
            "observed_time_ms", "observed_price", "max_drawdown_pct",
            "max_upside_pct", "long_return_pct",
        ]
        with destination.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dict(row) for row in rows)
        return len(rows)
