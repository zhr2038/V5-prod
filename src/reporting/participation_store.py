"""Atomic state and append-only decisions for one shared virtual portfolio."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class ParticipationStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def transaction(self):
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS portfolio "
                "(id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL, state TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS decisions "
                "(sequence INTEGER PRIMARY KEY, observed_ts REAL NOT NULL, "
                "bar_ts INTEGER NOT NULL UNIQUE, event TEXT NOT NULL)"
            )
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def load(connection, identity: str):
        row = connection.execute("SELECT identity,state FROM portfolio WHERE id=1").fetchone()
        if row is None:
            return None
        if row[0] != identity:
            raise ValueError(
                "participation policy or decision code changed; use a new explicit cohort/state path "
                "instead of combining strategy versions or resetting the existing ledger"
            )
        return json.loads(row[1])

    @staticmethod
    def save(connection, *, identity: str, state: dict, event: dict):
        encoded_state = json.dumps(state, allow_nan=False, sort_keys=True, separators=(",", ":"))
        encoded_event = json.dumps(event, allow_nan=False, sort_keys=True, separators=(",", ":"))
        connection.execute(
            "INSERT INTO decisions(observed_ts,bar_ts,event) VALUES(?,?,?)",
            (event["observed_ts"], event["bar_ts"], encoded_event),
        )
        connection.execute(
            "INSERT INTO portfolio(id,identity,state) VALUES(1,?,?) "
            "ON CONFLICT(id) DO UPDATE SET identity=excluded.identity,state=excluded.state",
            (identity, encoded_state),
        )
