from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class AccountState:
    """AccountState类"""
    cash_usdt: float
    equity_peak_usdt: float


class AccountStore:
    """AccountStore类"""
    def __init__(self, path: str = "reports/positions.sqlite"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS account_state (
              k TEXT PRIMARY KEY,
              cash_usdt REAL NOT NULL,
              equity_peak_usdt REAL NOT NULL
            )
            """
        )
        cur.execute(
            "INSERT OR IGNORE INTO account_state(k, cash_usdt, equity_peak_usdt) VALUES ('default', 100.0, 100.0)"
        )
        con.commit()
        con.close()

    def get(self) -> AccountState:
        """Get"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT cash_usdt, equity_peak_usdt FROM account_state WHERE k='default'")
        row = cur.fetchone()
        con.close()
        if not row:
            return AccountState(cash_usdt=100.0, equity_peak_usdt=100.0)
        return AccountState(cash_usdt=float(row[0]), equity_peak_usdt=float(row[1]))

    def set(self, st: AccountState) -> None:
        """Set"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "UPDATE account_state SET cash_usdt=?, equity_peak_usdt=? WHERE k='default'",
            (float(st.cash_usdt), float(st.equity_peak_usdt)),
        )
        con.commit()
        con.close()
