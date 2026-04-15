from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


@dataclass
class AccountState:
    """AccountState类 - 支持资金规模感知"""
    cash_usdt: float
    equity_peak_usdt: float
    scale_basis_usdt: float = 0.0  # 资金规模基准


class AccountStore:
    """AccountStore类 - 支持资金规模历史记录"""
    def __init__(self, path: str = "reports/positions.sqlite"):
        self.path = _resolve_path(path)
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
              equity_peak_usdt REAL NOT NULL,
              scale_basis_usdt REAL DEFAULT 0.0
            )
            """
        )
        # 迁移：添加新列（如果不存在）
        try:
            cur.execute("SELECT scale_basis_usdt FROM account_state LIMIT 1")
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE account_state ADD COLUMN scale_basis_usdt REAL DEFAULT 0.0")
        
        cur.execute(
            "INSERT OR IGNORE INTO account_state(k, cash_usdt, equity_peak_usdt, scale_basis_usdt) VALUES ('default', 100.0, 100.0, 0.0)"
        )
        con.commit()
        con.close()

    def get(self) -> AccountState:
        """Get"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute("SELECT cash_usdt, equity_peak_usdt, scale_basis_usdt FROM account_state WHERE k='default'")
        row = cur.fetchone()
        con.close()
        if not row:
            return AccountState(cash_usdt=100.0, equity_peak_usdt=100.0, scale_basis_usdt=0.0)
        return AccountState(
            cash_usdt=float(row[0]), 
            equity_peak_usdt=float(row[1]),
            scale_basis_usdt=float(row[2]) if row[2] else 0.0
        )

    def set(self, st: AccountState) -> None:
        """Set"""
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        cur.execute(
            "UPDATE account_state SET cash_usdt=?, equity_peak_usdt=?, scale_basis_usdt=? WHERE k='default'",
            (float(st.cash_usdt), float(st.equity_peak_usdt), float(st.scale_basis_usdt)),
        )
        con.commit()
        con.close()
    
    def update_scale_basis(self, new_basis: float, propagate_to_peak: bool = True) -> None:
        """更新资金规模基准
        
        Args:
            new_basis: 新的资金规模
            propagate_to_peak: 是否按比例调整峰值（用于加仓/减仓时）
        """
        con = sqlite3.connect(str(self.path))
        cur = con.cursor()
        
        if propagate_to_peak:
            # 读取旧值
            cur.execute("SELECT equity_peak_usdt, scale_basis_usdt FROM account_state WHERE k='default'")
            row = cur.fetchone()
            if row:
                old_peak, old_basis = float(row[0]), float(row[1]) if row[1] else 0.0
                if old_basis > 0 and new_basis > 0:
                    scale_ratio = new_basis / old_basis
                    new_peak = old_peak * scale_ratio
                    cur.execute(
                        "UPDATE account_state SET equity_peak_usdt=?, scale_basis_usdt=? WHERE k='default'",
                        (new_peak, new_basis)
                    )
                else:
                    cur.execute(
                        "UPDATE account_state SET scale_basis_usdt=? WHERE k='default'",
                        (new_basis,)
                    )
        else:
            cur.execute(
                "UPDATE account_state SET scale_basis_usdt=? WHERE k='default'",
                (new_basis,)
            )
        
        con.commit()
        con.close()
