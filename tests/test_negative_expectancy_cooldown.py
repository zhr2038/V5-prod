import sqlite3
import time
from pathlib import Path

from src.risk.negative_expectancy_cooldown import (
    NegativeExpectancyConfig,
    NegativeExpectancyCooldown,
)


def _prepare_orders_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            acc_fill_sz REAL,
            avg_px REAL,
            updated_ts INTEGER
        )
        """
    )
    now_ms = int(time.time() * 1000)
    rows = [
        ("BTC-USDT", "buy", "OPEN_LONG", "FILLED", 1.0, 100.0, now_ms - 60_000),
        ("BTC-USDT", "sell", "CLOSE_LONG", "FILLED", 1.0, 99.0, now_ms - 30_000),
    ]
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def test_negative_expectancy_refresh_persists_stats_and_bps(tmp_path: Path):
    orders_db = tmp_path / "orders.sqlite"
    state_path = tmp_path / "negative_expectancy.json"
    _prepare_orders_db(orders_db)

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_usdt=0.0,
            cooldown_hours=24,
            state_path=str(state_path),
            orders_db_path=str(orders_db),
        )
    )

    state = cooldown.refresh(force=True)
    stats = state["stats"]["BTC/USDT"]

    assert stats["closed_cycles"] == 1
    assert stats["pnl_sum_usdt"] == -1.0
    assert stats["closed_notional_usdt"] == 100.0
    assert stats["expectancy_usdt"] == -1.0
    assert stats["expectancy_bps"] == -100.0

    blocked = cooldown.is_blocked("BTC/USDT")
    assert blocked is not None
    assert blocked["expectancy_bps"] == -100.0

    symbol_stats = cooldown.get_symbol_stats("BTC-USDT")
    assert symbol_stats is not None
    assert symbol_stats["cooldown_active"] is True
    assert symbol_stats["expectancy_bps"] == -100.0
