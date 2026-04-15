from __future__ import annotations

import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.execution.fill_store import FillRow, FillStore
from src.risk.negative_expectancy_cooldown import (
    NegativeExpectancyConfig,
    NegativeExpectancyCooldown,
)


def test_negative_expectancy_prefers_net_bps_from_fills_with_fee_conversion(tmp_path):
    fills_path = tmp_path / "fills.sqlite"
    store = FillStore(path=str(fills_path))
    now_ms = int(time.time() * 1000)
    store.upsert_many(
        [
            FillRow(
                inst_id="BTC-USDT",
                trade_id="buy-1",
                ts_ms=now_ms - 60_000,
                side="buy",
                fill_px="100",
                fill_sz="1",
                fee="-0.01",
                fee_ccy="BTC",
            ),
            FillRow(
                inst_id="BTC-USDT",
                trade_id="sell-1",
                ts_ms=now_ms,
                side="sell",
                fill_px="101",
                fill_sz="1",
                fee="-0.5",
                fee_ccy="USDT",
            ),
        ]
    )

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(tmp_path / "negative_expectancy_state.json"),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(fills_path),
            prefer_net_from_fills=True,
            fast_fail_max_hold_minutes=120,
        )
    )

    state = cooldown.refresh(force=True)
    stats = (state.get("stats") or {}).get("BTC/USDT") or {}

    assert stats["source"] == "fills"
    assert stats["gross_pnl_sum_usdt"] == 1.0
    assert stats["net_pnl_sum_usdt"] == -0.5
    assert stats["gross_expectancy_bps"] == 100.0
    assert stats["net_expectancy_bps"] == -50.0
    assert stats["net_expectancy_bps"] < stats["gross_expectancy_bps"]
    blocked = cooldown.is_blocked("BTC/USDT")
    assert blocked is not None
    assert blocked["metric_used"] == "net_expectancy_bps"
