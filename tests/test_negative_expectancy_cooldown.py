from __future__ import annotations

import json
import logging
import time
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.execution.fill_store import FillRow, FillStore
from src.risk.negative_expectancy_cooldown import (
    NegativeExpectancyConfig,
    NegativeExpectancyCooldown,
)


def _write_round_trip(
    store: FillStore,
    *,
    inst_id: str,
    buy_px: float,
    sell_px: float,
    base_ts_ms: int,
) -> None:
    store.upsert_many(
        [
            FillRow(
                inst_id=inst_id,
                trade_id=f"{inst_id}-buy",
                ts_ms=base_ts_ms,
                side="buy",
                fill_px=str(buy_px),
                fill_sz="1",
                fee="0",
                fee_ccy="USDT",
            ),
            FillRow(
                inst_id=inst_id,
                trade_id=f"{inst_id}-sell",
                ts_ms=base_ts_ms + 60_000,
                side="sell",
                fill_px=str(sell_px),
                fill_sz="1",
                fee="0",
                fee_ccy="USDT",
            ),
        ]
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


def test_negative_expectancy_scope_filters_to_whitelist_positions_and_managed_symbols(tmp_path: Path) -> None:
    fills_path = tmp_path / "fills.sqlite"
    state_path = tmp_path / "negative_expectancy_state.json"
    store = FillStore(path=str(fills_path))
    now_ms = int(time.time() * 1000)
    _write_round_trip(store, inst_id="BTC-USDT", buy_px=100.0, sell_px=99.0, base_ts_ms=now_ms - 300_000)
    _write_round_trip(store, inst_id="ETH-USDT", buy_px=200.0, sell_px=198.0, base_ts_ms=now_ms - 240_000)
    _write_round_trip(store, inst_id="SOL-USDT", buy_px=50.0, sell_px=49.0, base_ts_ms=now_ms - 180_000)
    _write_round_trip(store, inst_id="DOGE-USDT", buy_px=10.0, sell_px=9.0, base_ts_ms=now_ms - 120_000)
    state_path.write_text(
        json.dumps(
            {
                "config_fingerprint": "scope-fp",
                "release_start_ts": now_ms - 600_000,
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(fills_path),
            prefer_net_from_fills=True,
            fast_fail_max_hold_minutes=120,
        )
    )
    cooldown.set_scope(
        whitelist_symbols=["BTC/USDT"],
        open_position_symbols=["ETH/USDT"],
        managed_symbols=["SOL/USDT"],
        config_fingerprint="scope-fp",
    )

    state = cooldown.refresh(force=True)

    assert state["config_fingerprint"] == "scope-fp"
    assert state["whitelist_symbols"] == ["BTC/USDT"]
    assert set(state["scope_symbols"]) == {"BTC/USDT", "ETH/USDT", "SOL/USDT"}
    assert set((state.get("stats") or {}).keys()) == {"BTC/USDT", "ETH/USDT", "SOL/USDT"}
    assert "DOGE/USDT" not in (state.get("stats") or {})


def test_negative_expectancy_fingerprint_change_resets_legacy_state_scope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_path = tmp_path / "negative_expectancy_state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_ts_ms": 111,
                "symbols": {
                    "DOGE/USDT": {
                        "cooldown_until_ms": 9999999999999,
                        "closed_cycles": 2,
                    }
                },
                "stats": {
                    "DOGE/USDT": {
                        "closed_cycles": 2,
                        "net_expectancy_bps": -100.0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.risk.negative_expectancy_cooldown.time.time", lambda: 2_000_000.0)

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(tmp_path / "fills.sqlite"),
            prefer_net_from_fills=True,
            fast_fail_max_hold_minutes=120,
        )
    )
    cooldown.set_scope(
        whitelist_symbols=["BTC/USDT"],
        config_fingerprint="new-scope-fp",
    )

    state = cooldown.refresh(force=True)

    assert state["config_fingerprint"] == "new-scope-fp"
    assert state["release_start_ts"] == 2_000_000_000
    assert state["whitelist_symbols"] == ["BTC/USDT"]
    assert state["symbols"] == {}
    assert state["stats"] == {}


def test_negative_expectancy_fingerprint_change_updates_release_start_ts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_path = tmp_path / "negative_expectancy_state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_ts_ms": 111,
                "config_fingerprint": "old-scope-fp",
                "release_start_ts": 1_000_000_000,
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.risk.negative_expectancy_cooldown.time.time", lambda: 2_000_000.0)

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(tmp_path / "fills.sqlite"),
        )
    )
    cooldown.set_scope(whitelist_symbols=["BTC/USDT"], config_fingerprint="new-scope-fp")

    state = cooldown.refresh(force=True)

    assert state["config_fingerprint"] == "new-scope-fp"
    assert state["release_start_ts"] == 2_000_000_000
    assert state["release_start_ts_status"] == "ok"


def test_negative_expectancy_same_fingerprint_keeps_release_start_ts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_path = tmp_path / "negative_expectancy_state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_ts_ms": 111,
                "config_fingerprint": "same-scope-fp",
                "release_start_ts": 1_500_000_000,
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.risk.negative_expectancy_cooldown.time.time", lambda: 2_000_000.0)

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(tmp_path / "fills.sqlite"),
        )
    )
    cooldown.set_scope(whitelist_symbols=["BTC/USDT"], config_fingerprint="same-scope-fp")

    state = cooldown.refresh(force=True)

    assert state["config_fingerprint"] == "same-scope-fp"
    assert state["release_start_ts"] == 1_500_000_000
    assert state["release_start_ts_status"] == "ok"


def test_negative_expectancy_zero_release_start_ts_writes_warning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_path = tmp_path / "negative_expectancy_state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_ts_ms": 111,
                "config_fingerprint": "same-scope-fp",
                "release_start_ts": 0,
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.risk.negative_expectancy_cooldown.time.time", lambda: 2_000_000.0)

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(tmp_path / "fills.sqlite"),
        )
    )
    cooldown.set_scope(whitelist_symbols=["BTC/USDT"], config_fingerprint="same-scope-fp")

    state = cooldown.refresh(force=True)

    assert state["config_fingerprint"] == "same-scope-fp"
    assert state["release_start_ts"] == "not_observable"
    assert state["release_start_ts_status"] == "not_observable"
    assert any("negative_expectancy_release_start_ts_not_observable" in warning for warning in state["warnings"])


def test_negative_expectancy_not_observable_marker_does_not_log_every_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state_path = tmp_path / "negative_expectancy_state.json"
    state_path.write_text(
        json.dumps(
            {
                "updated_ts_ms": 1_999_000_000,
                "config_fingerprint": "scope-fp",
                "release_start_ts": "not_observable",
                "release_start_ts_status": "not_observable",
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.risk.negative_expectancy_cooldown.time.time", lambda: 2_000_000.0)

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=24,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(tmp_path / "fills.sqlite"),
            prefer_net_from_fills=True,
            fast_fail_max_hold_minutes=120,
        )
    )
    cooldown.set_scope(
        whitelist_symbols=["BTC/USDT"],
        config_fingerprint="scope-fp",
    )

    with caplog.at_level(logging.WARNING, logger="src.risk.negative_expectancy_cooldown"):
        state = cooldown.refresh(force=True)

    assert state["release_start_ts"] == "not_observable"
    assert state["release_start_ts_status"] == "not_observable"
    assert any("negative_expectancy_release_start_ts_not_observable" in item for item in state["warnings"])
    assert not [
        record
        for record in caplog.records
        if "negative_expectancy_release_start_ts_not_observable" in record.getMessage()
    ]
