from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime
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


def _ts_ms(raw: str) -> int:
    return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)


def _fill(
    *,
    trade_id: str,
    ts: str,
    side: str,
    px: float,
    qty: float,
    fee: float = 0.0,
) -> FillRow:
    return FillRow(
        inst_id="BTC-USDT",
        trade_id=trade_id,
        ts_ms=_ts_ms(ts),
        side=side,
        fill_px=str(px),
        fill_sz=str(qty),
        fee=str(fee),
        fee_ccy="USDT",
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


def test_negative_expectancy_includes_cycle_when_entry_before_lookback_but_close_inside(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fills_path = tmp_path / "reports" / "fills.sqlite"
    orders_path = tmp_path / "reports" / "orders.sqlite"
    state_path = tmp_path / "reports" / "negative_expectancy_state.json"
    store = FillStore(path=str(fills_path))
    store.upsert_many(
        [
            _fill(
                trade_id="btc-normal-buy",
                ts="2026-04-30T20:00:00Z",
                side="buy",
                px=76412.1,
                qty=0.00020939,
                fee=-0.01645,
            ),
            _fill(
                trade_id="btc-normal-sell",
                ts="2026-05-01T15:00:00Z",
                side="sell",
                px=78271.7,
                qty=0.00020939,
                fee=-0.01645,
            ),
            _fill(
                trade_id="btc-probe-loss-buy",
                ts="2026-05-01T17:00:00Z",
                side="buy",
                px=78277.4,
                qty=0.00013414,
                fee=-0.0105,
            ),
            _fill(
                trade_id="btc-probe-loss-sell",
                ts="2026-05-01T21:00:00Z",
                side="sell",
                px=77880.1,
                qty=0.00013414,
                fee=-0.0105,
            ),
            _fill(
                trade_id="btc-zero-close-buy",
                ts="2026-05-03T21:00:00Z",
                side="buy",
                px=78914.8,
                qty=0.00013306,
                fee=-0.0105,
            ),
            _fill(
                trade_id="btc-zero-close-sell",
                ts="2026-05-03T22:00:00Z",
                side="sell",
                px=78978.7,
                qty=0.00013306,
                fee=-0.0105,
            ),
        ]
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "config_fingerprint": "btc-window-fp",
                "release_start_ts": _ts_ms("2026-04-30T00:00:00Z"),
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "src.risk.negative_expectancy_cooldown.time.time",
        lambda: _ts_ms("2026-05-03T22:30:00Z") / 1000.0,
    )
    summaries_dir = tmp_path / "reports" / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    summaries_dir.joinpath("trades_roundtrips.csv").write_text(
        "\n".join(
            [
                "open_time_utc,close_time_utc,symbol,qty,entry_px,exit_px,net_pnl_usdt,net_bps",
                "2026-04-30T20:00:00Z,2026-05-01T15:00:00Z,BTC/USDT,0.00020939,76412.1,78271.7,0.3566,222.9",
                "2026-05-01T17:00:00Z,2026-05-01T21:00:00Z,BTC/USDT,0.00013414,78277.4,77880.1,-0.0743,-70.8",
                "2026-05-03T21:00:00Z,2026-05-03T22:00:00Z,BTC/USDT,0.00013306,78914.8,78978.7,-0.0125,-11.9",
            ]
        ),
        encoding="utf-8",
    )

    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=72,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(orders_path),
            fills_db_path=str(fills_path),
            prefer_net_from_fills=True,
            fast_fail_max_hold_minutes=360,
        )
    )
    cooldown.set_scope(whitelist_symbols=["BTC/USDT"], config_fingerprint="btc-window-fp")

    state = cooldown.refresh(force=True)
    stats = state["stats"]["BTC/USDT"]

    assert stats["lookback_filter_mode"] == "close_ts"
    assert stats["closed_cycles"] == 3
    assert stats["closed_cycles_included_by_close_ts"] == 3
    assert stats["closed_cycles_with_entry_before_window"] == 1
    assert stats["missing_entry_leg_count"] == 0
    assert stats["net_pnl_sum_usdt"] == pytest.approx(0.2698, abs=0.01)
    assert stats["net_pnl_sum_usdt"] > 0.20
    assert stats["net_expectancy_bps"] > 0.0
    assert stats["roundtrip_summary_net_bps"] > 0.0
    assert abs(stats["mismatch_bps"]) < 5.0
    assert state["negative_expectancy_net_bps"] == pytest.approx(stats["net_expectancy_bps"])
    assert state["roundtrip_summary_net_bps"] == pytest.approx(stats["roundtrip_summary_net_bps"])
    assert state["mismatch_bps"] == pytest.approx(stats["mismatch_bps"])
    assert "negative_expectancy_roundtrip_mismatch" not in "; ".join(state["warnings"])


def test_negative_expectancy_missing_entry_close_is_degraded_not_negative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fills_path = tmp_path / "fills.sqlite"
    state_path = tmp_path / "negative_expectancy_state.json"
    FillStore(path=str(fills_path)).upsert_many(
        [
            _fill(
                trade_id="btc-sell-without-entry",
                ts="2026-05-01T15:00:00Z",
                side="sell",
                px=78271.7,
                qty=0.00020939,
                fee=-0.01645,
            )
        ]
    )
    state_path.write_text(
        json.dumps(
            {
                "config_fingerprint": "missing-entry-fp",
                "release_start_ts": _ts_ms("2026-04-30T00:00:00Z"),
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "src.risk.negative_expectancy_cooldown.time.time",
        lambda: _ts_ms("2026-05-01T16:00:00Z") / 1000.0,
    )
    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=72,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(tmp_path / "orders.sqlite"),
            fills_db_path=str(fills_path),
            prefer_net_from_fills=True,
        )
    )
    cooldown.set_scope(whitelist_symbols=["BTC/USDT"], config_fingerprint="missing-entry-fp")

    with caplog.at_level(logging.WARNING, logger="src.risk.negative_expectancy_cooldown"):
        state = cooldown.refresh(force=True)

    stats = state["stats"]["BTC/USDT"]
    assert stats["closed_cycles"] == 0
    assert stats["missing_entry_leg_count"] == 1
    assert stats["net_pnl_sum_usdt"] == 0.0
    assert stats["degraded"] is True
    assert cooldown.is_blocked("BTC/USDT") is None
    assert any("missing_entry_leg_for_close_cycle" in item for item in state["warnings"])
    assert any("missing_entry_leg_for_close_cycle" in record.getMessage() for record in caplog.records)


def test_negative_expectancy_orders_fallback_filters_by_close_ts_not_entry_ts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orders_path = tmp_path / "orders.sqlite"
    state_path = tmp_path / "negative_expectancy_state.json"
    conn = sqlite3.connect(str(orders_path))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            state TEXT,
            acc_fill_sz REAL,
            avg_px REAL,
            fee REAL,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO orders(inst_id, side, state, acc_fill_sz, avg_px, fee, created_ts, updated_ts) VALUES (?,?,?,?,?,?,?,?)",
        [
            ("BTC-USDT", "buy", "FILLED", 1.0, 100.0, 0.0, _ts_ms("2026-04-30T20:00:00Z"), 0),
            ("BTC-USDT", "sell", "FILLED", 1.0, 102.0, 0.0, _ts_ms("2026-05-01T15:00:00Z"), 0),
        ],
    )
    conn.commit()
    conn.close()
    state_path.write_text(
        json.dumps(
            {
                "config_fingerprint": "orders-close-ts-fp",
                "release_start_ts": _ts_ms("2026-04-30T00:00:00Z"),
                "symbols": {},
                "stats": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "src.risk.negative_expectancy_cooldown.time.time",
        lambda: _ts_ms("2026-05-03T22:30:00Z") / 1000.0,
    )
    cooldown = NegativeExpectancyCooldown(
        NegativeExpectancyConfig(
            enabled=True,
            lookback_hours=72,
            min_closed_cycles=1,
            expectancy_threshold_bps=0.0,
            state_path=str(state_path),
            orders_db_path=str(orders_path),
            fills_db_path=str(tmp_path / "fills.sqlite"),
            prefer_net_from_fills=False,
        )
    )
    cooldown.set_scope(whitelist_symbols=["BTC/USDT"], config_fingerprint="orders-close-ts-fp")

    state = cooldown.refresh(force=True)
    stats = state["stats"]["BTC/USDT"]

    assert stats["source"] == "orders"
    assert stats["lookback_filter_mode"] == "close_ts"
    assert stats["closed_cycles"] == 1
    assert stats["closed_cycles_with_entry_before_window"] == 1
    assert stats["net_pnl_sum_usdt"] == pytest.approx(2.0)
    assert stats["net_expectancy_bps"] == pytest.approx(200.0)


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


def test_negative_expectancy_zero_release_start_ts_recovers_with_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
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

    with caplog.at_level(logging.WARNING, logger="src.risk.negative_expectancy_cooldown"):
        state = cooldown.refresh(force=True)

    assert state["config_fingerprint"] == "same-scope-fp"
    assert state["release_start_ts"] == 2_000_000_000
    assert state["release_start_ts_status"] == "recovered"
    assert state["symbols"] == {}
    assert state["stats"] == {}
    assert any("negative_expectancy_release_start_ts_recovered" in warning for warning in state["warnings"])
    assert any("negative_expectancy_release_start_ts_recovered" in record.getMessage() for record in caplog.records)


def test_negative_expectancy_not_observable_marker_recovers_then_stays_quiet(
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

    assert state["release_start_ts"] == 2_000_000_000
    assert state["release_start_ts_status"] == "recovered"
    assert any("negative_expectancy_release_start_ts_recovered" in item for item in state["warnings"])
    assert any("negative_expectancy_release_start_ts_recovered" in record.getMessage() for record in caplog.records)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="src.risk.negative_expectancy_cooldown"):
        state = cooldown.refresh(force=True)

    assert state["release_start_ts"] == 2_000_000_000
    assert state["warnings"] == []
    assert not [
        record
        for record in caplog.records
        if "negative_expectancy_release_start_ts" in record.getMessage()
    ]
