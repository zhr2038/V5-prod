from __future__ import annotations

import csv
import json
import sqlite3
from datetime import UTC, datetime

from configs.schema import AppConfig
from src.reporting.cost_probe_plan import (
    CostProbeEngine,
    build_cost_probe_dry_run_plan,
    write_cost_probe_dry_run_outputs,
)

GENERATED_AT = datetime(2026, 6, 18, 12, tzinfo=UTC)


def test_cost_probe_plan_is_blocked_when_prod_switches_are_closed(tmp_path):
    cfg = AppConfig()
    cfg.execution.cost_bootstrap_enabled = False
    cfg.execution.cost_probe_enabled = False
    cfg.execution.cost_probe_dry_run = True
    cfg.execution.cost_probe_live_enabled = False

    rows, summary = build_cost_probe_dry_run_plan(
        cfg,
        generated_at=GENERATED_AT,
    )
    written_paths = write_cost_probe_dry_run_outputs(
        rows,
        summary,
        plan_path=tmp_path / "cost_probe_plan.csv",
        summary_path=tmp_path / "cost_probe_summary.json",
        orders_path=tmp_path / "cost_probe_orders.csv",
        roundtrips_path=tmp_path / "cost_probe_roundtrips.csv",
        runtime_guard_path=tmp_path / "runtime_cost_guard.csv",
        disagreement_path=tmp_path / "cost_disagreement.csv",
    )
    plan_path = written_paths["plan_path"]
    summary_path = written_paths["summary_path"]

    assert summary["state"] == "DISABLED"
    assert summary["no_order_submitted"] is True
    assert summary["live_enabled"] is False
    assert "cost_probe_enabled_false" in summary["blocked_reasons"]
    assert "cost_bootstrap_enabled_false" in summary["blocked_reasons"]
    written_rows = list(csv.DictReader(plan_path.open(encoding="utf-8")))
    assert written_rows
    assert {row["plan_status"] for row in written_rows} == {"blocked"}
    assert {row["no_order_submitted"] for row in written_rows} == {"True"}
    assert json.loads(summary_path.read_text(encoding="utf-8"))["state"] == "DISABLED"


def test_cost_probe_plan_ready_requires_dry_run_and_live_disabled():
    cfg = AppConfig()
    cfg.execution.cost_bootstrap_enabled = True
    cfg.execution.cost_probe_enabled = True
    cfg.execution.cost_probe_dry_run = True
    cfg.execution.cost_probe_live_enabled = False
    cfg.execution.cost_probe_use_exchange_min_notional = False
    cfg.execution.cost_probe_symbols = ["btcusdt", "ETH-USDT", "ETH/USDT"]
    cfg.execution.cost_probe_max_orders_per_day = 4
    cfg.execution.cost_probe_max_roundtrips_per_symbol_per_day = 1
    cfg.execution.cost_probe_max_notional_usdt = 5.0

    rows, summary = build_cost_probe_dry_run_plan(
        cfg,
        generated_at=GENERATED_AT,
    )

    assert summary["state"] == "DRY_RUN_PLAN_READY"
    assert summary["planned_symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert summary["blocked_symbols"] == []
    assert summary["no_order_submitted"] is True
    assert {row["plan_status"] for row in rows} == {"planned"}
    assert {row["entry_intent"] for row in rows} == {"DRY_RUN_ENTRY_ONLY_NO_ORDER"}
    assert {row["exit_intent"] for row in rows} == {"DRY_RUN_IMMEDIATE_FLAT_NO_ORDER"}


def test_cost_probe_plan_treats_max_orders_as_entry_exit_order_budget():
    cfg = AppConfig()
    cfg.execution.cost_bootstrap_enabled = True
    cfg.execution.cost_probe_enabled = True
    cfg.execution.cost_probe_dry_run = True
    cfg.execution.cost_probe_live_enabled = False
    cfg.execution.cost_probe_use_exchange_min_notional = False
    cfg.execution.cost_probe_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    cfg.execution.cost_probe_max_orders_per_day = 2
    cfg.execution.cost_probe_max_roundtrips_per_symbol_per_day = 1
    cfg.execution.cost_probe_max_notional_usdt = 5.0

    rows, summary = build_cost_probe_dry_run_plan(
        cfg,
        generated_at=GENERATED_AT,
    )

    assert summary["orders_per_roundtrip"] == 2
    assert summary["available_order_slots"] == 2
    assert summary["planned_symbols"] == ["BTC/USDT"]
    assert summary["blocked_symbols"] == ["ETH/USDT", "SOL/USDT"]
    blocked_rows = [row for row in rows if row["plan_status"] == "blocked"]
    assert all("daily_order_limit_exceeded" in row["blocked_reasons"] for row in blocked_rows)


def test_cost_probe_engine_writes_guarded_read_only_artifacts(tmp_path):
    cfg = _ready_cost_probe_config()
    _write_clean_runtime_state(tmp_path)

    engine = CostProbeEngine(
        cfg,
        reports_dir=tmp_path / "out",
        generated_at=GENERATED_AT,
        project_root=tmp_path,
    )
    payload = engine.build()

    assert payload["summary"]["state"] == "DRY_RUN_PLAN_READY"
    assert payload["summary"]["runtime_blockers"] == []
    assert payload["summary"]["no_order_submitted"] is True
    assert payload["summary"]["live_enabled"] is False
    assert {row["status"] for row in payload["guard_rows"]} == {"PASS"}
    assert {row["order_status"] for row in payload["order_rows"]} == {"not_submitted"}
    assert {row["live_order_effect"] for row in payload["order_rows"]} == {
        "none_read_only_dry_run_plan"
    }

    written_paths = engine.write()
    assert set(written_paths) == {
        "plan_path",
        "summary_path",
        "orders_path",
        "roundtrips_path",
        "runtime_guard_path",
        "disagreement_path",
    }
    assert all(path.exists() for path in written_paths.values())
    order_rows = list(csv.DictReader(written_paths["orders_path"].open(encoding="utf-8")))
    guard_rows = list(
        csv.DictReader(written_paths["runtime_guard_path"].open(encoding="utf-8"))
    )
    assert len(order_rows) == 2
    assert {row["no_order_submitted"] for row in order_rows} == {"True"}
    assert {row["status"] for row in guard_rows} == {"PASS"}
    assert json.loads(written_paths["summary_path"].read_text(encoding="utf-8"))[
        "state"
    ] == "DRY_RUN_PLAN_READY"


def test_cost_probe_engine_blocks_when_kill_switch_is_enabled(tmp_path):
    cfg = _ready_cost_probe_config()
    _write_clean_runtime_state(tmp_path, kill_switch_enabled=True)

    engine = CostProbeEngine(
        cfg,
        reports_dir=tmp_path / "out",
        generated_at=GENERATED_AT,
        project_root=tmp_path,
    )
    payload = engine.build()

    assert payload["summary"]["state"] == "DISABLED"
    assert "kill_switch_enabled" in payload["summary"]["runtime_blockers"]
    assert "kill_switch_enabled" in payload["summary"]["blocked_reasons"]
    assert {row["plan_status"] for row in payload["plan_rows"]} == {"blocked"}


def test_cost_probe_engine_blocks_on_dirty_order_or_position_state(tmp_path):
    cfg = _ready_cost_probe_config()
    _write_clean_runtime_state(tmp_path)
    _write_open_order(tmp_path / "runtime" / "orders.sqlite")
    _write_position(tmp_path / "runtime" / "positions.sqlite")

    engine = CostProbeEngine(
        cfg,
        reports_dir=tmp_path / "out",
        generated_at=GENERATED_AT,
        project_root=tmp_path,
    )
    payload = engine.build()

    assert payload["summary"]["state"] == "DISABLED"
    assert "order_store_open_orders" in payload["summary"]["runtime_blockers"]
    assert "existing_position_present" in payload["summary"]["runtime_blockers"]
    assert "position_state_dirty" in payload["summary"]["runtime_blockers"]


def test_cost_probe_engine_blocks_cooldown_but_can_plan_next_symbol(tmp_path):
    cfg = _ready_cost_probe_config()
    cfg.execution.cost_probe_symbols = ["BTC/USDT", "ETH/USDT"]
    cfg.execution.cost_probe_max_orders_per_day = 4
    _write_clean_runtime_state(tmp_path)
    reports_dir = tmp_path / "out"
    _write_cost_probe_order_history(
        reports_dir / "cost_probe_orders.csv",
        [
            {
                "generated_at": "2026-06-18T11:30:00Z",
                "symbol": "BTC/USDT",
                "leg": "entry",
                "side": "buy",
                "intent": "live_probe_entry",
                "order_status": "filled",
                "dry_run": False,
                "live_enabled": True,
                "no_order_submitted": False,
                "live_order_effect": "live_cost_probe_order",
            }
        ],
    )

    engine = CostProbeEngine(
        cfg,
        reports_dir=reports_dir,
        generated_at=GENERATED_AT,
        project_root=tmp_path,
    )
    payload = engine.build()

    assert payload["summary"]["state"] == "DRY_RUN_PLAN_READY"
    assert payload["summary"]["planned_symbols"] == ["ETH/USDT"]
    assert payload["summary"]["daily_order_used_count"] == 1
    assert payload["summary"]["available_order_slots"] == 3
    assert payload["summary"]["symbol_runtime_blockers"] == {
        "BTC/USDT": ["cost_probe_cooldown_active"]
    }


def test_cost_probe_engine_blocks_daily_loss_limit(tmp_path):
    cfg = _ready_cost_probe_config()
    _write_clean_runtime_state(tmp_path)
    reports_dir = tmp_path / "out"
    _write_cost_probe_roundtrip_history(
        reports_dir / "cost_probe_roundtrips.csv",
        [
            {
                "generated_at": "2026-06-18T11:10:00Z",
                "symbol": "BTC/USDT",
                "roundtrip_status": "closed",
                "entry_order_status": "filled",
                "exit_order_status": "filled",
                "no_order_submitted": False,
                "live_order_effect": "live_cost_probe_roundtrip",
                "net_pnl_usdt": -1.25,
            }
        ],
    )

    engine = CostProbeEngine(
        cfg,
        reports_dir=reports_dir,
        generated_at=GENERATED_AT,
        project_root=tmp_path,
    )
    payload = engine.build()

    assert payload["summary"]["state"] == "DISABLED"
    assert "daily_loss_limit_reached" in payload["summary"]["runtime_blockers"]
    assert payload["summary"]["daily_loss_usdt"] == 1.25
    assert {row["plan_status"] for row in payload["plan_rows"]} == {"blocked"}


def test_cost_probe_engine_blocks_symbol_roundtrip_limit(tmp_path):
    cfg = _ready_cost_probe_config()
    cfg.execution.cost_probe_symbols = ["BTC/USDT", "ETH/USDT"]
    cfg.execution.cost_probe_max_orders_per_day = 4
    cfg.execution.cost_probe_cooldown_minutes = 0
    _write_clean_runtime_state(tmp_path)
    reports_dir = tmp_path / "out"
    _write_cost_probe_roundtrip_history(
        reports_dir / "cost_probe_roundtrips.csv",
        [
            {
                "generated_at": "2026-06-18T10:30:00Z",
                "symbol": "BTC/USDT",
                "roundtrip_status": "closed",
                "entry_order_status": "filled",
                "exit_order_status": "filled",
                "no_order_submitted": False,
                "live_order_effect": "live_cost_probe_roundtrip",
                "net_pnl_usdt": 0.02,
            }
        ],
    )

    engine = CostProbeEngine(
        cfg,
        reports_dir=reports_dir,
        generated_at=GENERATED_AT,
        project_root=tmp_path,
    )
    payload = engine.build()

    assert payload["summary"]["state"] == "DRY_RUN_PLAN_READY"
    assert payload["summary"]["planned_symbols"] == ["ETH/USDT"]
    assert payload["summary"]["symbol_runtime_blockers"] == {
        "BTC/USDT": ["roundtrip_limit_reached"]
    }


def _ready_cost_probe_config() -> AppConfig:
    cfg = AppConfig()
    cfg.execution.order_store_path = "runtime/orders.sqlite"
    cfg.execution.cost_bootstrap_enabled = True
    cfg.execution.cost_probe_enabled = True
    cfg.execution.cost_probe_dry_run = True
    cfg.execution.cost_probe_live_enabled = False
    cfg.execution.cost_probe_use_exchange_min_notional = False
    cfg.execution.cost_probe_symbols = ["btcusdt"]
    cfg.execution.cost_probe_max_orders_per_day = 2
    cfg.execution.cost_probe_max_roundtrips_per_symbol_per_day = 1
    cfg.execution.cost_probe_max_notional_usdt = 5.0
    return cfg


def _write_clean_runtime_state(
    project_root,
    *,
    kill_switch_enabled: bool = False,
) -> None:
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "kill_switch.json").write_text(
        json.dumps({"enabled": kill_switch_enabled}),
        encoding="utf-8",
    )
    (runtime_dir / "reconcile_status.json").write_text(
        json.dumps(
            {
                "ok": True,
                "reason": "ok",
                "generated_ts_ms": int(GENERATED_AT.timestamp() * 1000),
            }
        ),
        encoding="utf-8",
    )


def _write_cost_probe_order_history(path, rows) -> None:
    fields = [
        "generated_at",
        "symbol",
        "leg",
        "side",
        "intent",
        "order_status",
        "dry_run",
        "live_enabled",
        "no_order_submitted",
        "notional_usdt",
        "order_style",
        "blocked_reasons",
        "live_order_effect",
    ]
    _write_history_csv(path, fields, rows)


def _write_cost_probe_roundtrip_history(path, rows) -> None:
    fields = [
        "generated_at",
        "symbol",
        "roundtrip_status",
        "entry_order_status",
        "exit_order_status",
        "max_open_seconds",
        "blocked_reasons",
        "no_order_submitted",
        "live_order_effect",
        "net_pnl_usdt",
    ]
    _write_history_csv(path, fields, rows)


def _write_history_csv(path, fields, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_open_order(path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(path)) as con:
        con.execute("CREATE TABLE orders (state TEXT)")
        con.execute("INSERT INTO orders (state) VALUES ('OPEN')")


def _write_position(path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(path)) as con:
        con.execute("CREATE TABLE positions (symbol TEXT, qty REAL)")
        con.execute("INSERT INTO positions (symbol, qty) VALUES ('BTC/USDT', 0.01)")
