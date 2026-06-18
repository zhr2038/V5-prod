from __future__ import annotations

import csv
import json
from datetime import UTC, datetime

from configs.schema import AppConfig
from src.reporting.cost_probe_plan import (
    build_cost_probe_dry_run_plan,
    write_cost_probe_dry_run_outputs,
)


def test_cost_probe_plan_is_blocked_when_prod_switches_are_closed(tmp_path):
    cfg = AppConfig()
    cfg.execution.cost_bootstrap_enabled = False
    cfg.execution.cost_probe_enabled = False
    cfg.execution.cost_probe_dry_run = True
    cfg.execution.cost_probe_live_enabled = False

    rows, summary = build_cost_probe_dry_run_plan(
        cfg,
        generated_at=datetime(2026, 6, 18, 12, tzinfo=UTC),
    )
    plan_path, summary_path = write_cost_probe_dry_run_outputs(
        rows,
        summary,
        plan_path=tmp_path / "cost_probe_plan.csv",
        summary_path=tmp_path / "cost_probe_summary.json",
    )

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
    cfg.execution.cost_probe_max_orders_per_day = 2
    cfg.execution.cost_probe_max_roundtrips_per_symbol_per_day = 1
    cfg.execution.cost_probe_max_notional_usdt = 5.0

    rows, summary = build_cost_probe_dry_run_plan(
        cfg,
        generated_at=datetime(2026, 6, 18, 12, tzinfo=UTC),
    )

    assert summary["state"] == "DRY_RUN_PLAN_READY"
    assert summary["planned_symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert summary["blocked_symbols"] == []
    assert summary["no_order_submitted"] is True
    assert {row["plan_status"] for row in rows} == {"planned"}
    assert {row["entry_intent"] for row in rows} == {"DRY_RUN_ENTRY_ONLY_NO_ORDER"}
    assert {row["exit_intent"] for row in rows} == {"DRY_RUN_IMMEDIATE_FLAT_NO_ORDER"}
