from __future__ import annotations

import csv
import json
from types import SimpleNamespace

from configs.schema import AppConfig
from main import _write_trade_opportunity_funnel_best_effort
from src.reporting.trade_opportunity_funnel import (
    blocker_counts_from_decisions,
    build_trade_opportunity_funnel,
    record_order_stage,
    write_trade_opportunity_funnel,
)


def _audit() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="funnel-run",
        counts={
            "universe": 8,
            "scored": 6,
            "selected": 3,
            "protect_entry_rsi_confirm_too_weak_count": 1,
            "target_zero_after_dd_throttle_count": 1,
        },
        rejects={"cost_edge_insufficient": 1},
        targets_post_risk={"BTC/USDT": 0.1, "SOL/USDT": 0.0, "ETH/USDT": 0.2},
        trade_funnel={"market_data_available": 7},
    )


def _order(symbol: str, side: str, intent: str) -> SimpleNamespace:
    return SimpleNamespace(symbol=symbol, side=side, intent=intent)


def test_trade_funnel_tracks_stage_losses_and_real_fill_boundary() -> None:
    audit = _audit()
    local = [
        _order("BTC/USDT", "buy", "OPEN_LONG"),
        _order("ETH/USDT", "buy", "OPEN_LONG"),
        _order("SOL/USDT", "sell", "CLOSE_LONG"),
    ]
    after_arbitration = [local[0], local[2]]
    record_order_stage(audit, "local_order_generation", local)
    record_order_stage(
        audit,
        "order_arbitration",
        after_arbitration,
        blockers={"same_symbol_cooldown": 1},
    )
    record_order_stage(audit, "live_preflight", after_arbitration)
    record_order_stage(audit, "quant_lab_guard", after_arbitration)
    lifecycle = [
        {
            "symbol": "BTC/USDT",
            "side": "buy",
            "intent": "OPEN_LONG",
            "order_state": "FILLED",
            "submit_ts": "2026-07-12T00:00:01Z",
            "exchange_order_id": "okx-1",
            "filled_qty": 0.001,
            "fill_count": 1,
        },
        {
            "symbol": "SOL/USDT",
            "side": "sell",
            "intent": "CLOSE_LONG",
            "order_state": "FILLED",
            "submit_ts": "2026-07-12T00:00:02Z",
            "exchange_order_id": "okx-2",
            "filled_qty": 0.1,
            "fill_count": 1,
        },
    ]

    rows = build_trade_opportunity_funnel(
        audit=audit,
        lifecycle_rows=lifecycle,
        execution_mode="live",
        ts_utc="2026-07-12T00:01:00Z",
    )
    by_stage = {row["stage"]: row for row in rows}

    assert by_stage["market_data"]["input_count"] == 8
    assert by_stage["market_data"]["output_count"] == 7
    assert by_stage["signal_selection"]["output_count"] == 3
    assert by_stage["risk_target"]["output_count"] == 2
    assert by_stage["local_order_generation"]["entry_output_count"] == 2
    assert by_stage["local_order_generation"]["exit_output_count"] == 1
    assert by_stage["order_arbitration"]["dropped_count"] == 1
    assert by_stage["order_arbitration"]["primary_blocker"] == "same_symbol_cooldown"
    assert by_stage["exchange_submit"]["entry_output_count"] == 1
    assert by_stage["exchange_submit"]["exit_output_count"] == 1
    assert by_stage["exchange_fill"]["entry_output_count"] == 1
    assert by_stage["exchange_fill"]["exit_output_count"] == 1
    assert by_stage["exchange_fill"]["live_order_effect"] == "read_only_observability"


def test_trade_funnel_dry_run_never_reports_real_submit_and_upserts_by_run(tmp_path) -> None:
    audit = _audit()
    order = _order("BTC/USDT", "buy", "OPEN_LONG")
    for stage in (
        "local_order_generation",
        "order_arbitration",
        "live_preflight",
        "quant_lab_guard",
    ):
        record_order_stage(audit, stage, [order], applied=stage != "live_preflight")
    lifecycle = [
        {
            "symbol": "BTC/USDT",
            "side": "buy",
            "intent": "OPEN_LONG",
            "order_state": "FILLED",
            "submit_ts": "simulated",
            "filled_qty": 1.0,
            "fill_count": 1,
        }
    ]

    for _ in range(2):
        rows = write_trade_opportunity_funnel(
            run_dir=tmp_path / "runs" / audit.run_id,
            reports_dir=tmp_path,
            audit=audit,
            lifecycle_rows=lifecycle,
            execution_mode="dry_run",
            ts_utc="2026-07-12T00:01:00Z",
        )

    by_stage = {row["stage"]: row for row in rows}
    aggregate = list(
        csv.DictReader((tmp_path / "trade_opportunity_funnel.csv").open())
    )
    assert by_stage["exchange_submit"]["output_count"] == 0
    assert by_stage["exchange_fill"]["output_count"] == 0
    assert len(aggregate) == 10
    assert json.loads(by_stage["order_arbitration"]["blocker_mix"]) == {}


def test_trade_funnel_reports_real_stage_loss_instead_of_progress_counters() -> None:
    audit = SimpleNamespace(
        run_id="truthful-funnel",
        counts={"universe": 4, "scored": 3, "selected": 1},
        rejects={"dd_throttle": 1},
        targets_post_risk={"ETH/USDT": 0.15},
        target_execution_explain=[
            {
                "symbol": "ETH/USDT",
                "router_action": "skip",
                "blocked_reason": "protect_entry_rsi_confirm_too_weak",
            }
        ],
        trade_funnel={"market_data_available": 4},
    )
    record_order_stage(
        audit,
        "local_order_generation",
        [],
        blockers=blocker_counts_from_decisions(audit.target_execution_explain),
    )

    rows = build_trade_opportunity_funnel(
        audit=audit,
        lifecycle_rows=[],
        execution_mode="live",
        ts_utc="2026-07-12T00:01:00Z",
    )
    by_stage = {row["stage"]: row for row in rows}

    assert by_stage["candidate_scoring"]["primary_blocker"] == (
        "unattributed_stage_loss"
    )
    assert "scored" not in by_stage["candidate_scoring"]["blocker_mix"]
    assert by_stage["signal_selection"]["primary_blocker"] == (
        "unattributed_stage_loss"
    )
    assert "selected" not in by_stage["signal_selection"]["blocker_mix"]
    assert by_stage["risk_target"]["primary_blocker"] == ""
    assert by_stage["local_order_generation"]["primary_blocker"] == (
        "protect_entry_rsi_confirm_too_weak"
    )


def test_best_effort_writer_covers_safe_early_exit_without_live_effect(tmp_path) -> None:
    cfg = AppConfig()
    cfg.execution.mode = "dry_run"
    audit = _audit()
    audit.counts = {"universe": 8, "scored": 0, "selected": 0}
    audit.trade_funnel = {
        "market_data_available": 0,
        "local_order_generation": {
            "total": 0,
            "entry": 0,
            "exit": 0,
            "blockers": {"market_data_coverage_insufficient": 1},
            "applied": True,
        },
    }
    notes: list[str] = []
    audit.add_note = notes.append
    audit.save = lambda _path: None

    count = _write_trade_opportunity_funnel_best_effort(
        cfg=cfg,
        runtime_run_dir=tmp_path / "runs" / audit.run_id,
        runtime_reports_dir=tmp_path,
        audit=audit,
    )

    rows = list(csv.DictReader((tmp_path / "trade_opportunity_funnel.csv").open()))
    by_stage = {row["stage"]: row for row in rows}
    assert count == 10
    assert len(rows) == 10
    assert by_stage["market_data"]["output_count"] == "0"
    assert by_stage["exchange_submit"]["output_count"] == "0"
    assert by_stage["exchange_fill"]["output_count"] == "0"
    assert notes == ["trade_opportunity_funnel rows=10"]
