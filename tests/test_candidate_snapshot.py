from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace

from src.core.models import Order
from src.reporting.candidate_snapshot import (
    CANDIDATE_SNAPSHOT_FIELDS,
    build_candidate_snapshot_rows,
    candidate_id_for,
    write_candidate_snapshot,
)


def test_candidate_snapshot_builds_symbol_rows_and_stable_ids(tmp_path: Path) -> None:
    audit = SimpleNamespace(
        top_scores=[
            {
                "symbol": "BNB/USDT",
                "score": 0.91,
                "rank": 1,
                "ml_overlay_score": 0.12,
            },
            {"symbol": "SOL/USDT", "score": 0.83, "rank": 2},
        ],
        targets_pre_risk={"BNB/USDT": 0.20, "SOL/USDT": 0.10},
        targets_post_risk={"BNB/USDT": 0.15, "SOL/USDT": 0.0},
        router_decisions=[
            {"symbol": "SOL/USDT", "action": "skip", "reason": "protect_entry_alpha6_score_too_low"}
        ],
        target_execution_explain=[
            {
                "symbol": "SOL/USDT",
                "f4_volume_expansion": 0.4,
                "f5_rsi_trend_confirm": 0.2,
                "alpha6_score": 0.28,
                "alpha6_side": "buy",
                "selected_rank": 2,
            }
        ],
        strategy_signals=[
            {
                "strategy": "Alpha6Factor",
                "signals": [
                    {
                        "symbol": "SOL/USDT",
                        "side": "buy",
                        "score": 0.28,
                        "metadata": {"z_factors": {"f3_vol_adj_ret": -0.2, "f4_volume_expansion": 0.4}},
                    }
                ],
            },
            {
                "strategy": "MeanReversion",
                "signals": [{"symbol": "BNB/USDT", "side": "buy", "score": 0.31}],
            },
        ],
    )
    order = Order(
        symbol="BNB/USDT",
        side="buy",
        intent="OPEN_LONG",
        notional_usdt=15.0,
        signal_price=600.0,
        meta={
            "expected_edge_bps": 60.0,
            "quant_lab": {
                "required_edge_bps": 45.0,
                "effective_total_cost_bps": 30.0,
                "selected_total_cost_bps": 28.0,
                "source": "public_spread_proxy",
                "cost_model_version": "cost_v2",
                "cost_gate_verified": True,
                "would_block_by_cost": False,
                "filter_reason": "cost_gate_passed",
            },
        },
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_001",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BNB/USDT", "SOL/USDT"],
        audit=audit,
        regime_state="Normal",
        risk_level="PROTECT",
        positions=[SimpleNamespace(symbol="BNB/USDT", qty=0.02)],
        prices={"BNB/USDT": 600.0},
        equity_usdt=100.0,
        orders=[order],
        local_cost_bps=30.0,
        local_cost_source_detail="execution.cost_aware_roundtrip_cost_bps",
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        cost_min_edge_multiplier=1.5,
        min_cost_bps_floor=5.0,
        score_proxy_floor=0.18,
        score_per_bps=0.003,
    )

    assert [row["symbol"] for row in rows] == ["BNB/USDT", "SOL/USDT"]
    assert all(row["cost_source"] for row in rows)
    bnb = rows[0]
    sol = rows[1]
    assert bnb["candidate_id"] == candidate_id_for("run_001", "BNB/USDT", "MeanReversion")
    assert bnb["current_weight"] == 0.12
    assert bnb["expected_edge_bps"] == 60.0
    assert bnb["required_edge_bps"] == 45.0
    assert bnb["cost_bps"] == 30.0
    assert bnb["selected_total_cost_bps"] == 28.0
    assert bnb["cost_source"] == "public_spread_proxy"
    assert bnb["cost_model_version"] == "cost_v2"
    assert bnb["cost_gate_verified"] is True
    assert bnb["would_block_by_cost"] is False
    assert bnb["cost_reason"] == "cost_gate_passed"
    assert bnb["final_decision"] == "OPEN_LONG"
    assert sol["strategy_candidate"] == "Alpha6Factor"
    assert sol["block_reason"] == "protect_entry_alpha6_score_too_low"
    assert sol["final_decision"] == "blocked"
    assert sol["f4_volume_expansion"] == 0.4
    assert sol["cost_source"] == "local_estimate"
    assert sol["cost_bps"] == 30.0
    assert sol["selected_total_cost_bps"] == 30.0
    assert sol["required_edge_bps"] == 45.0
    assert sol["expected_edge_bps"] == (0.83 - 0.18) / 0.003
    assert sol["cost_gate_verified"] is False
    assert sol["would_block_by_cost"] is False
    assert sol["cost_reason"] == "local_cost_estimate_no_quant_lab_cost:execution.cost_aware_roundtrip_cost_bps"

    run_dir = tmp_path / "reports" / "runs" / "run_001"
    reports_dir = tmp_path / "reports"
    write_candidate_snapshot(run_dir=run_dir, reports_dir=reports_dir, rows=rows)

    per_run_rows = list(csv.DictReader((run_dir / "candidate_snapshot.csv").read_text().splitlines()))
    aggregate_rows = list(csv.DictReader((reports_dir / "candidate_snapshot.csv").read_text().splitlines()))
    assert per_run_rows == aggregate_rows
    assert list(per_run_rows[0].keys()) == list(CANDIDATE_SNAPSHOT_FIELDS)
    assert per_run_rows[0]["candidate_id"] == bnb["candidate_id"]


def test_candidate_snapshot_uses_quant_lab_cost_estimates_for_blocked_candidate() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BTC/USDT", "score": 0.70, "rank": 1}],
        targets_pre_risk={"BTC/USDT": 0.10},
        targets_post_risk={"BTC/USDT": 0.0},
        router_decisions=[
            {"symbol": "BTC/USDT", "action": "skip", "reason": "protect_entry_alpha6_score_too_low"}
        ],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={
            "cost_estimates": [
                {
                    "symbol": "BTC/USDT",
                    "cost_source": "mixed_actual_proxy",
                    "source": "mixed_actual_proxy",
                    "effective_total_cost_bps": 18.5,
                    "selected_total_cost_bps": 17.0,
                    "cost_model_version": "cost_v2",
                    "expected_edge_bps": 40.0,
                    "required_edge_bps": 27.75,
                    "cost_gate_verified": True,
                    "would_block_by_cost": False,
                    "filter_reason": "cost_gate_passed",
                }
            ]
        },
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_qlab",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        cost_min_edge_multiplier=1.5,
        score_proxy_floor=0.18,
        score_per_bps=0.003,
    )

    btc = rows[0]
    assert btc["final_decision"] == "blocked"
    assert btc["cost_source"] == "mixed_actual_proxy"
    assert btc["cost_bps"] == 18.5
    assert btc["selected_total_cost_bps"] == 17.0
    assert btc["cost_model_version"] == "cost_v2"
    assert btc["expected_edge_bps"] == 40.0
    assert btc["required_edge_bps"] == 27.75
    assert btc["cost_gate_verified"] is True
    assert btc["would_block_by_cost"] is False


def test_candidate_snapshot_falls_back_to_local_estimate_without_remote_cost() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "ETH/USDT", "score": 0.20, "rank": 1}],
        targets_pre_risk={"ETH/USDT": 0.10},
        targets_post_risk={"ETH/USDT": 0.0},
        router_decisions=[{"symbol": "ETH/USDT", "action": "skip", "reason": "protect_entry_block"}],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_local",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["ETH/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_source_detail="execution.cost_aware_roundtrip_cost_bps",
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        cost_min_edge_multiplier=1.5,
        score_proxy_floor=0.18,
        score_per_bps=0.003,
    )

    eth = rows[0]
    assert eth["cost_source"] == "local_estimate"
    assert eth["cost_bps"] == 30.0
    assert eth["selected_total_cost_bps"] == 30.0
    assert eth["cost_model_version"] == "v5_local_execution.cost_aware_roundtrip_cost_bps"
    assert eth["required_edge_bps"] == 45.0
    assert eth["cost_gate_verified"] is False
    assert eth["cost_reason"] == "local_cost_estimate_no_quant_lab_cost:execution.cost_aware_roundtrip_cost_bps"


def test_write_candidate_snapshot_rewrites_old_aggregate_schema(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "candidate_snapshot.csv").write_text(
        "\n".join(
            [
                "candidate_id,run_id,ts_utc,symbol,strategy_candidate,final_decision",
                "old_cand,old_run,2026-05-14T00:00:00Z,BTC/USDT,Alpha6Factor,no_order",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rows = [
        {
            "candidate_id": "new_cand",
            "run_id": "new_run",
            "ts_utc": "2026-05-15T00:00:00Z",
            "symbol": "BTC/USDT",
            "strategy_candidate": "Alpha6Factor",
            "final_decision": "blocked",
            "cost_source": "local_estimate",
            "cost_bps": 30.0,
        }
    ]

    write_candidate_snapshot(run_dir=reports_dir / "runs" / "new_run", reports_dir=reports_dir, rows=rows)

    aggregate_rows = list(csv.DictReader((reports_dir / "candidate_snapshot.csv").read_text().splitlines()))
    assert list(aggregate_rows[0].keys()) == list(CANDIDATE_SNAPSHOT_FIELDS)
    assert [row["candidate_id"] for row in aggregate_rows] == ["old_cand", "new_cand"]
    assert aggregate_rows[1]["cost_source"] == "local_estimate"
