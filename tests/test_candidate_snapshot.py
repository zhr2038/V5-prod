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
                "source": "public_spread_proxy",
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
    )

    assert [row["symbol"] for row in rows] == ["BNB/USDT", "SOL/USDT"]
    bnb = rows[0]
    sol = rows[1]
    assert bnb["candidate_id"] == candidate_id_for("run_001", "BNB/USDT", "MeanReversion")
    assert bnb["current_weight"] == 0.12
    assert bnb["expected_edge_bps"] == 60.0
    assert bnb["required_edge_bps"] == 45.0
    assert bnb["cost_bps"] == 30.0
    assert bnb["final_decision"] == "OPEN_LONG"
    assert sol["strategy_candidate"] == "Alpha6Factor"
    assert sol["block_reason"] == "protect_entry_alpha6_score_too_low"
    assert sol["final_decision"] == "blocked"
    assert sol["f4_volume_expansion"] == 0.4

    run_dir = tmp_path / "reports" / "runs" / "run_001"
    reports_dir = tmp_path / "reports"
    write_candidate_snapshot(run_dir=run_dir, reports_dir=reports_dir, rows=rows)

    per_run_rows = list(csv.DictReader((run_dir / "candidate_snapshot.csv").read_text().splitlines()))
    aggregate_rows = list(csv.DictReader((reports_dir / "candidate_snapshot.csv").read_text().splitlines()))
    assert per_run_rows == aggregate_rows
    assert list(per_run_rows[0].keys()) == list(CANDIDATE_SNAPSHOT_FIELDS)
    assert per_run_rows[0]["candidate_id"] == bnb["candidate_id"]
