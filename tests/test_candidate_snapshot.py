from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace

from src.core.models import Order
from src.reporting.candidate_snapshot import (
    CANDIDATE_SNAPSHOT_FIELDS,
    build_candidate_snapshot_rows,
    candidate_id_for,
    load_latest_symbol_cost_table,
    load_quant_lab_cost_cache,
    write_candidate_snapshot,
    write_latest_symbol_cost_table,
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
                "latest_px": 151.2,
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
            "order_lifecycle": {
                "arrival_bid": 599.5,
                "arrival_ask": 600.5,
                "arrival_mid": 600.0,
                "quote_ts": "2026-05-15T00:00:01Z",
                "quote_age_ms": 275,
                "quote_source": "okx_books5",
            },
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
    assert bnb["candidate_id"] == candidate_id_for("run_001", "BNB/USDT", "portfolio_alpha6_factor")
    assert bnb["strategy_candidate"] == "portfolio_alpha6_factor"
    assert bnb["entry_px"] == 600.0
    assert bnb["latest_px"] == 600.0
    assert bnb["current_px"] == 600.0
    assert bnb["price_source"] == "prices"
    assert bnb["decision_px"] == 600.0
    assert bnb["arrival_bid"] == 599.5
    assert bnb["arrival_ask"] == 600.5
    assert bnb["arrival_mid"] == 600.0
    assert bnb["quote_ts"] == "2026-05-15T00:00:01Z"
    assert bnb["quote_age_ms"] == 275.0
    assert bnb["quote_source"] == "okx_books5"
    assert bnb["entry_reference_px"] == 600.0
    assert bnb["entry_price_source"] == "arrival_mid"
    assert bnb["price_observable"] == "strong"
    assert bnb["price_observability_reason"] == "arrival_mid_available"
    assert bnb["current_weight"] == 0.12
    assert bnb["expected_edge_bps"] == 60.0
    assert bnb["expected_edge_source"] == "order.meta.expected_edge_bps"
    assert bnb["required_edge_bps"] == 45.0
    assert bnb["cost_bps"] == 30.0
    assert bnb["selected_total_cost_bps"] == 28.0
    assert bnb["selected_entry_gate_cost_bps"] == 30.0
    assert bnb["cost_source"] == "public_spread_proxy"
    assert bnb["cost_source_quality"] == "public_proxy"
    assert bnb["cost_model_version"] == "cost_v2"
    assert bnb["cost_gate_verified"] is True
    assert bnb["would_block_by_cost"] is False
    assert bnb["cost_reason"] == "cost_gate_passed"
    assert bnb["final_decision"] == "OPEN_LONG"
    assert sol["strategy_candidate"] == "sol_protect_alpha6_low_exception"
    assert sol["block_reason"] == "protect_entry_alpha6_score_too_low"
    assert sol["no_signal_reason"] is None
    assert sol["final_decision"] == "blocked"
    assert sol["f4_volume_expansion"] == 0.4
    assert sol["entry_px"] == 151.2
    assert sol["latest_px"] == 151.2
    assert sol["current_px"] == 151.2
    assert sol["price_source"] == "target_execution_explain.latest_px"
    assert sol["decision_px"] == 151.2
    assert sol["entry_reference_px"] == 151.2
    assert sol["entry_price_source"] == "bar_close_fallback"
    assert sol["price_observable"] == "weak"
    assert (
        sol["price_observability_reason"]
        == "bar_close_fallback_no_arrival_mid:target_execution_explain.latest_px"
    )
    assert sol["cost_source"] == "local_estimate"
    assert sol["cost_source_quality"] == "local_estimate"
    assert sol["cost_bps"] == 30.0
    assert sol["selected_total_cost_bps"] == 30.0
    assert sol["selected_entry_gate_cost_bps"] == 30.0
    assert sol["required_edge_bps"] == 45.0
    assert sol["expected_edge_bps"] == (0.83 - 0.18) / 0.003
    assert sol["expected_edge_source"] == "score_proxy"
    assert sol["cost_gate_verified"] is False
    assert sol["would_block_by_cost"] is False
    assert sol["cost_reason"] == "cost_not_requested_no_order"

    run_dir = tmp_path / "reports" / "runs" / "run_001"
    reports_dir = tmp_path / "reports"
    write_candidate_snapshot(run_dir=run_dir, reports_dir=reports_dir, rows=rows)

    per_run_rows = list(csv.DictReader((run_dir / "candidate_snapshot.csv").read_text().splitlines()))
    aggregate_rows = list(csv.DictReader((reports_dir / "candidate_snapshot.csv").read_text().splitlines()))
    assert per_run_rows == aggregate_rows
    assert list(per_run_rows[0].keys()) == list(CANDIDATE_SNAPSHOT_FIELDS)
    assert per_run_rows[0]["candidate_id"] == bnb["candidate_id"]


def test_candidate_snapshot_uses_top_of_book_for_unordered_candidate_arrival_mid() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "SOL/USDT", "score": 0.83, "rank": 1}],
        targets_pre_risk={"SOL/USDT": 0.10},
        targets_post_risk={"SOL/USDT": 0.0},
        router_decisions=[
            {"symbol": "SOL/USDT", "action": "skip", "reason": "protect_entry_alpha6_score_too_low"}
        ],
        target_execution_explain=[],
        strategy_signals=[],
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_quote_only",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["SOL/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        top_of_book={
            "SOL/USDT": {
                "bid": 149.9,
                "ask": 150.1,
                "quote_ts": "2026-05-15T00:00:01Z",
                "quote_age_ms": 250,
                "source": "okx_books5",
            }
        },
    )

    sol = rows[0]
    assert sol["decision_px"] == 150.0
    assert sol["arrival_bid"] == 149.9
    assert sol["arrival_ask"] == 150.1
    assert sol["arrival_mid"] == 150.0
    assert sol["quote_ts"] == "2026-05-15T00:00:01Z"
    assert sol["quote_age_ms"] == 250.0
    assert sol["quote_source"] == "okx_books5"
    assert sol["entry_reference_px"] == 150.0
    assert sol["entry_price_source"] == "arrival_mid"
    assert sol["price_observable"] == "strong"
    assert sol["price_observability_reason"] == "arrival_mid_available"


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
                    "bootstrap_state": "BOOTSTRAP_PROBE_AVAILABLE",
                    "cost_evidence_tier": "bootstrap_cost_probe",
                    "cost_trust_level": "paper_or_shadow_only",
                    "sample_origin_mix": "cost_probe_only",
                    "trusted_for_paper": True,
                    "trusted_for_live": False,
                    "next_action": "collect strategy live samples before trusted live review",
                    "allowed_live_modes": ["shadow", "paper"],
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
    assert btc["cost_source_quality"] == "mixed_actual_proxy"
    assert btc["cost_bps"] == 18.5
    assert btc["selected_total_cost_bps"] == 17.0
    assert btc["cost_model_version"] == "cost_v2"
    assert btc["expected_edge_bps"] == 40.0
    assert btc["expected_edge_source"] == "quant_lab.expected_edge_bps"
    assert btc["selected_entry_gate_cost_bps"] == 30.0
    assert btc["required_edge_bps"] == 45.0
    assert btc["cost_gate_verified"] is True
    assert btc["would_block_by_cost"] is True
    assert btc["cost_bootstrap_state"] == "BOOTSTRAP_PROBE_AVAILABLE"
    assert btc["cost_evidence_tier"] == "bootstrap_cost_probe"
    assert btc["cost_trust_level"] == "paper_or_shadow_only"
    assert btc["cost_sample_origin_mix"] == "cost_probe_only"
    assert btc["cost_bootstrap_trusted_for_paper"] is True
    assert btc["cost_bootstrap_trusted_for_live"] is False
    assert btc["cost_bootstrap_next_action"] == "collect strategy live samples before trusted live review"
    assert btc["allowed_live_modes"] == ["shadow", "paper"]


def test_candidate_snapshot_records_all_in_cost_fields_and_entry_gate_floor() -> None:
    audit = SimpleNamespace(
        top_scores=[
            {"symbol": "BTC/USDT", "score": 0.70, "rank": 1},
            {"symbol": "ETH/USDT", "score": 0.69, "rank": 2},
        ],
        targets_pre_risk={},
        targets_post_risk={},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={
            "cost_estimates": [
                {
                    "symbol": "BTC/USDT",
                    "cost_source": "public_spread_proxy",
                    "one_way_all_in_cost_bps": 12.0,
                    "roundtrip_all_in_cost_bps": 24.0,
                    "cost_quality": "public_proxy",
                    "cost_trusted_for_paper": True,
                    "cost_trusted_for_live": False,
                    "cost_model_version": "cost_v3",
                },
                {
                    "symbol": "ETH/USDT",
                    "cost_source": "mixed_actual_proxy",
                    "roundtrip_all_in_cost_bps": 45.0,
                    "cost_quality": "mixed_actual_proxy",
                    "cost_trusted_for_paper": True,
                    "cost_trusted_for_live": True,
                    "cost_model_version": "cost_v3",
                },
            ]
        },
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_all_in_cost",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT", "ETH/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        cost_min_edge_multiplier=1.5,
    )

    by_symbol = {row["symbol"]: row for row in rows}
    btc = by_symbol["BTC/USDT"]
    assert btc["one_way_all_in_cost_bps"] == 12.0
    assert btc["roundtrip_all_in_cost_bps"] == 24.0
    assert btc["cost_bps"] == 24.0
    assert btc["selected_entry_gate_cost_bps"] == 30.0
    assert btc["required_edge_bps"] == 45.0
    assert btc["cost_quality"] == "public_proxy"
    assert btc["cost_trusted_for_paper"] is True
    assert btc["cost_trusted_for_live"] is False

    eth = by_symbol["ETH/USDT"]
    assert eth["roundtrip_all_in_cost_bps"] == 45.0
    assert eth["cost_bps"] == 45.0
    assert eth["selected_entry_gate_cost_bps"] == 45.0
    assert eth["required_edge_bps"] == 67.5
    assert eth["cost_trusted_for_live"] is True


def test_blocked_candidate_uses_recent_quant_lab_cached_cost() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BTC/USDT", "score": 0.64, "rank": 1}],
        targets_pre_risk={"BTC/USDT": 0.10},
        targets_post_risk={"BTC/USDT": 0.0},
        router_decisions=[
            {"symbol": "BTC/USDT", "action": "skip", "reason": "protect_entry_alpha6_score_too_low"}
        ],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_cached",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "BTC/USDT": {
                "cost_source": "quant_lab_cached",
                "effective_total_cost_bps": 19.0,
                "selected_total_cost_bps": 18.0,
                "cost_model_version": "cached_symbol_cost_v1",
                "cached_cost_estimate": True,
            }
        },
    )

    btc = rows[0]
    assert btc["final_decision"] == "blocked"
    assert btc["cost_source"] == "quant_lab_cached"
    assert btc["cost_source_quality"] == "quant_lab_cached"
    assert btc["candidate_cost_trusted"] is True
    assert btc["cost_resolution_reason"] == "quant_lab_cached_symbol_cost"
    assert btc["cost_bps"] == 19.0
    assert btc["selected_total_cost_bps"] == 18.0
    assert btc["cost_model_version"] == "cached_symbol_cost_v1"


def test_blocked_btc_sol_bnb_candidates_use_cached_symbol_costs() -> None:
    audit = SimpleNamespace(
        top_scores=[
            {"symbol": "BTC/USDT", "score": 0.64, "rank": 1},
            {"symbol": "SOL/USDT", "score": 0.62, "rank": 2},
            {"symbol": "BNB/USDT", "score": 0.59, "rank": 3},
        ],
        targets_pre_risk={"BTC/USDT": 0.10, "SOL/USDT": 0.10, "BNB/USDT": 0.10},
        targets_post_risk={"BTC/USDT": 0.0, "SOL/USDT": 0.0, "BNB/USDT": 0.0},
        router_decisions=[
            {"symbol": "BTC/USDT", "action": "skip", "reason": "protect_entry_alpha6_score_too_low"},
            {"symbol": "SOL/USDT", "action": "skip", "reason": "protect_entry_rsi_confirm_too_weak"},
            {"symbol": "BNB/USDT", "action": "skip", "reason": "protect_negative_expectancy_short_cycle_block"},
        ],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_cached_multi",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT", "SOL/USDT", "BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "BTC/USDT": {
                "cost_source": "quant_lab_cached",
                "effective_total_cost_bps": 18.0,
                "selected_total_cost_bps": 17.5,
                "cost_model_version": "cached_symbol_cost_v1",
                "cached_cost_estimate": True,
            },
            "SOL/USDT": {
                "cost_source": "mixed_actual_proxy",
                "effective_total_cost_bps": 24.0,
                "selected_total_cost_bps": 23.0,
                "cost_model_version": "mixed_actual_proxy_v1",
                "cached_cost_estimate": True,
            },
            "BNB-USDT": {
                "cost_source": "public_spread_proxy",
                "effective_total_cost_bps": 21.0,
                "selected_total_cost_bps": 20.0,
                "cost_model_version": "public_proxy_v1",
                "cached_cost_estimate": True,
            },
        },
    )

    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["BTC/USDT"]["cost_source"] == "quant_lab_cached"
    assert by_symbol["SOL/USDT"]["cost_source"] == "mixed_actual_proxy"
    assert by_symbol["BNB/USDT"]["cost_source"] == "public_spread_proxy"
    assert by_symbol["BTC/USDT"]["cost_source_quality"] == "quant_lab_cached"
    assert by_symbol["SOL/USDT"]["cost_source_quality"] == "mixed_actual_proxy"
    assert by_symbol["BNB/USDT"]["cost_source_quality"] == "public_proxy"
    assert {row["cost_bps"] for row in rows} == {18.0, 24.0, 21.0}
    assert {row["cost_bps"] for row in rows} != {30.0}


def test_bnb_f3_dominant_no_order_uses_cached_symbol_cost_not_global_default() -> None:
    audit = SimpleNamespace(
        top_scores=[
            {
                "symbol": "BNB/USDT",
                "score": 0.59,
                "rank": 3,
                "f3_vol_adj_ret": 1.8,
                "f4_volume_expansion": 0.1,
                "cost_estimate": {
                    "cost_source": "global_default",
                    "effective_total_cost_bps": 25.0,
                    "selected_total_cost_bps": 25.0,
                    "cost_model_version": "global_default_v0",
                    "fallback_level": "GLOBAL_DEFAULT",
                },
            }
        ],
        targets_pre_risk={"BNB/USDT": 0.0},
        targets_post_risk={"BNB/USDT": 0.0},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_bnb_f3_no_order",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "BNB-USDT": {
                "cost_source": "public_spread_proxy",
                "effective_total_cost_bps": 16.0,
                "selected_total_cost_bps": 15.5,
                "cost_model_version": "cost_bucket_daily:2026-05-14",
                "cached_cost_estimate": True,
            }
        },
    )

    bnb = rows[0]
    assert bnb["final_decision"] == "no_order"
    assert bnb["strategy_candidate"] == "f3_dominant_entry"
    assert bnb["cost_source"] == "public_spread_proxy"
    assert bnb["cost_source_quality"] == "public_proxy"
    assert bnb["degraded_cost_model"] is False
    assert bnb["candidate_cost_trusted"] is True
    assert bnb["cost_resolution_reason"] == "quant_lab_cached_symbol_cost"
    assert bnb["cost_bps"] == 16.0
    assert bnb["cost_model_version"] != "global_default_v0"


def test_bnb_global_default_cache_falls_back_to_latest_symbol_cost_table() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BNB/USDT", "score": 0.59, "rank": 3, "f3_vol_adj_ret": 1.8}],
        targets_pre_risk={"BNB/USDT": 0.0},
        targets_post_risk={"BNB/USDT": 0.0},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_bnb_table",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "BNB/USDT": {
                "cost_source": "global_default",
                "effective_total_cost_bps": 25.0,
                "cost_model_version": "global_default_v0",
                "cached_cost_estimate": True,
            }
        },
        symbol_cost_table={
            "BNB/USDT": {
                "cost_source": "mixed_actual_proxy",
                "effective_total_cost_bps": 14.0,
                "selected_total_cost_bps": 13.5,
                "cost_model_version": "cost_bucket_daily:2026-05-14",
                "cost_resolution_reason": "latest_symbol_cost_table_symbol_cost",
            }
        },
    )

    bnb = rows[0]
    assert bnb["cost_source"] == "mixed_actual_proxy"
    assert bnb["cost_source_quality"] == "mixed_actual_proxy"
    assert bnb["degraded_cost_model"] is False
    assert bnb["candidate_cost_trusted"] is True
    assert bnb["cost_resolution_reason"] == "latest_symbol_cost_table_symbol_cost"
    assert bnb["cost_model_version"] != "global_default_v0"


def test_bnb_global_default_cache_missing_without_symbol_table_uses_local_not_global_default() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BNB/USDT", "score": 0.59, "rank": 3, "f3_vol_adj_ret": 1.8}],
        targets_pre_risk={"BNB/USDT": 0.0},
        targets_post_risk={"BNB/USDT": 0.0},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_bnb_cache_missing",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "BNB/USDT": {
                "cost_source": "global_default",
                "effective_total_cost_bps": 25.0,
                "cost_model_version": "global_default_v0",
                "fallback_level": "GLOBAL_DEFAULT",
            }
        },
    )

    bnb = rows[0]
    assert bnb["strategy_candidate"] == "f3_dominant_entry"
    assert bnb["cost_source"] == "local_estimate"
    assert bnb["cost_model_version"] == "v5_local_execution.cost_aware_roundtrip_cost_bps"
    assert bnb["degraded_cost_model"] is False
    assert bnb["candidate_cost_trusted"] is False
    assert bnb["cost_bps"] == 30.0


def test_blocked_bnb_candidate_uses_symbol_level_cost_over_global_candidate_meta() -> None:
    audit = SimpleNamespace(
        top_scores=[
            {
                "symbol": "BNB/USDT",
                "score": 0.57,
                "rank": 4,
                "f3_vol_adj_ret": 1.5,
                "cost_estimate": {
                    "cost_source": "global_default",
                    "effective_total_cost_bps": 25.0,
                    "selected_total_cost_bps": 25.0,
                    "cost_model_version": "global_default_v0",
                    "fallback_level": "GLOBAL_DEFAULT",
                },
            }
        ],
        targets_pre_risk={"BNB/USDT": 0.10},
        targets_post_risk={"BNB/USDT": 0.0},
        router_decisions=[
            {"symbol": "BNB/USDT", "action": "skip", "reason": "protect_entry_rsi_confirm_too_weak"}
        ],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_bnb_blocked",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        symbol_cost_table={
            "BNB-USDT": {
                "cost_source": "mixed_actual_proxy",
                "effective_total_cost_bps": 15.0,
                "selected_total_cost_bps": 14.5,
                "cost_model_version": "cost_bucket_daily:2026-05-14",
                "cost_resolution_reason": "latest_symbol_cost_table_symbol_cost",
            }
        },
    )

    bnb = rows[0]
    assert bnb["final_decision"] == "blocked"
    assert bnb["strategy_candidate"] == "f3_dominant_entry"
    assert bnb["cost_source"] == "mixed_actual_proxy"
    assert bnb["cost_source_quality"] == "mixed_actual_proxy"
    assert bnb["degraded_cost_model"] is False
    assert bnb["candidate_cost_trusted"] is True
    assert bnb["cost_resolution_reason"] == "latest_symbol_cost_table_symbol_cost"
    assert bnb["cost_bps"] == 15.0
    assert bnb["cost_model_version"] != "global_default_v0"


def test_no_order_candidate_uses_public_proxy_cache_when_available() -> None:
    audit = SimpleNamespace(
        top_scores=[],
        targets_pre_risk={},
        targets_post_risk={},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_public_proxy",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["ETH/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "ETH-USDT": {
                "cost_source": "public_spread_proxy",
                "effective_total_cost_bps": 12.0,
                "selected_total_cost_bps": 11.0,
                "cost_model_version": "public_proxy_v1",
                "cached_cost_estimate": True,
            }
        },
    )

    eth = rows[0]
    assert eth["final_decision"] == "no_order"
    assert eth["cost_source"] == "public_spread_proxy"
    assert eth["cost_source_quality"] == "public_proxy"
    assert eth["cost_bps"] == 12.0
    assert eth["selected_total_cost_bps"] == 11.0
    assert eth["expected_edge_bps"] == 0.0
    assert eth["expected_edge_source"] == "not_available"


def test_no_order_candidate_uses_latest_symbol_cost_table_when_cache_missing() -> None:
    audit = SimpleNamespace(
        top_scores=[],
        targets_pre_risk={},
        targets_post_risk={},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_symbol_table",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["ETH/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        symbol_cost_table={
            "ETH-USDT": {
                "cost_source": "public_spread_proxy",
                "effective_total_cost_bps": 13.0,
                "selected_total_cost_bps": 12.5,
                "cost_model_version": "latest_symbol_public_proxy_v1",
            }
        },
    )

    eth = rows[0]
    assert eth["final_decision"] == "no_order"
    assert eth["cost_source"] == "public_spread_proxy"
    assert eth["cost_source_quality"] == "public_proxy"
    assert eth["cost_bps"] == 13.0
    assert eth["selected_total_cost_bps"] == 12.5
    assert eth["cost_model_version"] == "latest_symbol_public_proxy_v1"


def test_quant_lab_cost_cache_loader_keeps_latest_symbol_cost(tmp_path: Path) -> None:
    usage_path = tmp_path / "quant_lab_usage.jsonl"
    usage_path.write_text(
        "\n".join(
            [
                '{"symbol":"BTC/USDT","cost_source":"public_spread_proxy","effective_total_cost_bps":21,"cost_model_version":"old"}',
                '{"symbol":"BTC/USDT","cost_source":"mixed_actual_proxy","effective_total_cost_bps":18,"selected_total_cost_bps":17,"cost_model_version":"new"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cache = load_quant_lab_cost_cache(usage_path)

    btc = cache["BTC/USDT"]
    assert btc["cost_source"] == "mixed_actual_proxy"
    assert btc["effective_total_cost_bps"] == 18
    assert btc["selected_total_cost_bps"] == 17


def test_quant_lab_cost_cache_loader_does_not_let_global_default_override_symbol_cost(tmp_path: Path) -> None:
    usage_path = tmp_path / "quant_lab_usage.jsonl"
    usage_path.write_text(
        "\n".join(
            [
                '{"symbol":"BNB/USDT","cost_source":"public_spread_proxy","effective_total_cost_bps":16,"cost_model_version":"cost_bucket_daily:2026-05-14"}',
                '{"symbol":"BNB/USDT","cost_source":"global_default","effective_total_cost_bps":25,"cost_model_version":"global_default_v0","fallback_level":"GLOBAL_DEFAULT"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cache = load_quant_lab_cost_cache(usage_path)

    bnb = cache["BNB/USDT"]
    assert bnb["cost_source"] == "public_spread_proxy"
    assert bnb["effective_total_cost_bps"] == 16
    assert bnb["cost_model_version"] == "cost_bucket_daily:2026-05-14"


def test_quant_lab_cost_cache_loader_ignores_cache_missing_global_default(tmp_path: Path) -> None:
    usage_path = tmp_path / "quant_lab_usage.jsonl"
    usage_path.write_text(
        '{"symbol":"BNB/USDT","cost_source":"global_default","effective_total_cost_bps":25,'
        '"cost_model_version":"global_default_v0","fallback_level":"GLOBAL_DEFAULT"}\n',
        encoding="utf-8",
    )

    cache = load_quant_lab_cost_cache(usage_path)

    assert "BNB/USDT" not in cache
    assert "BNB-USDT" not in cache


def test_latest_symbol_cost_table_loader_reads_csv_and_keeps_latest(tmp_path: Path) -> None:
    table_path = tmp_path / "latest_symbol_costs.csv"
    table_path.write_text(
        "\n".join(
            [
                "symbol,cost_source,effective_total_cost_bps,selected_total_cost_bps,cost_model_version",
                "SOL/USDT,public_spread_proxy,26,25,old",
                "SOL/USDT,mixed_actual_proxy,22,21,new",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    table = load_latest_symbol_cost_table(table_path)

    sol = table["SOL/USDT"]
    assert sol["cost_source"] == "mixed_actual_proxy"
    assert sol["effective_total_cost_bps"] == "22"
    assert sol["selected_total_cost_bps"] == "21"
    assert sol["cost_model_version"] == "new"


def test_write_latest_symbol_cost_table_skips_cache_missing_global_default(tmp_path: Path) -> None:
    table_path = tmp_path / "latest_symbol_costs.csv"

    write_latest_symbol_cost_table(
        table_path,
        [
            {
                "symbol": "BNB/USDT",
                "cost_source": "global_default",
                "effective_total_cost_bps": 25.0,
                "cost_model_version": "global_default_v0",
                "fallback_level": "GLOBAL_DEFAULT",
            },
            {
                "symbol": "BNB/USDT",
                "cost_source": "public_spread_proxy",
                "effective_total_cost_bps": 16.0,
                "selected_total_cost_bps": 15.5,
                "cost_model_version": "cost_bucket_daily:2026-05-18",
            },
        ],
    )

    table = load_latest_symbol_cost_table(table_path)

    assert table["BNB/USDT"]["cost_source"] == "public_spread_proxy"
    assert table["BNB/USDT"]["cost_model_version"] == "cost_bucket_daily:2026-05-18"


def test_latest_symbol_cost_table_keeps_mixed_proxy_with_degraded_sample_flag(tmp_path: Path) -> None:
    table_path = tmp_path / "latest_symbol_costs.csv"

    write_latest_symbol_cost_table(
        table_path,
        [
            {
                "symbol": "BNB/USDT",
                "cost_source": "mixed_actual_proxy",
                "effective_total_cost_bps": 12.3,
                "selected_total_cost_bps": 12.3,
                "cost_model_version": "cost_bucket_daily.v0.1",
                "degraded_cost_model": True,
                "fallback_level": "REGIME_FALLBACK;SAMPLE_TOO_SMALL;SLIPPAGE_UNKNOWN;SPREAD_PROXY",
            }
        ],
    )

    table = load_latest_symbol_cost_table(table_path)
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BNB/USDT", "score": 0.59, "rank": 3, "f3_vol_adj_ret": 1.8}],
        targets_pre_risk={"BNB/USDT": 0.0},
        targets_post_risk={"BNB/USDT": 0.0},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )
    rows = build_candidate_snapshot_rows(
        run_id="run_bnb_mixed_degraded_flag",
        ts_utc="2026-05-18T00:00:00Z",
        symbols=["BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        symbol_cost_table=table,
    )

    bnb = rows[0]
    assert bnb["cost_source"] == "mixed_actual_proxy"
    assert bnb["cost_source_quality"] == "mixed_actual_proxy"
    assert bnb["degraded_cost_model"] is False
    assert bnb["candidate_cost_trusted"] is True
    assert bnb["cost_bps"] == 12.3


def test_candidate_snapshot_marks_global_default_cost_degraded() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BTC/USDT", "score": 0.64, "rank": 1}],
        targets_pre_risk={"BTC/USDT": 0.10},
        targets_post_risk={"BTC/USDT": 0.0},
        router_decisions=[{"symbol": "BTC/USDT", "action": "skip", "reason": "protect_entry_block"}],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_global_default",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
        quant_lab_cost_cache={
            "BTC/USDT": {
                "cost_source": "global_default",
                "effective_total_cost_bps": 30.0,
                "selected_total_cost_bps": 30.0,
                "cost_model_version": "global_default_v0",
                "cached_cost_estimate": True,
                "success": False,
                "error_type": "TimeoutError",
            }
        },
    )

    btc = rows[0]
    assert btc["cost_source"] == "global_default"
    assert btc["cost_source_quality"] == "global_default_degraded"
    assert btc["degraded_cost_model"] is True
    assert btc["candidate_cost_trusted"] is False
    assert btc["cost_resolution_reason"] == "service_unavailable"
    assert btc["cost_reason"] == "global_default_cost"


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
    assert eth["cost_source_quality"] == "local_estimate"
    assert eth["cost_bps"] == 30.0
    assert eth["selected_total_cost_bps"] == 30.0
    assert eth["cost_model_version"] == "v5_local_execution.cost_aware_roundtrip_cost_bps"
    assert eth["required_edge_bps"] == 45.0
    assert eth["cost_gate_verified"] is False
    assert eth["cost_reason"] == "cost_not_requested_no_order"
    assert eth["expected_edge_source"] == "score_proxy"


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
            "strategy_candidate": "portfolio_alpha6_factor",
            "final_decision": "blocked",
            "cost_source": "local_estimate",
            "cost_bps": 30.0,
        }
    ]

    write_candidate_snapshot(run_dir=reports_dir / "runs" / "new_run", reports_dir=reports_dir, rows=rows)

    aggregate_rows = list(csv.DictReader((reports_dir / "candidate_snapshot.csv").read_text().splitlines()))
    assert list(aggregate_rows[0].keys()) == list(CANDIDATE_SNAPSHOT_FIELDS)
    assert [row["candidate_id"] for row in aggregate_rows] == ["old_cand", "new_cand"]
    assert aggregate_rows[0]["cost_source"] == "local_estimate"
    assert aggregate_rows[0]["cost_reason"] == "legacy_candidate_snapshot_schema_backfilled_local_estimate"
    assert aggregate_rows[1]["cost_source"] == "local_estimate"


def test_candidate_snapshot_covers_full_universe_with_no_order_and_blocked_rows() -> None:
    audit = SimpleNamespace(
        top_scores=[{"symbol": "BTC/USDT", "score": 0.72, "rank": 1}],
        targets_pre_risk={"BTC/USDT": 0.0, "SOL/USDT": 0.0},
        targets_post_risk={"BTC/USDT": 0.0, "SOL/USDT": 0.0},
        router_decisions=[
            {"symbol": "SOL/USDT", "action": "skip", "reason": "protect_entry_alpha6_score_too_low"}
        ],
        target_execution_explain=[
            {
                "symbol": "SOL/USDT",
                "alpha6_score": 0.22,
                "alpha6_side": "buy",
                "f4_volume_expansion": 0.10,
                "f5_rsi_trend_confirm": 0.15,
            }
        ],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_full_universe",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"],
        audit=audit,
        regime_state="Normal",
        risk_level="PROTECT",
        local_cost_bps=30.0,
        local_cost_source_detail="execution.cost_aware_roundtrip_cost_bps",
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
    )

    by_symbol = {row["symbol"]: row for row in rows}
    assert list(by_symbol) == ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    assert len(rows) == 4
    assert by_symbol["ETH/USDT"]["final_decision"] == "no_order"
    assert by_symbol["ETH/USDT"]["no_signal_reason"] == "no_signal"
    assert by_symbol["ETH/USDT"]["eligible_before_filters"] == "false"
    assert by_symbol["ETH/USDT"]["final_score_missing_reason"] == "not_eligible_before_filters"
    assert by_symbol["ETH/USDT"]["eligibility_block_reason"] == "no_signal"
    assert by_symbol["ETH/USDT"]["rank_exclusion_reason"] == "not_eligible_before_filters"
    assert by_symbol["ETH/USDT"]["expected_edge_bps"] == 0.0
    assert by_symbol["ETH/USDT"]["expected_edge_source"] == "not_available"
    assert by_symbol["BNB/USDT"]["final_decision"] == "no_order"
    assert by_symbol["BNB/USDT"]["no_signal_reason"] == "no_signal"
    assert by_symbol["SOL/USDT"]["final_decision"] == "blocked"
    assert by_symbol["SOL/USDT"]["block_reason"] == "protect_entry_alpha6_score_too_low"
    assert by_symbol["SOL/USDT"]["alpha6_score"] == 0.22
    assert by_symbol["SOL/USDT"]["strategy_candidate"] == "sol_protect_alpha6_low_exception"
    assert all(row["expected_edge_bps"] is not None for row in rows)
    assert all(row["expected_edge_source"] for row in rows)
    assert all(row["required_edge_bps"] is not None for row in rows)
    assert all(row["cost_bps"] is not None for row in rows)
    assert all(row["cost_source"] == "local_estimate" for row in rows)
    assert all(row["cost_source_quality"] == "local_estimate" for row in rows)
    assert all(row["cost_model_version"] for row in rows)


def test_candidate_snapshot_maps_specific_btc_leadership_candidates() -> None:
    audit = SimpleNamespace(
        top_scores=[
            {"symbol": "BTC/USDT", "score": 0.78, "rank": 1},
            {"symbol": "ETH/USDT", "score": 0.62, "rank": 2, "source": "TrendFollowing"},
        ],
        targets_pre_risk={"BTC/USDT": 0.10, "ETH/USDT": 0.05},
        targets_post_risk={"BTC/USDT": 0.0, "ETH/USDT": 0.05},
        router_decisions=[
            {
                "symbol": "BTC/USDT",
                "action": "skip",
                "reason": "btc_leadership_probe_alpha6_score_too_low",
            }
        ],
        target_execution_explain=[],
        strategy_signals=[
            {"strategy": "TrendFollowing", "signals": [{"symbol": "ETH/USDT", "side": "buy", "score": 0.62}]}
        ],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_specific",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BTC/USDT", "ETH/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
    )
    by_symbol = {row["symbol"]: row for row in rows}

    assert by_symbol["BTC/USDT"]["strategy_candidate"] == "btc_leadership_alpha6_low_blocked"
    assert by_symbol["ETH/USDT"]["strategy_candidate"] == "portfolio_trend_following"


def test_candidate_snapshot_uses_alpha_factor_context_without_reclassifying_no_signal() -> None:
    audit = SimpleNamespace(
        top_scores=[],
        alpha_factor_snapshot={
            "ETH/USDT": {
                "final_score": 0.62,
                "rank": 3,
                "z_factors": {
                    "f1_mom_5d": 0.11,
                    "f2_mom_20d": 0.22,
                    "f3_vol_adj_ret_20d": 1.30,
                    "f4_volume_expansion": 0.44,
                    "f5_rsi_trend_confirm": 0.55,
                    "alpha6_display_score": 0.66,
                },
                "ml_overlay_score": 0.07,
            },
            "BNB/USDT": {
                "final_score": 0.51,
                "z_factors": {
                    "f1_mom_5d": 0.10,
                    "f2_mom_20d": 0.20,
                    "f3_vol_adj_ret_20d": 2.40,
                    "f4_volume_expansion": 1.10,
                    "f5_rsi_trend_confirm": 0.30,
                },
            },
        },
        targets_pre_risk={"ETH/USDT": 0.0, "BNB/USDT": 0.0},
        targets_post_risk={"ETH/USDT": 0.0, "BNB/USDT": 0.0},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[
            {"strategy": "TrendFollowing", "signals": [{"symbol": "ETH/USDT", "side": "buy", "score": 0.62}]}
        ],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_alpha_context",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["ETH/USDT", "BNB/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
    )
    by_symbol = {row["symbol"]: row for row in rows}

    eth = by_symbol["ETH/USDT"]
    assert eth["strategy_candidate"] == "portfolio_trend_following"
    assert eth["final_score"] == 0.62
    assert eth["rank"] == 3
    assert eth["f1_mom_5d"] == 0.11
    assert eth["f3_vol_adj_ret"] == 1.30
    assert eth["alpha6_score"] == 0.66
    assert eth["ml_score"] == 0.07

    bnb = by_symbol["BNB/USDT"]
    assert bnb["no_signal_reason"] == "no_signal"
    assert bnb["eligible_before_filters"] == "false"
    assert bnb["final_score"] is None
    assert bnb["f3_vol_adj_ret"] is None
    assert bnb["strategy_candidate"] == "portfolio_trend_following"


def test_candidate_snapshot_classifies_f3_and_f4_candidates() -> None:
    audit = SimpleNamespace(
        top_scores=[
            {"symbol": "BNB/USDT", "score": 0.66, "rank": 1, "f3_vol_adj_ret": 2.4, "f4_volume_expansion": 0.2},
            {"symbol": "SOL/USDT", "score": 0.55, "rank": 2, "f3_vol_adj_ret": 0.1, "f4_volume_expansion": 1.4},
        ],
        targets_pre_risk={"BNB/USDT": 0.10, "SOL/USDT": 0.10},
        targets_post_risk={"BNB/USDT": 0.10, "SOL/USDT": 0.10},
        router_decisions=[],
        target_execution_explain=[],
        strategy_signals=[],
        quant_lab={},
    )

    rows = build_candidate_snapshot_rows(
        run_id="run_factors",
        ts_utc="2026-05-15T00:00:00Z",
        symbols=["BNB/USDT", "SOL/USDT"],
        audit=audit,
        local_cost_bps=30.0,
        local_cost_model_version="v5_local_execution.cost_aware_roundtrip_cost_bps",
    )
    by_symbol = {row["symbol"]: row for row in rows}

    assert by_symbol["BNB/USDT"]["strategy_candidate"] == "f3_dominant_entry"
    assert by_symbol["SOL/USDT"]["strategy_candidate"] == "f4_volume_swing"
