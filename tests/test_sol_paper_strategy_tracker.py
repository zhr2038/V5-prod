from __future__ import annotations

import csv
import io
import json
import os
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.sol_paper_strategy_tracker import (
    _assess_advisory_rows,
    _daily_rows,
    _default_advisory_api_paths,
    _readiness_for_rows,
    update_sol_paper_strategy_tracker,
)


CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"


def test_strategy_advisory_default_api_paths_prefer_compact_without_legacy_alias() -> None:
    paths = _default_advisory_api_paths()

    assert paths[0] == "/v1/strategy-opportunity-advisory/v5-compact"
    assert "/v1/strategy-opportunity-advisory" in paths
    assert "/v1/strategy_opportunity_advisory" not in paths
    assert AppConfig().diagnostics.quant_lab_strategy_opportunity_advisory_api_paths == paths


def _series(symbol: str, start_s: int, prices: dict[int, float]) -> MarketSeries:
    hours = sorted(prices)
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=[(start_s + hour * 3600) * 1000 for hour in hours],
        open=[prices[hour] for hour in hours],
        high=[prices[hour] for hour in hours],
        low=[prices[hour] for hour in hours],
        close=[prices[hour] for hour in hours],
        volume=[1000.0 for _ in hours],
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    return list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_paper_strategy_daily_aggregates_horizon_pnl_when_primary_missing() -> None:
    rows = _daily_rows(
        [
            {
                "paper_date": "2026-05-20",
                "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "experiment_name": "eth_f3",
                "symbol": "ETH/USDT",
                "would_enter": True,
                "alpha6_side": "buy",
                "label_status": "complete",
                "paper_pnl_bps": None,
                "paper_pnl_bps_4h": 12.5,
                "paper_pnl_bps_8h": 25.0,
                "paper_pnl_bps_12h": -7.5,
            }
        ]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["avg_paper_pnl_bps"] == 10.0
    assert row["entry_day_count"] == 1
    assert row["avg_paper_pnl_bps_4h"] == 12.5
    assert row["avg_paper_pnl_bps_8h"] == 25.0
    assert row["avg_paper_pnl_bps_12h"] == -7.5
    avg_by_horizon = json.loads(row["avg_paper_pnl_bps_by_horizon"])
    complete_by_horizon = json.loads(row["complete_count_by_horizon"])
    win_rate_by_horizon = json.loads(row["win_rate_by_horizon"])
    observed_by_horizon = json.loads(row["paper_pnl_observed_count_by_horizon"])
    day_count_by_horizon = json.loads(row["paper_pnl_day_count_by_horizon"])
    assert avg_by_horizon == {"4h": 12.5, "8h": 25.0, "12h": -7.5}
    assert complete_by_horizon["4h"] == 1
    assert complete_by_horizon["8h"] == 1
    assert complete_by_horizon["12h"] == 1
    assert win_rate_by_horizon["4h"] == 1.0
    assert win_rate_by_horizon["8h"] == 1.0
    assert win_rate_by_horizon["12h"] == 0.0
    assert observed_by_horizon["4h"] == 1
    assert observed_by_horizon["8h"] == 1
    assert observed_by_horizon["12h"] == 1
    assert day_count_by_horizon["4h"] == 1
    assert day_count_by_horizon["8h"] == 1
    assert day_count_by_horizon["12h"] == 1


def test_paper_strategy_daily_does_not_count_string_false_entry() -> None:
    rows = _daily_rows(
        [
            {
                "paper_date": "2026-05-25",
                "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "experiment_name": "eth_f3",
                "symbol": "ETH/USDT",
                "would_enter": "False",
                "alpha6_side": "sell",
                "final_decision": "no_order",
                "no_sample_reason": "eth_f3_alpha6_side_not_buy_no_new_entry",
                "label_status": "heartbeat",
                "paper_pnl_bps_48h": 120.0,
            }
        ]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["entry_count"] == 0
    assert row["entry_day_count"] == 0
    assert row["avg_paper_pnl_bps"] is None
    observed_by_horizon = json.loads(row["paper_pnl_observed_count_by_horizon"])
    assert observed_by_horizon["48h"] == 0


def test_paper_strategy_daily_does_not_count_not_observable_entry() -> None:
    rows = _daily_rows(
        [
            {
                "paper_date": "2026-06-16",
                "strategy_id": "BOTTOM_ZONE_PROBE_PAPER_V1",
                "experiment_name": "v5.bottom_zone_probe_paper",
                "symbol": "IP/USDT",
                "would_enter": True,
                "would_size_usdt": 5.0,
                "label_status": "not_observable",
                "label_not_observable_reason": "missing_entry_px",
            }
        ]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["entry_count"] == 0
    assert row["entry_day_count"] == 0
    assert row["not_observable_count"] == 1
    assert row["avg_paper_pnl_bps"] is None


def test_readiness_does_not_count_not_observable_entry_day() -> None:
    readiness = _readiness_for_rows(
        [
            {
                "paper_date": "2026-06-16",
                "strategy_id": "BOTTOM_ZONE_PROBE_PAPER_V1",
                "experiment_name": "v5.bottom_zone_probe_paper",
                "symbol": "IP/USDT",
                "would_enter": True,
                "label_status": "not_observable",
                "label_not_observable_reason": "missing_entry_px",
                "extra_live_block_reasons": ["bottom_zone_probe_paper_only_no_live"],
            }
        ],
        required_days=1,
        required_entry_days=1,
        required_coverage=0.0,
        enable_live_experiment=False,
        allowed_cost_sources=set(),
    )

    assert readiness["entry_day_count"] == 0
    assert "insufficient_entry_days" in readiness["live_block_reason"]


def test_paper_strategy_daily_does_not_count_eth_f3_alpha6_sell_legacy_true_entry() -> None:
    rows = _daily_rows(
        [
            {
                "paper_date": "2026-05-25",
                "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "experiment_name": "eth_f3",
                "symbol": "ETH/USDT",
                "would_enter": True,
                "alpha6_side": "sell",
                "final_decision": "no_order",
                "label_status": "complete",
                "paper_pnl_bps": 10.0,
                "paper_pnl_bps_48h": 120.0,
            }
        ]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["entry_count"] == 0
    assert row["entry_day_count"] == 0
    assert row["avg_paper_pnl_bps"] is None
    observed_by_horizon = json.loads(row["paper_pnl_observed_count_by_horizon"])
    assert observed_by_horizon["48h"] == 0


def test_eth_f3_weak_short_horizon_but_positive_48h_stays_paper() -> None:
    readiness = _readiness_for_rows(
        [
            {
                "paper_date": "2026-05-20",
                "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "symbol": "ETH/USDT",
                "would_enter": True,
                "alpha6_side": "buy",
                "arrival_mid": 100,
                "estimated_spread_bps": 2,
                "slippage_covered": True,
                "cost_source": "mixed_actual_proxy",
                "paper_pnl_bps_24h": -5.0,
                "paper_pnl_bps_48h": 20.0,
            }
        ],
        required_days=1,
        required_entry_days=1,
        required_coverage=0.0,
        enable_live_experiment=True,
        allowed_cost_sources={"mixed_actual_proxy"},
    )

    assert readiness["readiness_status"] == "PAPER_READY"
    assert readiness["live_small_ready"] is False
    assert "eth_f3_paper_only_no_live" in readiness["live_block_reason"]
    assert "eth_f3_waiting_for_48h_complete_samples" in readiness["live_block_reason"]
    assert "eth_f3_negative_48h_paper_pnl" not in readiness["live_block_reason"]


def test_eth_f3_negative_48h_downgrades_to_keep_shadow() -> None:
    readiness = _readiness_for_rows(
        [
            {
                "paper_date": "2026-05-20",
                "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "symbol": "ETH/USDT",
                "would_enter": True,
                "alpha6_side": "buy",
                "arrival_mid": 100,
                "estimated_spread_bps": 2,
                "slippage_covered": True,
                "cost_source": "mixed_actual_proxy",
                "paper_pnl_bps_24h": 10.0,
                "paper_pnl_bps_48h": -1.0,
            }
        ],
        required_days=1,
        required_entry_days=1,
        required_coverage=0.0,
        enable_live_experiment=True,
        allowed_cost_sources={"mixed_actual_proxy"},
    )

    assert readiness["readiness_status"] == "KEEP_SHADOW"
    assert readiness["live_small_ready"] is False
    assert "eth_f3_negative_48h_paper_pnl" in readiness["live_block_reason"]


def test_eth_f3_positive_48h_sample_threshold_stays_paper_not_live() -> None:
    rows = [
        {
            "paper_date": f"2026-05-{day:02d}",
            "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
            "symbol": "ETH/USDT",
            "would_enter": True,
            "alpha6_side": "buy",
            "arrival_mid": 100,
            "estimated_spread_bps": 2,
            "slippage_covered": True,
            "cost_source": "mixed_actual_proxy",
            "paper_pnl_bps_48h": 5.0,
        }
        for day in range(1, 31)
    ]
    readiness = _readiness_for_rows(
        rows,
        required_days=1,
        required_entry_days=1,
        required_coverage=0.0,
        enable_live_experiment=True,
        allowed_cost_sources={"mixed_actual_proxy"},
    )

    assert readiness["readiness_status"] == "PAPER_READY"
    assert readiness["live_small_ready"] is False
    assert "eth_f3_48h_positive_continue_paper" in readiness["live_block_reason"]
    assert "eth_f3_waiting_for_48h_complete_samples" not in readiness["live_block_reason"]


def test_eth_f3_paper_ready_does_not_become_live_without_long_horizon_readiness() -> None:
    readiness = _readiness_for_rows(
        [
            {
                "paper_date": "2026-05-20",
                "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "symbol": "ETH/USDT",
                "would_enter": True,
                "arrival_mid": 100,
                "estimated_spread_bps": 2,
                "slippage_covered": True,
                "cost_source": "mixed_actual_proxy",
                "extra_live_block_reasons": "cost_source_not_actual_or_mixed;f3_global_evidence_negative;eth_f3_paper_only_no_live",
                "paper_pnl_bps_4h": 15.0,
                "paper_pnl_bps_8h": 20.0,
                "paper_pnl_bps_12h": 25.0,
            }
        ],
        required_days=1,
        required_entry_days=1,
        required_coverage=0.0,
        enable_live_experiment=True,
        allowed_cost_sources={"mixed_actual_proxy"},
    )

    assert readiness["readiness_status"] == "PAPER_READY"
    assert readiness["live_small_ready"] is False
    assert "f3_global_evidence_negative" in readiness["live_block_reason"]


def _write_candidate_snapshot(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "candidate_id": "cand_sol_alpha6_low",
            "run_id": "r1",
            "ts_utc": "2026-05-15T00:00:00Z",
            "symbol": "SOL/USDT",
            "final_decision": "blocked",
            "block_reason": "protect_entry_alpha6_score_too_low",
            "strategy_candidate": "sol_protect_alpha6_low_exception",
            "target_weight_raw": "0.12",
            "target_weight_after_risk": "0",
            "final_score": "0.88",
            "alpha6_score": "0.26",
            "alpha6_side": "buy",
            "f4_volume_expansion": "0.2",
            "f5_rsi_trend_confirm": "0.25",
            "risk_level": "PROTECT",
            "cost_source": "public_spread_proxy",
            "cost_source_quality": "public_proxy",
            "cost_model_version": "public_proxy_v1",
        },
        {
            "candidate_id": "cand_sol_f4",
            "run_id": "r1",
            "ts_utc": "2026-05-15T00:00:00Z",
            "symbol": "SOL/USDT",
            "final_decision": "no_order",
            "block_reason": "",
            "strategy_candidate": "f4_volume_swing",
            "target_weight_raw": "0.12",
            "target_weight_after_risk": "0",
            "final_score": "0.82",
            "alpha6_score": "0.34",
            "alpha6_side": "buy",
            "f4_volume_expansion": "1.2",
            "f5_rsi_trend_confirm": "0.40",
            "risk_level": "PROTECT",
            "cost_source": "mixed_actual_proxy",
            "cost_source_quality": "mixed_actual_proxy",
            "cost_model_version": "mixed_actual_proxy_v1",
        },
        {
            "candidate_id": "cand_sol_live",
            "run_id": "r1",
            "ts_utc": "2026-05-15T00:00:00Z",
            "symbol": "SOL/USDT",
            "final_decision": "OPEN_LONG",
            "block_reason": "",
            "strategy_candidate": "f4_volume_swing",
            "target_weight_raw": "0.12",
            "alpha6_side": "buy",
            "f4_volume_expansion": "1.4",
            "risk_level": "PROTECT",
            "cost_source": "mixed_actual_proxy",
        },
    ]
    fields = sorted({field for row in rows for field in row})
    with (run_dir / "candidate_snapshot.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _cfg(*, include_bnb: bool = False) -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.diagnostics.paper_strategy_tracking_enabled = True
    cfg.diagnostics.paper_strategy_enabled_shadow_only = True
    cfg.diagnostics.paper_strategy_enable_live_experiment = False
    cfg.diagnostics.paper_strategy_required_paper_days = 14
    cfg.diagnostics.paper_strategy_required_slippage_coverage = 0.8
    cfg.diagnostics.paper_strategy_required_entry_days = 3
    cfg.diagnostics.paper_strategy_horizons_hours = [4, 8, 12, 24, 48, 72]
    cfg.diagnostics.paper_strategy_rt_cost_bps = 30.0
    cfg.diagnostics.paper_strategy_configs = [
        {
            "strategy_id": "SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1",
            "experiment_name": "v5.sol_protect_alpha6_low_exception",
            "source_strategy_candidates": [
                "sol_protect_alpha6_low_exception",
                "sol_protect_rsi_weak_exception",
            ],
            "allowed_block_reasons": [
                "protect_entry_alpha6_score_too_low",
                "protect_entry_rsi_confirm_too_weak",
            ],
            "min_f4_volume_expansion": 0.0,
        },
        {
            "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
            "experiment_name": "v5.f4_volume_expansion_entry",
            "source_strategy_candidates": [
                "f4_volume_swing",
                "f4_volume_expansion_entry",
                "v5.f4_volume_expansion_entry",
                "f4_volume_expansion",
            ],
            "allowed_block_reasons": [],
            "min_f4_volume_expansion": 0.0,
        },
    ]
    if include_bnb:
        cfg.symbols.append("BNB/USDT")
        cfg.diagnostics.paper_strategy_configs.extend(
            [
                {
                    "strategy_id": "BNB_F3_DOMINANT_ENTRY_PAPER_V1",
                    "experiment_name": "v5.bnb_f3_dominant_entry",
                    "source_strategy_candidates": [
                        "f3_dominant_entry",
                        "v5.f3_dominant_entry",
                        "v5.bnb_f3_dominant_entry",
                    ],
                    "allowed_block_reasons": [],
                    "symbol": "BNB/USDT",
                    "primary_horizon_hours": 24,
                    "require_protect_level": False,
                    "require_no_cooldown": False,
                    "require_alpha6_buy": True,
                    "min_alpha6_score": 0.9,
                    "require_expected_edge_gt_required": True,
                    "require_cost_gate_verified": True,
                    "allowed_current_regimes": ["ALT_IMPULSE", "TRENDING", "TREND_UP"],
                    "extra_live_block_reasons": [
                        "bnb_paper_only_no_live",
                        "bnb_negative_expectancy_recovery_research_only",
                    ],
                },
                {
                    "strategy_id": "BNB_RISK_ON_BUY_PAPER_V1",
                    "experiment_name": "v5.bnb_risk_on_buy",
                    "source_strategy_candidates": [],
                    "allowed_block_reasons": [],
                    "symbol": "BNB/USDT",
                    "primary_horizon_hours": 24,
                    "require_protect_level": False,
                    "require_no_cooldown": False,
                    "require_alpha6_buy": True,
                    "min_alpha6_score": 0.9,
                    "require_expected_edge_gt_required": True,
                    "require_cost_gate_verified": True,
                    "allowed_current_regimes": ["ALT_IMPULSE", "TRENDING", "TREND_UP"],
                    "extra_live_block_reasons": [
                        "bnb_paper_only_no_live",
                        "bnb_negative_expectancy_recovery_research_only",
                    ],
                },
            ]
        )
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_paths = [
        "strategy_opportunity_advisory.csv",
        "reports/strategy_opportunity_advisory.csv",
        "quant_lab_latest/strategy_opportunity_advisory.csv",
        "reports/quant_lab_latest_bundle.zip",
        "reports/quant_lab_latest_bundle.tar.gz",
    ]
    return cfg


def _audit(run_id: str, ts_s: int) -> DecisionAudit:
    audit = DecisionAudit(run_id=run_id, now_ts=ts_s, window_end_ts=ts_s)
    audit.budget = {"current_equity_usdt": 100.0}
    return audit


def _write_single_sol_candidate(run_dir: Path, *, run_id: str, overrides: dict[str, str]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    row = {
        "candidate_id": "sol_candidate",
        "run_id": run_id,
        "ts_utc": "2026-05-15T00:00:00Z",
        "symbol": "SOL/USDT",
        "final_decision": "no_order",
        "strategy_candidate": "f4_volume_swing",
        "final_score": "0.91",
        "alpha6_score": "0.45",
        "alpha6_side": "buy",
        "f4_volume_expansion": "1.1",
        "f5_rsi_trend_confirm": "0.35",
        "risk_level": "PROTECT",
        "cost_source": "mixed_actual_proxy",
        "cost_source_quality": "mixed_actual_proxy",
        "cost_bps": "14",
    }
    row.update(overrides)
    with (run_dir / "candidate_snapshot.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=sorted(row))
        writer.writeheader()
        writer.writerow(row)


def _write_strategy_advisory(reports_dir: Path, rows: list[dict[str, str]]) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    fields = sorted({field for row in rows for field in row})
    with (reports_dir / "strategy_opportunity_advisory.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _fresh_meta(start_s: int, *, contract_version: str = CONTRACT_VERSION) -> dict[str, str]:
    return {
        "as_of_ts": str(start_s),
        "generated_at": str(start_s),
        "expires_at": str(start_s + 3600),
        "contract_version": contract_version,
        "quant_lab_git_commit": "test_commit",
        "source_version": "test_source_v1",
    }


def _stale_meta(start_s: int, *, contract_version: str = CONTRACT_VERSION) -> dict[str, str]:
    return {
        "as_of_ts": str(start_s - 10_000),
        "generated_at": str(start_s - 10_000),
        "expires_at": str(start_s - 9_000),
        "contract_version": contract_version,
        "quant_lab_git_commit": "stale_commit",
        "source_version": "stale_source_v1",
    }


def _write_paper_strategy_proposals(reports_dir: Path, rows: list[dict[str, str]]) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    fields = sorted({field for row in rows for field in row})
    with (reports_dir / "paper_strategy_proposals.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_paper_strategy_proposal_zip(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({field for row in rows for field in row})
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("reports/paper_strategy_proposals.csv", buffer.getvalue())


def _write_strategy_advisory_bundle(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({field for row in rows for field in row})
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    payload = buffer.getvalue().encode("utf-8")
    info = tarfile.TarInfo("quant-lab-pack/reports/strategy_opportunity_advisory.csv")
    info.size = len(payload)
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(info, io.BytesIO(payload))


def _write_strategy_advisory_zip(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({field for row in rows for field in row})
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("reports/strategy_opportunity_advisory.csv", buffer.getvalue())


def test_sol_paper_strategy_tracker_writes_runs_daily_and_slippage(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r1"
    _write_candidate_snapshot(run_dir)
    market = {
        "SOL/USDT": _series(
            "SOL/USDT",
            start_s,
            {0: 100.0, 4: 101.0, 8: 102.0, 12: 103.0, 24: 110.0, 48: 112.0, 72: 115.0},
        )
    }

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r1", start_s),
        market_data_1h=market,
        cfg=cfg,
        cache_dir=tmp_path / "cache",
        top_of_book={"SOL/USDT": {"bid": 99.9, "ask": 100.1}},
    )
    assert result["enabled"] is True
    assert result["new_records"] == 2
    assert result["total_records"] == 2

    mature_result = update_sol_paper_strategy_tracker(
        run_dir=tmp_path / "reports" / "runs" / "r2",
        audit=_audit("r2", start_s + 72 * 3600),
        market_data_1h=market,
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )
    assert mature_result["new_records"] == 2
    assert mature_result["total_records"] == 4

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    assert {row["strategy_id"] for row in runs} == {
        "SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1",
        "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
    }
    entered_runs = [row for row in runs if row["would_enter"] == "True"]
    heartbeat_runs = [row for row in runs if row["would_enter"] == "False"]
    assert len(entered_runs) == 2
    assert len(heartbeat_runs) == 2
    assert {row["would_size_notional"] for row in entered_runs} == {"12.0"}
    assert {row["would_size_usdt"] for row in entered_runs} == {"12.0"}
    assert {row["expected_exit_horizon"] for row in entered_runs} == {"24h"}
    assert {row["f4_threshold"] for row in entered_runs} == {"0.0"}
    assert {row["paper_pnl_bps_24h"] for row in entered_runs} == {"970.0"}
    assert {row["paper_pnl_usdt_24h"] for row in entered_runs} == {"1.164"}
    assert {row["arrival_bid"] for row in entered_runs} == {"99.9"}
    assert {row["arrival_ask"] for row in entered_runs} == {"100.1"}
    assert {row["arrival_mid"] for row in entered_runs} == {"100.0"}
    assert {row["estimated_spread_bps"] for row in entered_runs} == {"20.0"}
    assert {row["expected_order_type"] for row in entered_runs} == {"paper_market_buy"}
    assert {row["estimated_fill_px"] for row in entered_runs} == {"100.1"}
    assert "cand_sol_live" not in {row["candidate_id"] for row in runs}

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    assert len(daily) == 4
    entry_daily = [row for row in daily if row["entry_count"] == "1"]
    heartbeat_daily = [row for row in daily if row["entry_count"] == "0"]
    assert len(entry_daily) == 2
    assert len(heartbeat_daily) == 2
    assert {row["paper_days_to_date"] for row in entry_daily} == {"1"}
    assert {row["paper_days_to_date"] for row in heartbeat_daily} == {"2"}
    assert {row["entry_day_count"] for row in entry_daily} == {"1"}
    assert {row["entry_day_count"] for row in heartbeat_daily} == {"1"}
    assert {row["avg_paper_pnl_bps"] for row in entry_daily} == {"970.0"}
    assert all("avg_paper_pnl_bps_by_horizon" in row for row in daily)
    assert all(row["avg_paper_pnl_bps_24h"] == "970.0" for row in entry_daily)
    assert all(json.loads(row["paper_pnl_observed_count_by_horizon"])["24h"] == 1 for row in entry_daily)

    coverage = _read_csv(tmp_path / "reports" / "summaries" / "paper_slippage_coverage.csv")
    by_strategy = {row["strategy_id"]: row for row in coverage}
    f4 = by_strategy["SOL_F4_VOLUME_EXPANSION_PAPER_V1"]
    assert f4["paper_days"] == "2"
    assert f4["required_paper_days"] == "14"
    assert f4["slippage_coverage"] == "0.5"
    assert f4["arrival_mid_coverage"] == "0.5"
    assert f4["spread_observation_coverage"] == "0.5"
    assert '"mixed_actual_proxy": 1' in f4["cost_source_mix"]
    assert f4["readiness_status"] == "PAPER_READY"
    assert f4["live_small_ready"] == "False"
    assert "no_paper_days" in f4["live_block_reason"]
    assert "insufficient_entry_days" in f4["live_block_reason"]
    assert "arrival_mid_coverage_insufficient" in f4["live_block_reason"]
    assert "no_live_slippage_coverage" in f4["live_block_reason"]

    alpha6_low = by_strategy["SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1"]
    assert alpha6_low["latest_cost_source"] == "local_estimate"
    assert "cost_source_not_actual_or_mixed" in alpha6_low["live_block_reason"]
    assert "no_live_slippage_coverage" in alpha6_low["live_block_reason"]


def test_bnb_paper_strategy_tracks_alpha6_buy_no_order_without_live(tmp_path: Path) -> None:
    cfg = _cfg(include_bnb=True)
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_bnb"
    run_dir.mkdir(parents=True, exist_ok=True)
    row = {
        "candidate_id": "bnb_20260530_03",
        "run_id": "r_bnb",
        "ts_utc": "2026-05-30T03:00:00Z",
        "symbol": "BNB/USDT",
        "final_decision": "no_order",
        "block_reason": "negative_expectancy_fast_fail_open_block",
        "strategy_candidate": "f3_dominant_entry",
        "target_weight_raw": "0.12",
        "target_weight_after_risk": "0",
        "final_score": "-0.12",
        "alpha6_score": "0.994",
        "alpha6_side": "buy",
        "f4_volume_expansion": "5.82",
        "f5_rsi_trend_confirm": "0.832",
        "risk_level": "NORMAL",
        "current_regime": "TREND_UP",
        "expected_edge_bps": "180",
        "required_edge_bps": "45",
        "cost_gate_verified": "true",
        "cost_source": "mixed_actual_proxy",
        "cost_source_quality": "mixed_actual_proxy",
        "cost_bps": "28",
    }
    with (run_dir / "candidate_snapshot.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_bnb", start_s),
        market_data_1h={
            "BNB/USDT": _series(
                "BNB/USDT",
                start_s,
                {0: 642.3, 4: 660.0, 8: 670.0, 12: 675.0, 24: 716.8, 48: 720.0, 72: 725.0},
            )
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
        top_of_book={"BNB/USDT": {"bid": 642.2, "ask": 642.4}},
    )

    assert result["enabled"] is True
    assert result["bnb_paper_strategy_rows"] == 2

    mature_result = update_sol_paper_strategy_tracker(
        run_dir=tmp_path / "reports" / "runs" / "r_bnb_mature",
        audit=_audit("r_bnb_mature", start_s + 72 * 3600),
        market_data_1h={
            "BNB/USDT": _series(
                "BNB/USDT",
                start_s,
                {0: 642.3, 4: 660.0, 8: 670.0, 12: 675.0, 24: 716.8, 48: 720.0, 72: 725.0},
            )
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )
    assert mature_result["bnb_paper_strategy_rows"] == 4

    bnb_runs = _read_csv(tmp_path / "reports" / "summaries" / "bnb_paper_strategy_runs.csv")
    entered_bnb_runs = [item for item in bnb_runs if item["would_enter"] == "True"]
    assert {item["strategy_id"] for item in entered_bnb_runs} == {
        "BNB_F3_DOMINANT_ENTRY_PAPER_V1",
        "BNB_RISK_ON_BUY_PAPER_V1",
    }
    assert {item["final_decision"] for item in entered_bnb_runs} == {"no_order"}
    assert {item["enable_live_experiment"] for item in entered_bnb_runs} == {"False"}
    assert {item["live_small_ready"] for item in bnb_runs} == {"False"}
    assert all("bnb_paper_only_no_live" in item["live_block_reason"] for item in bnb_runs)
    assert all(float(item["paper_pnl_bps_4h"]) > 200.0 for item in entered_bnb_runs)
    assert all(float(item["paper_pnl_bps_24h"]) > 1000.0 for item in entered_bnb_runs)

    bnb_daily = _read_csv(tmp_path / "reports" / "summaries" / "bnb_paper_strategy_daily.csv")
    entry_daily = [item for item in bnb_daily if item["entry_count"] == "1"]
    assert {item["strategy_id"] for item in entry_daily} == {
        "BNB_F3_DOMINANT_ENTRY_PAPER_V1",
        "BNB_RISK_ON_BUY_PAPER_V1",
    }
    assert all("24h" in json.loads(item["avg_paper_pnl_bps_by_horizon"]) for item in entry_daily)


def test_sol_paper_strategy_tracker_writes_strategy_heartbeats_without_candidate(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_heartbeat"
    run_dir.mkdir(parents=True)
    with (run_dir / "candidate_snapshot.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "candidate_id",
                "run_id",
                "ts_utc",
                "symbol",
                "final_decision",
                "strategy_candidate",
                "cost_source",
                "cost_source_quality",
                "cost_bps",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "candidate_id": "eth_no_order",
                "run_id": "r_heartbeat",
                "ts_utc": "2026-05-15T00:00:00Z",
                "symbol": "ETH/USDT",
                "final_decision": "no_order",
                "strategy_candidate": "f4_volume_swing",
                "cost_source": "mixed_actual_proxy",
                "cost_source_quality": "mixed_actual_proxy",
                "cost_bps": "12",
            }
        )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_heartbeat", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["enabled"] is True
    assert result["new_records"] == 2
    assert result["total_records"] == 2
    labels = _read_jsonl(tmp_path / "reports" / "sol_paper_strategy_labels.jsonl")
    assert {row["strategy_id"] for row in labels} == {
        "SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1",
        "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
    }
    assert {row["would_enter"] for row in labels} == {False}
    assert {row["label_status"] for row in labels} == {"heartbeat"}
    assert {row["cost_source"] for row in labels} == {"local_estimate"}

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    assert len(runs) == 2
    assert {row["would_enter"] for row in runs} == {"False"}
    assert {row["entry_reason"] for row in runs} == {"paper_strategy_heartbeat"}
    assert {row["would_size_usdt"] for row in runs} == {""}
    assert {row["expected_exit_horizon"] for row in runs} == {""}
    assert {row["f4_threshold"] for row in runs} == {""}
    assert {row["estimated_cost_bps"] for row in runs} == {"30.0"}
    assert {row["label_status"] for row in runs} == {"heartbeat"}
    assert {row["no_sample_reason"] for row in runs} == {"no_sol_candidate"}
    assert {row["sol_candidate_present"] for row in runs} == {"False"}

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    assert {row["entry_count"] for row in daily} == {"0"}
    assert {row["paper_days_to_date"] for row in daily} == {"1"}

    coverage = _read_csv(tmp_path / "reports" / "summaries" / "paper_slippage_coverage.csv")
    assert {row["total_rows"] for row in coverage} == {"1"}
    assert {row["slippage_coverage"] for row in coverage} == {"0.0"}


def test_sol_paper_public_spread_does_not_become_live_without_arrival_coverage() -> None:
    rows = [
        {
            "paper_date": f"2026-05-{day:02d}",
            "would_enter": True,
            "cost_source": "public_spread_proxy",
        }
        for day in range(1, 15)
    ]

    readiness = _readiness_for_rows(
        rows,
        required_days=14,
        required_entry_days=3,
        required_coverage=0.8,
        enable_live_experiment=True,
        allowed_cost_sources={"actual_fills", "mixed_actual_proxy"},
    )

    assert readiness["live_small_ready"] is False
    assert "cost_source_not_actual_or_mixed" in readiness["live_block_reason"]
    assert "arrival_mid_coverage_insufficient" in readiness["live_block_reason"]
    assert "no_live_slippage_coverage" in readiness["live_block_reason"]


def test_sol_paper_strategy_tracker_heartbeat_explains_alpha6_not_buy(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_alpha6_not_buy"
    _write_single_sol_candidate(run_dir, run_id="r_alpha6_not_buy", overrides={"alpha6_side": "sell"})

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_alpha6_not_buy", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["new_records"] == 2
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    assert {row["would_enter"] for row in runs} == {"False"}
    assert {row["no_sample_reason"] for row in runs} == {"alpha6_not_buy"}
    assert {row["sol_candidate_present"] for row in runs} == {"True"}
    assert {row["alpha6_side"] for row in runs} == {"sell"}
    assert {row["risk_level"] for row in runs} == {"PROTECT"}
    assert {row["risk_off"] for row in runs} == {"False"}
    assert {row["cooldown_active"] for row in runs} == {"False"}
    assert {row["f4_volume_expansion"] for row in runs} == {"1.1"}
    assert {row["f4_threshold"] for row in runs} == {"0.0"}
    assert {row["f5_rsi_trend_confirm"] for row in runs} == {"0.35"}
    assert {row["cost_source"] for row in runs} == {"mixed_actual_proxy"}
    assert {row["label_24h_reason"] for row in runs} == {"alpha6_not_buy"}


@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        ({"risk_level": "NORMAL"}, "risk_not_protect"),
        ({"cooldown_active": "true"}, "cooldown_active"),
        ({"regime_state": "Risk-Off"}, "risk_off"),
        ({"f4_volume_expansion": "-0.1"}, "f4_below_threshold"),
    ],
)
def test_sol_paper_strategy_tracker_heartbeat_explains_blocking_conditions(
    tmp_path: Path,
    overrides: dict[str, str],
    expected_reason: str,
) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_id = f"r_{expected_reason}"
    run_dir = tmp_path / "reports" / "runs" / run_id
    _write_single_sol_candidate(run_dir, run_id=run_id, overrides=overrides)

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit(run_id, start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["new_records"] == 2
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    assert {row["would_enter"] for row in runs} == {"False"}
    assert {row["no_sample_reason"] for row in runs} == {expected_reason}
    assert {row["sol_candidate_present"] for row in runs} == {"True"}
    assert {row["f4_threshold"] for row in runs} == {"0.0"}
    assert {row["label_24h_reason"] for row in runs} == {expected_reason}


def test_sol_paper_no_entry_row_includes_required_diagnostics(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_id = "r_sol_no_entry_diag"
    run_dir = tmp_path / "reports" / "runs" / run_id
    _write_single_sol_candidate(
        run_dir,
        run_id=run_id,
        overrides={
            "strategy_candidate": "f4_volume_swing",
            "f4_volume_expansion": "-0.2",
            "alpha6_score": "0.41",
            "alpha6_side": "buy",
            "f5_rsi_trend_confirm": "0.38",
            "risk_level": "PROTECT",
        },
    )
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "SOL-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "horizon_hours": "24",
            }
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit(run_id, start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["would_enter"] == "False"
    assert f4["no_sample_reason"] == "f4_below_threshold"
    assert f4["risk_level"] == "PROTECT"
    assert f4["alpha6_score"] == "0.41"
    assert f4["alpha6_side"] == "buy"
    assert f4["f4_volume_expansion"] == "-0.2"
    assert f4["f4_threshold"] == "0.0"
    assert f4["f5_rsi_trend_confirm"] == "0.38"
    assert f4["advisory_decision"] == "PAPER_READY"
    assert f4["advisory_match_key"] == "sol_f4_volume_expansion_paper_v1"


def test_sol_paper_strategy_tracker_uses_standard_no_sample_reason_for_source_mismatch(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_id = "r_source_mismatch"
    run_dir = tmp_path / "reports" / "runs" / run_id
    _write_single_sol_candidate(
        run_dir,
        run_id=run_id,
        overrides={"strategy_candidate": "unrelated_sol_candidate", "block_reason": "unrelated_block"},
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit(run_id, start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["new_records"] == 2
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    assert {row["would_enter"] for row in runs} == {"False"}
    assert {row["no_sample_reason"] for row in runs} == {"no_sol_candidate"}
    assert "no_qualifying_candidate" not in {row["no_sample_reason"] for row in runs}
    assert {row["sol_candidate_present"] for row in runs} == {"True"}


def test_paper_strategy_tracker_adds_eth_f3_heartbeat_from_proposal(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_eth_heartbeat"
    run_dir.mkdir(parents=True)
    (run_dir / "candidate_snapshot.csv").write_text(
        "run_id,ts_utc,symbol,final_decision,strategy_candidate\n",
        encoding="utf-8",
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "48h",
                "live_block_reason": '["cost_source_not_actual_or_mixed"]',
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_eth_heartbeat", start_s),
        market_data_1h={"ETH/USDT": _series("ETH/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["proposal_rows"] == 1
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    eth = next(row for row in runs if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth["symbol"] == "ETH/USDT"
    assert eth["would_enter"] == "False"
    assert eth["no_sample_reason"] == "no_eth_candidate"
    assert eth["proposal_present"] == "True"
    assert eth["proposal_source"].endswith("paper_strategy_proposals.csv")
    assert "cost_source_not_actual_or_mixed" in eth["live_block_reason"]
    assert "f3_global_evidence_negative" in eth["live_block_reason"]
    assert "eth_f3_paper_only_no_live" in eth["live_block_reason"]

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    assert any(row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1" for row in daily)
    coverage = _read_csv(tmp_path / "reports" / "summaries" / "paper_slippage_coverage.csv")
    assert any(row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1" for row in coverage)


def test_paper_strategy_tracker_acknowledges_current_quant_lab_proposals(tmp_path: Path) -> None:
    cfg = _cfg(include_bnb=True)
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_current_proposals"
    run_dir.mkdir(parents=True)
    (run_dir / "candidate_snapshot.csv").write_text(
        "run_id,ts_utc,symbol,final_decision,strategy_candidate\n",
        encoding="utf-8",
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "BNB-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "120h",
            },
            {
                "proposal_id": "ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "4h",
            },
            {
                "proposal_id": "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "SOL-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "12h",
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_current_proposals", start_s),
        market_data_1h={
            "BNB/USDT": _series("BNB/USDT", start_s, {0: 600.0}),
            "ETH/USDT": _series("ETH/USDT", start_s, {0: 1600.0}),
            "SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["proposal_rows"] == 3
    assert result["paper_strategy_proposal_ack_rows"] == 3
    ack = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_proposal_ack.csv")
    by_proposal = {row["proposal_id"]: row for row in ack}
    assert by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["accepted"] == "True"
    assert by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["proposal_hash"]
    assert by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["accepted_at"]
    assert by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["paper_only"] == "True"
    assert by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["max_live_notional_usdt"] == "0.0"
    assert by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["paper_tracker_id"] == "BNB_F3_DOMINANT_ENTRY_PAPER_V1"
    assert by_proposal["SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["accepted"] == "True"
    assert by_proposal["SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["paper_tracker_id"] == "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"
    assert by_proposal["ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"]["accepted"] == "True"
    assert by_proposal["ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"]["paper_tracker_id"] == "ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"
    assert {row["live_order_effect"] for row in ack} == {"read_only_no_live_order"}

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    run_by_proposal = {row["proposal_id"]: row for row in runs if row["proposal_id"] in by_proposal}
    assert run_by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["paper_tracker_id"] == "BNB_F3_DOMINANT_ENTRY_PAPER_V1"
    assert run_by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["strategy_id"] == "BNB_F3_DOMINANT_ENTRY_PAPER_V1"
    assert run_by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["expected_exit_horizon"] == ""
    assert run_by_proposal["SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["strategy_id"] == "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"
    assert run_by_proposal["SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["expected_exit_horizon"] == ""
    assert run_by_proposal["ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"]["strategy_id"] == "ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    daily_by_proposal = {row["proposal_id"]: row for row in daily if row["proposal_id"] in by_proposal}
    assert daily_by_proposal["BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["paper_tracker_id"] == "BNB_F3_DOMINANT_ENTRY_PAPER_V1"
    assert daily_by_proposal["SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["paper_tracker_id"] == "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"


def test_paper_strategy_tracker_prefers_bundle_proposals_over_stale_bare_csv(tmp_path: Path) -> None:
    cfg = _cfg(include_bnb=True)
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_bundle_proposals"
    run_dir.mkdir(parents=True)
    (run_dir / "candidate_snapshot.csv").write_text(
        "run_id,ts_utc,symbol,final_decision,strategy_candidate\n",
        encoding="utf-8",
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "120h",
            }
        ],
    )
    _write_paper_strategy_proposal_zip(
        tmp_path / "reports" / "quant_lab_latest_bundle.zip",
        [
            {
                "proposal_id": "BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "BNB-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "120h",
            },
            {
                "proposal_id": "ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "4h",
            },
            {
                "proposal_id": "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "SOL-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "48h",
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_bundle_proposals", start_s),
        market_data_1h={
            "BNB/USDT": _series("BNB/USDT", start_s, {0: 600.0}),
            "ETH/USDT": _series("ETH/USDT", start_s, {0: 1600.0}),
            "SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["proposal_rows"] == 3
    ack = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_proposal_ack.csv")
    by_proposal = {row["proposal_id"]: row for row in ack}
    assert set(by_proposal) == {
        "BNB_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
        "ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1",
        "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
    }
    assert by_proposal["ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"]["accepted"] == "True"
    assert by_proposal["ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"]["paper_tracker_id"] == (
        "ETH_USDT_F4_VOLUME_EXPANSION_ENTRY_PAPER_V1"
    )
    assert by_proposal["SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"]["suggested_horizon"] == "48h"
    assert "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1" not in by_proposal


def test_paper_strategy_tracker_ignores_stale_bundle_proposals(tmp_path: Path) -> None:
    cfg = _cfg(include_bnb=True)
    cfg.diagnostics.quant_lab_paper_strategy_proposals_max_age_minutes = 60
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_stale_bundle_proposals"
    run_dir.mkdir(parents=True)
    (run_dir / "candidate_snapshot.csv").write_text(
        "run_id,ts_utc,symbol,final_decision,strategy_candidate\n",
        encoding="utf-8",
    )
    stale_bundle = tmp_path / "reports" / "quant_lab_latest_bundle.zip"
    _write_paper_strategy_proposal_zip(
        stale_bundle,
        [
            {
                "proposal_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "120h",
            },
            {
                "proposal_id": "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "SOL-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "120h",
            },
        ],
    )
    stale_s = start_s - 4 * 3600
    os.utime(stale_bundle, (stale_s, stale_s))

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_stale_bundle_proposals", start_s),
        market_data_1h={
            "BNB/USDT": _series("BNB/USDT", start_s, {0: 600.0}),
            "ETH/USDT": _series("ETH/USDT", start_s, {0: 1600.0}),
            "SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["proposal_rows"] == 0
    ack = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_proposal_ack.csv")
    assert ack == []


def test_paper_strategy_tracker_tracks_eth_f3_dominant_48h_candidate(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_eth_f3"
    _write_single_sol_candidate(
        run_dir,
        run_id="r_eth_f3",
        overrides={
            "candidate_id": "eth_f3",
            "symbol": "ETH/USDT",
            "strategy_candidate": "f3_dominant_entry",
            "target_weight_raw": "0.10",
            "risk_level": "NORMAL",
            "alpha6_side": "buy",
            "f4_volume_expansion": "-0.2",
            "regime_state": "Trending",
        },
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "48h",
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_eth_f3", start_s),
        market_data_1h={
            "ETH/USDT": _series("ETH/USDT", start_s, {0: 100.0, 24: 104.0, 48: 106.0}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["proposal_rows"] == 1
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    eth = next(row for row in runs if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth["symbol"] == "ETH/USDT"
    assert eth["would_enter"] == "True"
    assert eth["source_strategy_candidate"] == "f3_dominant_entry"
    assert eth["expected_exit_horizon"] == "48h"
    assert eth["label_status"] == "pending"

    update_sol_paper_strategy_tracker(
        run_dir=tmp_path / "reports" / "runs" / "r_eth_f3_mature",
        audit=_audit("r_eth_f3_mature", start_s + 48 * 3600),
        market_data_1h={
            "ETH/USDT": _series("ETH/USDT", start_s, {0: 100.0, 24: 104.0, 48: 106.0}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    eth = next(
        row
        for row in runs
        if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1" and row["would_enter"] == "True"
    )
    assert eth["paper_pnl_bps_48h"] == "570.0"
    assert eth["paper_pnl_bps"] == "570.0"
    assert "f3_global_evidence_negative" in eth["live_block_reason"]


def test_paper_strategy_tracker_blocks_eth_f3_when_alpha6_side_not_buy(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_eth_f3_alpha6_sell"
    _write_single_sol_candidate(
        run_dir,
        run_id="r_eth_f3_alpha6_sell",
        overrides={
            "candidate_id": "eth_f3_sell",
            "symbol": "ETH/USDT",
            "strategy_candidate": "f3_dominant_entry",
            "final_decision": "no_order",
            "target_weight_raw": "0.10",
            "risk_level": "NORMAL",
            "alpha6_side": "sell",
            "f4_volume_expansion": "0.4",
            "regime_state": "Trending",
        },
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "48h",
            }
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_eth_f3_alpha6_sell", start_s),
        market_data_1h={"ETH/USDT": _series("ETH/USDT", start_s, {0: 100.0, 48: 106.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    eth = next(row for row in runs if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth["would_enter"] == "False"
    assert eth["alpha6_side"] == "sell"
    assert eth["final_decision"] == "heartbeat"
    assert eth["no_sample_reason"] == "eth_f3_alpha6_side_not_buy_no_new_entry"
    assert eth["label_status"] == "heartbeat"

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    eth_daily = next(row for row in daily if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth_daily["entry_count"] == "0"


def test_paper_strategy_tracker_rewrites_legacy_eth_f3_alpha6_sell_entries(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True)
    legacy = {
        "strategy_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
        "experiment_name": "v5.eth_f3_dominant_entry",
        "run_id": "r_eth_f3_legacy_sell",
        "ts_utc": "2026-05-25T00:00:00Z",
        "entry_ts_ms": start_s * 1000,
        "paper_date": "2026-05-25",
        "symbol": "ETH/USDT",
        "source_strategy_candidate": "f3_dominant_entry",
        "candidate_id": "eth_f3_legacy_sell",
        "final_decision": "no_order",
        "alpha6_side": "sell",
        "would_enter": True,
        "would_size_notional": 25.0,
        "would_size_usdt": 25.0,
        "label_status": "pending",
        "paper_pnl_bps_48h": 120.0,
    }
    (reports_dir / "sol_paper_strategy_labels.jsonl").write_text(json.dumps(legacy) + "\n", encoding="utf-8")

    result = update_sol_paper_strategy_tracker(
        run_dir=reports_dir / "runs" / "r_eth_f3_followup",
        audit=_audit("r_eth_f3_followup", start_s + 3600),
        market_data_1h={"ETH/USDT": _series("ETH/USDT", start_s, {0: 100.0, 48: 106.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["eth_f3_alpha6_gate_rewrites"] == 1
    runs = _read_csv(reports_dir / "summaries" / "paper_strategy_runs.csv")
    eth = next(row for row in runs if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth["would_enter"] == "False"
    assert eth["alpha6_side"] == "sell"
    assert eth["final_decision"] == "no_order"
    assert eth["no_sample_reason"] == "eth_f3_alpha6_side_not_buy_no_new_entry"
    assert eth["label_status"] == "heartbeat"
    assert eth["paper_pnl_bps_48h"] == ""

    daily = _read_csv(reports_dir / "summaries" / "paper_strategy_daily.csv")
    eth_daily = next(row for row in daily if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth_daily["entry_count"] == "0"


def test_paper_strategy_tracker_blocks_eth_f3_when_risk_off(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_eth_f3_risk_off"
    _write_single_sol_candidate(
        run_dir,
        run_id="r_eth_f3_risk_off",
        overrides={
            "candidate_id": "eth_f3",
            "symbol": "ETH/USDT",
            "strategy_candidate": "f3_dominant_entry",
            "risk_level": "PROTECT",
            "regime_state": "Risk-Off",
        },
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1",
                "strategy_candidate": "v5.f3_dominant_entry",
                "symbol": "ETH-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "48h",
            }
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_eth_f3_risk_off", start_s),
        market_data_1h={"ETH/USDT": _series("ETH/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    eth = next(row for row in runs if row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1")
    assert eth["would_enter"] == "False"
    assert eth["no_sample_reason"] == "risk_off"
    assert eth["risk_off"] == "True"


def test_sol_paper_strategy_tracker_reads_paper_ready_advisory(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_advisory_paper"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "reason": "paper_only",
                "max_paper_notional_usdt": "12",
                "max_live_notional_usdt": "50",
                "live_block_reasons": "no_live_slippage_coverage",
                **_fresh_meta(start_s),
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_advisory_paper", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["advisory_rows"] == 1
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["would_enter"] == "False"
    assert f4["advisory_present"] == "True"
    assert f4["advisory_decision"] == "PAPER_READY"
    assert f4["advisory_recommended_mode"] == "paper"
    assert f4["advisory_strategy_candidate"] == "f4_volume_swing"
    assert f4["advisory_response_action"] == "paper_tracking"
    assert f4["advisory_max_paper_notional_usdt"] == "12.0"
    assert f4["advisory_max_live_notional_usdt_ignored"] == "True"
    assert f4["advisory_live_block_reasons"] == "no_live_slippage_coverage"

    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["strategy_candidate"] == "f4_volume_swing"
    assert advisory[0]["response_action"] == "paper_tracking"
    assert advisory[0]["negative_advisory"] == "False"
    assert advisory[0]["max_paper_notional_usdt"] == "12.0"


def test_sol_f4_proposal_uses_same_horizon_paper_advisory_over_kill(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_sol_f4_horizon_advisory"
    run_dir.mkdir(parents=True)
    (run_dir / "candidate_snapshot.csv").write_text(
        "run_id,ts_utc,symbol,final_decision,strategy_candidate\n",
        encoding="utf-8",
    )
    _write_paper_strategy_proposals(
        tmp_path / "reports",
        [
            {
                "proposal_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "SOL-USDT",
                "recommended_mode": "paper",
                "suggested_horizon": "72h",
            }
        ],
    )
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "SOL-USDT",
                "decision": "KILL",
                "recommended_mode": "none",
                "horizon_hours": "4",
                "live_block_reasons": "short_horizon_negative",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "SOL-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "horizon_hours": "72",
                "max_paper_notional_usdt": "100",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_sol_f4_horizon_advisory", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["proposal_rows"] == 1
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["would_enter"] == "False"
    assert f4["no_sample_reason"] == "no_sol_candidate"
    assert f4["advisory_decision"] == "PAPER_READY"
    assert f4["advisory_recommended_mode"] == "paper"
    assert f4["advisory_response_action"] == "paper_tracking"
    assert f4["advisory_match_key"] == "v5.f4_volume_expansion_entry:72h"
    assert f4["advisory_match_reason"] == "proposal_candidate_same_horizon"
    assert f4["advisory_max_paper_notional_usdt"] == "100.0"


def test_sol_paper_strategy_tracker_reads_advisory_from_expert_pack_tar(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_paths = [
        "reports/quant_lab_latest_bundle.tar.gz"
    ]
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_advisory_tar"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory_bundle(
        tmp_path / "reports" / "quant_lab_latest_bundle.tar.gz",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "11",
                "live_block_reasons": "no_paper_days",
                **_fresh_meta(start_s),
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_advisory_tar", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["advisory_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["source_path"].endswith(
        "quant_lab_latest_bundle.tar.gz:quant-lab-pack/reports/strategy_opportunity_advisory.csv"
    )
    assert advisory[0]["strategy_candidate"] == "f4_volume_swing"
    assert advisory[0]["response_action"] == "paper_tracking"
    assert advisory[0]["max_paper_notional_usdt"] == "11.0"


def test_sol_paper_strategy_tracker_reads_advisory_from_expert_pack_zip(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_paths = [
        "reports/quant_lab_latest_bundle.zip"
    ]
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_advisory_zip"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory_zip(
        tmp_path / "reports" / "quant_lab_latest_bundle.zip",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "13",
                **_fresh_meta(start_s),
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_advisory_zip", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["advisory_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["source_path"].endswith(
        "quant_lab_latest_bundle.zip:reports/strategy_opportunity_advisory.csv"
    )
    assert advisory[0]["strategy_candidate"] == "f4_volume_swing"
    assert advisory[0]["response_action"] == "paper_tracking"
    assert advisory[0]["max_paper_notional_usdt"] == "13.0"


def test_sol_paper_strategy_tracker_records_kill_advisory_without_paper_entry(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_advisory_kill"
    _write_single_sol_candidate(run_dir, run_id="r_advisory_kill", overrides={})
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "experiment_name": "v5.f4_volume_expansion_entry",
                "symbol": "SOL/USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                "reason": "negative_evidence",
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_advisory_kill", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["advisory_rows"] == 1
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["would_enter"] == "False"
    assert f4["no_sample_reason"] == "quant_lab_advisory_kill"
    assert f4["advisory_negative"] == "True"
    assert f4["advisory_response_action"] == "negative_advisory"
    assert f4["advisory_decision"] == "KILL"


def test_sol_paper_strategy_tracker_ignores_live_small_notional_by_default(tmp_path: Path) -> None:
    cfg = _cfg()
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_advisory_live_small"
    _write_single_sol_candidate(run_dir, run_id="r_advisory_live_small", overrides={})
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "experiment_name": "v5.f4_volume_expansion_entry",
                "symbol": "SOL/USDT",
                "decision": "LIVE_SMALL_READY",
                "recommended_mode": "live",
                "max_live_notional_usdt": "25",
                **_fresh_meta(start_s),
            }
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_advisory_live_small", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["would_enter"] == "True"
    assert f4["enable_live_small_from_quant_lab"] == "False"
    assert f4["advisory_response_action"] == "ignored_live_small_disabled"
    assert f4["advisory_max_live_notional_usdt"] == "25.0"
    assert f4["advisory_max_live_notional_usdt_ignored"] == "True"


def test_sol_paper_strategy_tracker_reads_api_advisory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.base_url = "https://quant-lab.local"
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_paths = [str(tmp_path / "missing.csv")]
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_advisory_api"
    run_dir.mkdir(parents=True)

    api_call_count = 0

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            nonlocal api_call_count
            api_call_count += 1
            return SimpleNamespace(
                ok=True,
                data={
                    "rows": [
                        {
                            "strategy_candidate": "f4_volume_swing",
                            "symbol": "SOL/USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "max_paper_notional_usdt": 10,
                            **_fresh_meta(start_s),
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_advisory_api", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["advisory_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["source_path"] == "api:/v1/strategy-opportunity-advisory/v5-compact"
    assert advisory[0]["strategy_candidate"] == "f4_volume_swing"
    assert advisory[0]["response_action"] == "paper_tracking"


def test_strategy_advisory_entry_quality_rows_are_display_only(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_entry_quality_advisory"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "v5.entry_quality_missed_low_audit",
                "symbol": "BTC/USDT",
                "decision": "KEEP_RESEARCH",
                "recommended_mode": "research",
                "no_sample_reason": "research_only",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.late_entry_chase_guard_shadow",
                "symbol": "BTC/USDT",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "would_block_if_enabled": "true",
                "no_sample_reason": "late_chase_loss_shadow",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.pullback_reversal_shadow_sol",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "would_enter": "true",
                "max_live_notional_usdt": "50",
                **_fresh_meta(start_s),
            },
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_entry_quality_advisory", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    by_candidate = {row["strategy_candidate"]: row for row in advisory}
    missed_low = by_candidate["v5.entry_quality_missed_low_audit"]
    late_chase = by_candidate["v5.late_entry_chase_guard_shadow"]
    pullback = by_candidate["v5.pullback_reversal_shadow_sol"]
    assert missed_low["recommended_mode"] == "research"
    assert missed_low["response_action"] == "research_display_only"
    assert late_chase["response_action"] == "shadow_tracking"
    assert late_chase["would_block_if_enabled"] == "True"
    assert late_chase["no_sample_reason"] == "late_chase_loss_shadow"
    assert pullback["response_action"] == "paper_tracking"
    assert pullback["would_enter"] == "True"
    assert pullback["max_live_notional_usdt_ignored"] == "True"


def test_expanded_paper_universe_advisory_is_read_only(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_expanded_paper"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "TRX_EXPANDED_PAPER_V1",
                "strategy_candidate": "v5.expanded_paper_trx_breakout",
                "symbol": "TRX-USDT",
                "universe_type": "expanded_paper",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "7",
                "max_live_notional_usdt": "100",
                **_fresh_meta(start_s),
            },
            {
                "strategy_id": "HYPE_EXPANDED_SHADOW_V1",
                "strategy_candidate": "v5.expanded_paper_hype_shadow",
                "symbol": "HYPE-USDT",
                "universe_type": "expanded_paper",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "no_sample_reason": "needs_more_samples",
                **_fresh_meta(start_s),
            },
            {
                "strategy_id": "SUI_EXPANDED_KILL_V1",
                "strategy_candidate": "v5.expanded_paper_sui_reversal",
                "symbol": "SUI-USDT",
                "universe_type": "expanded_paper",
                "decision": "KILL",
                "recommended_mode": "paper",
                "live_block_reasons": "negative_evidence",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_expanded_paper", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert cfg.symbols == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert result["expanded_universe_advisory_rows"] == 3
    assert result["expanded_universe_paper_rows"] == 3

    advisory = _read_csv(tmp_path / "reports" / "summaries" / "expanded_universe_advisory_reader.csv")
    by_symbol = {row["symbol"]: row for row in advisory}
    assert by_symbol["TRX/USDT"]["response_action"] == "paper_tracking"
    assert by_symbol["TRX/USDT"]["symbol_in_live_universe"] == "False"
    assert by_symbol["TRX/USDT"]["max_live_notional_usdt"] == "0.0"
    assert by_symbol["TRX/USDT"]["max_live_notional_usdt_ignored"] == "True"
    assert by_symbol["HYPE/USDT"]["response_action"] == "shadow_tracking"
    assert by_symbol["SUI/USDT"]["response_action"] == "negative_advisory"
    assert all(row["live_order_effect"] == "read_only_no_live_order" for row in advisory)

    runs = _read_csv(tmp_path / "reports" / "summaries" / "expanded_universe_paper_runs.csv")
    run_by_symbol = {row["symbol"]: row for row in runs}
    assert run_by_symbol["TRX/USDT"]["tracking_mode"] == "paper"
    assert run_by_symbol["TRX/USDT"]["would_enter"] == "True"
    assert run_by_symbol["TRX/USDT"]["would_size_usdt"] == "7.0"
    assert run_by_symbol["HYPE/USDT"]["tracking_mode"] == "shadow"
    assert run_by_symbol["HYPE/USDT"]["would_enter"] == "False"
    assert run_by_symbol["HYPE/USDT"]["no_sample_reason"] == "needs_more_samples"
    assert run_by_symbol["SUI/USDT"]["tracking_mode"] == "negative"
    assert run_by_symbol["SUI/USDT"]["would_enter"] == "False"
    assert run_by_symbol["SUI/USDT"]["no_sample_reason"] == "negative_advisory"


def test_hype_wld_expanded_paper_ready_advisory_generates_paper_strategy_rows(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_hype_wld_expanded"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_id": "HYPE_EXPANDED_UNIVERSE_PAPER_V1",
                "strategy_candidate": "v5.expanded_universe_hype_paper",
                "symbol": "HYPE-USDT",
                "universe_type": "expanded_paper",
                "expanded_universe_maturity_state": "PAPER_READY",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "8",
                "max_live_notional_usdt": "0",
                "cost_source": "public_spread_proxy",
                "cost_bps": "18",
                "label_4h_net_bps": "42",
                **_fresh_meta(start_s),
            },
            {
                "strategy_id": "WLD_EXPANDED_UNIVERSE_PAPER_V1",
                "strategy_candidate": "v5.expanded_universe_wld_paper",
                "symbol": "WLD-USDT",
                "universe_type": "expanded_paper",
                "expanded_universe_maturity_state": "PAPER_READY",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "6",
                "max_live_notional_usdt": "0",
                "cost_source": "mixed_actual_proxy",
                "cost_bps": "15",
                "label_4h_after_cost_bps": "55",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_hype_wld_expanded", start_s),
        market_data_1h={
            "HYPE/USDT": _series("HYPE/USDT", start_s, {0: 30.0}),
            "WLD/USDT": _series("WLD/USDT", start_s, {0: 2.5}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert cfg.symbols == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert result["expanded_universe_paper_rows"] == 2
    runs = _read_csv(reports_dir / "summaries" / "paper_strategy_runs.csv")
    by_strategy = {row["strategy_id"]: row for row in runs}
    hype = by_strategy["HYPE_EXPANDED_UNIVERSE_PAPER_V1"]
    wld = by_strategy["WLD_EXPANDED_UNIVERSE_PAPER_V1"]
    assert hype["would_enter"] == "True"
    assert hype["would_size_usdt"] == "8.0"
    assert hype["advisory_response_action"] == "paper_tracking"
    assert hype["cost_source"] == "public_spread_proxy"
    assert hype["live_order_effect"] == "read_only_no_live_order"
    assert wld["would_enter"] == "True"
    assert wld["would_size_usdt"] == "6.0"
    assert wld["cost_source"] == "mixed_actual_proxy"
    assert wld["live_order_effect"] == "read_only_no_live_order"
    assert all(row["live_symbols_unchanged"] == "True" for row in runs)

    expanded_runs = _read_csv(reports_dir / "summaries" / "expanded_universe_paper_runs.csv")
    expanded_by_strategy = {row["strategy_id"]: row for row in expanded_runs}
    assert expanded_by_strategy["HYPE_EXPANDED_UNIVERSE_PAPER_V1"]["paper_pnl_bps_4h"] == "42"
    assert expanded_by_strategy["WLD_EXPANDED_UNIVERSE_PAPER_V1"]["paper_pnl_bps_4h"] == "55"
    assert all(row["live_order_effect"] == "read_only_no_live_order" for row in expanded_runs)

    expanded_daily = _read_csv(reports_dir / "summaries" / "expanded_universe_paper_daily.csv")
    daily_by_strategy = {row["strategy_id"]: row for row in expanded_daily}
    assert daily_by_strategy["HYPE_EXPANDED_UNIVERSE_PAPER_V1"]["entry_count"] == "1"
    assert daily_by_strategy["HYPE_EXPANDED_UNIVERSE_PAPER_V1"]["avg_paper_pnl_bps_4h"] == "42.0"
    assert daily_by_strategy["WLD_EXPANDED_UNIVERSE_PAPER_V1"]["avg_paper_pnl_bps_4h"] == "55.0"
    assert all(row["live_order_effect"] == "read_only_no_live_order" for row in expanded_daily)


def test_expanded_relative_strength_advisory_without_universe_type_is_tracked(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_expanded_relative_strength"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_id": "ALLO_USDT_V5_EXPANDED_RELATIVE_STRENGTH_TOP1_SHADOW",
                "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                "template_family": "expanded_relative_strength",
                "symbol": "ALLO-USDT",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "horizon_hours": "24",
                "sample_count": "1600",
                "complete_sample_count": "1590",
                "expanded_universe_maturity_state": "KEEP_SHADOW",
                "no_sample_reason": "shadow_only",
                **_fresh_meta(start_s),
            },
            {
                "strategy_id": "ALL_REGIME_ROUTER_V5_EXPANDED_RELATIVE_STRENGTH_TOP1_SHADOW",
                "strategy_candidate": "regime_router:v5.expanded_relative_strength_top1_shadow",
                "symbol": "ALL",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "no_sample_reason": "regime_router_summary_only",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_expanded_relative_strength", start_s),
        market_data_1h={"ALLO/USDT": _series("ALLO/USDT", start_s, {0: 0.4})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["expanded_universe_advisory_rows"] == 2
    assert result["expanded_universe_paper_rows"] == 1

    advisory = _read_csv(reports_dir / "summaries" / "expanded_universe_advisory_reader.csv")
    assert {row["symbol"] for row in advisory} == {"ALLO/USDT", "ALL"}
    allo = next(row for row in advisory if row["symbol"] == "ALLO/USDT")
    assert allo["universe_type"] == "expanded_paper"
    assert allo["response_action"] == "shadow_tracking"
    assert allo["live_order_effect"] == "read_only_no_live_order"

    runs = _read_csv(reports_dir / "summaries" / "expanded_universe_paper_runs.csv")
    assert [row["symbol"] for row in runs] == ["ALLO/USDT"]
    assert runs[0]["tracking_mode"] == "shadow"
    assert runs[0]["no_sample_reason"] == "shadow_only"


def test_expanded_paper_no_entry_rows_have_standard_no_sample_reason(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_expanded_no_entry"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_id": "HYPE_EXPANDED_UNIVERSE_PAPER_V1",
                "strategy_candidate": "v5.expanded_universe_hype_paper",
                "symbol": "HYPE-USDT",
                "universe_type": "expanded_paper",
                "expanded_universe_maturity_state": "PAPER_READY",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "would_enter": "false",
                "cost_source": "global_default_v0",
                "max_live_notional_usdt": "0",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.expanded_universe_wld_paper",
                "symbol": "WLD-USDT",
                "universe_type": "expanded_paper",
                "expanded_universe_maturity_state": "PAPER_READY",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "would_enter": "false",
                "cost_source": "public_spread_proxy",
                "max_live_notional_usdt": "0",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_expanded_no_entry", start_s),
        market_data_1h={"HYPE/USDT": _series("HYPE/USDT", start_s, {0: 30.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["expanded_universe_paper_rows"] == 2
    runs = _read_csv(reports_dir / "summaries" / "expanded_universe_paper_runs.csv")
    by_symbol = {row["symbol"]: row for row in runs}
    assert by_symbol["HYPE/USDT"]["would_enter"] == "False"
    assert by_symbol["HYPE/USDT"]["no_sample_reason"] == "cost_source_global_default"
    assert by_symbol["WLD/USDT"]["would_enter"] == "False"
    assert by_symbol["WLD/USDT"]["no_sample_reason"] == "missing_strategy_id"
    assert all(row["live_order_effect"] == "read_only_no_live_order" for row in runs)


def test_expanded_paper_stale_display_only_outputs_no_entry_diagnostic(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.quant_lab.enabled = True
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_api_enabled = False
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_expanded_stale"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_id": "HYPE_EXPANDED_UNIVERSE_PAPER_V1",
                "strategy_candidate": "v5.expanded_universe_hype_paper",
                "symbol": "HYPE-USDT",
                "universe_type": "expanded_paper",
                "expanded_universe_maturity_state": "PAPER_READY",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "would_enter": "true",
                "max_paper_notional_usdt": "8",
                "max_live_notional_usdt": "0",
                "generated_at": str(start_s),
                "as_of_ts": str(start_s),
                "expires_at": str(start_s + 600),
                "contract_version": CONTRACT_VERSION,
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_expanded_stale", start_s + 1200),
        market_data_1h={"HYPE/USDT": _series("HYPE/USDT", start_s + 1200, {0: 30.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["expanded_universe_advisory_rows"] == 1
    assert result["expanded_universe_paper_rows"] == 1
    runs = _read_csv(reports_dir / "summaries" / "expanded_universe_paper_runs.csv")
    row = runs[0]
    assert row["response_action"] == "stale_paper_display_only"
    assert row["would_enter"] == "False"
    assert row["would_size_usdt"] == "0.0"
    assert row["no_sample_reason"] == "stale_advisory_display_only"
    daily = _read_csv(reports_dir / "summaries" / "expanded_universe_paper_daily.csv")
    assert daily[0]["entry_count"] == "0"


def test_bottom_zone_probe_paper_advisory_generates_read_only_paper_row(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_bottom_zone"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_id": "BOTTOM_ZONE_PROBE_PAPER_V1",
                "strategy_candidate": "v5.bottom_zone_probe_paper",
                "symbol": "BNB-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "would_enter": "true",
                "horizon_hours": "24",
                "max_paper_notional_usdt": "5",
                "max_live_notional_usdt": "0",
                "cost_source": "public_spread_proxy",
                "cost_bps": "20",
                **_fresh_meta(start_s),
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_bottom_zone", start_s),
        market_data_1h={"BNB/USDT": _series("BNB/USDT", start_s, {0: 640.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["total_records"] >= 1
    runs = _read_csv(reports_dir / "summaries" / "paper_strategy_runs.csv")
    bottom = next(row for row in runs if row["strategy_id"] == "BOTTOM_ZONE_PROBE_PAPER_V1")
    assert bottom["symbol"] == "BNB/USDT"
    assert bottom["would_enter"] == "True"
    assert bottom["would_size_usdt"] == "5.0"
    assert bottom["advisory_response_action"] == "paper_tracking"
    assert bottom["live_order_effect"] == "read_only_no_live_order"
    assert bottom["enable_live_experiment"] == "False"
    bottom_runs = _read_csv(reports_dir / "summaries" / "bottom_zone_probe_paper_runs.csv")
    assert bottom_runs[0]["strategy_id"] == "BOTTOM_ZONE_PROBE_PAPER_V1"
    assert bottom_runs[0]["would_enter"] == "True"
    assert bottom_runs[0]["live_order_effect"] == "read_only_no_live_order"
    bottom_daily = _read_csv(reports_dir / "summaries" / "bottom_zone_probe_paper_daily.csv")
    assert bottom_daily[0]["strategy_id"] == "BOTTOM_ZONE_PROBE_PAPER_V1"
    assert bottom_daily[0]["entry_count"] == "1"
    assert cfg.symbols == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]


def test_alpha_factory_advisory_reader_is_read_only(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_alpha_factory"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "source_module": "alpha_factory",
                "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                "symbol": "TRX-USDT",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "promotion_state": "stage2_shadow",
                "alpha_factory_score": "0.77",
                "max_live_notional_usdt": "100",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.af.factory_research_only",
                "symbol": "SUI-USDT",
                "decision": "KEEP_RESEARCH",
                "recommended_mode": "research",
                "promotion_state": "research",
                "alpha_factory_score": "0.51",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.futures_downtrend_short_proxy_shadow",
                "symbol": "BTC-USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                "promotion_state": "killed",
                "alpha_factory_score": "0.12",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.btc_strict_probe_exit_policy_review",
                "symbol": "BTC-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "promotion_state": "review",
                "alpha_factory_score": "0.63",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.not_alpha_factory",
                "symbol": "SOL-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_alpha_factory", start_s),
        market_data_1h={"BTC/USDT": _series("BTC/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["alpha_factory_advisory_rows"] == 4
    assert result["alpha_factory_family_rows"] == 4
    reader = _read_csv(tmp_path / "reports" / "summaries" / "alpha_factory_advisory_reader.csv")
    by_candidate = {row["strategy_candidate"]: row for row in reader}
    assert by_candidate["v5.expanded_relative_strength_top1_shadow"]["response_action"] == "shadow_tracking"
    assert by_candidate["v5.expanded_relative_strength_top1_shadow"]["alpha_factory_score"] == "0.77"
    assert by_candidate["v5.af.factory_research_only"]["response_action"] == "display_only"
    assert by_candidate["v5.futures_downtrend_short_proxy_shadow"]["response_action"] == "negative_advisory"
    assert by_candidate["v5.btc_strict_probe_exit_policy_review"]["response_action"] == "paper_tracking"
    assert "v5.not_alpha_factory" not in by_candidate
    assert by_candidate["v5.expanded_relative_strength_top1_shadow"]["stale_reason"] == ""
    assert by_candidate["v5.expanded_relative_strength_top1_shadow"]["stale_response_downgraded"] == "False"
    assert all(row["max_live_notional_usdt_ignored"] == "True" for row in reader)
    assert all(row["live_order_effect"] == "read_only_no_live_order" for row in reader)

    families = _read_csv(tmp_path / "reports" / "summaries" / "alpha_factory_family_summary.csv")
    by_family = {row["family"]: row for row in families}
    assert by_family["expanded"]["shadow_tracking_count"] == "1"
    assert by_family["futures"]["negative_advisory_count"] == "1"
    assert by_family["exit_policy"]["paper_tracking_count"] == "1"
    assert by_family["other"]["display_only_count"] == "1"


def test_alpha_factory_stale_advisory_downgrades_tracking_actions(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_api_enabled = False
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_alpha_factory_stale"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "source_module": "alpha_factory",
                "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                "symbol": "TRX-USDT",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "promotion_state": "stage2_shadow",
                "alpha_factory_score": "0.77",
                **_stale_meta(start_s),
            },
            {
                "strategy_candidate": "v5.btc_strict_probe_exit_policy_review",
                "symbol": "BTC-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "promotion_state": "review",
                "alpha_factory_score": "0.63",
                **_stale_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_alpha_factory_stale", start_s),
        market_data_1h={"BTC/USDT": _series("BTC/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["alpha_factory_advisory_rows"] == 2
    reader = _read_csv(tmp_path / "reports" / "summaries" / "alpha_factory_advisory_reader.csv")
    by_candidate = {row["strategy_candidate"]: row for row in reader}
    expanded = by_candidate["v5.expanded_relative_strength_top1_shadow"]
    exit_policy = by_candidate["v5.btc_strict_probe_exit_policy_review"]
    assert expanded["response_action"] == "stale_shadow_display_only"
    assert exit_policy["response_action"] == "stale_paper_display_only"
    assert expanded["stale_response_downgraded"] == "True"
    assert exit_policy["stale_response_downgraded"] == "True"
    assert expanded["stale_reason"]
    assert "expired" in exit_policy["stale_reason"]

    families = _read_csv(tmp_path / "reports" / "summaries" / "alpha_factory_family_summary.csv")
    by_family = {row["family"]: row for row in families}
    assert by_family["expanded"]["display_only_count"] == "1"
    assert by_family["expanded"]["shadow_tracking_count"] == "0"
    assert by_family["exit_policy"]["display_only_count"] == "1"
    assert by_family["exit_policy"]["paper_tracking_count"] == "0"


def test_risk_on_multi_buy_shadow_is_read_only(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_risk_on_multi_buy"
    run_dir.mkdir(parents=True)
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n"
        "2026-05-15T00:00:00Z,r_risk_on_multi_buy,SOL/USDT,OPEN_LONG,buy,1,100,100,0.01\n",
        encoding="utf-8",
    )
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top1_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "selected_symbols": '["ETH-USDT","SOL-USDT","BNB-USDT"]',
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top2_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "selected_symbols": '["ETH-USDT","SOL-USDT","BNB-USDT"]',
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top3_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "would_buy_symbols": "ETH/USDT;SOL/USDT;BNB/USDT",
                **_fresh_meta(start_s),
            },
            {
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "symbol": "SOL-USDT",
                **_fresh_meta(start_s),
            },
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_risk_on_multi_buy", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert cfg.symbols == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert result["risk_on_multi_buy_shadow_rows"] == 3
    rows = _read_csv(tmp_path / "reports" / "summaries" / "risk_on_multi_buy_shadow.csv")
    assert len(rows) == 3
    top1, top2, top3 = rows
    assert top1["current_regime"] == "ALT_IMPULSE"
    assert top1["top_k"] == "1"
    assert json.loads(top1["selected_symbols"]) == ["ETH/USDT"]
    assert json.loads(top1["would_buy_symbols"]) == ["ETH/USDT"]
    assert json.loads(top1["missed_symbols"]) == ["ETH/USDT"]
    assert top1["source_detail_available"] == "False"
    assert top2["current_regime"] == "ALT_IMPULSE"
    assert top2["top_k"] == "2"
    assert json.loads(top2["selected_symbols"]) == ["ETH/USDT", "SOL/USDT"]
    assert json.loads(top2["would_buy_symbols"]) == ["ETH/USDT", "SOL/USDT"]
    assert json.loads(top2["actual_bought_symbols"]) == ["SOL/USDT"]
    assert json.loads(top2["missed_symbols"]) == ["ETH/USDT"]
    assert top2["source_detail_available"] == "False"
    assert top3["top_k"] == "3"
    assert json.loads(top3["would_buy_symbols"]) == ["ETH/USDT", "SOL/USDT", "BNB/USDT"]
    assert json.loads(top3["missed_symbols"]) == ["ETH/USDT", "BNB/USDT"]
    assert all(row["response_action"] == "shadow_tracking" for row in rows)
    assert all(row["live_order_effect"] == "read_only_no_live_order" for row in rows)


def test_risk_on_multi_buy_prefers_detail_file_selected_symbols(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_risk_on_detail"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top2_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "symbol": "MULTI",
                **_fresh_meta(start_s),
            }
        ],
    )
    detail_path = reports_dir / "quant_lab" / "latest" / "reports" / "risk_on_multi_buy_shadow.csv"
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    with detail_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["run_id", "decision_ts", "top_k", "current_regime", "selected_symbols", "would_buy_symbol"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "r_old",
                "decision_ts": "2026-05-25T00:00:00Z",
                "top_k": "2",
                "current_regime": "ALT_IMPULSE",
                "selected_symbols": '["ETH-USDT","SOL-USDT"]',
                "would_buy_symbol": "ETH-USDT",
            }
        )
        writer.writerow(
            {
                "run_id": "r_risk_on_detail",
                "decision_ts": "2026-05-26T00:00:00Z",
                "top_k": "2",
                "current_regime": "ALT_IMPULSE",
                "selected_symbols": '["BNB-USDT","SOL-USDT"]',
                "would_buy_symbol": "BNB-USDT",
            }
        )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_risk_on_detail", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["risk_on_multi_buy_shadow_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "risk_on_multi_buy_shadow.csv")
    row = rows[0]
    assert json.loads(row["selected_symbols"]) == ["BNB/USDT", "SOL/USDT"]
    assert json.loads(row["would_buy_symbols"]) == ["BNB/USDT"]
    assert row["source_detail_available"] == "True"


def test_risk_on_multi_buy_reads_reports_raw_reports_detail_file(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_risk_on_reports_raw"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top1_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "symbol": "MULTI",
                **_fresh_meta(start_s),
            }
        ],
    )
    detail_path = reports_dir / "raw" / "reports" / "risk_on_multi_buy_shadow.csv"
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    with detail_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["run_id", "decision_ts", "top_k", "current_regime", "selected_symbols", "would_buy_symbol"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "r_risk_on_reports_raw",
                "decision_ts": "2026-05-26T00:00:00Z",
                "top_k": "1",
                "current_regime": "ALT_IMPULSE",
                "selected_symbols": '["BNB-USDT"]',
                "would_buy_symbol": "BNB-USDT",
            }
        )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_risk_on_reports_raw", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["risk_on_multi_buy_shadow_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "risk_on_multi_buy_shadow.csv")
    row = rows[0]
    assert json.loads(row["selected_symbols"]) == ["BNB/USDT"]
    assert json.loads(row["would_buy_symbols"]) == ["BNB/USDT"]
    assert row["source_detail_available"] == "True"


def test_late_breakout_failure_protect_shadow_reads_quant_lab_detail(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_late_breakout"
    run_dir.mkdir(parents=True)
    detail_path = reports_dir / "quant_lab" / "latest" / "reports" / "late_breakout_failure_shadow.csv"
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    with detail_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "run_id",
                "ts_utc",
                "symbol",
                "alpha6_score",
                "overextension_score",
                "would_block_entry",
                "future_4h_net_bps",
                "future_8h_net_bps",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "r_late_breakout",
                "ts_utc": "2026-05-30T03:00:00Z",
                "symbol": "BNB-USDT",
                "alpha6_score": "0.98",
                "overextension_score": "0.77",
                "would_block_entry": "true",
                "future_4h_net_bps": "-35.5",
                "future_8h_net_bps": "62.0",
            }
        )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_late_breakout", start_s),
        market_data_1h={"BNB/USDT": _series("BNB/USDT", start_s, {0: 640.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["late_breakout_failure_protect_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "late_breakout_failure_protect_shadow.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "BNB/USDT"
    assert row["would_block_entry"] == "True"
    assert row["future_4h_net_bps"] == "-35.5"
    assert row["future_8h_net_bps"] == "62.0"
    assert row["would_block_loss_count"] == "1"
    assert row["would_block_profit_count"] == "1"
    assert row["live_order_effect"] == "read_only_no_live_order"


def test_late_breakout_failure_protect_shadow_reads_advisory_fallback(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_late_breakout_advisory"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_candidate": "v5.late_breakout_failure_protect_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "symbol": "SOL-USDT",
                "alpha6_score": "0.96",
                "overextension_score": "0.82",
                "would_block_if_enabled": "true",
                "future_4h_net_bps": "-55.0",
                "future_8h_net_bps": "-35.0",
                **_fresh_meta(start_s),
            }
        ],
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_late_breakout_advisory", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["late_breakout_failure_protect_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "late_breakout_failure_protect_shadow.csv")
    row = rows[0]
    assert row["symbol"] == "SOL/USDT"
    assert row["alpha6_score"] == "0.96"
    assert row["overextension_score"] == "0.82"
    assert row["would_block_entry"] == "True"
    assert row["future_4h_net_bps"] == "-55.0"
    assert row["live_order_effect"] == "read_only_no_live_order"


def test_backtest_advisory_reader_is_read_only(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_backtest_advisory"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "symbol": "SOL-USDT",
                **_fresh_meta(start_s),
            }
        ],
    )
    report_path = reports_dir / "quant_lab" / "latest" / "reports" / "research_promotion_decision.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "strategy_id",
                "symbol",
                "horizon_hours",
                "sample_count",
                "complete_sample_count",
                "avg_net_bps",
                "p25_net_bps",
                "win_rate",
                "recommended_stage",
                "decision_reasons",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "strategy_id": "BOTTOM_ZONE_PROBE_BACKTEST",
                "symbol": "BNB-USDT",
                "horizon_hours": "24",
                "sample_count": "42",
                "complete_sample_count": "38",
                "avg_net_bps": "81.5",
                "p25_net_bps": "-20",
                "win_rate": "0.61",
                "recommended_stage": "PAPER",
                "decision_reasons": "paper_days_or_entries_insufficient",
            }
        )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_backtest_advisory", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["backtest_advisory_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "backtest_advisory_reader.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["strategy_id"] == "BOTTOM_ZONE_PROBE_BACKTEST"
    assert row["response_action"] == "paper_tracking"
    assert row["max_live_notional_usdt_ignored"] == "True"
    assert row["live_order_effect"] == "read_only_no_live_order"


def test_fast_microstructure_strategy_shadow_reads_quant_lab_review(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_fast_micro_shadow"
    run_dir.mkdir(parents=True)
    review_path = reports_dir / "quant_lab" / "latest" / "reports" / "fast_microstructure_strategy_review.csv"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    with review_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "strategy_candidate_id",
                "feature_name",
                "symbol",
                "regime",
                "horizon_hours",
                "forward_sample_count",
                "rank_ic",
                "long_short_bps",
                "p25_net_bps",
                "hit_rate",
                "recent_7d_score",
                "lookback_bars",
                "recommended_stage",
                "review_blocking_reasons",
                "response_action",
                "max_live_notional_usdt",
                "live_order_effect",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "strategy_candidate_id": "v5.fast_microstructure.hype_usdt.24h",
                "feature_name": "order_book_imbalance",
                "symbol": "HYPE-USDT",
                "regime": "ALT_IMPULSE",
                "horizon_hours": "24",
                "forward_sample_count": "36",
                "rank_ic": "0.19",
                "long_short_bps": "82.5",
                "p25_net_bps": "12.0",
                "hit_rate": "0.67",
                "recent_7d_score": "0.74",
                "lookback_bars": "720",
                "recommended_stage": "SHADOW_REVIEW",
                "review_blocking_reasons": "",
                "response_action": "shadow_review",
                "max_live_notional_usdt": "999",
                "live_order_effect": "read_only_no_live_order",
            }
        )
        writer.writerow(
            {
                "strategy_candidate_id": "v5.fast_microstructure.sol_usdt.24h",
                "feature_name": "order_book_imbalance",
                "symbol": "SOL-USDT",
                "regime": "RANGE",
                "horizon_hours": "24",
                "forward_sample_count": "36",
                "rank_ic": "0.03",
                "long_short_bps": "5.0",
                "p25_net_bps": "-15.0",
                "hit_rate": "0.51",
                "recent_7d_score": "0.14",
                "lookback_bars": "720",
                "recommended_stage": "VALIDATION_ONLY",
                "review_blocking_reasons": "rank_ic_below_threshold",
                "response_action": "validation_only",
                "max_live_notional_usdt": "0",
                "live_order_effect": "read_only_no_live_order",
            }
        )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_fast_micro_shadow", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["fast_microstructure_strategy_shadow_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "fast_microstructure_strategy_shadow.csv")
    assert len(rows) == 1
    row = rows[0]
    assert row["strategy_candidate_id"] == "v5.fast_microstructure.hype_usdt.24h"
    assert row["symbol"] == "HYPE/USDT"
    assert row["recommended_stage"] == "SHADOW_REVIEW"
    assert row["response_action"] == "shadow_review"
    assert row["max_live_notional_usdt_ignored"] == "True"
    assert row["live_order_effect"] == "read_only_no_live_order"


def test_risk_on_multi_buy_reads_quant_lab_bundle_reports_member(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_risk_on_archive"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top1_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "symbol": "MULTI",
                **_fresh_meta(start_s),
            }
        ],
    )

    stale_path = tmp_path / "raw" / "reports" / "risk_on_multi_buy_shadow.csv"
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    with stale_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["run_id", "decision_ts", "top_k", "current_regime", "selected_symbols", "would_buy_symbol"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "r_stale",
                "decision_ts": "2026-05-25T00:00:00Z",
                "top_k": "1",
                "current_regime": "ALT_IMPULSE",
                "selected_symbols": '["ETH-USDT"]',
                "would_buy_symbol": "ETH-USDT",
            }
        )

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "run_id",
            "decision_ts",
            "top_k",
            "current_regime",
            "generated_at",
            "selected_symbols",
            "would_buy",
            "would_buy_symbol",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "run_id": "r_old_archive",
            "decision_ts": "2026-05-25T00:00:00Z",
            "top_k": "1",
            "current_regime": "ALT_IMPULSE",
            "generated_at": "2026-05-31T03:36:34Z",
            "selected_symbols": '["BTC-USDT"]',
            "would_buy": "True",
            "would_buy_symbol": "BTC-USDT",
        }
    )
    writer.writerow(
        {
            "run_id": "r_risk_on_archive",
            "decision_ts": "2026-05-26T00:00:00Z",
            "top_k": "1",
            "current_regime": "ALT_IMPULSE",
            "generated_at": "2026-05-31T03:36:34Z",
            "selected_symbols": '["BNB-USDT"]',
            "would_buy": "True",
            "would_buy_symbol": "BNB-USDT",
        }
    )
    with zipfile.ZipFile(reports_dir / "quant_lab_latest_bundle.zip", "w") as archive:
        archive.writestr("reports/risk_on_multi_buy_shadow.csv", buffer.getvalue())

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_risk_on_archive", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["risk_on_multi_buy_shadow_rows"] == 1
    rows = _read_csv(reports_dir / "summaries" / "risk_on_multi_buy_shadow.csv")
    row = rows[0]
    assert json.loads(row["selected_symbols"]) == ["BNB/USDT"]
    assert json.loads(row["would_buy_symbols"]) == ["BNB/USDT"]
    assert row["source_detail_available"] == "True"


def test_risk_on_multi_buy_advisory_multi_symbol_is_not_observable_without_detail(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    reports_dir = tmp_path / "reports"
    run_dir = reports_dir / "runs" / "r_risk_on_multi_only"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        reports_dir,
        [
            {
                "strategy_candidate": "v5.risk_on_multi_buy_top2_shadow",
                "decision": "KEEP_SHADOW",
                "recommended_mode": "shadow",
                "current_regime": "ALT_IMPULSE",
                "symbol": "MULTI",
                **_fresh_meta(start_s),
            }
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_risk_on_multi_only", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    rows = _read_csv(reports_dir / "summaries" / "risk_on_multi_buy_shadow.csv")
    row = rows[0]
    assert row["selected_symbols"] == "not_observable"
    assert row["would_buy_symbols"] == "not_observable"
    assert row["source_detail_available"] == "False"


def test_strategy_advisory_uses_fresh_local_without_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_api_enabled = False
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_fresh_local"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "10",
                **_fresh_meta(start_s),
            }
        ],
    )

    from src.quant_lab_client import client as client_mod

    def fail_if_called(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("API should not be called when local advisory is fresh")

    monkeypatch.setattr(client_mod.QuantLabClient, "from_config", classmethod(lambda cls, *args, **kwargs: fail_if_called()))

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_fresh_local", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["advisory_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["advisory_source"] == "local"
    assert advisory[0]["advisory_fresh"] == "True"
    assert advisory[0]["freshness_status"] == "fresh"
    assert advisory[0]["stale_reason"] == ""
    assert advisory[0]["api_fallback_attempted"] == "False"


def test_strategy_advisory_fresh_local_still_uses_newer_api_bottom_zone(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_fresh_local_new_api"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_paper_notional_usdt": "10",
                **_fresh_meta(start_s),
            }
        ],
    )
    api_call_count = 0

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            nonlocal api_call_count
            api_call_count += 1
            return SimpleNamespace(
                ok=True,
                headers={
                    "x-quant-lab-advisory-dataset-generated-at": str(start_s + 120),
                    "x-quant-lab-advisory-row-count": "2",
                },
                cached=False,
                data={
                    "rows": [
                        {
                            "strategy_candidate": "f4_volume_swing",
                            "symbol": "SOL/USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "max_paper_notional_usdt": 10,
                            **_fresh_meta(start_s + 120),
                        },
                        {
                            "strategy_id": "BOTTOM_ZONE_PROBE_PAPER_V1",
                            "strategy_candidate": "v5.bottom_zone_probe_paper",
                            "symbol": "BTC-USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "would_enter": "true",
                            "horizon_hours": "24",
                            "max_paper_notional_usdt": 5,
                            "max_live_notional_usdt": 0,
                            "live_order_effect": "read_only_no_live_order",
                            **_fresh_meta(start_s + 120),
                        },
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_fresh_local_new_api", start_s + 120),
        market_data_1h={
            "SOL/USDT": _series("SOL/USDT", start_s + 120, {0: 100.0}),
            "BTC/USDT": _series("BTC/USDT", start_s + 120, {0: 65000.0}),
        },
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert api_call_count == 1
    assert result["advisory_rows"] == 2
    assert result["bottom_zone_probe_paper_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    bottom = next(row for row in advisory if row["strategy_id"] == "BOTTOM_ZONE_PROBE_PAPER_V1")
    assert bottom["advisory_source"] == "api"
    assert bottom["response_action"] == "paper_tracking"
    assert bottom["max_paper_notional_usdt"] == "5.0"
    bottom_runs = _read_csv(tmp_path / "reports" / "summaries" / "bottom_zone_probe_paper_runs.csv")
    assert bottom_runs[0]["strategy_id"] == "BOTTOM_ZONE_PROBE_PAPER_V1"
    assert bottom_runs[0]["would_enter"] == "True"
    cached = _read_csv(tmp_path / "reports" / "strategy_opportunity_advisory.csv")
    assert any(row["strategy_id"] == "BOTTOM_ZONE_PROBE_PAPER_V1" for row in cached)


def test_strategy_advisory_stale_local_uses_api_and_updates_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_stale_api"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                "max_paper_notional_usdt": "1",
                **_stale_meta(start_s),
            }
        ],
    )
    api_call_count = 0
    api_params: list[dict[str, str]] = []

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            nonlocal api_call_count
            api_call_count += 1
            api_params.append(dict(params or {}))
            return SimpleNamespace(
                ok=True,
                data={
                    "rows": [
                        {
                            "strategy_candidate": "f4_volume_swing",
                            "symbol": "SOL/USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "max_paper_notional_usdt": 99,
                            **_fresh_meta(start_s),
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_stale_api", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert api_call_count == 1
    assert api_params == [
        {
            "format": "json",
            "fields": "minimal",
            "latest_only": "true",
            "fresh_only": "true",
        }
    ]
    assert result["advisory_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["advisory_source"] == "api"
    assert advisory[0]["api_fallback_attempted"] == "True"
    assert advisory[0]["api_fallback_success"] == "True"
    assert advisory[0]["max_paper_notional_usdt"] == "99.0"
    cached = _read_csv(tmp_path / "reports" / "strategy_opportunity_advisory.csv")
    assert cached[0]["max_paper_notional_usdt"] == "99.0"
    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    assert health[0]["local_row_count"] == "1"
    assert health[0]["api_row_count"] == "1"
    assert health[0]["selected_row_count"] == "1"
    assert health[0]["selected_source"] == "api"
    assert health[0]["api_fallback_attempted"] == "True"
    assert health[0]["api_fallback_success"] == "True"


def test_alpha_factory_reader_uses_selected_api_advisory_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_alpha_api"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "source_module": "alpha_factory",
                "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                "symbol": "TRX/USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                "alpha_factory_score": "0.1",
                **_stale_meta(start_s),
            }
        ],
    )

    api_call_count = 0

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            nonlocal api_call_count
            api_call_count += 1
            return SimpleNamespace(
                ok=True,
                data={
                    "rows": [
                        {
                            "source_module": "alpha_factory",
                            "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                            "symbol": "TRX/USDT",
                            "decision": "KEEP_SHADOW",
                            "recommended_mode": "shadow",
                            "promotion_state": "stage2_shadow",
                            "alpha_factory_score": "0.77",
                            **_fresh_meta(start_s),
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_alpha_api", start_s),
        market_data_1h={},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    assert health[0]["selected_source"] == "api"
    assert api_call_count == 1
    reader = _read_csv(tmp_path / "reports" / "summaries" / "alpha_factory_advisory_reader.csv")
    assert len(reader) == 1
    assert reader[0]["strategy_candidate"] == "v5.expanded_relative_strength_top1_shadow"
    assert reader[0]["advisory_source"] == "api"
    assert reader[0]["selected_source"] == "api"
    assert reader[0]["source_health_freshness_status"] == "fresh"
    assert reader[0]["advisory_fresh"] == "True"
    assert reader[0]["response_action"] == "shadow_tracking"
    assert reader[0]["stale_response_downgraded"] == "False"
    assert reader[0]["stale_reason"] == ""
    assert reader[0]["alpha_factory_score"] == "0.77"
    assert reader[0]["advisory_source"] != "stale_local"


def test_strategy_advisory_fresh_api_beats_newer_stale_local(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_fresh_api_beats_stale_local"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "source_module": "alpha_factory",
                "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                "symbol": "TRX/USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                "alpha_factory_score": "0.1",
                "as_of_ts": str(start_s - 100),
                "generated_at": str(start_s - 100),
                "expires_at": str(start_s - 10),
                "contract_version": CONTRACT_VERSION,
            }
        ],
    )

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            return SimpleNamespace(
                ok=True,
                data={
                    "rows": [
                        {
                            "source_module": "alpha_factory",
                            "strategy_candidate": "v5.expanded_relative_strength_top1_shadow",
                            "symbol": "TRX/USDT",
                            "decision": "KEEP_SHADOW",
                            "recommended_mode": "shadow",
                            "promotion_state": "stage2_shadow",
                            "alpha_factory_score": "0.88",
                            "as_of_ts": str(start_s - 200),
                            "generated_at": str(start_s - 200),
                            "expires_at": str(start_s + 3600),
                            "contract_version": CONTRACT_VERSION,
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_fresh_api_beats_stale_local", start_s),
        market_data_1h={},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    assert health[0]["local_fresh"] == "False"
    assert health[0]["api_fresh"] == "True"
    assert health[0]["selected_source"] == "api"
    assert health[0]["selection_reason"] == "fresh_api_over_stale_local"
    assert health[0]["stale_local_overrode_api"] == "False"
    reader = _read_csv(tmp_path / "reports" / "summaries" / "alpha_factory_advisory_reader.csv")
    assert reader[0]["selected_source"] == "api"
    assert reader[0]["source_health_freshness_status"] == "fresh"
    assert reader[0]["advisory_fresh"] == "True"
    assert reader[0]["response_action"] == "shadow_tracking"
    assert reader[0]["stale_reason"] == ""
    assert reader[0]["alpha_factory_score"] == "0.88"


def test_strategy_advisory_source_health_records_api_lake_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_api_metadata"
    run_dir.mkdir(parents=True)

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            return SimpleNamespace(
                ok=True,
                cached=True,
                headers={
                    "x-quant-lab-advisory-dataset-generated-at": "2026-05-27T16:24:34+00:00",
                    "x-quant-lab-advisory-row-count": "1",
                    "x-quant-lab-lake-root-hash": "abc123def4567890",
                    "x-quant-lab-api-cache-hit": "true",
                },
                data={
                    "rows": [
                        {
                            "strategy_candidate": "f4_volume_swing",
                            "symbol": "SOL/USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "max_paper_notional_usdt": 99,
                            **_fresh_meta(start_s),
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_api_metadata", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    assert health[0]["selected_source"] == "api"
    assert health[0]["api_lake_generated_at"] == "2026-05-27T16:24:34+00:00"
    assert health[0]["api_cache_hit"] == "True"
    assert health[0]["selected_source_is_stale"] == "False"


def test_strategy_advisory_stale_local_api_fail_is_paper_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    cfg.diagnostics.enable_live_small_from_quant_lab = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_stale_api_fail"
    _write_single_sol_candidate(run_dir, run_id="r_stale_api_fail", overrides={})
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "symbol": "SOL/USDT",
                "decision": "LIVE_SMALL_READY",
                "recommended_mode": "live",
                "max_live_notional_usdt": "25",
                **_stale_meta(start_s),
            }
        ],
    )

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            return SimpleNamespace(ok=False, data={})

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_stale_api_fail", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["advisory_source"] == "stale_local"
    assert advisory[0]["stale_advisory_used"] == "True"
    assert advisory[0]["api_fallback_attempted"] == "True"
    assert advisory[0]["api_fallback_success"] == "False"
    assert advisory[0]["max_live_notional_usdt"] == "0.0"
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["advisory_response_action"] == "stale_advisory_live_disabled"
    assert f4["advisory_max_live_notional_usdt"] == "0.0"
    assert f4["advisory_fresh"] == "False"
    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    assert health[0]["selected_source"] == "stale_local"
    assert health[0]["stale_reason"]
    assert health[0]["selected_source_is_stale"] == "True"


def test_strategy_advisory_source_health_explains_expires_rule_without_inconsistency(
    tmp_path: Path,
) -> None:
    cfg = _cfg()
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_api_enabled = False
    cfg.diagnostics.quant_lab_strategy_opportunity_advisory_max_age_minutes = 90
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_expired_before_max_age"
    _write_single_sol_candidate(run_dir, run_id="r_expired_before_max_age", overrides={})
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "generated_at": str(start_s),
                "as_of_ts": str(start_s),
                "expires_at": str(start_s + 600),
                "contract_version": CONTRACT_VERSION,
            }
        ],
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_expired_before_max_age", start_s + 1200),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s + 1200, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    row = health[0]
    assert row["advisory_age_sec"] == "1200.0"
    assert row["advisory_max_age_sec"] == "5400.0"
    assert row["advisory_expires_at"]
    assert row["expires_before_generated_at"] == "False"
    assert row["expiry_corrected"] == "False"
    assert row["freshness_basis"] == "row_expires_at"
    assert row["stale_reason"] == "expired"
    assert row["freshness_inconsistency_warning"] == ""
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["response_action"] == "stale_paper_display_only"
    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["advisory_response_action"] == "stale_paper_display_only"


def test_strategy_advisory_invalid_expires_before_generated_is_not_marked_expired() -> None:
    cfg = _cfg()
    start_s = 1_779_000_000

    meta = _assess_advisory_rows(
        [
            {
                "strategy_candidate": "v5.entry_quality_missed_low_audit",
                "generated_at": str(start_s),
                "as_of_ts": str(start_s),
                "expires_at": str(start_s - 60),
                "contract_version": CONTRACT_VERSION,
            }
        ],
        diagnostics=cfg.diagnostics,
        now_ms=(start_s + 300) * 1000,
        source="local",
    )

    assert meta["advisory_fresh"] is True
    assert meta["expires_before_generated_at"] is True
    assert meta["expiry_corrected"] is True
    assert meta["freshness_basis"] == "generated_at_plus_advisory_max_age"
    assert "expired" not in str(meta["stale_reason"])
    assert "age_exceeds_max" not in str(meta["stale_reason"])
    assert meta["stale_reason"] == ""
    assert meta["advisory_expires_at"] == "2026-05-17T08:10:00Z"


def test_strategy_advisory_age_within_max_does_not_report_age_exceeds_max() -> None:
    cfg = _cfg()
    start_s = 1_779_000_000

    meta = _assess_advisory_rows(
        [
            {
                "strategy_candidate": "v5.entry_quality_missed_low_audit",
                "generated_at": str(start_s),
                "as_of_ts": str(start_s),
                "expires_at": str(start_s - 60),
                "contract_version": CONTRACT_VERSION,
            }
        ],
        diagnostics=cfg.diagnostics,
        now_ms=(start_s + 5_073) * 1000,
        source="api",
    )

    assert meta["advisory_age_sec"] == 5073.0
    assert meta["advisory_max_age_sec"] == 5400.0
    assert "age_exceeds_max" not in str(meta["stale_reason"])
    assert "expired" not in str(meta["stale_reason"])
    assert meta["expires_before_generated_at"] is True
    assert meta["expiry_corrected"] is True


def test_strategy_advisory_source_health_warns_when_api_selected_older_than_local(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_api_older"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                "max_paper_notional_usdt": "1",
                **_stale_meta(start_s),
            }
        ],
    )

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            return SimpleNamespace(
                ok=True,
                data={
                    "rows": [
                        {
                            "strategy_candidate": "f4_volume_swing",
                            "symbol": "SOL/USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "generated_at": str(start_s - 11_000),
                            "as_of_ts": str(start_s - 11_000),
                            "expires_at": str(start_s - 10_500),
                            "contract_version": CONTRACT_VERSION,
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_api_older", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    health = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_source_health.csv")
    assert int(health[0]["local_row_count"]) >= 1
    assert health[0]["api_row_count"] == "1"
    assert health[0]["selected_source"] == "stale_local"
    assert health[0]["selected_source_is_stale"] == "True"
    assert health[0]["warning"] == "selected_local_newer_than_api"
    assert health[0]["suggested_fix"] == "refresh_quant_lab_api_lake_from_latest_bundle"
    assert "expired" in health[0]["stale_reason"]
    assert health[0]["stale_reason_detail"]


def test_strategy_advisory_contract_mismatch_falls_back_to_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_contract_mismatch"
    run_dir.mkdir(parents=True)
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_candidate": "f4_volume_swing",
                "symbol": "SOL/USDT",
                "decision": "KILL",
                "recommended_mode": "shadow",
                **_fresh_meta(start_s, contract_version="old.contract"),
            }
        ],
    )

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            return SimpleNamespace(
                ok=True,
                data={
                    "rows": [
                        {
                            "strategy_candidate": "f4_volume_swing",
                            "symbol": "SOL/USDT",
                            "decision": "PAPER_READY",
                            "recommended_mode": "paper",
                            "max_paper_notional_usdt": 44,
                            **_fresh_meta(start_s),
                        }
                    ]
                },
            )

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_contract_mismatch", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["advisory_source"] == "api"
    assert advisory[0]["advisory_contract_match"] == "True"
    assert advisory[0]["max_paper_notional_usdt"] == "44.0"


def test_strategy_advisory_stale_paper_ready_keeps_paper_without_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
    cfg.diagnostics.enable_live_small_from_quant_lab = True
    start_s = 1_779_000_000
    run_dir = tmp_path / "reports" / "runs" / "r_stale_paper"
    _write_single_sol_candidate(run_dir, run_id="r_stale_paper", overrides={})
    _write_strategy_advisory(
        tmp_path / "reports",
        [
            {
                "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
                "symbol": "SOL/USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "max_live_notional_usdt": "25",
                **_stale_meta(start_s),
            }
        ],
    )

    class FakeClient:
        def get_json(self, endpoint: str, params: dict | None = None) -> SimpleNamespace:
            raise RuntimeError("api unavailable")

    from src.quant_lab_client import client as client_mod

    monkeypatch.setattr(
        client_mod.QuantLabClient,
        "from_config",
        classmethod(lambda cls, *args, **kwargs: FakeClient()),
    )

    update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r_stale_paper", start_s),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    f4 = next(row for row in runs if row["strategy_id"] == "SOL_F4_VOLUME_EXPANSION_PAPER_V1")
    assert f4["would_enter"] == "True"
    assert f4["advisory_response_action"] == "stale_paper_display_only"
    assert f4["advisory_source"] == "stale_local"
    assert f4["advisory_max_live_notional_usdt"] == "0.0"


def test_sol_paper_strategy_tracker_disabled_writes_no_files(tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.diagnostics.paper_strategy_tracking_enabled = False
    run_dir = tmp_path / "reports" / "runs" / "r1"
    _write_candidate_snapshot(run_dir)

    result = update_sol_paper_strategy_tracker(
        run_dir=run_dir,
        audit=_audit("r1", 1_779_000_000),
        market_data_1h={"SOL/USDT": _series("SOL/USDT", 1_779_000_000, {0: 100.0})},
        cfg=cfg,
        cache_dir=tmp_path / "cache",
    )

    assert result["enabled"] is False
    assert not (tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv").exists()
