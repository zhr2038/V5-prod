from __future__ import annotations

import csv
import io
import json
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.sol_paper_strategy_tracker import (
    _daily_rows,
    _readiness_for_rows,
    update_sol_paper_strategy_tracker,
)


CONTRACT_VERSION = "v5.quant_lab.telemetry.v2"


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


def test_eth_f3_negative_24h_or_48h_downgrades_to_keep_shadow() -> None:
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

    assert readiness["readiness_status"] == "KEEP_SHADOW"
    assert readiness["live_small_ready"] is False
    assert "eth_f3_negative_24h_or_48h_paper_pnl" in readiness["live_block_reason"]


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
                "extra_live_block_reasons": "cost_source_not_actual_or_mixed;f3_global_evidence_negative;no_paper_pnl_observations",
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


def _cfg() -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.diagnostics.paper_strategy_tracking_enabled = True
    cfg.diagnostics.paper_strategy_enabled_shadow_only = True
    cfg.diagnostics.paper_strategy_enable_live_experiment = False
    cfg.diagnostics.paper_strategy_required_paper_days = 14
    cfg.diagnostics.paper_strategy_required_slippage_coverage = 0.8
    cfg.diagnostics.paper_strategy_required_entry_days = 3
    cfg.diagnostics.paper_strategy_horizons_hours = [4, 8, 12, 24, 48, 72]
    cfg.diagnostics.paper_strategy_rt_cost_bps = 30.0
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
    assert "no_paper_pnl_observations" in eth["live_block_reason"]

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    assert any(row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1" for row in daily)
    coverage = _read_csv(tmp_path / "reports" / "summaries" / "paper_slippage_coverage.csv")
    assert any(row["strategy_id"] == "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1" for row in coverage)


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
            "alpha6_side": "sell",
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
            },
            {
                "strategy_candidate": "v5.f4_volume_expansion_entry",
                "symbol": "SOL-USDT",
                "decision": "PAPER_READY",
                "recommended_mode": "paper",
                "horizon_hours": "72",
                "max_paper_notional_usdt": "100",
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
                            "max_paper_notional_usdt": 10,
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
    assert advisory[0]["source_path"] == "api:/v1/strategy-opportunity-advisory"
    assert advisory[0]["strategy_candidate"] == "f4_volume_swing"
    assert advisory[0]["response_action"] == "paper_tracking"


def test_strategy_advisory_uses_fresh_local_without_api(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _cfg()
    cfg.quant_lab.enabled = True
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
    assert advisory[0]["api_fallback_attempted"] == "False"


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

    assert result["advisory_rows"] == 1
    advisory = _read_csv(tmp_path / "reports" / "summaries" / "strategy_opportunity_advisory_reader.csv")
    assert advisory[0]["advisory_source"] == "api"
    assert advisory[0]["api_fallback_attempted"] == "True"
    assert advisory[0]["api_fallback_success"] == "True"
    assert advisory[0]["max_paper_notional_usdt"] == "99.0"
    cached = _read_csv(tmp_path / "reports" / "strategy_opportunity_advisory.csv")
    assert cached[0]["max_paper_notional_usdt"] == "99.0"


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
    assert f4["advisory_response_action"] == "paper_tracking"
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
