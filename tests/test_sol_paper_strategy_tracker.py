from __future__ import annotations

import csv
from pathlib import Path

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.sol_paper_strategy_tracker import update_sol_paper_strategy_tracker


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
            "f4_volume_expansion": "0.2",
            "f5_rsi_trend_confirm": "0.25",
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
            "f4_volume_expansion": "1.2",
            "f5_rsi_trend_confirm": "0.40",
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
            "f4_volume_expansion": "1.4",
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
    cfg.diagnostics.paper_strategy_horizons_hours = [4, 8, 12, 24, 48, 72]
    cfg.diagnostics.paper_strategy_rt_cost_bps = 30.0
    return cfg


def _audit(run_id: str, ts_s: int) -> DecisionAudit:
    audit = DecisionAudit(run_id=run_id, now_ts=ts_s, window_end_ts=ts_s)
    audit.budget = {"current_equity_usdt": 100.0}
    return audit


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
    assert mature_result["new_records"] == 0
    assert mature_result["total_records"] == 2

    runs = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_runs.csv")
    assert {row["strategy_id"] for row in runs} == {
        "SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1",
        "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
    }
    assert {row["would_enter"] for row in runs} == {"True"}
    assert {row["would_size_notional"] for row in runs} == {"12.0"}
    assert {row["paper_pnl_bps_24h"] for row in runs} == {"970.0"}
    assert {row["paper_pnl_usdt_24h"] for row in runs} == {"1.164"}
    assert "cand_sol_live" not in {row["candidate_id"] for row in runs}

    daily = _read_csv(tmp_path / "reports" / "summaries" / "paper_strategy_daily.csv")
    assert len(daily) == 2
    assert {row["paper_days_to_date"] for row in daily} == {"1"}
    assert {row["avg_paper_pnl_bps"] for row in daily} == {"970.0"}

    coverage = _read_csv(tmp_path / "reports" / "summaries" / "paper_slippage_coverage.csv")
    by_strategy = {row["strategy_id"]: row for row in coverage}
    f4 = by_strategy["SOL_F4_VOLUME_EXPANSION_PAPER_V1"]
    assert f4["paper_days"] == "1"
    assert f4["required_paper_days"] == "14"
    assert f4["slippage_coverage"] == "1.0"
    assert f4["readiness_status"] == "PAPER_READY"
    assert f4["live_small_ready"] == "False"
    assert f4["live_block_reason"] == "no_paper_days"

    alpha6_low = by_strategy["SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1"]
    assert alpha6_low["latest_cost_source"] == "public_spread_proxy"
    assert "cost_source_not_actual_or_mixed" in alpha6_low["live_block_reason"]
    assert "no_live_slippage_coverage" in alpha6_low["live_block_reason"]


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
