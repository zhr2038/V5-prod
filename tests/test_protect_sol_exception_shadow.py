from __future__ import annotations

import csv
from pathlib import Path

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.protect_sol_exception_shadow import update_protect_sol_exception_shadow_evaluator


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


def _cfg() -> AppConfig:
    cfg = AppConfig(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"])
    cfg.diagnostics.protect_sol_exception_enabled_shadow_only = True
    cfg.diagnostics.protect_sol_exception_enable_live_experiment = False
    cfg.diagnostics.protect_sol_exception_horizons_hours = [4, 8, 12, 24, 48, 72]
    cfg.diagnostics.protect_sol_exception_f3_weight_candidates = [0.20, 0.25]
    cfg.diagnostics.protect_sol_exception_f4_weight_candidates = [0.25, 0.30]
    cfg.diagnostics.protect_sol_exception_min_complete_samples_warning = 5
    return cfg


def _audit(run_id: str, ts_s: int) -> DecisionAudit:
    audit = DecisionAudit(run_id=run_id, now_ts=ts_s, window_end_ts=ts_s)
    audit.budget = {"current_equity_usdt": 100.0}
    audit.effective_alpha6_weights = {
        "f1_mom_5d": 0.10,
        "f2_mom_20d": 0.30,
        "f3_vol_adj_ret": 0.35,
        "f4_volume_expansion": 0.15,
        "f5_rsi_trend_confirm": 0.10,
    }
    audit.target_execution_explain = [
        {
            "symbol": "SOL/USDT",
            "router_action": "skip",
            "router_reason": "protect_entry_rsi_confirm_too_weak",
            "high_score_but_not_executed": True,
            "final_score": 0.88,
            "target_w": 0.12,
            "entry_px": 100.0,
            "alpha6_score": 0.28,
            "f4_volume_expansion": 0.12,
            "f5_rsi_trend_confirm": 0.25,
            "current_level": "PROTECT",
        },
        {
            "symbol": "ETH/USDT",
            "router_action": "skip",
            "router_reason": "protect_entry_rsi_confirm_too_weak",
            "high_score_but_not_executed": True,
            "final_score": 0.91,
            "target_w": 0.12,
            "entry_px": 100.0,
            "f4_volume_expansion": 0.5,
        },
        {
            "symbol": "BTC/USDT",
            "router_action": "skip",
            "router_reason": "btc_leadership_probe_alpha6_score_too_low",
            "high_score_but_not_executed": True,
            "final_score": 0.92,
            "target_w": 0.10,
            "entry_px": 100.0,
            "f4_volume_expansion": 0.5,
        },
    ]
    audit.strategy_signals = [
        {
            "strategy": "Alpha6Factor",
            "signals": [
                {
                    "symbol": "SOL/USDT",
                    "side": "buy",
                    "score": 0.28,
                    "metadata": {
                        "z_factors": {
                            "f3_vol_adj_ret": -0.2,
                            "f4_volume_expansion": 0.4,
                            "f5_rsi_trend_confirm": 0.25,
                        }
                    },
                }
            ],
        }
    ]
    return audit


def test_protect_sol_exception_shadow_records_sol_only_and_labels_matured(tmp_path: Path):
    cfg = _cfg()
    start_s = 1_779_000_000
    market = {
        "SOL/USDT": _series(
            "SOL/USDT",
            start_s,
            {0: 100.0, 4: 101.0, 8: 102.0, 12: 103.0, 24: 110.0, 48: 112.0, 72: 115.0},
        )
    }
    run_dir = tmp_path / "reports" / "runs" / "r1"

    result = update_protect_sol_exception_shadow_evaluator(
        run_dir=run_dir,
        audit=_audit("r1", start_s),
        market_data_1h=market,
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=tmp_path / "cache",
    )
    assert result["enabled"] is True
    assert result["new_records"] == 4
    assert result["total_records"] == 4

    mature_audit = DecisionAudit(run_id="r2", now_ts=start_s + 72 * 3600, window_end_ts=start_s + 72 * 3600)
    mature_result = update_protect_sol_exception_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "r2",
        audit=mature_audit,
        market_data_1h=market,
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=tmp_path / "cache",
    )
    assert mature_result["new_records"] == 0
    assert mature_result["total_records"] == 4

    rows = _read_csv(tmp_path / "reports" / "summaries" / "protect_sol_exception_shadow_outcomes.csv")
    assert {row["symbol"] for row in rows} == {"SOL/USDT"}
    assert {row["original_block_reason"] for row in rows} == {"protect_entry_rsi_confirm_too_weak"}
    assert {row["would_enter"] for row in rows} == {"True"}
    assert {row["enable_live_experiment"] for row in rows} == {"False"}
    assert {row["would_size_notional"] for row in rows} == {"12.0"}
    assert {row["label_status"] for row in rows} == {"complete"}
    assert {row["would_pnl_bps_24h"] for row in rows} == {"970.0"}

    by_horizon = _read_csv(
        tmp_path / "reports" / "summaries" / "protect_sol_exception_shadow_outcomes_by_symbol_reason_horizon.csv"
    )
    h24 = next(row for row in by_horizon if row["horizon_hours"] == "24")
    assert h24["symbol"] == "SOL/USDT"
    assert h24["better_than_current_strategy"] == "True"
    assert h24["sample_warning"] == "insufficient_samples_min_5"


def test_protect_sol_exception_shadow_requires_positive_f4(tmp_path: Path):
    cfg = _cfg()
    start_s = 1_779_000_000
    audit = _audit("r1", start_s)
    audit.target_execution_explain[0]["f4_volume_expansion"] = 0.0
    result = update_protect_sol_exception_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "r1",
        audit=audit,
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0, 24: 110.0})},
        cfg=cfg,
        current_level="PROTECT",
        cache_dir=tmp_path / "cache",
    )
    assert result["new_records"] == 0
    assert result["total_records"] == 0


def test_protect_sol_exception_shadow_only_in_protect(tmp_path: Path):
    cfg = _cfg()
    start_s = 1_779_000_000
    audit = _audit("r1", start_s)
    audit.target_execution_explain[0]["current_level"] = "NORMAL"
    result = update_protect_sol_exception_shadow_evaluator(
        run_dir=tmp_path / "reports" / "runs" / "r1",
        audit=audit,
        market_data_1h={"SOL/USDT": _series("SOL/USDT", start_s, {0: 100.0, 24: 110.0})},
        cfg=cfg,
        current_level="NORMAL",
        cache_dir=tmp_path / "cache",
    )
    assert result["new_records"] == 0
    assert result["total_records"] == 0
