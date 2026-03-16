import json

import pytest

from src.alpha.alpha_engine import AlphaSnapshot
from src.alpha.ic_monitor import AlphaICMonitor, AlphaICMonitorConfig
from src.strategy.multi_strategy_system import Alpha6FactorStrategy


def test_alpha_ic_monitor_prefers_telemetry_scores(tmp_path):
    cfg = AlphaICMonitorConfig(
        history_path=str(tmp_path / "alpha_ic_history.jsonl"),
        timeseries_path=str(tmp_path / "alpha_ic_timeseries.jsonl"),
        summary_path=str(tmp_path / "alpha_ic_monitor.json"),
        min_cross_section=2,
        roll_points_short=8,
        roll_points_long=8,
    )
    monitor = AlphaICMonitor(cfg)

    prev = AlphaSnapshot(
        raw_factors={},
        z_factors={
            "FLOW/USDT": {"f1_mom_5d": 1.0},
            "HYPE/USDT": {"f1_mom_5d": -1.0},
        },
        scores={"FLOW/USDT": 0.4},
        telemetry_scores={"FLOW/USDT": 0.7, "HYPE/USDT": -0.5},
    )
    cur = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"FLOW/USDT": 0.2},
        telemetry_scores={"FLOW/USDT": 0.2, "HYPE/USDT": -0.1},
    )

    assert monitor.update(now_ts_ms=1_000, alpha_snapshot=prev, closes={"FLOW/USDT": 100.0, "HYPE/USDT": 100.0}) is None

    summary = monitor.update(
        now_ts_ms=2_000,
        alpha_snapshot=cur,
        closes={"FLOW/USDT": 110.0, "HYPE/USDT": 90.0},
    )

    assert summary is not None
    assert summary["score_source"] == "telemetry_scores"
    assert summary["factor_ic"]["f1_mom_5d"]["rank_ic_short"]["count"] == 1
    assert summary["factor_ic"]["f1_mom_5d"]["rank_ic_short"]["mean"] == pytest.approx(1.0)


def test_alpha6_dynamic_ic_weighting_downweights_negative_factors_without_flipping_sign(tmp_path):
    ic_path = tmp_path / "alpha_ic_monitor.json"
    ic_path.write_text(
        json.dumps(
            {
                "factor_ic": {
                    "f1_mom_5d": {
                        "rank_ic_short": {"mean": -0.04, "count": 16},
                        "rank_ic_long": {"mean": -0.03, "count": 32},
                    },
                    "f2_mom_20d": {
                        "rank_ic_short": {"mean": 0.05, "count": 16},
                        "rank_ic_long": {"mean": 0.04, "count": 32},
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    strategy = Alpha6FactorStrategy(
        config={
            "weights": {
                "f1_mom_5d": 0.5,
                "f2_mom_20d": 0.5,
            },
            "use_sentiment": False,
            "alpha158_enabled": False,
            "dynamic_ic_weighting": {
                "enabled": True,
                "ic_monitor_path": str(ic_path),
                "min_abs_ic": 0.003,
            },
        }
    )

    weights = strategy._resolve_dynamic_weights(strategy.factor_weights)

    assert weights["f1_mom_5d"] > 0.0
    assert weights["f2_mom_20d"] > weights["f1_mom_5d"]
