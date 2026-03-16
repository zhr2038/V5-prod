from __future__ import annotations

from src.research.shadow_ab_monitor import build_shadow_cycle_summary


def test_build_shadow_cycle_summary_recommends_champion_when_hotpath_and_shadow_are_strong():
    hotpath_report = {
        "generated_at": "2026-03-14T00:00:00Z",
        "workers": 21,
        "parallel_granularity": "job",
        "evaluations": [
            {"name": "full_cached_latest"},
            {"name": "recent_1440"},
            {"name": "recent_720"},
        ],
        "results": [
            {
                "name": "core6_cost018",
                "aggregate": {
                    "mean_total_return": 0.0002,
                    "mean_sharpe": -0.5,
                    "max_max_dd": 0.012,
                    "mean_turnover": 0.0030,
                    "positive_windows": 1,
                    "negative_windows": 2,
                    "flat_windows": 0,
                },
                "windows": [
                    {"name": "full_cached_latest", "summary": {"metrics": {"total_return": -0.001, "sharpe": -1.0, "max_dd": 0.010, "turnover": 0.003}}},
                    {"name": "recent_1440", "summary": {"metrics": {"total_return": 0.0, "sharpe": 0.0, "max_dd": 0.0, "turnover": 0.0}}},
                    {"name": "recent_720", "summary": {"metrics": {"total_return": 0.002, "sharpe": 1.0, "max_dd": 0.004, "turnover": 0.002}}},
                ],
            },
            {
                "name": "avax_015",
                "aggregate": {
                    "mean_total_return": 0.0010,
                    "mean_sharpe": 0.8,
                    "max_max_dd": 0.010,
                    "mean_turnover": 0.0028,
                    "positive_windows": 2,
                    "negative_windows": 0,
                    "flat_windows": 1,
                },
                "windows": [
                    {"name": "full_cached_latest", "summary": {"metrics": {"total_return": 0.004, "sharpe": 1.8, "max_dd": 0.008, "turnover": 0.002}}},
                    {"name": "recent_1440", "summary": {"metrics": {"total_return": 0.003, "sharpe": 1.6, "max_dd": 0.003, "turnover": 0.002}}},
                    {"name": "recent_720", "summary": {"metrics": {"total_return": 0.0, "sharpe": 0.0, "max_dd": 0.0, "turnover": 0.0}}},
                ],
            },
        ],
    }
    shadow_report = {
        "generated_at": "2026-03-14T00:05:00Z",
        "symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "AVAX/USDT"],
        "overrides": {
            "execution.cost_aware_min_score_floor": 0.15,
            "execution.cost_aware_score_per_bps": 0.00325,
        },
        "workers": 4,
        "windows": [
            {"name": "full_cached_latest", "summary": {"metrics": {"total_return": 0.004, "sharpe": 1.8, "max_dd": 0.008, "turnover": 0.002, "profit_factor": 2.0}}},
            {"name": "recent_1440", "summary": {"metrics": {"total_return": 0.003, "sharpe": 1.6, "max_dd": 0.003, "turnover": 0.002, "profit_factor": 3.0}}},
            {"name": "recent_720", "summary": {"metrics": {"total_return": 0.0, "sharpe": 0.0, "max_dd": 0.0, "turnover": 0.0, "profit_factor": 0.0}}},
        ],
    }

    summary = build_shadow_cycle_summary(
        hotpath_report=hotpath_report,
        shadow_report=shadow_report,
        champion_name="avax_015",
        baseline_name="core6_cost018",
    )

    assert summary["decision"]["recommend_shadow"] is True
    assert summary["decision"]["reason"] == "champion_beats_baseline_and_shadow_is_positive"
    assert summary["hotpath"]["champion_window_wins"] == 2
    assert summary["hotpath"]["baseline_window_wins"] == 1
    assert summary["shadow"]["positive_windows"] == 2
