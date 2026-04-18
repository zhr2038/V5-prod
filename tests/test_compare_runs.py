from __future__ import annotations

from scripts.compare_runs import compare


def test_compare_includes_negative_expectancy_counts() -> None:
    markdown = compare(
        {"run_id": "v4", "num_trades": 1},
        {"run_id": "v5", "num_trades": 2},
        window="[1, 2)",
        v5_audit={
            "counts": {
                "negative_expectancy_score_penalty": 3,
                "negative_expectancy_cooldown": 4,
                "negative_expectancy_open_block": 5,
                "negative_expectancy_fast_fail_open_block": 6,
            }
        },
    )

    assert "- v5 negative_expectancy_score_penalty: 3" in markdown
    assert "- v5 negative_expectancy_cooldown: 4" in markdown
    assert "- v5 negative_expectancy_open_block: 5" in markdown
    assert "- v5 negative_expectancy_fast_fail_open_block: 6" in markdown


def test_compare_uses_new_turnover_budget_units() -> None:
    markdown = compare(
        {
            "run_id": "v4",
            "num_trades": 1,
        },
        {
            "run_id": "v5",
            "num_trades": 2,
            "budget": {
                "exceeded": False,
                "reason": None,
                "turnover_used_usdt": 16.0,
                "turnover_budget_usdt": 64.2,
                "turnover_used_ratio": 0.1495,
                "turnover_budget_ratio": 0.60,
                "cost_used_bps": 12.0,
                "cost_budget_bps_per_day": 40.0,
            },
        },
        window="[1, 2)",
        v5_audit=None,
    )

    assert "- v5 budget_turnover_usdt: 16/64.2" in markdown
    assert "- v5 budget_turnover_ratio: 0.1495/0.6" in markdown
    assert "- v5 budget_cost_bps: 12/40" in markdown
