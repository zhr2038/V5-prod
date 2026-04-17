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
