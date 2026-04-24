from __future__ import annotations

from src.reporting.robust_evaluation import check_time_alignment


def test_check_time_alignment_sorts_price_times_before_searchsorted() -> None:
    result = check_time_alignment(
        snapshot_times=[3600],
        price_times=[7200, 0, 3600],
        forward_horizon_hours=1,
    )

    assert result["passed"] is True
    assert result["errors"] == []
    assert result["warnings"] == []
