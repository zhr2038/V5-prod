from __future__ import annotations

from src.reporting.robust_evaluation import check_time_alignment, purged_time_series_split


def test_check_time_alignment_sorts_price_times_before_searchsorted() -> None:
    result = check_time_alignment(
        snapshot_times=[3600],
        price_times=[7200, 0, 3600],
        forward_horizon_hours=1,
    )

    assert result["passed"] is True
    assert result["errors"] == []
    assert result["warnings"] == []


def test_purged_time_series_split_returns_original_indices_when_timestamps_are_unsorted() -> None:
    splits = purged_time_series_split(
        [7200, 0, 3600, 10800],
        n_splits=2,
        purge_gap_hours=0,
        embargo_hours=0,
    )

    assert splits == [([0, 3], [1, 2]), ([1, 2], [0, 3])]
