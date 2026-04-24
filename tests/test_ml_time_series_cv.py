from __future__ import annotations

import pandas as pd

from src.execution.ml_time_series_cv import GroupedTimeSeriesSplit


def test_grouped_time_series_split_sorts_groups_before_splitting() -> None:
    X = pd.DataFrame({"x": range(8)})
    groups = pd.Series([3, 3, 1, 1, 2, 2, 4, 4])
    cv = GroupedTimeSeriesSplit(n_splits=2, test_group_size=1, gap_groups=0)

    splits = list(cv.split(X, groups=groups))

    assert [groups.iloc[idx].tolist() for idx in splits[0]] == [[3, 3, 1, 1, 2, 2], [4, 4]]
    assert [groups.iloc[idx].tolist() for idx in splits[1]] == [[1, 1, 2, 2], [3, 3]]
