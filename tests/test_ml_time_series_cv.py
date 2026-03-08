from __future__ import annotations

import math

import pandas as pd

from src.execution.ml_time_series_cv import GroupedTimeSeriesSplit, cross_sectional_ic, time_series_cv_score


class _SignalModel:
    def fit(self, X, y):
        return self

    def predict(self, X):
        return X["signal"].to_numpy()


def test_grouped_time_series_split_keeps_timestamp_groups_intact() -> None:
    X = pd.DataFrame({"value": range(12)})
    groups = pd.Series([1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6])

    cv = GroupedTimeSeriesSplit(n_splits=3, gap_groups=1)
    splits = list(cv.split(X, groups=groups))

    assert len(splits) >= 1
    for train_idx, test_idx in splits:
        train_groups = set(groups.iloc[train_idx].tolist())
        test_groups = set(groups.iloc[test_idx].tolist())
        assert train_groups.isdisjoint(test_groups)
        assert max(train_groups) < min(test_groups)


def test_cross_sectional_ic_uses_within_group_ranking() -> None:
    groups = pd.Series([1, 1, 2, 2, 3, 3, 4, 4])
    y = pd.Series([0.1, 0.2, -0.1, 0.3, 0.4, 0.6, -0.2, 0.5])
    pred = pd.Series([1, 2, 1, 2, 1, 2, 1, 2])

    score = cross_sectional_ic(groups, y, pred)

    assert math.isclose(score, 1.0, rel_tol=1e-9)


def test_time_series_cv_score_supports_grouped_cross_sectional_ic() -> None:
    X = pd.DataFrame({"signal": [1, 2, 1, 2, 1, 2, 1, 2]})
    y = pd.Series([0.1, 0.2, -0.1, 0.3, 0.4, 0.6, -0.2, 0.5])
    groups = pd.Series([1, 1, 2, 2, 3, 3, 4, 4])

    res = time_series_cv_score(
        _SignalModel(),
        X,
        y,
        cv=GroupedTimeSeriesSplit(n_splits=2, gap_groups=0),
        metric="cs_ic",
        groups=groups,
    )

    assert len(res["scores"]) == 2
    assert all(math.isclose(score, 1.0, rel_tol=1e-9) for score in res["scores"])
