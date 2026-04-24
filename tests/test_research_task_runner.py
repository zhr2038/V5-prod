from __future__ import annotations

import pandas as pd

from src.research.task_runner import _split_holdout_by_groups


def test_split_holdout_by_groups_sorts_groups_before_selecting_validation() -> None:
    X = pd.DataFrame({"x": range(8)})
    y = pd.Series([float(i) for i in range(8)])
    groups = pd.Series([4, 4, 1, 1, 2, 2, 3, 3])

    X_train, X_valid, y_train, y_valid, train_groups, valid_groups = _split_holdout_by_groups(
        X,
        y,
        groups,
        holdout_fraction=0.25,
        gap_groups=0,
    )

    assert valid_groups.tolist() == [4, 4]
    assert X_valid["x"].tolist() == [0, 1]
    assert y_valid.tolist() == [0.0, 1.0]
    assert train_groups.tolist() == [1, 1, 2, 2, 3, 3]
    assert X_train["x"].tolist() == [2, 3, 4, 5, 6, 7]
