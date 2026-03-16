"""
Time-series cross-validation helpers for ML training.
"""

from __future__ import annotations

from typing import Generator

import numpy as np
import pandas as pd

try:
    from sklearn.model_selection import BaseCrossValidator
except Exception:
    class BaseCrossValidator:
        def split(self, X, y=None, groups=None):
            raise NotImplementedError

        def get_n_splits(self, X=None, y=None, groups=None):
            raise NotImplementedError


class TimeSeriesSplit(BaseCrossValidator):
    def __init__(self, n_splits: int = 5, test_size: int | None = None, gap: int = 0):
        self.n_splits = int(n_splits)
        self.test_size = test_size
        self.gap = int(gap)

    def split(self, X, y=None, groups=None):
        n_samples = len(X)
        indices = np.arange(n_samples)
        test_size = self.test_size or max(1, n_samples // (self.n_splits + 1))

        for i in range(self.n_splits):
            test_end = n_samples - i * test_size
            test_start = test_end - test_size
            train_end = test_start - self.gap
            if train_end <= 0:
                break
            yield indices[:train_end], indices[test_start:test_end]

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_splits


class PurgedKFold(BaseCrossValidator):
    def __init__(self, n_splits: int = 5, purge_gap: int = 10):
        self.n_splits = int(n_splits)
        self.purge_gap = int(purge_gap)

    def split(self, X, y=None, groups=None):
        n_samples = len(X)
        fold_size = max(1, n_samples // self.n_splits)

        for i in range(self.n_splits):
            test_start = i * fold_size
            test_end = min((i + 1) * fold_size, n_samples)
            train_indices = list(range(0, max(0, test_start - self.purge_gap))) + list(
                range(min(n_samples, test_end + self.purge_gap), n_samples)
            )
            test_indices = list(range(test_start, test_end))
            yield np.array(train_indices), np.array(test_indices)

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_splits


class GroupedTimeSeriesSplit(BaseCrossValidator):
    def __init__(self, n_splits: int = 5, test_group_size: int | None = None, gap_groups: int = 0):
        self.n_splits = int(n_splits)
        self.test_group_size = test_group_size
        self.gap_groups = int(gap_groups)

    def split(self, X, y=None, groups=None):
        if groups is None:
            raise ValueError("GroupedTimeSeriesSplit requires groups")

        groups_s = pd.Series(groups).reset_index(drop=True)
        unique_groups = pd.Index(groups_s.drop_duplicates().tolist())
        n_groups = len(unique_groups)
        test_group_size = self.test_group_size or max(1, n_groups // (self.n_splits + 1))

        for i in range(self.n_splits):
            test_end = n_groups - i * test_group_size
            test_start = test_end - test_group_size
            train_end = test_start - self.gap_groups
            if train_end <= 0:
                break

            train_groups = set(unique_groups[:train_end].tolist())
            test_groups = set(unique_groups[test_start:test_end].tolist())
            train_idx = np.flatnonzero(groups_s.isin(train_groups).to_numpy())
            test_idx = np.flatnonzero(groups_s.isin(test_groups).to_numpy())
            yield train_idx, test_idx

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_splits


def cross_sectional_ic(group_values, y_true, y_pred) -> float:
    frame = pd.DataFrame({"group": group_values, "y": y_true, "pred": y_pred})
    vals = []
    for _, g in frame.groupby("group"):
        if len(g) < 2:
            continue
        y_std = float(pd.Series(g["y"]).std(ddof=0))
        pred_std = float(pd.Series(g["pred"]).std(ddof=0))
        if y_std <= 0.0 or pred_std <= 0.0:
            continue
        score = float(np.corrcoef(g["y"], g["pred"])[0, 1])
        if np.isfinite(score):
            vals.append(score)
    return float(np.mean(vals)) if vals else 0.0


def time_series_cv_score(model, X: pd.DataFrame, y: pd.Series, cv=None, metric: str = "ic", groups=None) -> dict:
    if cv is None:
        cv = TimeSeriesSplit(n_splits=5, gap=24)

    scores = []
    fold_details = []

    print("=" * 60)
    print("Time-Series Cross Validation")
    print("=" * 60)

    splitter = cv.split(X, y, groups=groups) if groups is not None else cv.split(X, y)

    for fold, (train_idx, test_idx) in enumerate(splitter):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        if metric == "ic":
            score = float(np.corrcoef(y_test, y_pred)[0, 1])
            if not np.isfinite(score):
                score = 0.0
        elif metric == "cs_ic":
            if groups is None:
                raise ValueError("cs_ic requires groups")
            groups_test = pd.Series(groups).iloc[test_idx].reset_index(drop=True)
            score = cross_sectional_ic(groups_test, y_test.reset_index(drop=True), pd.Series(y_pred))
        elif metric == "rmse":
            score = -float(np.sqrt(np.mean((y_test - y_pred) ** 2)))
        elif metric == "mae":
            score = -float(np.mean(np.abs(y_test - y_pred)))
        else:
            raise ValueError(f"Unknown metric: {metric}")

        scores.append(score)
        fold_details.append(
            {
                "fold": fold + 1,
                "train_size": len(train_idx),
                "test_size": len(test_idx),
                "score": score,
            }
        )
        print(f"  Fold {fold + 1}: Train={len(train_idx)}, Test={len(test_idx)}, IC={score:.4f}")

    mean_score = float(np.mean(scores))
    std_score = float(np.std(scores))

    print(f"\n{'=' * 60}")
    print(f"CV Results ({metric.upper()}):")
    print(f"  Mean: {mean_score:.4f}")
    print(f"  Std:  {std_score:.4f}")
    print(f"  Min:  {float(np.min(scores)):.4f}")
    print(f"  Max:  {float(np.max(scores)):.4f}")
    print(f"{'=' * 60}")

    return {
        "mean_score": mean_score,
        "std_score": std_score,
        "scores": scores,
        "fold_details": fold_details,
    }


def create_walk_forward_splits(
    df: pd.DataFrame,
    train_days: int = 30,
    test_days: int = 7,
    step_days: int = 7,
) -> Generator:
    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")
        else:
            raise ValueError("DataFrame needs DatetimeIndex or 'timestamp' column")

    start_date = df.index.min()
    end_date = df.index.max()
    current_train_start = start_date

    while True:
        train_start = current_train_start
        train_end = train_start + pd.Timedelta(days=train_days)
        test_start = train_end
        test_end = test_start + pd.Timedelta(days=test_days)
        if test_end > end_date:
            break

        train_df = df[(df.index >= train_start) & (df.index < train_end)]
        test_df = df[(df.index >= test_start) & (df.index < test_end)]
        if len(train_df) > 0 and len(test_df) > 0:
            yield train_df, test_df, train_start, train_end, test_start, test_end

        current_train_start += pd.Timedelta(days=step_days)
