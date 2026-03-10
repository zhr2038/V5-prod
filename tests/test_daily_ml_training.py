from pathlib import Path

import pandas as pd
import pytest

from scripts import daily_ml_training as daily_training


SCRIPT_PATH = Path("scripts/daily_ml_training.py")


def test_daily_ml_training_defaults_to_rank_target():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_TARGET_MODE", "forward_edge_rank"' in text


def test_daily_ml_training_defaults_to_ridge_only():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_CANDIDATES", "ridge"' in text
    assert 'return out or ["ridge"]' in text


def test_daily_ml_training_uses_stronger_ridge_regularization():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_RIDGE_ALPHA", "50.0"' in text


def test_daily_ml_training_uses_wider_symbol_coverage_and_stable_feature_selection():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_MIN_SYMBOL_SAMPLES", "48"' in text
    assert 'V5_ML_FEATURE_SELECTOR", "stable"' in text


def test_daily_ml_training_uses_rolling_window_and_recency_weighting_defaults():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_ROLLING_WINDOW_DAYS' in text
    assert 'V5_ML_MIN_GROUP_SIZE", "2"' in text
    assert 'V5_ML_MIN_GROUP_COVERAGE_RATIO", "0.9"' in text
    assert 'V5_ML_RECENCY_HALFLIFE_DAYS", "5"' in text
    assert 'V5_ML_RECENCY_MAX_WEIGHT", "3.0"' in text


def test_recency_weights_favor_latest_groups():
    groups = pd.Series([
        1_700_000_000_000,
        1_700_000_000_000,
        1_700_043_200_000,
        1_700_086_400_000,
    ])

    weights = daily_training._build_recency_sample_weights(
        groups,
        half_life_days=1.0,
        max_weight=3.0,
    )

    assert len(weights) == len(groups)
    assert weights.iloc[-1] > weights.iloc[0]
    assert float(weights.mean()) == pytest.approx(1.0)


def test_rolling_window_keeps_only_recent_groups():
    X = pd.DataFrame({"f": range(6)})
    y = pd.Series(range(6))
    groups = pd.Series([
        1_700_000_000_000,
        1_700_000_000_000,
        1_700_086_400_000,
        1_700_086_400_000,
        1_700_172_800_000,
        1_700_172_800_000,
    ])

    X_out, y_out, groups_out, meta = daily_training._apply_rolling_window(
        X,
        y,
        groups,
        lookback_days=1.5,
    )

    assert meta["enabled"] is True
    assert len(X_out) == len(y_out) == len(groups_out) == 4
    assert int(groups_out.nunique()) == 2
