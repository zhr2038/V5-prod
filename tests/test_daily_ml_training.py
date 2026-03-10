from pathlib import Path


SCRIPT_PATH = Path("scripts/daily_ml_training.py")


def test_daily_ml_training_defaults_to_rank_target():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_TARGET_MODE", "cross_sectional_rank"' in text


def test_daily_ml_training_defaults_to_ridge_only():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_CANDIDATES", "ridge"' in text
    assert 'return out or ["ridge"]' in text


def test_daily_ml_training_uses_stronger_ridge_regularization():
    text = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'V5_ML_RIDGE_ALPHA", "50.0"' in text
