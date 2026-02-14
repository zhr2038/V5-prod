from src.backtest.walk_forward import build_folds


def test_build_folds_basic():
    folds = build_folds(100, folds=4)
    assert len(folds) == 4
    assert folds[0][1] == (0, 25)
    assert folds[-1][1] == (75, 100)
