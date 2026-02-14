from src.utils.math import zscore_cross_section


def test_zscore_zero_std():
    zs = zscore_cross_section({"A": 1.0, "B": 1.0})
    assert zs["A"] == 0.0
    assert zs["B"] == 0.0


def test_zscore_mean_zero():
    zs = zscore_cross_section({"A": 0.0, "B": 2.0})
    assert abs((zs["A"] + zs["B"])) < 1e-9
