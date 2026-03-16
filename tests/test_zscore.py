from src.reporting.alpha_evaluation import robust_zscore_cross_section
from src.utils.math import zscore_cross_section


def test_zscore_zero_std():
    zs = zscore_cross_section({"A": 1.0, "B": 1.0})
    assert zs["A"] == 0.0
    assert zs["B"] == 0.0


def test_zscore_mean_zero():
    zs = zscore_cross_section({"A": 0.0, "B": 2.0})
    assert abs((zs["A"] + zs["B"])) < 1e-9


def test_robust_zscore_zero_mad_zero_std_returns_zero():
    zs = robust_zscore_cross_section({"A": 1.0, "B": 1.0, "C": 1.0})
    assert zs == {"A": 0.0, "B": 0.0, "C": 0.0}


def test_robust_zscore_zero_mad_falls_back_to_standard_zscore():
    zs = robust_zscore_cross_section({"A": 1.0, "B": 1.0, "C": 1.0, "D": 2.0})

    assert any(abs(v) > 0.0 for v in zs.values())
    assert zs["D"] > 0.0
    assert zs["A"] < 0.0
    assert abs(sum(zs.values())) < 1e-9
