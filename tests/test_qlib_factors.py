import warnings

from src.alpha.qlib_factors import compute_alpha158_style_factors


def test_compute_alpha158_style_factors_constant_inputs_avoid_runtime_warnings():
    close = [100.0] * 30
    volume = [1_000.0] * 30

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        factors = compute_alpha158_style_factors(
            close=close,
            high=close,
            low=close,
            volume=volume,
            corr_window=10,
            rank_window=20,
            aroon_window=14,
        )

    assert factors["f6_corr_pv_10"] == 0.0
    assert factors["f7_cord_10"] == 0.0
