import json

from configs.schema import AlphaConfig
from src.alpha.alpha_engine import AlphaEngine


def test_alpha_engine_uses_regime_specific_classic_weights(tmp_path):
    weights_path = tmp_path / "alpha_dynamic_weights_by_regime.json"
    weights_path.write_text(
        json.dumps(
            {
                "regimes": {
                    "Trending": {
                        "weights": {
                            "f1_mom_5d": 0.7,
                            "f2_mom_20d": 0.2,
                            "f3_vol_adj_ret_20d": 0.1,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    engine = AlphaEngine(
        AlphaConfig(
            dynamic_weights_by_regime_enabled=True,
            dynamic_weights_by_regime_path=str(weights_path),
        )
    )
    engine.set_regime_context("TRENDING")

    resolved = engine._resolve_classic_base_weights(
        {
            "f1_mom_5d": 0.2,
            "f2_mom_20d": 0.3,
            "f3_vol_adj_ret_20d": 0.25,
            "f4_volume_expansion": 0.15,
            "f5_rsi_trend_confirm": 0.1,
        }
    )

    assert resolved["f1_mom_5d"] == 0.7
    assert resolved["f2_mom_20d"] == 0.2
    assert resolved["f3_vol_adj_ret_20d"] == 0.1
    assert resolved["f4_volume_expansion"] == 0.15


def test_alpha_engine_maps_regime_weights_into_multi_strategy_alpha6(tmp_path):
    weights_path = tmp_path / "alpha_dynamic_weights_by_regime.json"
    weights_path.write_text(
        json.dumps(
            {
                "regimes": {
                    "Risk-Off": {
                        "weights": {
                            "f1_mom_5d": 0.05,
                            "f3_vol_adj_ret_20d": 0.6,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    engine = AlphaEngine(
        AlphaConfig(
            use_multi_strategy=True,
            dynamic_weights_by_regime_enabled=True,
            dynamic_weights_by_regime_path=str(weights_path),
        )
    )
    engine.set_regime_context("RISK_OFF")
    engine._apply_multi_strategy_regime_weights()

    assert engine.alpha6_strategy.factor_weights["f1_mom_5d"] == 0.05
    assert engine.alpha6_strategy.factor_weights["f3_vol_adj_ret"] == 0.6
