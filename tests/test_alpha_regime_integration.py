import json

import pandas as pd

from configs.schema import AlphaConfig, MLFactorLiveConfig
from src.alpha.alpha_engine import AlphaEngine
from src.core.models import MarketSeries


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


def _build_market_series(symbol: str, base_price: float, slope: float) -> MarketSeries:
    bars = 24 * 25
    ts = [1_700_000_000_000 + i * 3_600_000 for i in range(bars)]
    close = [base_price + slope * i + ((i % 9) - 4) * 0.15 for i in range(bars)]
    open_ = [close[0], *close[:-1]]
    high = [px * 1.01 for px in close]
    low = [px * 0.99 for px in close]
    volume = [1_000.0 + (i % 24) * 12.0 + slope * 100.0 for i in range(bars)]
    return MarketSeries(
        symbol=symbol,
        timeframe="1h",
        ts=ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def test_alpha_engine_ml_overlay_runs_in_live_multi_strategy_path(tmp_path, monkeypatch):
    model_base = tmp_path / "ml_factor_model"
    (tmp_path / "ml_factor_model.pkl").write_bytes(b"test")
    (tmp_path / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")

    pointer_path = tmp_path / "ml_factor_model_active.txt"
    pointer_path.write_text(str(model_base), encoding="utf-8")

    decision_path = tmp_path / "model_promotion_decision.json"
    decision_path.write_text(
        json.dumps({"passed": True, "ts": "2026-03-10T00:40:00Z"}),
        encoding="utf-8",
    )

    runtime_path = tmp_path / "ml_runtime_status.json"

    feature_cols = [
        "returns_24h",
        "momentum_5d",
        "momentum_20d",
        "volatility_24h",
        "volatility_ratio",
        "volume_ratio",
        "obv",
        "rsi",
        "macd",
        "macd_signal",
        "bb_position",
        "price_position",
    ]

    def _fake_load_model(self, path: str) -> None:
        self.feature_names = list(feature_cols)
        self.is_trained = True

    def _fake_predict_batch(self, features_df: pd.DataFrame) -> pd.Series:
        values = pd.Series(range(1, len(features_df) + 1), index=features_df.index, dtype=float)
        return values

    monkeypatch.setattr("src.execution.ml_factor_model.MLFactorModel.load_model", _fake_load_model)
    monkeypatch.setattr("src.execution.ml_factor_model.MLFactorModel.predict_batch", _fake_predict_batch)

    engine = AlphaEngine(
        AlphaConfig(
            ml_factor=MLFactorLiveConfig(
                enabled=True,
                ml_weight=0.20,
                overlay_transform="tanh",
                overlay_transform_scale=1.6,
                overlay_transform_max_abs=1.1,
                model_path=str(model_base),
                active_model_pointer_path=str(pointer_path),
                promotion_decision_path=str(decision_path),
                runtime_status_path=str(runtime_path),
                require_promotion_passed=True,
                min_symbols=3,
            )
        )
    )
    engine.use_multi_strategy = True
    engine.multi_strategy_adapter = object()
    engine._compute_multi_strategy_scores = lambda _: {"AAA/USDT": 0.40}

    market_data = {
        "AAA/USDT": _build_market_series("AAA/USDT", 10.0, 0.05),
        "BBB/USDT": _build_market_series("BBB/USDT", 20.0, 0.03),
        "CCC/USDT": _build_market_series("CCC/USDT", 30.0, 0.01),
    }

    snapshot = engine.compute_snapshot(market_data)

    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    assert runtime["used_in_latest_snapshot"] is True
    assert runtime["prediction_count"] == 3
    assert runtime["reason"] == "ok"
    assert runtime["overlay_transform"] == "tanh"
    assert runtime["overlay_transform_max_abs"] == 1.1
    assert set(runtime["symbols_used"]) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.base_scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.ml_overlay_scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert max(abs(v) for v in snapshot.ml_overlay_scores.values()) <= 1.1 + 1e-9
    assert any(snapshot.raw_factors[sym]["ml_pred_raw"] != 0.0 for sym in snapshot.raw_factors)
    assert any(snapshot.z_factors[sym]["ml_pred_zscore"] != 0.0 for sym in snapshot.z_factors)
    assert any("ml_overlay_score" in snapshot.raw_factors[sym] for sym in snapshot.raw_factors)
