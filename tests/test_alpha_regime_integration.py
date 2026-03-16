import json

import pandas as pd
import pytest

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


def test_alpha_engine_reweights_mean_reversion_allocation_by_regime():
    engine = AlphaEngine(
        AlphaConfig(
            use_multi_strategy=True,
            mean_reversion={
                "allocation": 0.25,
                "allocation_multiplier_trending": 0.7,
                "allocation_multiplier_sideways": 1.2,
                "allocation_multiplier_risk_off": 0.9,
            },
        )
    )

    engine.set_regime_context("TRENDING")
    engine._apply_multi_strategy_regime_weights()
    trend_alloc = {
        name: float(value)
        for name, value in engine.multi_strategy_adapter.orchestrator.strategy_allocations.items()
    }

    engine.set_regime_context("SIDEWAYS")
    engine._apply_multi_strategy_regime_weights()
    sideways_alloc = {
        name: float(value)
        for name, value in engine.multi_strategy_adapter.orchestrator.strategy_allocations.items()
    }

    assert sum(trend_alloc.values()) == pytest.approx(1.0)
    assert sum(sideways_alloc.values()) == pytest.approx(1.0)
    assert sideways_alloc["MeanReversion"] > trend_alloc["MeanReversion"]


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
    impact_summary_path = tmp_path / "ml_overlay_impact.json"
    impact_history_path = tmp_path / "ml_overlay_impact_history.jsonl"

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
                    impact_summary_path=str(impact_summary_path),
                    impact_history_path=str(impact_history_path),
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
    assert runtime["overlay_mode"] == "observe"
    assert runtime["effective_ml_weight"] == 0.2
    assert runtime["overlay_transform"] == "tanh"
    assert runtime["overlay_transform_max_abs"] == 1.1
    assert set(runtime["symbols_used"]) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.base_scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.ml_attribution_scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert set(snapshot.ml_overlay_scores) == {"AAA/USDT", "BBB/USDT", "CCC/USDT"}
    assert max(abs(v) for v in snapshot.ml_overlay_scores.values()) <= 1.1 + 1e-9
    assert any(snapshot.raw_factors[sym]["ml_pred_raw"] != 0.0 for sym in snapshot.raw_factors)
    assert any(snapshot.z_factors[sym]["ml_pred_zscore"] != 0.0 for sym in snapshot.z_factors)
    assert any("ml_overlay_score" in snapshot.raw_factors[sym] for sym in snapshot.raw_factors)


def test_alpha_engine_ml_overlay_enters_shadow_but_keeps_attribution_scores(tmp_path, monkeypatch):
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
    impact_summary_path = tmp_path / "ml_overlay_impact.json"
    impact_history_path = tmp_path / "ml_overlay_impact_history.jsonl"
    impact_summary_path.write_text(
        json.dumps(
            {
                "rolling_24h": {"points": 8, "coverage_hours": 24.0, "topn_delta_mean_bps": -12.5, "status": "negative"},
                "rolling_48h": {"points": 16, "coverage_hours": 48.0, "topn_delta_mean_bps": -6.8, "status": "negative"},
            }
        ),
        encoding="utf-8",
    )

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
        return pd.Series([2.0, 1.0, -1.0], index=features_df.index, dtype=float)

    monkeypatch.setattr("src.execution.ml_factor_model.MLFactorModel.load_model", _fake_load_model)
    monkeypatch.setattr("src.execution.ml_factor_model.MLFactorModel.predict_batch", _fake_predict_batch)

    engine = AlphaEngine(
        AlphaConfig(
            ml_factor=MLFactorLiveConfig(
                enabled=True,
                ml_weight=0.20,
                online_control_enabled=True,
                online_control_24h_min_points=6,
                online_control_48h_min_points=12,
                online_control_downweight_ml_weight=0.08,
                model_path=str(model_base),
                active_model_pointer_path=str(pointer_path),
                promotion_decision_path=str(decision_path),
                runtime_status_path=str(runtime_path),
                impact_summary_path=str(impact_summary_path),
                impact_history_path=str(impact_history_path),
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

    assert runtime["overlay_mode"] == "shadow"
    assert runtime["effective_ml_weight"] == 0.0
    assert runtime["used_in_latest_snapshot"] is False
    assert runtime["online_control_reason"] == "rolling_48h_negative"
    assert snapshot.scores == snapshot.base_scores
    assert snapshot.ml_attribution_scores != snapshot.base_scores


def test_alpha_engine_ml_overlay_requires_latest_promotion_pass(tmp_path, monkeypatch):
    promoted_model = tmp_path / "promoted_ml_factor_model"
    (tmp_path / "promoted_ml_factor_model.pkl").write_bytes(b"test")
    (tmp_path / "promoted_ml_factor_model_config.json").write_text("{}", encoding="utf-8")

    candidate_model = tmp_path / "ml_factor_model"
    pointer_path = tmp_path / "ml_factor_model_active.txt"
    pointer_path.write_text(str(promoted_model), encoding="utf-8")

    decision_path = tmp_path / "model_promotion_decision.json"
    decision_path.write_text(
        json.dumps(
            {
                "passed": False,
                "ts": "2026-03-16T00:41:07Z",
                "fail_reasons": ["valid_ic<0.00", "cv_mean_ic<0.01"],
            }
        ),
        encoding="utf-8",
    )

    runtime_path = tmp_path / "ml_runtime_status.json"
    impact_summary_path = tmp_path / "ml_overlay_impact.json"
    impact_history_path = tmp_path / "ml_overlay_impact_history.jsonl"

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
    loaded_paths: list[str] = []

    def _fake_load_model(self, path: str) -> None:
        loaded_paths.append(path)
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
                model_path=str(candidate_model),
                active_model_pointer_path=str(pointer_path),
                promotion_decision_path=str(decision_path),
                runtime_status_path=str(runtime_path),
                impact_summary_path=str(impact_summary_path),
                impact_history_path=str(impact_history_path),
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

    assert loaded_paths == []
    assert runtime["used_in_latest_snapshot"] is False
    assert runtime["prediction_count"] == 0
    assert runtime["reason"] == "promotion_not_passed"
    assert runtime["promotion_passed"] is False
    assert runtime["latest_decision_passed"] is False
    assert runtime["promotion_fallback_active"] is False
    assert runtime["promotion_source"] == "none"
    assert runtime["promotion_fail_reasons"] == ["valid_ic<0.00", "cv_mean_ic<0.01"]
    assert runtime["model_path"] == str(promoted_model)
    assert snapshot.ml_overlay_scores == {}
