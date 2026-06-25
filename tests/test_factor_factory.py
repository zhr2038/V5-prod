from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from src.factor_factory.defaults import build_alpha6_factor_specs
from src.factor_factory.executor import FactorExecutor
from src.factor_factory.expression import FactorExpressionError, FactorExpressionExecutor, collect_features
from src.factor_factory.models import FactorSpec, FactorStatus
from src.factor_factory.registry import FactorRegistry
from src.factor_factory.store import history_key, latest_key, registry_key


def test_expression_executor_uses_whitelisted_causal_operators() -> None:
    frame = pd.DataFrame(
        {
            "RET_16": [0.01, 0.02, 0.04, 0.03, 0.08],
            "VOLUME_RATIO_20": [0.2, 0.8, 1.2, 2.0, 4.0],
        }
    )
    expression = {
        "op": "MUL",
        "args": [
            {"op": "ROLL_ZSCORE", "args": [{"feature": "RET_16"}, {"const": 3}]},
            {"op": "CLIP", "args": [{"feature": "VOLUME_RATIO_20"}, {"const": 0.0}, {"const": 3.0}]},
        ],
    }

    out = FactorExpressionExecutor().evaluate(expression, frame)

    assert collect_features(expression) == {"RET_16", "VOLUME_RATIO_20"}
    assert out.iloc[-1] == pytest.approx(3.0 * ((0.08 - (0.04 + 0.03 + 0.08) / 3) / pd.Series([0.04, 0.03, 0.08]).std()))


def test_expression_executor_rejects_unknown_operator_and_negative_lag() -> None:
    with pytest.raises(FactorExpressionError):
        collect_features({"op": "EVAL", "args": [{"feature": "RET_1"}]})

    frame = pd.DataFrame({"RET_1": [0.1, 0.2]})
    with pytest.raises(ValueError):
        FactorExpressionExecutor().evaluate(
            {"op": "DELAY", "args": [{"feature": "RET_1"}, {"const": -1}]},
            frame,
        )


def test_registry_tracks_versions_statuses_and_required_bars() -> None:
    shadow = FactorSpec(
        factor_id="ret_zscore",
        name="ret_zscore",
        version="v1",
        expression={"op": "ROLL_ZSCORE", "args": [{"feature": "RET_16"}, {"const": 96}]},
        inputs=["RET_16"],
        timeframe="15m",
        lookback_bars=96,
        warmup_bars=4,
        availability_lag_bars=1,
        status=FactorStatus.SHADOW,
    )
    retired = shadow.model_copy(update={"version": "v0", "status": FactorStatus.RETIRED})
    registry = FactorRegistry([retired, shadow])

    assert registry.get("ret_zscore").version == "v1"
    assert registry.get_online_factors("15m") == [shadow]
    assert registry.required_bars(timeframe="15m", statuses=[FactorStatus.SHADOW]) == 101
    with pytest.raises(ValueError):
        registry.register(shadow)


def test_executor_separates_event_and_available_time() -> None:
    spec = FactorSpec(
        factor_id="alpha",
        name="alpha",
        version="v1",
        expression={"op": "SAFE_DIV", "args": [{"feature": "RET_16"}, {"feature": "VOL"}]},
        inputs=["RET_16", "VOL"],
        timeframe="15m",
        lookback_bars=2,
        availability_lag_bars=1,
        status=FactorStatus.SHADOW,
    )
    frame = pd.DataFrame(
        {
            "timestamp": [1_800_000_000_000, 1_800_000_900_000],
            "RET_16": [0.1, 0.2],
            "VOL": [2.0, 4.0],
        }
    )
    [snapshot] = FactorExecutor().execute(
        [spec],
        frame,
        symbol="BTC/USDT",
        data_version="unit",
    )

    assert snapshot.value == pytest.approx(0.05)
    assert snapshot.event_time == datetime.fromtimestamp(1_800_000_900, tz=timezone.utc)
    assert snapshot.available_time.timestamp() == pytest.approx(1_800_000_900 + 900)
    assert latest_key("BTC/USDT", "15m", "alpha") == snapshot.redis_latest_key()
    assert history_key("BTC/USDT", "15m", "alpha", 1_800_000_900) == snapshot.redis_history_key()
    assert registry_key("alpha", "v1") == "factor:registry:alpha:v1"


def test_alpha6_legacy_factors_register_as_shadow_specs() -> None:
    specs = build_alpha6_factor_specs(timeframe="1h")
    registry = FactorRegistry(specs)

    assert registry.get("f1_mom_5d").status is FactorStatus.SHADOW
    assert registry.required_bars(timeframe="1h", statuses=[FactorStatus.SHADOW]) == 481
