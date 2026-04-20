from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.loader import load_config
from src.alpha.alpha_engine import AlphaEngine
import src.strategy.multi_strategy_system as multi_strategy_system
from src.strategy.multi_strategy_system import Alpha6FactorStrategy


def _write_yaml(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _market_df(symbol: str) -> pd.DataFrame:
    closes = [100.0 + float(i) for i in range(80)]
    return pd.DataFrame(
        {
            "symbol": [symbol] * len(closes),
            "close": closes,
            "high": closes,
            "low": closes,
            "volume": [1000.0] * len(closes),
        }
    )


def test_load_config_maps_legacy_f3_alias_into_multi_strategy_runtime_weights(tmp_path, caplog):
    cfg_path = _write_yaml(
        tmp_path,
        """
symbols:
  - BTC/USDT
alpha:
  use_multi_strategy: true
  weights:
    f1_mom_5d: 0.10
    f2_mom_20d: 0.20
    f3_vol_adj_ret_20d: 0.30
    f4_volume_expansion: 0.15
    f5_rsi_trend_confirm: 0.25
  alpha158_overlay:
    enabled: false
""".strip(),
    )

    with caplog.at_level(logging.WARNING):
        cfg = load_config(str(cfg_path), env_path=None)

    assert "alpha.weights alias mapped: f3_vol_adj_ret_20d -> f3_vol_adj_ret" in caplog.text

    engine = AlphaEngine(cfg.alpha)
    assert engine.alpha6_strategy is not None
    assert "f3_vol_adj_ret_20d" not in engine.alpha6_strategy.factor_weights
    assert engine.alpha6_strategy.factor_weights["f3_vol_adj_ret"] == pytest.approx(0.30)
    assert engine.alpha6_strategy.config["alpha158_overlay"]["enabled"] is False


def test_alpha158_overlay_disabled_skips_compute_and_blend(monkeypatch):
    def _boom(*args, **kwargs):
        raise AssertionError("alpha158 overlay should not be computed when disabled")

    monkeypatch.setattr(multi_strategy_system, "compute_alpha158_style_factors", _boom)

    strategy = Alpha6FactorStrategy(
        config={
            "weights": {
                "f1_mom_5d": 1.0,
                "f2_mom_20d": 0.0,
                "f3_vol_adj_ret": 0.0,
                "f4_volume_expansion": 0.0,
                "f5_rsi_trend_confirm": 0.0,
                "f6_sentiment": 0.0,
            },
            "alpha158_overlay": {
                "enabled": False,
                "blend_weight": 1.0,
                "weights": {
                    "f6_corr_pv_10": 1.0,
                },
            },
        }
    )

    factors = strategy._calculate_factors(_market_df("BTC/USDT"), "BTC/USDT")
    assert "f6_corr_pv_10" not in factors

    score = strategy._calculate_score(
        {
            "f1_mom_5d": 2.5,
            "f6_corr_pv_10": 99.0,
        },
        strategy.factor_weights,
    )
    assert score == pytest.approx(2.5)


@pytest.mark.parametrize(
    ("yaml_body", "expected_fragment"),
    [
        (
            """
symbols:
  - BTC/USDT
alpha:
  weights:
    f1_mom_5d: 0.20
    unknown_base_factor: 0.80
""".strip(),
            "Unknown alpha.weights keys: ['unknown_base_factor']",
        ),
        (
            """
symbols:
  - BTC/USDT
alpha:
  alpha158_overlay:
    weights:
      unknown_overlay_factor: 1.0
""".strip(),
            "Unknown alpha.alpha158_overlay.weights keys: ['unknown_overlay_factor']",
        ),
    ],
)
def test_unknown_factor_keys_fail_fast_in_config_load(tmp_path, yaml_body, expected_fragment):
    cfg_path = _write_yaml(tmp_path, yaml_body)

    with pytest.raises(ValidationError) as exc_info:
        load_config(str(cfg_path), env_path=None)
    assert expected_fragment in str(exc_info.value)


def test_alpha6_sentiment_factor_prefers_filename_timestamp_over_mtime(tmp_path):
    strategy = Alpha6FactorStrategy()
    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    strategy.sentiment_cache_dir = cache_dir

    older = cache_dir / "funding_BTC-USDT_20260419_22.json"
    newer = cache_dir / "funding_BTC-USDT_20260419_23.json"
    older.write_text('{"f6_sentiment": -0.4}', encoding="utf-8")
    newer.write_text('{"f6_sentiment": 0.35}', encoding="utf-8")
    os.utime(older, (2_000_000_000, 2_000_000_000))
    os.utime(newer, (1_000_000_000, 1_000_000_000))

    assert strategy._load_sentiment_factor("BTC/USDT") == pytest.approx(0.35)
