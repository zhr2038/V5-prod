from __future__ import annotations

import json
import logging
import os
import sys
from types import SimpleNamespace
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs.loader import load_config
from src.alpha.alpha_engine import AlphaEngine
from src.execution.account_store import AccountState, AccountStore
from src.execution.fill_store import derive_position_store_path, derive_runtime_named_json_path
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

    with caplog.at_level(logging.INFO):
        cfg = load_config(str(cfg_path), env_path=None)

    assert "alpha.weights alias mapped: f3_vol_adj_ret_20d -> f3_vol_adj_ret" in caplog.text

    engine = AlphaEngine(cfg.alpha)
    assert engine.alpha6_strategy is not None
    assert "f3_vol_adj_ret_20d" not in engine.alpha6_strategy.factor_weights
    assert engine.alpha6_strategy.factor_weights["f3_vol_adj_ret"] == pytest.approx(0.30)
    assert engine.alpha6_strategy.config["alpha158_overlay"]["enabled"] is False


def test_load_config_accepts_canonical_f3_key_without_alias_warning(tmp_path, caplog):
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
    f3_vol_adj_ret: 0.30
    f4_volume_expansion: 0.15
    f5_rsi_trend_confirm: 0.25
  alpha158_overlay:
    enabled: false
""".strip(),
    )

    with caplog.at_level(logging.INFO):
        cfg = load_config(str(cfg_path), env_path=None)

    assert "alias mapped" not in caplog.text

    engine = AlphaEngine(cfg.alpha)
    assert engine.alpha6_strategy is not None
    assert engine.alpha6_strategy.factor_weights["f3_vol_adj_ret"] == pytest.approx(0.30)


def test_alpha_engine_ignores_stale_equity_snapshot_and_uses_account_store(tmp_path, monkeypatch):
    order_store = tmp_path / "reports" / "orders.sqlite"
    positions_db = derive_position_store_path(order_store)
    AccountStore(path=str(positions_db)).set(
        AccountState(cash_usdt=106.8, equity_peak_usdt=132.0, scale_basis_usdt=0.0)
    )
    equity_file = derive_runtime_named_json_path(order_store, "equity_validation")
    equity_file.parent.mkdir(parents=True, exist_ok=True)
    equity_file.write_text(
        json.dumps(
            {
                "timestamp": 1,
                "okx_total_eq": 134.94,
                "calculated_total_eq": 134.94,
            }
        ),
        encoding="utf-8",
    )

    engine = AlphaEngine.__new__(AlphaEngine)
    engine.cfg = SimpleNamespace()
    engine.repo_root = tmp_path
    monkeypatch.setattr(engine, "_resolve_runtime_order_store_path", lambda: order_store)

    assert engine._resolve_total_capital_usdt() == pytest.approx(106.8)


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


def test_alpha_engine_latest_model_artifact_mtime_ignores_newer_config_file(tmp_path):
    base_path = tmp_path / "models" / "ml_factor_model"
    base_path.parent.mkdir(parents=True, exist_ok=True)
    model_file = Path(f"{base_path}.pkl")
    config_file = Path(f"{base_path}_config.json")
    model_file.write_bytes(b"model")
    config_file.write_text("{}", encoding="utf-8")
    os.utime(model_file, (1_000_000_000, 1_000_000_000))
    os.utime(config_file, (2_000_000_000, 2_000_000_000))

    latest_mtime_ns = AlphaEngine._latest_model_artifact_mtime_ns(base_path)

    assert latest_mtime_ns == model_file.stat().st_mtime_ns


def test_alpha6_calculate_factors_prefers_latest_row_when_dataframe_is_unsorted():
    strategy = Alpha6FactorStrategy(
        config={
            "alpha158_overlay": {"enabled": False},
            "use_sentiment": False,
        }
    )
    closes = [100.0 + float(i) for i in range(80)]
    ordered = pd.DataFrame(
        {
            "symbol": ["BTC/USDT"] * len(closes),
            "timestamp": pd.date_range("2026-04-01 00:00:00", periods=len(closes), freq="h"),
            "close": closes,
            "high": closes,
            "low": closes,
            "volume": [1000.0] * len(closes),
        }
    )
    unsorted = pd.concat([ordered.iloc[[-1]], ordered.iloc[:-1]], ignore_index=True)

    ordered_factors = strategy._calculate_factors(ordered, "BTC/USDT")
    unsorted_factors = strategy._calculate_factors(unsorted, "BTC/USDT")

    assert unsorted_factors["f1_mom_5d"] == pytest.approx(ordered_factors["f1_mom_5d"])
    assert unsorted_factors["f2_mom_20d"] == pytest.approx(ordered_factors["f2_mom_20d"])
    assert unsorted_factors["f3_vol_adj_ret"] == pytest.approx(ordered_factors["f3_vol_adj_ret"])
