from __future__ import annotations

import builtins
import json
from pathlib import Path

from configs.schema import AppConfig
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline


def test_ml_impact_monitor_uses_latest_close_when_market_series_is_unsorted(tmp_path: Path) -> None:
    pipe = V5Pipeline(AppConfig(symbols=["BTC/USDT"]))

    state_path = tmp_path / "ml_overlay_impact_state.json"
    history_path = tmp_path / "ml_overlay_impact_history.jsonl"
    summary_path = tmp_path / "ml_overlay_impact.json"

    def _resolve(attr_name: str, default_name: str) -> Path:
        mapping = {
            "impact_state_path": state_path,
            "impact_history_path": history_path,
            "impact_summary_path": summary_path,
        }
        return mapping[attr_name]

    pipe._resolve_ml_impact_path = _resolve  # type: ignore[method-assign]

    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"BTC/USDT": 1.0},
        base_scores={"BTC/USDT": 0.8},
        ml_attribution_scores={"BTC/USDT": 1.0},
        ml_overlay_scores={"BTC/USDT": 0.2},
        ml_overlay_raw_scores={"BTC/USDT": 1.3},
    )
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[1_710_216_000_000, 1_710_212_400_000],
            open=[119.0, 80.0],
            high=[130.0, 81.0],
            low=[118.0, 79.0],
            close=[120.0, 80.0],
            volume=[1.0, 1.0],
        )
    }

    pipe._update_ml_impact_monitor(alpha, market_data, snapshot_ts_ms=1_710_216_000_000)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["closes"]["BTC/USDT"] == 120.0


def test_ml_disabled_skips_collector_and_writes_disabled_audit_status(tmp_path: Path) -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str(tmp_path / "orders.sqlite")
    cfg.alpha.ml_factor.enabled = False
    cfg.execution.collect_ml_training_data = False

    pipe = V5Pipeline(cfg)

    assert pipe.data_collector is None

    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"BTC/USDT": 0.5},
        base_scores={"BTC/USDT": 0.5},
        ml_runtime={
            "configured_enabled": False,
            "overlay_mode": "disabled",
            "reason": "disabled_in_live_prod",
            "prediction_count": 0,
        },
    )

    overview = pipe._build_ml_audit_overview(alpha)

    assert overview["configured_enabled"] is False
    assert overview["live_active"] is False
    assert overview["prediction_count"] == 0
    assert overview["active_symbols"] == 0
    assert overview["overlay_mode"] == "disabled"
    assert overview["reason"] == "disabled_in_live_prod"


def test_pipeline_starts_without_xgboost_when_ml_is_disabled(monkeypatch, tmp_path: Path) -> None:
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "xgboost" or name.startswith("xgboost."):
            raise AssertionError("production pipeline must not import xgboost when ML is disabled")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.order_store_path = str(tmp_path / "orders.sqlite")
    cfg.alpha.ml_factor.enabled = False
    cfg.execution.collect_ml_training_data = False

    pipe = V5Pipeline(cfg)

    assert pipe.data_collector is None
