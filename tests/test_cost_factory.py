from __future__ import annotations

from configs.schema import AppConfig
from src.backtest.cost_factory import make_cost_model_from_cfg


def test_make_cost_model_from_cfg_disabled_defaults(tmp_path):
    cfg = AppConfig()
    cfg.backtest.cost_model = "default"
    model, meta = make_cost_model_from_cfg(cfg)
    d = meta.to_dict()
    assert d["mode"] == "default"
    assert d["reason"] == "cost_model_disabled"


def test_make_cost_model_from_cfg_no_stats_defaults(tmp_path):
    cfg = AppConfig()
    cfg.backtest.cost_model = "calibrated"
    cfg.backtest.cost_stats_dir = str(tmp_path / "nope")
    model, meta = make_cost_model_from_cfg(cfg)
    d = meta.to_dict()
    assert d["mode"] == "default"
    assert d["reason"] == "no_stats_found_or_too_old"
