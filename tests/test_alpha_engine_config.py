import json

import pytest

from configs.schema import AlphaConfig
from src.alpha.alpha_engine import AlphaEngine, MULTI_STRATEGY_AVAILABLE


def test_alpha_engine_multi_strategy_respects_zero_valued_overrides():
    if not MULTI_STRATEGY_AVAILABLE:
        pytest.skip("multi-strategy dependencies are unavailable")

    cfg = AlphaConfig(use_multi_strategy=True)
    cfg.mean_reversion.allocation = 0.0
    cfg.mean_reversion.position_size_pct = 0.0
    cfg.mean_reversion.mean_rev_threshold = 0.0
    cfg.mean_reversion.buy_score_multiplier = 0.0
    cfg.mean_reversion.sell_score_multiplier = 0.0
    cfg.mean_reversion.allocation_multiplier_sideways = 0.0
    cfg.multi_strategy_conflict_min_confidence = 0.0
    cfg.multi_strategy_conflict_penalty_strength = 0.0
    cfg.alpha158_overlay.blend_weight = 0.0
    cfg.dynamic_ic_weighting.enabled = True
    cfg.dynamic_ic_weighting.min_abs_ic = 0.0

    engine = AlphaEngine(cfg)

    assert engine.mean_reversion_strategy is not None
    assert engine.alpha6_strategy is not None
    assert engine.mean_reversion_strategy.config["position_size_pct"] == 0.0
    assert engine.mean_reversion_strategy.config["mean_rev_threshold"] == 0.0
    assert engine.mean_reversion_strategy.config["buy_score_multiplier"] == 0.0
    assert engine.mean_reversion_strategy.config["sell_score_multiplier"] == 0.0
    assert engine.alpha6_strategy.config["alpha158_blend_weight"] == 0.0
    assert engine.alpha6_strategy.config["dynamic_ic_weighting"]["min_abs_ic"] == 0.0
    assert engine.multi_strategy_adapter.orchestrator.conflict_min_confidence == 0.0
    assert engine.multi_strategy_adapter.orchestrator.conflict_penalty_strength == 0.0
    assert float(engine.multi_strategy_adapter.orchestrator.strategy_allocations[engine.mean_reversion_strategy.name]) == 0.0


def test_alpha_engine_resolves_equity_validation_from_repo_root(monkeypatch, tmp_path):
    workspace = tmp_path / "workspace"
    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "equity_validation.json").write_text(
        json.dumps({"okx_total_eq": 123.45}),
        encoding="utf-8",
    )
    other_cwd = tmp_path / "elsewhere"
    other_cwd.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(other_cwd)

    engine = AlphaEngine(AlphaConfig())
    engine.repo_root = workspace

    assert engine._resolve_total_capital_usdt() == pytest.approx(123.45)
