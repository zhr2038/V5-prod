from configs.schema import AppConfig
from src.research.trend_quality_experiment import (
    DEFAULT_RESEARCH_SYMBOLS,
    build_experiment_configs,
    seed_sandbox_read_only_artifacts,
)


def test_build_experiment_configs_isolated_and_safe(tmp_path):
    base_cfg = AppConfig()
    base_cfg.alpha.use_multi_strategy = True
    base_cfg.alpha.ml_factor.enabled = True
    base_cfg.regime.use_ensemble = True

    configs = build_experiment_configs(
        base_cfg,
        project_root=tmp_path,
        research_symbols=DEFAULT_RESEARCH_SYMBOLS,
    )

    baseline = configs["baseline"]
    candidate = configs["trend_quality"]
    candidate_v2 = configs["trend_quality_v2"]

    assert base_cfg.execution.dry_run is True
    assert base_cfg.alpha.ml_factor.enabled is True
    assert base_cfg.regime.use_ensemble is True

    assert baseline.symbols == list(DEFAULT_RESEARCH_SYMBOLS)
    assert baseline.universe.enabled is False
    assert baseline.universe.use_universe_symbols is False
    assert baseline.execution.mode == "dry_run"
    assert baseline.execution.collect_ml_training_data is False
    assert baseline.regime.use_ensemble is False
    assert baseline.alpha.use_multi_strategy is False
    assert baseline.alpha.ml_factor.enabled is False
    assert baseline.backtest.cost_model == "calibrated"
    assert baseline.backtest.cost_stats_dir == str(tmp_path / "reports" / "cost_stats_clean")
    assert baseline.alpha.optimizer_state_path == "reports/portfolio_optimizer_state.json"

    assert candidate.alpha.use_multi_strategy is False
    assert candidate.alpha.alpha158_overlay.enabled is False
    assert candidate.alpha.dynamic_ic_weighting.enabled is False
    assert candidate.alpha.dynamic_weights_by_regime_enabled is False
    assert candidate.alpha.topk_dropout.topk_override == 2
    assert candidate.risk.max_positions_override == 2
    assert candidate.execution.rank_exit_confirm_rounds == 3
    assert candidate.execution.max_rebalance_turnover_per_cycle == 0.45

    assert candidate_v2.alpha.use_multi_strategy is False
    assert candidate_v2.alpha.alpha158_overlay.enabled is True
    assert candidate_v2.alpha.alpha158_overlay.blend_weight == 0.20
    assert candidate_v2.alpha.dynamic_ic_weighting.enabled is False
    assert candidate_v2.alpha.dynamic_weights_by_regime_enabled is False
    assert candidate_v2.alpha.topk_dropout.topk_override == 3
    assert candidate_v2.risk.max_positions_override == 3
    assert candidate_v2.risk.max_single_weight == 0.30
    assert candidate_v2.execution.rank_exit_confirm_rounds == 3
    assert candidate_v2.execution.max_rebalance_turnover_per_cycle == 1.0


def test_seed_sandbox_read_only_artifacts_copies_instrument_cache(tmp_path):
    project_root = tmp_path / "project"
    sandbox_dir = tmp_path / "sandbox"
    src = project_root / "reports" / "okx_spot_instruments.json"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text('{"ts": 1, "data": []}', encoding="utf-8")

    seed_sandbox_read_only_artifacts(project_root, sandbox_dir)

    dst = sandbox_dir / "reports" / "okx_spot_instruments.json"
    assert dst.exists()
    assert dst.read_text(encoding="utf-8") == '{"ts": 1, "data": []}'
