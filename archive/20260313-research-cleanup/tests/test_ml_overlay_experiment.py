from configs.schema import AppConfig
from src.research.ml_overlay_experiment import (
    DEFAULT_RESEARCH_SYMBOLS,
    build_experiment_configs,
)


def test_build_experiment_configs_isolated_and_model_specific(tmp_path):
    base_cfg = AppConfig()
    base_cfg.alpha.use_multi_strategy = True
    base_cfg.alpha.ml_factor.enabled = True
    base_cfg.regime.use_ensemble = True

    configs = build_experiment_configs(
        base_cfg,
        project_root=tmp_path,
        research_symbols=DEFAULT_RESEARCH_SYMBOLS,
    )

    no_ml = configs["no_ml"]
    active_ml = configs["active_ml"]
    tuned_ml = configs["tuned_ml"]

    assert base_cfg.alpha.ml_factor.enabled is True
    assert base_cfg.regime.use_ensemble is True

    assert no_ml.symbols == list(DEFAULT_RESEARCH_SYMBOLS)
    assert no_ml.execution.dry_run is True
    assert no_ml.universe.enabled is False
    assert no_ml.regime.use_ensemble is False
    assert no_ml.alpha.use_multi_strategy is True
    assert no_ml.alpha.ml_factor.enabled is False

    assert active_ml.alpha.ml_factor.enabled is True
    assert active_ml.alpha.ml_factor.require_promotion_passed is False
    assert active_ml.alpha.ml_factor.model_path == str(tmp_path / "models" / "ml_factor_model")
    assert active_ml.alpha.ml_factor.active_model_pointer_path == "reports/unused_ml_model_pointer.txt"

    assert tuned_ml.alpha.ml_factor.enabled is True
    assert tuned_ml.alpha.ml_factor.require_promotion_passed is False
    assert tuned_ml.alpha.ml_factor.model_path == str(tmp_path / "models" / "ml_factor_model_gpu_tuned")
    assert tuned_ml.alpha.ml_factor.active_model_pointer_path == "reports/unused_ml_model_pointer.txt"
