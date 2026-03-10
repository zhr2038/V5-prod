from configs.loader import load_config


def test_config_loads():
    cfg = load_config("configs/config.yaml", env_path=None)
    assert cfg.symbols
    assert cfg.timeframe_main == "1h"


def test_live_prod_exclude_symbols_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert "PAXG/USDT" in cfg.universe.exclude_symbols
    assert "XAUT/USDT" in cfg.universe.exclude_symbols


def test_live_prod_preflight_self_heal_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.execution.reconcile_abs_usdt_tol == 1.0
    assert cfg.execution.auto_clear_kill_switch_if_ok is True
    assert cfg.execution.preflight_bootstrap_patch_enabled is True
    assert cfg.execution.preflight_bootstrap_patch_max_total_usdt == 100.0
    assert cfg.execution.preflight_bootstrap_patch_min_interval_sec == 300


def test_live_prod_rank_exit_and_peak_drawdown_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.execution.rank_exit_strict_mode is True
    assert cfg.execution.peak_drawdown_exit.enabled is True
    assert cfg.execution.peak_drawdown_exit.tier1_profit_pct == 0.08
    assert cfg.execution.peak_drawdown_exit.tier1_retrace_pct == 0.025
    assert cfg.execution.peak_drawdown_exit.tier1_sell_pct == 0.33
    assert cfg.execution.peak_drawdown_exit.tier3_sell_pct == 1.0


def test_live_prod_ml_factor_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.alpha.ml_factor.enabled is True
    assert cfg.alpha.ml_factor.ml_weight == 0.20
    assert cfg.alpha.ml_factor.require_promotion_passed is True
    assert cfg.alpha.ml_factor.model_path == "models/ml_factor_model"
