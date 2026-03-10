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
