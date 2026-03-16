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
    assert cfg.execution.min_hold_minutes_before_rank_exit == 30
    assert cfg.execution.peak_drawdown_exit.enabled is True
    assert cfg.execution.peak_drawdown_exit.tier1_profit_pct == 0.08
    assert cfg.execution.peak_drawdown_exit.tier1_retrace_pct == 0.025
    assert cfg.execution.peak_drawdown_exit.tier1_sell_pct == 0.33
    assert cfg.execution.peak_drawdown_exit.tier3_sell_pct == 1.0


def test_live_prod_ml_factor_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.alpha.ml_factor.enabled is True
    assert cfg.alpha.ml_factor.ml_weight == 0.20
    assert cfg.alpha.ml_factor.online_control_enabled is True
    assert cfg.alpha.ml_factor.online_control_24h_min_points == 6
    assert cfg.alpha.ml_factor.online_control_24h_min_coverage_hours == 18
    assert cfg.alpha.ml_factor.online_control_48h_min_points == 12
    assert cfg.alpha.ml_factor.online_control_48h_min_coverage_hours == 36
    assert cfg.alpha.ml_factor.online_control_downweight_ml_weight == 0.08
    assert cfg.alpha.ml_factor.overlay_transform == "tanh"
    assert cfg.alpha.ml_factor.overlay_transform_scale == 1.6
    assert cfg.alpha.ml_factor.overlay_transform_max_abs == 1.6
    assert cfg.alpha.ml_factor.require_promotion_passed is True
    assert cfg.alpha.ml_factor.model_path == "models/ml_factor_model"


def test_live_prod_conflict_penalty_and_negative_expectancy_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.alpha.multi_strategy_conflict_penalty_enabled is True
    assert cfg.alpha.multi_strategy_conflict_dominance_ratio == 1.35
    assert cfg.alpha.multi_strategy_conflict_penalty_strength == 0.65
    assert cfg.alpha.multi_strategy_score_transform == "tanh"
    assert cfg.alpha.multi_strategy_score_transform_scale == 1.0
    assert cfg.alpha.mean_reversion.allocation == 0.25
    assert cfg.alpha.mean_reversion.allocation_multiplier_trending == 0.70
    assert cfg.alpha.mean_reversion.allocation_multiplier_sideways == 1.20
    assert cfg.alpha.mean_reversion.buy_score_multiplier == 0.75
    assert cfg.alpha.mean_reversion.sell_score_multiplier == 1.0
    assert cfg.execution.negative_expectancy_score_penalty_enabled is True
    assert cfg.execution.negative_expectancy_score_penalty_floor_bps == 5.0
    assert cfg.execution.negative_expectancy_score_penalty_per_bps == 0.015
    assert cfg.execution.negative_expectancy_open_block_enabled is True
    assert cfg.execution.negative_expectancy_open_block_floor_bps == 15.0


def test_live_prod_sideways_churn_controls_load():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.regime.pos_mult_sideways == 0.6
    assert cfg.rebalance.deadband_sideways == 0.07
    assert cfg.execution.open_long_cooldown_minutes == 120
    assert cfg.execution.cost_aware_score_per_bps == 0.0030
    assert cfg.execution.cost_aware_min_score_floor == 0.14
    assert cfg.execution.low_price_entry_guard_enabled is True
    assert cfg.execution.low_price_entry_threshold_usdt == 0.05
    assert cfg.execution.low_price_entry_extra_score_floor == 0.08
    assert cfg.execution.low_price_entry_extra_cost_bps == 12.0


def test_live_prod_funding_thresholds_load():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.regime.funding_trending_threshold == 0.10
    assert cfg.regime.funding_risk_off_threshold == -0.10
    assert cfg.regime.funding_breadth_threshold == 0.68
    assert cfg.regime.funding_extreme_sentiment_threshold == 0.12
    assert cfg.regime.funding_extreme_breadth_threshold == 0.55
