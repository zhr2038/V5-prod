from configs.loader import load_config
from configs.schema import ExecutionConfig


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
    assert cfg.execution.negative_expectancy_score_penalty_floor_bps == 15.0
    assert cfg.execution.negative_expectancy_score_penalty_per_bps == 0.03
    assert cfg.execution.negative_expectancy_score_penalty_max == 0.90
    assert cfg.execution.negative_expectancy_open_block_enabled is True
    assert cfg.execution.negative_expectancy_open_block_floor_bps == 25.0
    assert cfg.execution.negative_expectancy_lookback_hours == 72
    assert cfg.execution.negative_expectancy_cooldown_hours == 48
    assert cfg.execution.negative_expectancy_fast_fail_max_hold_minutes == 360
    assert cfg.execution.negative_expectancy_fast_fail_open_block_enabled is True
    assert cfg.execution.negative_expectancy_fast_fail_open_block_min_closed_cycles == 2
    assert cfg.execution.negative_expectancy_fast_fail_open_block_floor_bps == 5.0


def test_live_prod_sideways_churn_controls_load():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.universe.top_n_market_cap == 35
    assert cfg.universe.min_24h_quote_volume_usdt == 10000000
    assert cfg.regime.pos_mult_sideways == 0.6
    assert cfg.rebalance.deadband_sideways == 0.07
    assert cfg.execution.open_long_cooldown_minutes == 120
    assert cfg.execution.cost_aware_score_per_bps == 0.0030
    assert cfg.execution.cost_aware_min_score_floor == 0.18
    assert cfg.execution.low_price_entry_guard_enabled is True
    assert cfg.execution.low_price_entry_threshold_usdt == 0.20
    assert cfg.execution.low_price_entry_extra_score_floor == 0.12
    assert cfg.execution.low_price_entry_extra_cost_bps == 20.0
    assert cfg.execution.force_close_unscored_positions is True


def test_live_prod_funding_thresholds_load():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert cfg.regime.funding_trending_threshold == 0.10
    assert cfg.regime.funding_risk_off_threshold == -0.10
    assert cfg.regime.funding_breadth_threshold == 0.68
    assert cfg.regime.funding_extreme_sentiment_threshold == 0.12
    assert cfg.regime.funding_extreme_breadth_threshold == 0.55


def test_execution_config_accepts_custom_reconcile_failure_state_path():
    cfg = ExecutionConfig(reconcile_failure_state_path="reports/custom_reconcile_failure_state.json")
    assert cfg.reconcile_failure_state_path == "reports/custom_reconcile_failure_state.json"
