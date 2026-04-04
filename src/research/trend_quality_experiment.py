from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence
import os
import shutil

from configs.schema import AppConfig


DEFAULT_RESEARCH_SYMBOLS: tuple[str, ...] = (
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "BNB/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "LINK/USDT",
    "AVAX/USDT",
)


def _resolve_project_path(project_root: Path, raw_path: str | None, fallback: str) -> str:
    value = str(raw_path or fallback).strip() or fallback
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str((project_root / path).resolve())


def _bind_read_only_paths(cfg: AppConfig, project_root: Path) -> None:
    cfg.universe.blacklist_path = _resolve_project_path(
        project_root,
        cfg.universe.blacklist_path,
        "configs/blacklist.json",
    )
    cfg.execution.high_risk_blacklist_path = _resolve_project_path(
        project_root,
        cfg.execution.high_risk_blacklist_path,
        "configs/borrow_prevention_rules.json",
    )
    cfg.backtest.cost_stats_dir = _resolve_project_path(
        project_root,
        cfg.backtest.cost_stats_dir,
        "reports/cost_stats_clean",
    )
    cfg.alpha.dynamic_ic_weighting.ic_monitor_path = _resolve_project_path(
        project_root,
        cfg.alpha.dynamic_ic_weighting.ic_monitor_path,
        "reports/alpha_ic_monitor.json",
    )
    cfg.alpha.dynamic_weights_by_regime_path = _resolve_project_path(
        project_root,
        cfg.alpha.dynamic_weights_by_regime_path,
        "reports/alpha_dynamic_weights_by_regime.json",
    )
    cfg.alpha.ml_factor.model_path = _resolve_project_path(
        project_root,
        cfg.alpha.ml_factor.model_path,
        "models/ml_factor_model",
    )
    cfg.alpha.ml_factor.active_model_pointer_path = _resolve_project_path(
        project_root,
        cfg.alpha.ml_factor.active_model_pointer_path,
        "models/ml_factor_model_active.txt",
    )
    cfg.alpha.ml_factor.promotion_decision_path = _resolve_project_path(
        project_root,
        cfg.alpha.ml_factor.promotion_decision_path,
        "reports/model_promotion_decision.json",
    )


def _bind_sandbox_write_paths(cfg: AppConfig) -> None:
    cfg.universe.cache_path = "reports/universe_cache.json"
    cfg.alpha.optimizer_state_path = "reports/portfolio_optimizer_state.json"
    cfg.alpha.topk_dropout.state_path = "reports/topk_dropout_state.json"
    cfg.alpha.ml_factor.runtime_status_path = "reports/ml_runtime_status.json"
    cfg.alpha.ml_factor.impact_summary_path = "reports/ml_overlay_impact.json"
    cfg.alpha.ml_factor.impact_history_path = "reports/ml_overlay_impact_history.jsonl"
    cfg.alpha.ml_factor.impact_state_path = "reports/ml_overlay_impact_state.json"
    cfg.regime.regime_history_db_path = "reports/regime_history.db"
    cfg.execution.order_store_path = "reports/orders.sqlite"
    cfg.execution.kill_switch_path = "reports/kill_switch.json"
    cfg.execution.reconcile_status_path = "reports/reconcile_status.json"
    cfg.execution.slippage_db_path = "reports/slippage.sqlite"
    cfg.execution.negative_expectancy_state_path = "reports/negative_expectancy_cooldown.json"


def seed_sandbox_read_only_artifacts(project_root: Path, sandbox_dir: Path) -> None:
    """Seed local read-only artifacts required for fully offline research runs."""
    src = project_root / "reports" / "okx_spot_instruments.json"
    if not src.exists():
        return
    dst = sandbox_dir / "reports" / "okx_spot_instruments.json"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _apply_common_research_overrides(
    cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str],
) -> AppConfig:
    cfg.symbols = [str(symbol) for symbol in research_symbols]
    cfg.timeframe_main = "1h"

    cfg.universe.enabled = False
    cfg.universe.use_universe_symbols = False
    cfg.universe.include_symbols = []

    cfg.execution.mode = "dry_run"
    cfg.execution.dry_run = True
    cfg.execution.collect_ml_training_data = False

    # Keep the experiment price-driven and deterministic when running off cache.
    cfg.regime.use_ensemble = False
    cfg.regime.use_hmm = False
    cfg.regime.sentiment_regime_override_enabled = False
    cfg.regime.regime_monitor_enabled = False

    cfg.backtest.initial_equity_usdt = 100.0
    cfg.backtest.cost_model = "calibrated"
    cfg.backtest.cost_stats_dir = "reports/cost_stats_clean"
    cfg.backtest.fee_quantile = "p75"
    cfg.backtest.slippage_quantile = "p90"
    cfg.backtest.min_fills_global = 20
    cfg.backtest.min_fills_bucket = 8
    cfg.backtest.max_stats_age_days = 30

    _bind_read_only_paths(cfg, project_root)
    _bind_sandbox_write_paths(cfg)
    return cfg


def build_baseline_config(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> AppConfig:
    cfg = base_cfg.model_copy(deep=True)
    cfg = _apply_common_research_overrides(
        cfg,
        project_root=project_root,
        research_symbols=research_symbols,
    )
    cfg.alpha.use_multi_strategy = False
    cfg.alpha.ml_factor.enabled = False
    return cfg


def build_trend_quality_candidate_config(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> AppConfig:
    cfg = build_baseline_config(
        base_cfg,
        project_root=project_root,
        research_symbols=research_symbols,
    )

    cfg.alpha.use_multi_strategy = False
    cfg.alpha.alpha158_overlay.enabled = False
    cfg.alpha.dynamic_ic_weighting.enabled = False
    cfg.alpha.dynamic_weights_by_regime_enabled = False
    cfg.alpha.ml_factor.enabled = False
    cfg.alpha.long_top_pct = 0.34
    cfg.alpha.min_score_threshold = 0.0
    cfg.alpha.weights.f1_mom_5d = 0.05
    cfg.alpha.weights.f2_mom_20d = 0.45
    cfg.alpha.weights.f3_vol_adj_ret_20d = 0.35
    cfg.alpha.weights.f4_volume_expansion = 0.10
    cfg.alpha.weights.f5_rsi_trend_confirm = 0.05
    cfg.alpha.topk_dropout.enabled = True
    cfg.alpha.topk_dropout.topk_override = 2
    cfg.alpha.topk_dropout.n_drop_per_cycle = 1
    cfg.alpha.topk_dropout.hold_cycles = 4

    cfg.risk.max_positions_override = 2
    cfg.risk.max_single_weight = 0.40

    cfg.regime.pos_mult_trending = 1.0
    cfg.regime.pos_mult_sideways = 0.35
    cfg.regime.pos_mult_risk_off = 0.0

    cfg.rebalance.deadband_trending = 0.07
    cfg.rebalance.deadband_sideways = 0.10
    cfg.rebalance.deadband_riskoff = 0.10
    cfg.rebalance.new_position_deadband_multiplier = 1.5
    cfg.rebalance.close_only_deadband_multiplier = 0.35

    cfg.execution.open_long_cooldown_minutes = 240
    cfg.execution.rank_exit_max_rank = 2
    cfg.execution.rank_exit_confirm_rounds = 3
    cfg.execution.rank_exit_reentry_cooldown_minutes = 180
    cfg.execution.min_hold_minutes_before_rank_exit = 180
    cfg.execution.min_hold_minutes_before_regime_exit = 240
    cfg.execution.max_rebalance_turnover_per_cycle = 0.45
    cfg.execution.cost_aware_entry_enabled = False
    return cfg


def build_trend_quality_candidate_v2_config(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> AppConfig:
    cfg = build_baseline_config(
        base_cfg,
        project_root=project_root,
        research_symbols=research_symbols,
    )

    cfg.alpha.use_multi_strategy = False
    cfg.alpha.alpha158_overlay.enabled = True
    cfg.alpha.alpha158_overlay.blend_weight = 0.20
    cfg.alpha.dynamic_ic_weighting.enabled = False
    cfg.alpha.dynamic_weights_by_regime_enabled = False
    cfg.alpha.ml_factor.enabled = False
    cfg.alpha.long_top_pct = 0.50
    cfg.alpha.min_score_threshold = 0.0
    cfg.alpha.weights.f1_mom_5d = 0.08
    cfg.alpha.weights.f2_mom_20d = 0.32
    cfg.alpha.weights.f3_vol_adj_ret_20d = 0.28
    cfg.alpha.weights.f4_volume_expansion = 0.12
    cfg.alpha.weights.f5_rsi_trend_confirm = 0.20
    cfg.alpha.topk_dropout.enabled = True
    cfg.alpha.topk_dropout.topk_override = 3
    cfg.alpha.topk_dropout.n_drop_per_cycle = 1
    cfg.alpha.topk_dropout.hold_cycles = 3

    cfg.risk.max_positions_override = 3
    cfg.risk.max_single_weight = 0.30

    cfg.regime.pos_mult_trending = 1.0
    cfg.regime.pos_mult_sideways = 0.45
    cfg.regime.pos_mult_risk_off = 0.0

    cfg.rebalance.deadband_trending = 0.08
    cfg.rebalance.deadband_sideways = 0.12
    cfg.rebalance.deadband_riskoff = 0.12
    cfg.rebalance.new_position_deadband_multiplier = 1.8
    cfg.rebalance.close_only_deadband_multiplier = 0.35

    cfg.execution.open_long_cooldown_minutes = 240
    cfg.execution.rank_exit_max_rank = 4
    cfg.execution.rank_exit_confirm_rounds = 3
    cfg.execution.rank_exit_reentry_cooldown_minutes = 180
    cfg.execution.min_hold_minutes_before_rank_exit = 180
    cfg.execution.min_hold_minutes_before_regime_exit = 240
    cfg.execution.max_rebalance_turnover_per_cycle = 1.0
    cfg.execution.cost_aware_entry_enabled = False
    return cfg


def build_experiment_configs(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> dict[str, AppConfig]:
    return {
        "baseline": build_baseline_config(
            base_cfg,
            project_root=project_root,
            research_symbols=research_symbols,
        ),
        "trend_quality": build_trend_quality_candidate_config(
            base_cfg,
            project_root=project_root,
            research_symbols=research_symbols,
        ),
        "trend_quality_v2": build_trend_quality_candidate_v2_config(
            base_cfg,
            project_root=project_root,
            research_symbols=research_symbols,
        ),
    }


@contextmanager
def sandbox_working_directory(path: str | Path) -> Iterator[Path]:
    target = Path(path).resolve()
    target.mkdir(parents=True, exist_ok=True)
    (target / "reports").mkdir(parents=True, exist_ok=True)
    prev = Path.cwd()
    try:
        os.chdir(target)
        yield target
    finally:
        os.chdir(prev)
