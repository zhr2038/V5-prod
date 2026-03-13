from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence
import os

from configs.schema import AppConfig


DEFAULT_RESEARCH_SYMBOLS: tuple[str, ...] = (
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "BNB/USDT",
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

    # Keep the overlay comparison deterministic when running off cached OHLCV.
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


def build_no_ml_config(
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
    cfg.alpha.ml_factor.enabled = False
    return cfg


def _build_ml_model_config(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    model_base_path: str,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> AppConfig:
    cfg = base_cfg.model_copy(deep=True)
    cfg = _apply_common_research_overrides(
        cfg,
        project_root=project_root,
        research_symbols=research_symbols,
    )
    cfg.alpha.ml_factor.enabled = True
    cfg.alpha.ml_factor.model_path = _resolve_project_path(project_root, model_base_path, model_base_path)
    cfg.alpha.ml_factor.active_model_pointer_path = "reports/unused_ml_model_pointer.txt"
    cfg.alpha.ml_factor.promotion_decision_path = "reports/unused_model_promotion_decision.json"
    cfg.alpha.ml_factor.require_promotion_passed = False
    cfg.alpha.ml_factor.max_model_age_hours = 24 * 30
    return cfg


def build_active_ml_config(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> AppConfig:
    return _build_ml_model_config(
        base_cfg,
        project_root=project_root,
        model_base_path="models/ml_factor_model",
        research_symbols=research_symbols,
    )


def build_tuned_ml_config(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> AppConfig:
    return _build_ml_model_config(
        base_cfg,
        project_root=project_root,
        model_base_path="models/ml_factor_model_gpu_tuned",
        research_symbols=research_symbols,
    )


def build_experiment_configs(
    base_cfg: AppConfig,
    *,
    project_root: Path,
    research_symbols: Sequence[str] = DEFAULT_RESEARCH_SYMBOLS,
) -> dict[str, AppConfig]:
    return {
        "no_ml": build_no_ml_config(
            base_cfg,
            project_root=project_root,
            research_symbols=research_symbols,
        ),
        "active_ml": build_active_ml_config(
            base_cfg,
            project_root=project_root,
            research_symbols=research_symbols,
        ),
        "tuned_ml": build_tuned_ml_config(
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
