from __future__ import annotations

import csv
import io
import json
import tarfile
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from configs.schema import AppConfig, DiagnosticsConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.skipped_candidate_tracker import (
    HORIZON_PREFIX,
    _coerce_epoch_ms,
    _default_ohlcv_provider_for_cfg,
    _find_close_at_or_after,
    _iso_from_ms,
    _normalize_bool,
    _normalize_float,
    _normalize_horizons,
    _record_entry_ts_ms,
    _resolve_reports_dir,
    _series_for_symbol,
    _summaries_dir,
    _update_labels,
    _write_csv,
    _write_records,
)


SOL_SYMBOL = "SOL/USDT"
ETH_SYMBOL = "ETH/USDT"
BNB_SYMBOL = "BNB/USDT"
ETH_F3_DOMINANT_STRATEGY_ID = "ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1"
BNB_F3_DOMINANT_STRATEGY_ID = "BNB_F3_DOMINANT_ENTRY_PAPER_V1"
BNB_RISK_ON_BUY_STRATEGY_ID = "BNB_RISK_ON_BUY_PAPER_V1"
BOTTOM_ZONE_PROBE_STRATEGY_ID = "BOTTOM_ZONE_PROBE_PAPER_V1"
HYPE_EXPANDED_UNIVERSE_STRATEGY_ID = "HYPE_EXPANDED_UNIVERSE_PAPER_V1"
WLD_EXPANDED_UNIVERSE_STRATEGY_ID = "WLD_EXPANDED_UNIVERSE_PAPER_V1"
ETH_F3_DOMINANT_MIN_48H_COMPLETE_COUNT = 30
ETH_F3_ALPHA6_NOT_BUY_REASON = "eth_f3_alpha6_side_not_buy_no_new_entry"
ETH_F3_DOMINANT_LIVE_BLOCK_REASONS = [
    "cost_source_not_actual_or_mixed",
    "f3_global_evidence_negative",
    "eth_f3_paper_only_no_live",
]
BNB_PAPER_LIVE_BLOCK_REASONS = [
    "bnb_paper_only_no_live",
    "bnb_negative_expectancy_recovery_research_only",
]
EXPANDED_UNIVERSE_PAPER_LIVE_BLOCK_REASONS = [
    "expanded_universe_paper_only_no_live",
    "not_in_v5_live_universe",
]
BOTTOM_ZONE_PAPER_LIVE_BLOCK_REASONS = [
    "bottom_zone_probe_paper_only_no_live",
    "bottom_zone_reversal_research_only",
]
BNB_ALLOWED_PAPER_REGIMES = {"TREND_UP", "ALT_IMPULSE", "TRENDING"}
DEFAULT_HORIZONS = [4, 8, 12, 24, 48, 72]
PRIMARY_HORIZON = 24
LIVE_SMALL_READY_COST_SOURCES = {"actual_fills", "mixed_actual_proxy"}
ADVISORY_ALLOWED_RECOMMENDED_MODES = {"paper", "shadow"}
ADVISORY_DISPLAY_ONLY_RECOMMENDED_MODES = {"research"}
ALPHA_FACTORY_SECOND_STAGE_CANDIDATES = {
    "v5.expanded_relative_strength_top1_shadow",
    "v5.expanded_relative_strength_top3_shadow",
    "v5.futures_risk_off_hedge_proxy_shadow",
    "v5.futures_downtrend_short_proxy_shadow",
    "v5.btc_strict_probe_exit_policy_review",
    "v5.pair_trade_eth_btc_shadow",
}
RISK_ON_MULTI_BUY_SHADOW_CANDIDATES = {
    "v5.risk_on_multi_buy_top1_shadow",
    "v5.risk_on_multi_buy_top2_shadow",
    "v5.risk_on_multi_buy_top3_shadow",
}
BOTTOM_ZONE_ADVISORY_CANDIDATES = {
    "v5.bottom_zone_probe_paper",
    "v5.bottom_zone_reversal_shadow",
    "bottom_zone_reversal_shadow",
    "bottom_zone_probe_paper",
}

DEFAULT_PAPER_STRATEGY_CONFIGS = [
    {
        "strategy_id": "SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1",
        "experiment_name": "v5.sol_protect_alpha6_low_exception",
        "source_strategy_candidates": [
            "sol_protect_alpha6_low_exception",
            "sol_protect_rsi_weak_exception",
        ],
        "allowed_block_reasons": [
            "protect_entry_alpha6_score_too_low",
            "protect_entry_rsi_confirm_too_weak",
        ],
        "min_f4_volume_expansion": 0.0,
    },
    {
        "strategy_id": "SOL_F4_VOLUME_EXPANSION_PAPER_V1",
        "experiment_name": "v5.f4_volume_expansion_entry",
        "source_strategy_candidates": [
            "f4_volume_swing",
            "f4_volume_expansion_entry",
            "v5.f4_volume_expansion_entry",
            "f4_volume_expansion",
        ],
        "allowed_block_reasons": [],
        "min_f4_volume_expansion": 0.0,
    },
    {
        "strategy_id": BNB_F3_DOMINANT_STRATEGY_ID,
        "experiment_name": "v5.bnb_f3_dominant_entry",
        "source_strategy_candidates": [
            "f3_dominant_entry",
            "v5.f3_dominant_entry",
            "v5.bnb_f3_dominant_entry",
        ],
        "allowed_block_reasons": [],
        "symbol": BNB_SYMBOL,
        "primary_horizon_hours": 24,
        "require_protect_level": False,
        "require_no_cooldown": False,
        "require_alpha6_buy": True,
        "min_alpha6_score": 0.9,
        "require_expected_edge_gt_required": True,
        "require_cost_gate_verified": True,
        "allowed_current_regimes": sorted(BNB_ALLOWED_PAPER_REGIMES),
        "extra_live_block_reasons": list(BNB_PAPER_LIVE_BLOCK_REASONS),
    },
    {
        "strategy_id": BNB_RISK_ON_BUY_STRATEGY_ID,
        "experiment_name": "v5.bnb_risk_on_buy",
        "source_strategy_candidates": [],
        "allowed_block_reasons": [],
        "symbol": BNB_SYMBOL,
        "primary_horizon_hours": 24,
        "require_protect_level": False,
        "require_no_cooldown": False,
        "require_alpha6_buy": True,
        "min_alpha6_score": 0.9,
        "require_expected_edge_gt_required": True,
        "require_cost_gate_verified": True,
        "allowed_current_regimes": sorted(BNB_ALLOWED_PAPER_REGIMES),
        "extra_live_block_reasons": list(BNB_PAPER_LIVE_BLOCK_REASONS),
    },
    {
        "strategy_id": HYPE_EXPANDED_UNIVERSE_STRATEGY_ID,
        "experiment_name": "v5.hype_expanded_universe_paper",
        "source_strategy_candidates": [],
        "allowed_block_reasons": [],
        "symbol": "HYPE/USDT",
        "primary_horizon_hours": 24,
        "require_protect_level": False,
        "require_no_cooldown": False,
        "require_alpha6_buy": False,
        "expanded_universe_paper_only": True,
        "required_expanded_universe_maturity_state": "PAPER_READY",
        "require_cost_source_not_global_default": True,
        "require_zero_live_notional": True,
        "extra_live_block_reasons": list(EXPANDED_UNIVERSE_PAPER_LIVE_BLOCK_REASONS),
    },
    {
        "strategy_id": WLD_EXPANDED_UNIVERSE_STRATEGY_ID,
        "experiment_name": "v5.wld_expanded_universe_paper",
        "source_strategy_candidates": [],
        "allowed_block_reasons": [],
        "symbol": "WLD/USDT",
        "primary_horizon_hours": 24,
        "require_protect_level": False,
        "require_no_cooldown": False,
        "require_alpha6_buy": False,
        "expanded_universe_paper_only": True,
        "required_expanded_universe_maturity_state": "PAPER_READY",
        "require_cost_source_not_global_default": True,
        "require_zero_live_notional": True,
        "extra_live_block_reasons": list(EXPANDED_UNIVERSE_PAPER_LIVE_BLOCK_REASONS),
    },
]

PAPER_RUN_FIELDS = [
    "strategy_id",
    "experiment_name",
    "enabled_shadow_only",
    "enable_live_experiment",
    "live_symbols_unchanged",
    "run_id",
    "ts_utc",
    "paper_date",
    "symbol",
    "source_strategy_candidate",
    "candidate_id",
    "final_decision",
    "no_sample_reason",
    "sol_candidate_present",
    "risk_level",
    "original_block_reason",
    "cooldown_active",
    "risk_off",
    "entry_reason",
    "experiment_reason",
    "would_enter",
    "would_exit",
    "would_exit_time",
    "would_exit_rule",
    "expected_exit_horizon",
    "would_size_notional",
    "would_size_usdt",
    "paper_pnl_bps",
    "paper_pnl_usdt",
    "entry_px",
    "arrival_bid",
    "arrival_ask",
    "arrival_mid",
    "estimated_spread_bps",
    "expected_order_type",
    "estimated_fill_px",
    "final_score",
    "alpha6_score",
    "alpha6_side",
    "f4_volume_expansion",
    "f4_threshold",
    "f5_rsi_trend_confirm",
    "cost_source",
    "cost_source_quality",
    "estimated_cost_bps",
    "cost_model_version",
    "cost_source_live_ready",
    "slippage_covered",
    "required_paper_days",
    "required_entry_days",
    "required_slippage_coverage",
    "live_small_ready",
    "readiness_status",
    "live_block_reason",
    "advisory_present",
    "advisory_source",
    "advisory_source_path",
    "advisory_fresh",
    "advisory_age_sec",
    "advisory_contract_match",
    "stale_advisory_used",
    "api_fallback_attempted",
    "api_fallback_success",
    "advisory_strategy_id",
    "advisory_strategy_candidate",
    "advisory_decision",
    "advisory_recommended_mode",
    "advisory_negative",
    "advisory_response_action",
    "advisory_match_key",
    "advisory_match_reason",
    "advisory_max_paper_notional_usdt",
    "advisory_max_live_notional_usdt",
    "advisory_max_live_notional_usdt_ignored",
    "advisory_live_block_reasons",
    "enable_live_small_from_quant_lab",
    "live_order_effect",
    "proposal_present",
    "proposal_source",
    "label_status",
    "label_not_observable_reason",
]
for _horizon in DEFAULT_HORIZONS:
    PAPER_RUN_FIELDS.extend(
        [
            f"paper_pnl_bps_{_horizon}h",
            f"paper_pnl_usdt_{_horizon}h",
            f"{HORIZON_PREFIX}{_horizon}h_status",
            f"{HORIZON_PREFIX}{_horizon}h_reason",
        ]
    )

PAPER_DAILY_FIELDS = [
    "paper_date",
    "strategy_id",
    "experiment_name",
    "symbol",
    "entry_count",
    "entry_day_count",
    "complete_count",
    "pending_count",
    "not_observable_count",
    "avg_paper_pnl_bps",
    "avg_paper_pnl_bps_by_horizon",
    "complete_count_by_horizon",
    "win_rate_by_horizon",
    "paper_pnl_observed_count_by_horizon",
    "paper_pnl_day_count_by_horizon",
    "paper_pnl_usdt_sum",
    "win_rate",
    "paper_days_to_date",
]
for _horizon in DEFAULT_HORIZONS:
    PAPER_DAILY_FIELDS.append(f"avg_paper_pnl_bps_{_horizon}h")

PAPER_SLIPPAGE_FIELDS = [
    "strategy_id",
    "experiment_name",
    "symbol",
    "paper_days",
    "required_paper_days",
    "required_entry_days",
    "total_rows",
    "slippage_covered_rows",
    "slippage_coverage",
    "arrival_mid_coverage",
    "spread_observation_coverage",
    "cost_source_mix",
    "required_slippage_coverage",
    "latest_cost_source",
    "allowed_live_cost_sources",
    "live_small_ready",
    "readiness_status",
    "live_block_reason",
]

STRATEGY_ADVISORY_FIELDS = [
    "source_path",
    "advisory_source",
    "advisory_fresh",
    "freshness_status",
    "stale_reason",
    "advisory_age_sec",
    "advisory_contract_match",
    "stale_advisory_used",
    "api_fallback_attempted",
    "api_fallback_success",
    "as_of_ts",
    "generated_at",
    "expires_at",
    "contract_version",
    "quant_lab_git_commit",
    "source_version",
    "strategy_id",
    "strategy_candidate",
    "experiment_name",
    "symbol",
    "decision",
    "recommended_mode",
    "universe_type",
    "horizon_hours",
    "sample_count",
    "complete_sample_count",
    "expanded_universe_maturity_state",
    "cost_source",
    "cost_source_quality",
    "cost_bps",
    "cost_model_version",
    "advisory_status",
    "advisory_reason",
    "max_paper_notional_usdt",
    "max_live_notional_usdt",
    "live_block_reasons",
    "would_block_if_enabled",
    "would_enter",
    "no_sample_reason",
    "enable_live_small_from_quant_lab",
    "response_action",
    "negative_advisory",
    "max_live_notional_usdt_ignored",
]

STRATEGY_ADVISORY_SOURCE_HEALTH_FIELDS = [
    "run_id",
    "ts_utc",
    "local_row_count",
    "api_row_count",
    "selected_row_count",
    "local_fresh",
    "api_fresh",
    "selection_reason",
    "stale_local_overrode_api",
    "latest_local_generated_at",
    "latest_api_generated_at",
    "local_latest_file_mtime",
    "latest_quant_lab_bundle_seen",
    "api_lake_generated_at",
    "api_cache_hit",
    "selected_latest_generated_at",
    "advisory_age_sec",
    "advisory_max_age_sec",
    "advisory_expires_at",
    "expires_before_generated_at",
    "expiry_corrected",
    "freshness_basis",
    "freshness_status",
    "freshness_reason",
    "advisory_source_lag_sec",
    "selected_source",
    "selected_source_is_stale",
    "api_fallback_attempted",
    "api_fallback_success",
    "stale_reason",
    "stale_reason_detail",
    "suggested_fix",
    "warning",
    "freshness_inconsistency_warning",
]

EXPANDED_UNIVERSE_ADVISORY_FIELDS = [
    "run_id",
    "ts_utc",
    "source_path",
    "advisory_source",
    "advisory_fresh",
    "advisory_age_sec",
    "advisory_contract_match",
    "stale_advisory_used",
    "api_fallback_attempted",
    "api_fallback_success",
    "as_of_ts",
    "generated_at",
    "expires_at",
    "contract_version",
    "quant_lab_git_commit",
    "source_version",
    "universe_type",
    "symbol",
    "symbol_in_live_universe",
    "live_symbols_unchanged",
    "strategy_id",
    "strategy_candidate",
    "experiment_name",
    "decision",
    "recommended_mode",
    "horizon_hours",
    "sample_count",
    "complete_sample_count",
    "expanded_universe_maturity_state",
    "cost_source",
    "cost_source_quality",
    "cost_bps",
    "cost_model_version",
    "response_action",
    "negative_advisory",
    "paper_tracking_allowed",
    "shadow_tracking_allowed",
    "max_paper_notional_usdt",
    "max_live_notional_usdt",
    "max_live_notional_usdt_ignored",
    "live_block_reasons",
    "would_block_if_enabled",
    "would_enter",
    "no_sample_reason",
    "advisory_reason",
    "live_order_effect",
]

EXPANDED_UNIVERSE_PAPER_RUN_FIELDS = [
    "run_id",
    "ts_utc",
    "paper_date",
    "universe_type",
    "symbol",
    "symbol_in_live_universe",
    "live_symbols_unchanged",
    "strategy_id",
    "strategy_candidate",
    "experiment_name",
    "tracking_mode",
    "decision",
    "recommended_mode",
    "response_action",
    "negative_advisory",
    "would_enter",
    "would_size_usdt",
    "max_paper_notional_usdt",
    "max_live_notional_usdt_ignored",
    "no_sample_reason",
    "advisory_source",
    "advisory_source_path",
    "advisory_fresh",
    "advisory_contract_match",
    "live_block_reasons",
    "live_order_effect",
]

ALPHA_FACTORY_ADVISORY_FIELDS = [
    "run_id",
    "ts_utc",
    "strategy_candidate",
    "symbol",
    "decision",
    "recommended_mode",
    "promotion_state",
    "alpha_factory_score",
    "advisory_source",
    "selected_source",
    "source_health_freshness_status",
    "advisory_fresh",
    "advisory_age_sec",
    "stale_reason",
    "stale_response_downgraded",
    "response_action",
    "max_live_notional_usdt_ignored",
    "live_order_effect",
]

ALPHA_FACTORY_FAMILY_SUMMARY_FIELDS = [
    "run_id",
    "ts_utc",
    "family",
    "row_count",
    "display_only_count",
    "shadow_tracking_count",
    "paper_tracking_count",
    "negative_advisory_count",
    "max_live_notional_usdt_ignored",
    "live_order_effect",
    "strategy_candidates",
]

RISK_ON_MULTI_BUY_SHADOW_FIELDS = [
    "run_id",
    "ts_utc",
    "current_regime",
    "top_k",
    "selected_symbols",
    "would_buy_symbols",
    "actual_bought_symbols",
    "missed_symbols",
    "source_detail_available",
    "source_detail_missing_paths",
    "response_action",
    "live_order_effect",
]


@dataclass
class AdvisoryReadResult:
    rows: list[dict[str, Any]]
    source_health_rows: list[dict[str, Any]]


@dataclass
class AdvisoryApiReadResult:
    rows: list[dict[str, Any]]
    meta: dict[str, Any]


def _diagnostics_cfg(cfg: Any) -> DiagnosticsConfig:
    diagnostics = getattr(cfg, "diagnostics", None)
    return diagnostics if diagnostics is not None else DiagnosticsConfig()


def _labels_path(reports_dir: Path) -> Path:
    return reports_dir / "sol_paper_strategy_labels.jsonl"


def _symbol_text(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "/")


def _truthy(value: Any) -> bool:
    parsed = _normalize_bool(value)
    if parsed is not None:
        return bool(parsed)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _risk_off_text(value: Any) -> bool:
    text = str(value or "").strip().lower().replace("_", "-")
    return text in {"risk-off", "riskoff"} or text.startswith("risk-off")


def _horizons(diagnostics: DiagnosticsConfig) -> list[int]:
    return _normalize_horizons(
        getattr(diagnostics, "paper_strategy_horizons_hours", None),
        DEFAULT_HORIZONS,
    )


def _strategy_configs(diagnostics: DiagnosticsConfig) -> list[dict[str, Any]]:
    raw = getattr(diagnostics, "paper_strategy_configs", None) or DEFAULT_PAPER_STRATEGY_CONFIGS
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        strategy_id = str(item.get("strategy_id") or "").strip()
        experiment_name = str(item.get("experiment_name") or "").strip()
        if not strategy_id or not experiment_name:
            continue
        payload = dict(item)
        payload["source_strategy_candidates"] = {
            str(value or "").strip()
            for value in item.get("source_strategy_candidates", []) or []
            if str(value or "").strip()
        }
        payload["allowed_block_reasons"] = {
            str(value or "").strip()
            for value in item.get("allowed_block_reasons", []) or []
            if str(value or "").strip()
        }
        out.append(payload)
    return out or [dict(item) for item in DEFAULT_PAPER_STRATEGY_CONFIGS]


def _strategy_symbol(spec: Mapping[str, Any]) -> str:
    symbol = _symbol_text(spec.get("symbol"))
    return symbol or SOL_SYMBOL


def _no_candidate_reason(spec: Mapping[str, Any]) -> str:
    configured = str(spec.get("no_candidate_reason") or "").strip()
    if configured:
        return configured
    base = _strategy_symbol(spec).split("/", 1)[0].strip().lower()
    return f"no_{base}_candidate" if base else "no_strategy_candidate"


def _spec_bool(spec: Mapping[str, Any], key: str, default: bool) -> bool:
    if key not in spec:
        return bool(default)
    parsed = _normalize_bool(spec.get(key))
    if parsed is not None:
        return bool(parsed)
    return bool(spec.get(key))


def _parse_horizon_hours(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if text.endswith("h"):
        text = text[:-1]
    parsed = _normalize_float(text)
    if parsed is None or parsed <= 0:
        return None
    return int(parsed)


def _primary_horizon_for_spec(spec: Mapping[str, Any], horizons: Iterable[int]) -> int:
    available = {int(h) for h in horizons}
    configured = (
        _parse_horizon_hours(spec.get("primary_horizon_hours"))
        or _parse_horizon_hours(spec.get("suggested_horizon"))
    )
    if configured and configured in available:
        return configured
    if PRIMARY_HORIZON in available:
        return PRIMARY_HORIZON
    return max(available)


def _record_primary_horizon(record: Mapping[str, Any], horizons: Iterable[int]) -> int:
    configured = _parse_horizon_hours(record.get("primary_horizon_hours"))
    if configured and configured in {int(h) for h in horizons}:
        return configured
    return _primary_horizon_for_spec(record, horizons)


def _load_existing_records(path: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        if isinstance(payload, dict):
            records[_record_key(payload)] = payload
    return records


def _apply_eth_f3_alpha6_entry_gate(record: dict[str, Any]) -> bool:
    if str(record.get("strategy_id") or "") != ETH_F3_DOMINANT_STRATEGY_ID:
        return False
    alpha6_side = str(record.get("alpha6_side") or "").strip()
    source_candidate = str(record.get("source_strategy_candidate") or "").strip()
    final_decision = str(record.get("final_decision") or "").strip().lower()
    if (
        not alpha6_side
        and source_candidate == "heartbeat"
        and final_decision == "heartbeat"
    ):
        return False
    if not _eth_f3_alpha6_entry_blocked(record):
        return False
    changed = (
        _normalize_bool(record.get("would_enter")) is True
        or str(record.get("no_sample_reason") or "") != ETH_F3_ALPHA6_NOT_BUY_REASON
    )
    record["would_enter"] = False
    record["would_exit"] = False
    record["would_size_notional"] = 0.0
    record["would_size_usdt"] = 0.0
    record["no_sample_reason"] = ETH_F3_ALPHA6_NOT_BUY_REASON
    record["skip_reason"] = ETH_F3_ALPHA6_NOT_BUY_REASON
    record["label_status"] = "heartbeat"
    record["label_not_observable_reason"] = ""
    record["paper_pnl_bps"] = None
    record["paper_pnl_usdt"] = None
    return changed


def _eth_f3_alpha6_entry_blocked(record: Mapping[str, Any]) -> bool:
    return (
        str(record.get("strategy_id") or "") == ETH_F3_DOMINANT_STRATEGY_ID
        and not _is_alpha6_buy(record.get("alpha6_side"))
    )


def _paper_would_enter(record: Mapping[str, Any]) -> bool:
    if _eth_f3_alpha6_entry_blocked(record):
        return False
    return _normalize_bool(record.get("would_enter")) is True


def _read_candidate_snapshot(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh) if row]
    except Exception:
        return []


def _strategy_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _default_advisory_paths() -> list[str]:
    return [
        "/var/lib/v5-prod/strategy_opportunity_advisory.csv",
        "/var/lib/v5-prod/quant_lab_latest_bundle.zip",
        "/var/lib/v5-prod/quant_lab_latest_bundle.tar.gz",
        "strategy_opportunity_advisory.csv",
        "quant_lab/strategy_opportunity_advisory.csv",
        "quant_lab_latest/strategy_opportunity_advisory.csv",
        "quant_lab/latest/reports/strategy_opportunity_advisory.csv",
        "reports/strategy_opportunity_advisory.csv",
        "reports/quant_lab_latest/strategy_opportunity_advisory.csv",
        "reports/quant_lab/latest/reports/strategy_opportunity_advisory.csv",
        "reports/quant_lab_latest_bundle.zip",
        "reports/quant_lab_latest_bundle.tar.gz",
        "reports/quant_lab/latest_bundle.zip",
        "reports/quant_lab/latest_bundle.tar.gz",
    ]


def _default_advisory_api_paths() -> list[str]:
    return [
        "/v1/strategy-opportunity-advisory/v5-compact",
        "/v1/strategy-opportunity-advisory",
        "/v1/strategy_opportunity_advisory",
        "/v1/reports/strategy-opportunity-advisory",
    ]


def _default_proposal_paths() -> list[str]:
    return [
        "/var/lib/v5-prod/paper_strategy_proposals.csv",
        "/var/lib/v5-prod/quant_lab_latest_bundle.zip",
        "/var/lib/v5-prod/quant_lab_latest_bundle.tar.gz",
        "paper_strategy_proposals.csv",
        "quant_lab/paper_strategy_proposals.csv",
        "quant_lab_latest/paper_strategy_proposals.csv",
        "quant_lab/latest/reports/paper_strategy_proposals.csv",
        "reports/paper_strategy_proposals.csv",
        "reports/quant_lab_latest/paper_strategy_proposals.csv",
        "reports/quant_lab/latest/reports/paper_strategy_proposals.csv",
        "reports/quant_lab_latest_bundle.zip",
        "reports/quant_lab_latest_bundle.tar.gz",
        "reports/quant_lab/latest_bundle.zip",
        "reports/quant_lab/latest_bundle.tar.gz",
    ]


def _candidate_advisory_paths(raw_path: str, *, run_path: Path, reports_dir: Path) -> list[Path]:
    path = Path(str(raw_path or "").strip())
    if not str(path):
        return []
    if path.is_absolute():
        return [path]
    candidates = [reports_dir / path, run_path / path, Path.cwd() / path]
    parts = path.parts
    if parts and parts[0].lower() == "reports":
        candidates.append(reports_dir.parent / path)
        candidates.append(reports_dir / Path(*parts[1:]) if len(parts) > 1 else reports_dir)
    return list(dict.fromkeys(candidates))


def _advisory_first(row: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return ""


def _advisory_time_ms(value: Any) -> Optional[int]:
    parsed = _coerce_epoch_ms(value)
    if parsed is not None:
        return parsed
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt_value = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=timezone.utc)
    return int(dt_value.timestamp() * 1000.0)


def _advisory_expected_contract_version(diagnostics: DiagnosticsConfig) -> str:
    return str(
        getattr(diagnostics, "enforce_readiness_required_contract_version", "")
        or "v5.quant_lab.telemetry.v2"
    ).strip()


def _normalize_advisory_row(row: Mapping[str, Any], *, source_path: str) -> dict[str, Any]:
    strategy_id = str(
        _advisory_first(row, ("strategy_id", "strategy", "strategy_name", "alpha_id", "proposal_id"))
        or ""
    ).strip()
    strategy_candidate = str(
        _advisory_first(row, ("strategy_candidate", "candidate", "candidate_name", "source_strategy_candidate"))
        or ""
    ).strip()
    experiment_name = str(
        _advisory_first(row, ("experiment_name", "alpha_name", "strategy_family"))
        or ""
    ).strip()
    decision = str(
        _advisory_first(row, ("decision", "readiness_status", "board_decision", "status"))
        or ""
    ).strip().upper()
    recommended_mode = str(
        _advisory_first(row, ("recommended_mode", "mode", "target_mode"))
        or ""
    ).strip().lower().replace("-", "_")
    return {
        "source_path": source_path,
        "strategy_id": strategy_id,
        "strategy_candidate": strategy_candidate,
        "experiment_name": experiment_name,
        "source_module": str(_advisory_first(row, ("source_module", "module", "origin_module")) or "").strip().lower(),
        "symbol": _symbol_text(_advisory_first(row, ("symbol", "instId", "instrument", "normalized_symbol"))),
        "decision": decision,
        "recommended_mode": recommended_mode,
        "promotion_state": str(_advisory_first(row, ("promotion_state", "promotion", "promoted_state")) or "").strip(),
        "alpha_factory_score": _normalize_float(
            _advisory_first(row, ("alpha_factory_score", "factory_score", "advisory_score", "score"))
        ),
        "universe_type": str(_advisory_first(row, ("universe_type", "paper_universe_type", "universe")) or "").strip().lower(),
        "horizon_hours": _parse_horizon_hours(row.get("horizon_hours") or row.get("suggested_horizon")),
        "sample_count": _normalize_float(row.get("sample_count")),
        "complete_sample_count": _normalize_float(row.get("complete_sample_count")),
        "advisory_status": str(_advisory_first(row, ("status", "readiness_status", "decision")) or "").strip(),
        "advisory_reason": str(_advisory_first(row, ("reason", "block_reason", "live_block_reason", "live_block_reasons", "notes")) or "").strip(),
        "expanded_universe_maturity_state": str(
            _advisory_first(row, ("expanded_universe_maturity_state", "maturity_state", "readiness_state"))
            or ""
        ).strip(),
        "cost_source": str(_advisory_first(row, ("cost_source", "latest_cost_source", "cost_model_source")) or "").strip(),
        "cost_source_quality": str(_advisory_first(row, ("cost_source_quality", "cost_quality")) or "").strip(),
        "cost_quality": str(_advisory_first(row, ("cost_quality", "cost_source_quality")) or "").strip(),
        "cost_bps": _normalize_float(
            _advisory_first(row, ("cost_bps", "selected_total_cost_bps", "roundtrip_all_in_cost_bps"))
        ),
        "selected_total_cost_bps": _normalize_float(row.get("selected_total_cost_bps")),
        "cost_model_version": str(_advisory_first(row, ("cost_model_version", "cost_contract_version")) or "").strip(),
        "max_paper_notional_usdt": _normalize_float(row.get("max_paper_notional_usdt")),
        "max_live_notional_usdt": _normalize_float(row.get("max_live_notional_usdt")),
        "live_block_reasons": str(_advisory_first(row, ("live_block_reasons", "live_block_reason")) or "").strip(),
        "would_block_if_enabled": _normalize_bool(
            _advisory_first(
                row,
                (
                    "would_block_if_enabled",
                    "would_block_if_enforced",
                    "would_block",
                    "would_filter",
                ),
            )
        ),
        "would_enter": _normalize_bool(_advisory_first(row, ("would_enter", "would_enter_if_enabled"))),
        "top_k": _normalize_float(_advisory_first(row, ("top_k", "k", "rank_k"))),
        "selected_symbols": _advisory_first(
            row,
            (
                "selected_symbols",
                "selected_symbol_list",
                "top_symbols",
                "top_n_symbols",
                "symbols",
            ),
        ),
        "would_buy_symbols": _advisory_first(
            row,
            (
                "would_buy_symbols",
                "would_enter_symbols",
                "would_open_symbols",
                "paper_buy_symbols",
            ),
        ),
        "current_regime": str(
            _advisory_first(row, ("current_regime", "regime_state", "market_regime", "risk_regime"))
            or ""
        ).strip(),
        "regime_state": str(
            _advisory_first(row, ("regime_state", "current_regime", "market_regime", "risk_regime"))
            or ""
        ).strip(),
        "no_sample_reason": str(
            _advisory_first(row, ("no_sample_reason", "no_entry_reason", "not_observable_reason"))
            or ""
        ).strip(),
        "as_of_ts": str(_advisory_first(row, ("as_of_ts", "as_of", "asof_ts", "as_of_ts_utc")) or "").strip(),
        "generated_at": str(_advisory_first(row, ("generated_at", "generated_ts", "generated_ts_utc", "generated_at_utc", "ts_utc", "created_at")) or "").strip(),
        "expires_at": str(_advisory_first(row, ("expires_at", "expires_ts", "expires_at_utc")) or "").strip(),
        "contract_version": str(_advisory_first(row, ("contract_version", "telemetry_contract_version")) or "").strip(),
        "quant_lab_git_commit": str(_advisory_first(row, ("quant_lab_git_commit", "git_commit", "source_git_commit")) or "").strip(),
        "source_version": str(_advisory_first(row, ("source_version", "version")) or "").strip(),
    }


def _normalize_advisory_csv_rows(handle: Any, *, source_path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in csv.DictReader(handle):
        if not row:
            continue
        normalized = _normalize_advisory_row(row, source_path=source_path)
        if normalized.get("strategy_id") or normalized.get("strategy_candidate") or normalized.get("experiment_name"):
            rows.append(normalized)
    return rows


def _archive_csv_members(names: Iterable[str], target_filename: str) -> list[str]:
    normalized = [(name, str(name).replace("\\", "/")) for name in names]
    report_target = f"reports/{target_filename}"
    primary = [
        name
        for name, clean_name in normalized
        if clean_name.endswith(report_target)
    ]
    if primary:
        return primary
    return [
        name
        for name, clean_name in normalized
        if clean_name.endswith(target_filename)
    ]


def _read_csv_path(path: Path, *, target_filename: str) -> list[dict[str, Any]]:
    lower_name = path.name.lower()
    if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        rows: list[dict[str, Any]] = []
        try:
            with tarfile.open(path, "r:*") as archive:
                members = {member.name: member for member in archive.getmembers() if member.isfile()}
                for member_name in _archive_csv_members(members, target_filename):
                    member = members.get(member_name)
                    if member is None:
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    with extracted:
                        with io.TextIOWrapper(extracted, encoding="utf-8", newline="") as handle:
                            rows.extend(
                                _normalize_advisory_csv_rows(
                                    handle,
                                    source_path=f"{path}:{member_name}",
                                )
                            )
            return rows
        except Exception:
            return []
    if lower_name.endswith(".zip"):
        rows = []
        try:
            with zipfile.ZipFile(path) as archive:
                members = [name for name in archive.namelist() if not name.endswith("/")]
                for member_name in _archive_csv_members(members, target_filename):
                    with archive.open(member_name) as extracted:
                        with io.TextIOWrapper(extracted, encoding="utf-8", newline="") as handle:
                            rows.extend(
                                _normalize_advisory_csv_rows(
                                    handle,
                                    source_path=f"{path}:{member_name}",
                                )
                            )
            return rows
        except Exception:
            return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return _normalize_advisory_csv_rows(handle, source_path=str(path))
    except Exception:
        return []


def _read_advisory_path(path: Path) -> list[dict[str, Any]]:
    return _read_csv_path(path, target_filename="strategy_opportunity_advisory.csv")


def _raw_csv_rows(handle: Any, *, source_path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in csv.DictReader(handle):
        if not row:
            continue
        payload = dict(row)
        payload["source_path"] = source_path
        rows.append(payload)
    return rows


def _read_raw_csv_path(path: Path, *, target_filename: str) -> list[dict[str, Any]]:
    lower_name = path.name.lower()
    if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        rows: list[dict[str, Any]] = []
        try:
            with tarfile.open(path, "r:*") as archive:
                members = {member.name: member for member in archive.getmembers() if member.isfile()}
                for member_name in _archive_csv_members(members, target_filename):
                    member = members.get(member_name)
                    if member is None:
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    with extracted:
                        with io.TextIOWrapper(extracted, encoding="utf-8", newline="") as handle:
                            rows.extend(_raw_csv_rows(handle, source_path=f"{path}:{member_name}"))
            return rows
        except Exception:
            return []
    if lower_name.endswith(".zip"):
        rows = []
        try:
            with zipfile.ZipFile(path) as archive:
                members = [name for name in archive.namelist() if not name.endswith("/")]
                for member_name in _archive_csv_members(members, target_filename):
                    with archive.open(member_name) as extracted:
                        with io.TextIOWrapper(extracted, encoding="utf-8", newline="") as handle:
                            rows.extend(_raw_csv_rows(handle, source_path=f"{path}:{member_name}"))
            return rows
        except Exception:
            return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return _raw_csv_rows(handle, source_path=str(path))
    except Exception:
        return []


def _extract_advisory_items(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if not isinstance(payload, Mapping):
        return []
    for key in (
        "strategy_opportunity_advisory",
        "advisory",
        "advisories",
        "rows",
        "items",
        "data",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
        if isinstance(value, Mapping):
            nested = _extract_advisory_items(value)
            if nested:
                return nested
    return []


def _read_strategy_opportunity_advisory_api(
    *,
    cfg: AppConfig,
    diagnostics: DiagnosticsConfig,
    run_id: str,
) -> AdvisoryApiReadResult:
    if not bool(getattr(diagnostics, "quant_lab_strategy_opportunity_advisory_api_enabled", True)):
        return AdvisoryApiReadResult(rows=[], meta={"api_fallback_attempted": False, "api_fallback_success": False})
    qcfg = getattr(cfg, "quant_lab", None)
    if qcfg is None or not bool(getattr(qcfg, "enabled", False)):
        return AdvisoryApiReadResult(rows=[], meta={"api_fallback_attempted": False, "api_fallback_success": False})
    endpoints = (
        getattr(diagnostics, "quant_lab_strategy_opportunity_advisory_api_paths", None)
        or _default_advisory_api_paths()
    )
    try:
        from src.quant_lab_client.client import QuantLabClient
    except Exception:
        return AdvisoryApiReadResult(rows=[], meta={"api_fallback_attempted": True, "api_fallback_success": False})
    try:
        client = QuantLabClient.from_config(qcfg, run_id=run_id, phase="strategy_advisory_reader")
    except Exception:
        return AdvisoryApiReadResult(rows=[], meta={"api_fallback_attempted": True, "api_fallback_success": False})
    rows: list[dict[str, Any]] = []
    meta: dict[str, Any] = {
        "api_fallback_attempted": True,
        "api_fallback_success": False,
        "api_lake_generated_at": "",
        "api_cache_hit": False,
    }
    for endpoint in endpoints:
        try:
            response = client.get_json(
                str(endpoint),
                params={"format": "json", "fields": "minimal", "latest_only": "true"},
            )
        except Exception:
            continue
        if not bool(getattr(response, "ok", False)):
            continue
        headers = getattr(response, "headers", {}) or {}
        meta.update(
            {
                "api_fallback_success": True,
                "api_lake_generated_at": _header_lookup(
                    headers,
                    "x-quant-lab-advisory-dataset-generated-at",
                ),
                "api_cache_hit": bool(getattr(response, "cached", False))
                or _truthy(_header_lookup(headers, "x-quant-lab-api-cache-hit"))
                or _truthy(_header_lookup(headers, "x-quant-lab-client-cache-hit")),
                "advisory_row_count_header": _header_lookup(
                    headers,
                    "x-quant-lab-advisory-row-count",
                ),
                "lake_root_hash": _header_lookup(headers, "x-quant-lab-lake-root-hash"),
            }
        )
        for item in _extract_advisory_items(getattr(response, "data", None)):
            normalized = _normalize_advisory_row(item, source_path=f"api:{endpoint}")
            if normalized.get("strategy_id") or normalized.get("strategy_candidate") or normalized.get("experiment_name"):
                rows.append(normalized)
        if rows:
            break
    return AdvisoryApiReadResult(rows=rows, meta=meta)


def _advisory_cache_write_path(
    configured: Iterable[Any],
    *,
    run_path: Path,
    reports_dir: Path,
) -> Optional[Path]:
    for raw_path in configured:
        text = str(raw_path or "").strip()
        if not text:
            continue
        lower = text.lower()
        if not lower.endswith(".csv") or "strategy_opportunity_advisory" not in lower:
            continue
        for path in _candidate_advisory_paths(text, run_path=run_path, reports_dir=reports_dir):
            return path
    return None


def _advisory_source_cache_path(rows: Iterable[Mapping[str, Any]]) -> Optional[Path]:
    for row in rows:
        source_path = str(row.get("source_path") or "").strip()
        lower = source_path.lower()
        if not source_path or source_path.startswith("api:"):
            continue
        if not lower.endswith(".csv"):
            continue
        if ".zip:" in lower or ".tar:" in lower or ".tar.gz:" in lower or ".tgz:" in lower:
            continue
        return Path(source_path)
    return None


def _advisory_source_file_mtime_ms(row: Mapping[str, Any]) -> Optional[int]:
    source_path = str(row.get("source_path") or "").strip()
    if not source_path or source_path.startswith("api:"):
        return None
    path_text = source_path
    for marker in (".tar.gz:", ".tgz:", ".tar:", ".zip:"):
        if marker in path_text.lower():
            idx = path_text.lower().find(marker)
            path_text = path_text[: idx + len(marker) - 1]
            break
    return _path_mtime_ms(Path(path_text))


def _advisory_dedupe_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    identity = str(
        row.get("strategy_id")
        or row.get("strategy_candidate")
        or row.get("experiment_name")
        or ""
    ).strip()
    return (
        identity,
        str(row.get("strategy_candidate") or "").strip(),
        str(row.get("symbol") or "").strip(),
        str(row.get("horizon_hours") or "").strip(),
    )


def _preferred_advisory_row(new: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    new_reference = _advisory_row_reference_ms(new) or -1
    current_reference = _advisory_row_reference_ms(current) or -1
    if new_reference != current_reference:
        return new_reference > current_reference
    new_mtime = _advisory_source_file_mtime_ms(new) or -1
    current_mtime = _advisory_source_file_mtime_ms(current) or -1
    if new_mtime != current_mtime:
        return new_mtime > current_mtime
    return len([value for value in new.values() if value not in (None, "")]) > len(
        [value for value in current.values() if value not in (None, "")]
    )


def _dedupe_advisory_rows_by_identity(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in rows:
        key = _advisory_dedupe_key(row)
        if not any(key):
            passthrough.append(dict(row))
            continue
        current = selected.get(key)
        if current is None or _preferred_advisory_row(row, current):
            selected[key] = dict(row)
    return passthrough + list(selected.values())


def _write_advisory_cache_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
    if not path or not rows:
        return
    fields = sorted({field for row in rows for field in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(path)


def _advisory_row_reference_ms(row: Mapping[str, Any]) -> Optional[int]:
    values = [
        _advisory_time_ms(row.get("as_of_ts")),
        _advisory_time_ms(row.get("generated_at")),
    ]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _latest_advisory_generated_ms(rows: Iterable[Mapping[str, Any]]) -> Optional[int]:
    values = [_advisory_time_ms(row.get("generated_at")) for row in rows]
    values = [value for value in values if value is not None]
    if values:
        return max(values)
    references = [_advisory_row_reference_ms(row) for row in rows]
    references = [value for value in references if value is not None]
    return max(references) if references else None


def _latest_advisory_generated_at(rows: Iterable[Mapping[str, Any]]) -> str:
    value = _latest_advisory_generated_ms(rows)
    return _iso_from_ms(value) if value is not None else ""


def _path_mtime_ms(path: Path) -> Optional[int]:
    try:
        return int(path.stat().st_mtime * 1000.0)
    except OSError:
        return None


def _latest_path_mtime_ms(paths: Iterable[Path]) -> Optional[int]:
    values = [_path_mtime_ms(path) for path in paths]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _is_quant_lab_bundle_path(path: Path) -> bool:
    name = path.name.lower()
    text = str(path).lower()
    return (
        "quant_lab" in text
        and (
            name.endswith((".zip", ".tar", ".tar.gz", ".tgz"))
            or "bundle" in name
            or "latest" in name
        )
    )


def _latest_quant_lab_bundle_seen_ms(paths: Iterable[Path]) -> Optional[int]:
    return _latest_path_mtime_ms(path for path in paths if _is_quant_lab_bundle_path(path))


def _header_lookup(headers: Mapping[str, Any], name: str) -> str:
    target = str(name or "").strip().lower()
    for key, value in dict(headers or {}).items():
        if str(key).strip().lower() == target:
            return str(value or "").strip()
    return ""


def _stale_reason_detail(meta: Mapping[str, Any], *, now_ms: int) -> str:
    reasons = [part for part in str(meta.get("stale_reason") or "").split(";") if part]
    details: list[str] = []
    age_sec = _normalize_float(meta.get("advisory_age_sec"))
    max_age_sec = _normalize_float(meta.get("advisory_max_age_sec") or meta.get("max_age_sec"))
    expires_at = str(meta.get("advisory_expires_at") or "").strip()
    for reason in reasons:
        if reason == "age_exceeds_max" and age_sec is not None and max_age_sec is not None:
            details.append(f"age_sec={age_sec:.3f}>max_age_sec={max_age_sec:.3f}")
        elif reason == "expired" and expires_at:
            details.append(f"expires_at={expires_at}<=now={_iso_from_ms(now_ms)}")
        elif reason == "contract_mismatch":
            details.append("contract_version_mismatch")
        else:
            details.append(reason)
    return ";".join(details)


def _api_lake_generated_ms(api_meta: Mapping[str, Any], api_rows: Iterable[Mapping[str, Any]]) -> Optional[int]:
    header_ms = _advisory_time_ms(api_meta.get("api_lake_generated_at"))
    if header_ms is not None:
        return header_ms
    return _latest_advisory_generated_ms(api_rows)


def _local_newer_than_api(
    *,
    local_meta: Mapping[str, Any],
    local_rows: Iterable[Mapping[str, Any]],
    api_meta: Mapping[str, Any],
    api_rows: Iterable[Mapping[str, Any]],
) -> bool:
    api_ms = _api_lake_generated_ms(api_meta, api_rows)
    if api_ms is None:
        return False
    local_candidates = [
        _latest_advisory_generated_ms(local_rows),
        _advisory_time_ms(local_meta.get("latest_quant_lab_bundle_seen")),
    ]
    local_candidates = [value for value in local_candidates if value is not None]
    return bool(local_candidates and max(local_candidates) > api_ms)


def _selected_source_suggested_fix(
    *,
    selected_meta: Mapping[str, Any],
    local_meta: Mapping[str, Any],
    api_meta: Mapping[str, Any] | None,
    warning: str,
) -> str:
    stale_reason = str(selected_meta.get("stale_reason") or "").strip()
    if "selected_api_older_than_local_bundle" in warning:
        return "refresh_quant_lab_api_lake_or_use_latest_local_bundle"
    if "selected_local_newer_than_api" in warning:
        return "refresh_quant_lab_api_lake_from_latest_bundle"
    if "expired" in stale_reason:
        return "refresh_quant_lab_advisory_or_extend_expires_at"
    if "age_exceeds_max" in stale_reason:
        return "sync_latest_quant_lab_bundle_and_regenerate_advisory"
    if "contract_mismatch" in stale_reason:
        return "align_quant_lab_advisory_contract_version"
    if not selected_meta.get("advisory_source") or selected_meta.get("advisory_source") == "missing":
        return "sync_quant_lab_bundle_or_enable_api"
    if not (api_meta or {}).get("api_fallback_success") and local_meta.get("stale_reason"):
        return "check_quant_lab_api_connectivity_and_refresh_local_bundle"
    return ""


def _assess_advisory_rows(
    rows: list[dict[str, Any]],
    *,
    diagnostics: DiagnosticsConfig,
    now_ms: int,
    source: str,
    api_fallback_attempted: bool = False,
    api_fallback_success: bool = False,
) -> dict[str, Any]:
    max_age_minutes = float(
        getattr(diagnostics, "quant_lab_strategy_opportunity_advisory_max_age_minutes", 90.0)
        or 90.0
    )
    require_contract = bool(
        getattr(diagnostics, "quant_lab_strategy_opportunity_advisory_require_contract_version", True)
    )
    expected_contract = _advisory_expected_contract_version(diagnostics)
    references = [_advisory_row_reference_ms(row) for row in rows]
    references = [value for value in references if value is not None]
    reference_ms = max(references) if references else None
    expires_values = []
    expires_before_generated_at = False
    expiry_corrected = False
    max_age_ms = int(max_age_minutes * 60.0 * 1000.0)
    for row in rows:
        expires_ms = _advisory_time_ms(row.get("expires_at"))
        row_reference_ms = _advisory_row_reference_ms(row)
        if expires_ms is not None and row_reference_ms is not None and expires_ms < row_reference_ms:
            expires_before_generated_at = True
            expires_ms = row_reference_ms + max_age_ms
            expiry_corrected = True
        if expires_ms is None and row_reference_ms is not None:
            expires_ms = row_reference_ms + max_age_ms
            expiry_corrected = True
        if expires_ms is None:
            continue
        expires_values.append(expires_ms)
    min_expires_ms = min(expires_values) if expires_values else None
    age_sec = None
    if reference_ms is not None and now_ms > 0:
        age_sec = max(0.0, (float(now_ms) - float(reference_ms)) / 1000.0)
    has_time_context = reference_ms is not None or min_expires_ms is not None
    age_ok = bool(age_sec is not None and age_sec <= max_age_minutes * 60.0)
    expires_ok = min_expires_ms is None or now_ms <= min_expires_ms
    if min_expires_ms is not None and reference_ms is None:
        age_ok = expires_ok
    contracts = {str(row.get("contract_version") or "").strip() for row in rows}
    contracts.discard("")
    if not require_contract:
        contract_match = True
    elif not expected_contract:
        contract_match = bool(contracts)
    else:
        contract_match = bool(contracts) and contracts == {expected_contract}
    fresh = bool(
        rows
        and has_time_context
        and age_ok
        and expires_ok
        and contract_match
    )
    stale_reasons: list[str] = []
    if not rows:
        stale_reasons.append("no_rows")
    if rows and not has_time_context:
        stale_reasons.append("missing_time_context")
    if rows and age_sec is not None and not age_ok:
        stale_reasons.append("age_exceeds_max")
    if rows and not expires_ok:
        stale_reasons.append("expired")
    if rows and not contract_match:
        stale_reasons.append("contract_mismatch")
    if expiry_corrected:
        freshness_basis = "generated_at_plus_advisory_max_age"
    elif min_expires_ms is not None:
        freshness_basis = "row_expires_at"
    elif reference_ms is not None:
        freshness_basis = "age_only"
    else:
        freshness_basis = "missing_time_context"
    advisory_source = source
    if source == "local" and not fresh:
        advisory_source = "stale_local"
    payload = {
        "advisory_source": advisory_source,
        "advisory_fresh": fresh,
        "advisory_age_sec": round(age_sec, 3) if age_sec is not None else None,
        "advisory_max_age_sec": float(max_age_minutes) * 60.0,
        "advisory_expires_at": _iso_from_ms(min_expires_ms) if min_expires_ms is not None else "",
        "expires_before_generated_at": expires_before_generated_at,
        "expiry_corrected": expiry_corrected,
        "freshness_basis": freshness_basis,
        "advisory_contract_match": contract_match,
        "stale_advisory_used": advisory_source == "stale_local",
        "api_fallback_attempted": bool(api_fallback_attempted),
        "api_fallback_success": bool(api_fallback_success),
        "stale_reason": ";".join(stale_reasons),
        "max_age_sec": float(max_age_minutes) * 60.0,
    }
    payload["stale_reason_detail"] = _stale_reason_detail(payload, now_ms=now_ms)
    return payload


def _annotate_advisory_rows(rows: list[dict[str, Any]], meta: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload.update(meta)
        out.append(payload)
    return out


def _selected_advisory_freshness_meta(source_health_rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    source_health = next(iter(source_health_rows), {})
    if not source_health:
        return {}
    selected_source = str(source_health.get("selected_source") or source_health.get("advisory_source") or "")
    freshness_status = str(source_health.get("freshness_status") or "").strip().lower()
    selected_is_stale = _normalize_bool(source_health.get("selected_source_is_stale"))
    if not freshness_status and selected_is_stale is not None:
        freshness_status = "stale" if selected_is_stale else "fresh"
    advisory_fresh: Optional[bool]
    if freshness_status == "fresh":
        advisory_fresh = True
    elif freshness_status in {"stale", "invalid_timestamp"}:
        advisory_fresh = False
    elif selected_is_stale is not None:
        advisory_fresh = not bool(selected_is_stale)
    else:
        advisory_fresh = None
    stale_reason = str(source_health.get("stale_reason") or source_health.get("freshness_reason") or "").strip()
    if advisory_fresh is True:
        stale_reason = ""
    return {
        "advisory_source": selected_source,
        "selected_source": selected_source,
        "freshness_status": freshness_status,
        "advisory_fresh": advisory_fresh,
        "stale_reason": stale_reason,
    }


def _normalize_selected_advisory_rows(
    rows: Iterable[Mapping[str, Any]],
    source_health_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    meta = _selected_advisory_freshness_meta(source_health_rows)
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        if meta.get("advisory_source"):
            payload["advisory_source"] = meta["advisory_source"]
        if meta.get("selected_source"):
            payload["selected_source"] = meta["selected_source"]
        if meta.get("freshness_status"):
            payload["freshness_status"] = meta["freshness_status"]
        if meta.get("advisory_fresh") is not None:
            payload["advisory_fresh"] = bool(meta["advisory_fresh"])
        if meta.get("stale_reason") is not None:
            payload["stale_reason"] = meta.get("stale_reason") or ""
        out.append(payload)
    return out


def _advisory_source_health_row(
    *,
    run_id: str,
    now_ms: int,
    local_rows: list[dict[str, Any]],
    api_rows: list[dict[str, Any]],
    selected_rows: list[dict[str, Any]],
    selected_meta: Mapping[str, Any],
    local_meta: Mapping[str, Any],
    api_meta: Mapping[str, Any] | None,
) -> dict[str, Any]:
    local_latest_ms = _latest_advisory_generated_ms(local_rows)
    api_latest_ms = _latest_advisory_generated_ms(api_rows)
    selected_latest_ms = _latest_advisory_generated_ms(selected_rows)
    local_fresh = bool(_normalize_bool(local_meta.get("advisory_fresh")))
    api_fresh = bool(_normalize_bool((api_meta or {}).get("advisory_fresh")))
    selection_reason = str(selected_meta.get("selection_reason") or "").strip()
    if not selection_reason:
        if selected_meta.get("advisory_source") == "api":
            selection_reason = "api_selected"
        elif selected_meta.get("advisory_source") == "local":
            selection_reason = "fresh_local"
        elif selected_meta.get("advisory_source") == "stale_local":
            selection_reason = "stale_local_fallback"
        else:
            selection_reason = "missing"
    stale_local_overrode_api = bool(
        selected_meta.get("advisory_source") == "stale_local"
        and api_fresh
    )
    lag_sec = None
    if local_latest_ms is not None and selected_latest_ms is not None:
        lag_sec = max(0.0, (float(local_latest_ms) - float(selected_latest_ms)) / 1000.0)
    warning = ""
    if lag_sec is not None and lag_sec > 0:
        warning = "selected_advisory_older_than_local_bundle"
    local_latest_file_mtime = str(local_meta.get("local_latest_file_mtime") or "")
    latest_quant_lab_bundle_seen = str(local_meta.get("latest_quant_lab_bundle_seen") or "")
    api_lake_generated_at = str((api_meta or {}).get("api_lake_generated_at") or "")
    api_lake_ms = _advisory_time_ms(api_lake_generated_at)
    local_compare_values = [
        _advisory_time_ms(latest_quant_lab_bundle_seen),
        local_latest_ms,
    ]
    local_compare_values = [value for value in local_compare_values if value is not None]
    selected_source = str(selected_meta.get("advisory_source") or "missing")
    if (
        api_lake_ms is not None
        and local_compare_values
        and max(local_compare_values) > api_lake_ms
    ):
        if selected_source == "api":
            warning = "selected_api_older_than_local_bundle"
        elif selected_source in {"local", "stale_local"}:
            warning = "selected_local_newer_than_api"
    max_age_sec = float(selected_meta.get("max_age_sec") or local_meta.get("max_age_sec") or 0.0)
    age_sec = _normalize_float(selected_meta.get("advisory_age_sec"))
    contract_match = _normalize_bool(selected_meta.get("advisory_contract_match"))
    freshness_inconsistency_warning = ""
    if (
        selected_rows
        and not bool(_normalize_bool(selected_meta.get("advisory_fresh")))
        and age_sec is not None
        and max_age_sec > 0
        and age_sec < max_age_sec
        and contract_match is True
            and not str(selected_meta.get("stale_reason") or "").strip()
    ):
        freshness_inconsistency_warning = "freshness_inconsistency_warning"
    selected_is_stale = not bool(_normalize_bool(selected_meta.get("advisory_fresh")))
    freshness_status = "stale" if selected_is_stale else "fresh"
    if bool(selected_meta.get("expires_before_generated_at")):
        freshness_status = "invalid_timestamp"
    suggested_fix = _selected_source_suggested_fix(
        selected_meta=selected_meta,
        local_meta=local_meta,
        api_meta=api_meta,
        warning=warning,
    )
    return {
        "run_id": run_id,
        "ts_utc": _iso_from_ms(now_ms),
        "local_row_count": len(local_rows),
        "api_row_count": len(api_rows),
        "selected_row_count": len(selected_rows),
        "local_fresh": local_fresh,
        "api_fresh": api_fresh,
        "selection_reason": selection_reason,
        "stale_local_overrode_api": stale_local_overrode_api,
        "latest_local_generated_at": _iso_from_ms(local_latest_ms) if local_latest_ms is not None else "",
        "latest_api_generated_at": _iso_from_ms(api_latest_ms) if api_latest_ms is not None else "",
        "local_latest_file_mtime": local_latest_file_mtime,
        "latest_quant_lab_bundle_seen": latest_quant_lab_bundle_seen,
        "api_lake_generated_at": api_lake_generated_at,
        "api_cache_hit": bool((api_meta or {}).get("api_cache_hit")),
        "selected_latest_generated_at": _iso_from_ms(selected_latest_ms) if selected_latest_ms is not None else "",
        "advisory_age_sec": selected_meta.get("advisory_age_sec") if selected_meta.get("advisory_age_sec") is not None else "",
        "advisory_max_age_sec": selected_meta.get("advisory_max_age_sec") or selected_meta.get("max_age_sec") or "",
        "advisory_expires_at": selected_meta.get("advisory_expires_at") or "",
        "expires_before_generated_at": bool(selected_meta.get("expires_before_generated_at")),
        "expiry_corrected": bool(selected_meta.get("expiry_corrected")),
        "freshness_basis": selected_meta.get("freshness_basis") or "",
        "freshness_status": freshness_status,
        "freshness_reason": selected_meta.get("stale_reason") or ("fresh" if freshness_status == "fresh" else freshness_status),
        "advisory_source_lag_sec": round(lag_sec, 3) if lag_sec is not None else "",
        "selected_source": selected_source,
        "selected_source_is_stale": selected_is_stale,
        "api_fallback_attempted": bool(
            selected_meta.get("api_fallback_attempted")
            or (api_meta or {}).get("api_fallback_attempted")
        ),
        "api_fallback_success": bool(
            selected_meta.get("api_fallback_success")
            or (api_meta or {}).get("api_fallback_success")
        ),
        "stale_reason": selected_meta.get("stale_reason") or "",
        "stale_reason_detail": selected_meta.get("stale_reason_detail") or "",
        "suggested_fix": suggested_fix,
        "warning": warning,
        "freshness_inconsistency_warning": freshness_inconsistency_warning,
    }


def _read_strategy_opportunity_advisory_result(
    *,
    run_path: Path,
    reports_dir: Path,
    diagnostics: DiagnosticsConfig,
    cfg: AppConfig,
    run_id: str,
    now_ms: int,
) -> AdvisoryReadResult:
    if not bool(getattr(diagnostics, "quant_lab_strategy_opportunity_advisory_enabled", True)):
        return AdvisoryReadResult(rows=[], source_health_rows=[])
    configured = (
        getattr(diagnostics, "quant_lab_strategy_opportunity_advisory_paths", None)
        or _default_advisory_paths()
    )
    rows: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    local_source_paths: list[Path] = []
    for raw_path in configured:
        for path in _candidate_advisory_paths(str(raw_path), run_path=run_path, reports_dir=reports_dir):
            if path in seen_paths:
                continue
            seen_paths.add(path)
            if not path.is_file():
                continue
            local_source_paths.append(path)
            rows.extend(_read_advisory_path(path))
    rows = _dedupe_advisory_rows_by_identity(rows)
    local_meta = _assess_advisory_rows(
        rows,
        diagnostics=diagnostics,
        now_ms=now_ms,
        source="local",
    )
    local_mtime_ms = _latest_path_mtime_ms(local_source_paths)
    local_bundle_seen_ms = _latest_quant_lab_bundle_seen_ms(local_source_paths)
    local_meta["local_latest_file_mtime"] = (
        _iso_from_ms(local_mtime_ms) if local_mtime_ms is not None else ""
    )
    local_meta["latest_quant_lab_bundle_seen"] = (
        _iso_from_ms(local_bundle_seen_ms) if local_bundle_seen_ms is not None else ""
    )
    if rows and bool(local_meta.get("advisory_fresh")):
        local_meta["selection_reason"] = "fresh_local"
        selected_rows = _annotate_advisory_rows(rows, local_meta)
        return AdvisoryReadResult(
            rows=selected_rows,
            source_health_rows=[
                _advisory_source_health_row(
                    run_id=run_id,
                    now_ms=now_ms,
                    local_rows=rows,
                    api_rows=[],
                    selected_rows=selected_rows,
                    selected_meta=local_meta,
                    local_meta=local_meta,
                    api_meta=None,
                )
            ],
        )

    api_result = _read_strategy_opportunity_advisory_api(cfg=cfg, diagnostics=diagnostics, run_id=run_id)
    api_rows = api_result.rows
    api_meta: dict[str, Any] | None = None
    if api_rows:
        api_rows = _dedupe_advisory_rows_by_identity(api_rows)
        api_meta = _assess_advisory_rows(
            api_rows,
            diagnostics=diagnostics,
            now_ms=now_ms,
            source="api",
            api_fallback_attempted=True,
            api_fallback_success=True,
        )
        api_meta.update(api_result.meta)
        if not api_meta.get("api_lake_generated_at"):
            latest_api_ms = _latest_advisory_generated_ms(api_rows)
            api_meta["api_lake_generated_at"] = (
                _iso_from_ms(latest_api_ms) if latest_api_ms is not None else ""
            )
        api_is_fresh = bool(_normalize_bool(api_meta.get("advisory_fresh")))
        local_is_fresh = bool(_normalize_bool(local_meta.get("advisory_fresh")))
        if rows and local_is_fresh and not api_is_fresh:
            selected_meta = dict(local_meta)
            selected_meta["api_fallback_attempted"] = True
            selected_meta["api_fallback_success"] = True
            selected_meta["selection_reason"] = "fresh_local_over_stale_api"
            selected_rows = _annotate_advisory_rows(rows, selected_meta)
            return AdvisoryReadResult(
                rows=selected_rows,
                source_health_rows=[
                    _advisory_source_health_row(
                        run_id=run_id,
                        now_ms=now_ms,
                        local_rows=rows,
                        api_rows=api_rows,
                        selected_rows=selected_rows,
                        selected_meta=selected_meta,
                        local_meta=local_meta,
                        api_meta=api_meta,
                    )
                ],
            )
        if rows and local_is_fresh and api_is_fresh and _local_newer_than_api(
            local_meta=local_meta,
            local_rows=rows,
            api_meta=api_meta,
            api_rows=api_rows,
        ):
            selected_meta = dict(local_meta)
            selected_meta["api_fallback_attempted"] = True
            selected_meta["api_fallback_success"] = True
            selected_meta["selection_reason"] = "fresh_local_newer_than_fresh_api"
            selected_rows = _annotate_advisory_rows(rows, selected_meta)
            return AdvisoryReadResult(
                rows=selected_rows,
                source_health_rows=[
                    _advisory_source_health_row(
                        run_id=run_id,
                        now_ms=now_ms,
                        local_rows=rows,
                        api_rows=api_rows,
                        selected_rows=selected_rows,
                        selected_meta=selected_meta,
                        local_meta=local_meta,
                        api_meta=api_meta,
                    )
                ],
            )
        if rows and not local_is_fresh and not api_is_fresh and _local_newer_than_api(
            local_meta=local_meta,
            local_rows=rows,
            api_meta=api_meta,
            api_rows=api_rows,
        ):
            selected_meta = dict(local_meta)
            selected_meta["api_fallback_attempted"] = True
            selected_meta["api_fallback_success"] = True
            selected_meta["advisory_source"] = "stale_local"
            selected_meta["stale_advisory_used"] = True
            selected_meta["selection_reason"] = "both_stale_local_newer_than_api"
            selected_rows = _annotate_advisory_rows(rows, selected_meta)
            return AdvisoryReadResult(
                rows=selected_rows,
                source_health_rows=[
                    _advisory_source_health_row(
                        run_id=run_id,
                        now_ms=now_ms,
                        local_rows=rows,
                        api_rows=api_rows,
                        selected_rows=selected_rows,
                        selected_meta=selected_meta,
                        local_meta=local_meta,
                        api_meta=api_meta,
                    )
                ],
            )
        if rows and not local_is_fresh and api_is_fresh:
            api_meta["selection_reason"] = "fresh_api_over_stale_local"
        elif rows and local_is_fresh and api_is_fresh:
            api_meta["selection_reason"] = "fresh_api_newer_or_equal"
        else:
            api_meta["selection_reason"] = "api_fallback_success"
        selected_rows = _annotate_advisory_rows(api_rows, api_meta)
        cache_path = _advisory_source_cache_path(rows) or _advisory_cache_write_path(
            configured,
            run_path=run_path,
            reports_dir=reports_dir,
        )
        if cache_path is not None:
            try:
                _write_advisory_cache_atomic(cache_path, api_rows)
            except Exception:
                pass
        return AdvisoryReadResult(
            rows=selected_rows,
            source_health_rows=[
                _advisory_source_health_row(
                    run_id=run_id,
                    now_ms=now_ms,
                    local_rows=rows,
                    api_rows=api_rows,
                    selected_rows=selected_rows,
                    selected_meta=api_meta,
                    local_meta=local_meta,
                    api_meta=api_meta,
                )
            ],
        )

    if rows:
        stale_meta = dict(local_meta)
        stale_meta["api_fallback_attempted"] = True
        stale_meta["api_fallback_success"] = False
        stale_meta["advisory_source"] = "stale_local"
        stale_meta["stale_advisory_used"] = True
        stale_meta["selection_reason"] = "api_unavailable_stale_local_fallback"
        selected_rows = _annotate_advisory_rows(rows, stale_meta)
        return AdvisoryReadResult(
            rows=selected_rows,
            source_health_rows=[
                _advisory_source_health_row(
                    run_id=run_id,
                    now_ms=now_ms,
                    local_rows=rows,
                    api_rows=[],
                    selected_rows=selected_rows,
                    selected_meta=stale_meta,
                    local_meta=local_meta,
                    api_meta=None,
                )
            ],
        )
    missing_meta = _assess_advisory_rows(
        [],
        diagnostics=diagnostics,
        now_ms=now_ms,
        source="missing",
        api_fallback_attempted=True,
        api_fallback_success=False,
    )
    missing_meta["selection_reason"] = "missing_local_and_api"
    return AdvisoryReadResult(
        rows=[],
        source_health_rows=[
            _advisory_source_health_row(
                run_id=run_id,
                now_ms=now_ms,
                local_rows=[],
                api_rows=[],
                selected_rows=[],
                selected_meta=missing_meta,
                local_meta=local_meta,
                api_meta=None,
            )
        ],
    )


def _read_strategy_opportunity_advisory(
    *,
    run_path: Path,
    reports_dir: Path,
    diagnostics: DiagnosticsConfig,
    cfg: AppConfig,
    run_id: str,
    now_ms: int,
) -> list[dict[str, Any]]:
    return _read_strategy_opportunity_advisory_result(
        run_path=run_path,
        reports_dir=reports_dir,
        diagnostics=diagnostics,
        cfg=cfg,
        run_id=run_id,
        now_ms=now_ms,
    ).rows


def _dedupe_rows(rows: Iterable[Mapping[str, Any]], keys: Iterable[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    key_list = list(keys)
    for row in rows:
        fingerprint = tuple(str(row.get(key) or "") for key in key_list)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        out.append(dict(row))
    return out


def _read_paper_strategy_proposals(
    *,
    run_path: Path,
    reports_dir: Path,
    diagnostics: DiagnosticsConfig,
) -> list[dict[str, Any]]:
    if not bool(getattr(diagnostics, "quant_lab_paper_strategy_proposals_enabled", True)):
        return []
    configured = (
        getattr(diagnostics, "quant_lab_paper_strategy_proposals_paths", None)
        or _default_proposal_paths()
    )
    rows: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    for raw_path in configured:
        for path in _candidate_advisory_paths(str(raw_path), run_path=run_path, reports_dir=reports_dir):
            if path in seen_paths:
                continue
            seen_paths.add(path)
            if not path.is_file():
                continue
            rows.extend(_read_raw_csv_path(path, target_filename="paper_strategy_proposals.csv"))
    return _dedupe_rows(
        rows,
        [
            "proposal_id",
            "strategy_id",
            "strategy_candidate",
            "symbol",
            "recommended_mode",
            "suggested_horizon",
            "entry_conditions",
        ],
    )


def _proposal_horizon_hours(row: Mapping[str, Any]) -> Optional[int]:
    direct = _parse_horizon_hours(row.get("horizon_hours")) or _parse_horizon_hours(row.get("suggested_horizon"))
    if direct:
        return direct
    raw_conditions = str(row.get("entry_conditions") or "").strip()
    if not raw_conditions:
        return None
    try:
        conditions = json.loads(raw_conditions)
    except Exception:
        return None
    if isinstance(conditions, Mapping):
        return _parse_horizon_hours(conditions.get("horizon_hours"))
    return None


def _proposal_strategy_candidate(row: Mapping[str, Any]) -> str:
    candidate = str(row.get("strategy_candidate") or "").strip()
    if candidate:
        return candidate
    raw_conditions = str(row.get("entry_conditions") or "").strip()
    if not raw_conditions:
        return ""
    try:
        conditions = json.loads(raw_conditions)
    except Exception:
        return ""
    if isinstance(conditions, Mapping):
        return str(conditions.get("strategy_candidate") or "").strip()
    return ""


def _proposal_symbol(row: Mapping[str, Any]) -> str:
    symbol = _symbol_text(row.get("symbol") or row.get("v5_symbol"))
    if symbol:
        return symbol
    raw_conditions = str(row.get("entry_conditions") or "").strip()
    if not raw_conditions:
        return ""
    try:
        conditions = json.loads(raw_conditions)
    except Exception:
        return ""
    if isinstance(conditions, Mapping):
        return _symbol_text(conditions.get("symbol") or conditions.get("v5_symbol"))
    return ""


def _eth_f3_proposal_to_spec(row: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    proposal_id = str(row.get("proposal_id") or row.get("strategy_id") or "").strip()
    candidate = _proposal_strategy_candidate(row)
    symbol = _proposal_symbol(row)
    mode = str(row.get("recommended_mode") or "").strip().lower().replace("-", "_")
    candidate_key = _strategy_key(candidate)
    if proposal_id != ETH_F3_DOMINANT_STRATEGY_ID and not (
        symbol == ETH_SYMBOL and candidate_key in {"f3_dominant_entry", "v5.f3_dominant_entry"}
    ):
        return None
    if mode and mode != "paper":
        return None
    return {
        "strategy_id": ETH_F3_DOMINANT_STRATEGY_ID,
        "experiment_name": "v5.eth_f3_dominant_entry",
        "source_strategy_candidates": {"f3_dominant_entry", "v5.f3_dominant_entry"},
        "allowed_block_reasons": set(),
        "symbol": ETH_SYMBOL,
        "primary_horizon_hours": _proposal_horizon_hours(row) or 48,
        "require_protect_level": False,
        "require_alpha6_buy": True,
        "require_no_cooldown": False,
        "min_f4_volume_expansion": None,
        "extra_live_block_reasons": list(ETH_F3_DOMINANT_LIVE_BLOCK_REASONS),
        "proposal_present": True,
        "proposal_source": str(row.get("source_path") or ""),
        "ignore_strategy_opportunity_advisory": True,
    }


def _proposal_matches_spec(row: Mapping[str, Any], spec: Mapping[str, Any]) -> bool:
    proposal_id = str(row.get("proposal_id") or row.get("strategy_id") or "").strip()
    if proposal_id and proposal_id == str(spec.get("strategy_id") or ""):
        return True
    candidate = _strategy_key(_proposal_strategy_candidate(row))
    if candidate and candidate in {_strategy_key(value) for value in (spec.get("source_strategy_candidates") or set())}:
        symbol = _proposal_symbol(row)
        return not symbol or symbol == _strategy_symbol(spec)
    return False


def _merge_proposal_into_spec(spec: dict[str, Any], row: Mapping[str, Any]) -> None:
    proposal_id = str(row.get("proposal_id") or row.get("strategy_id") or "").strip()
    if proposal_id:
        spec["proposal_id"] = proposal_id
    horizon = _proposal_horizon_hours(row)
    if horizon:
        spec["primary_horizon_hours"] = horizon
        spec["suggested_horizon"] = f"{horizon}h"
    candidate = _proposal_strategy_candidate(row)
    if candidate:
        spec["proposal_strategy_candidate"] = candidate
    spec["proposal_present"] = True
    spec["proposal_source"] = str(row.get("source_path") or "")


def _strategy_configs_with_proposals(
    diagnostics: DiagnosticsConfig,
    proposal_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    specs = _strategy_configs(diagnostics)
    by_id = {str(spec.get("strategy_id") or ""): spec for spec in specs}
    for row in proposal_rows:
        spec = _eth_f3_proposal_to_spec(row)
        if spec:
            existing = by_id.get(ETH_F3_DOMINANT_STRATEGY_ID)
            if existing is None:
                specs.append(spec)
                by_id[ETH_F3_DOMINANT_STRATEGY_ID] = spec
            else:
                existing.update(spec)
            continue
        for existing in specs:
            if _proposal_matches_spec(row, existing):
                _merge_proposal_into_spec(existing, row)
                break
    return specs


def _advisory_keys(row: Mapping[str, Any]) -> set[str]:
    return {
        key
        for key in (
            _strategy_key(row.get("strategy_id")),
            _strategy_key(row.get("strategy_candidate")),
            _strategy_key(row.get("experiment_name")),
        )
        if key
    }


def _advisory_by_strategy(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    out: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        for key in _advisory_keys(row):
            out[key].append(row)
    return dict(out)


def _advisory_decision(row: Mapping[str, Any]) -> str:
    return str(row.get("decision") or "").strip().upper()


def _advisory_mode(row: Mapping[str, Any]) -> str:
    return str(row.get("recommended_mode") or "").strip().lower().replace("-", "_")


def _advisory_is_positive(row: Mapping[str, Any]) -> bool:
    return _advisory_decision(row) == "PAPER_READY" or _advisory_mode(row) in ADVISORY_ALLOWED_RECOMMENDED_MODES


def _advisory_rank(row: Mapping[str, Any]) -> tuple[int, int, float]:
    decision = _advisory_decision(row)
    if decision == "PAPER_READY":
        decision_rank = 3
    elif _advisory_mode(row) in ADVISORY_ALLOWED_RECOMMENDED_MODES:
        decision_rank = 2
    elif decision == "KILL":
        decision_rank = 0
    else:
        decision_rank = 1
    sample_count = _normalize_float(row.get("complete_sample_count")) or _normalize_float(row.get("sample_count")) or 0.0
    return (decision_rank, 1 if _advisory_is_positive(row) else 0, float(sample_count))


def _with_advisory_match(row: Mapping[str, Any], *, key: str, reason: str) -> Mapping[str, Any]:
    payload = dict(row)
    payload["_match_key"] = key
    payload["_match_reason"] = reason
    return payload


def _advisory_for_spec(
    spec: Mapping[str, Any],
    advisory_by_strategy: Mapping[str, list[Mapping[str, Any]]] | None,
) -> Mapping[str, Any]:
    if _spec_bool(spec, "ignore_strategy_opportunity_advisory", False):
        return {}
    if not advisory_by_strategy:
        return {}
    proposal_keys = [
        key
        for key in (
            _strategy_key(spec.get("proposal_id")),
            _strategy_key(spec.get("strategy_id")),
        )
        if key
    ]
    strategy_symbol = _strategy_symbol(spec)
    proposal_horizon = _primary_horizon_for_spec(spec, DEFAULT_HORIZONS)
    for key in proposal_keys:
        exact_rows = [
            row
            for row in advisory_by_strategy.get(key, [])
            if not row.get("symbol") or _symbol_text(row.get("symbol")) == strategy_symbol
        ]
        if exact_rows:
            same_horizon = [
                row
                for row in exact_rows
                if _parse_horizon_hours(row.get("horizon_hours")) == proposal_horizon
            ]
            rows = same_horizon or exact_rows
            return _with_advisory_match(
                sorted(rows, key=_advisory_rank, reverse=True)[0],
                key=key,
                reason="proposal_id_or_strategy_id_exact",
            )

    candidate_keys = [
        _strategy_key(spec.get("proposal_strategy_candidate")),
        _strategy_key(spec.get("experiment_name")),
        *[_strategy_key(value) for value in (spec.get("source_strategy_candidates") or set())],
    ]
    has_proposal = bool(spec.get("proposal_present") or spec.get("proposal_id"))
    for key in [item for item in candidate_keys if item]:
        candidate_rows = [
            row
            for row in advisory_by_strategy.get(key, [])
            if not row.get("symbol") or _symbol_text(row.get("symbol")) == strategy_symbol
        ]
        if not candidate_rows:
            continue
        if has_proposal:
            same_horizon = [
                row
                for row in candidate_rows
                if _parse_horizon_hours(row.get("horizon_hours")) == proposal_horizon
            ]
            if same_horizon:
                return _with_advisory_match(
                    sorted(same_horizon, key=_advisory_rank, reverse=True)[0],
                    key=f"{key}:{proposal_horizon}h",
                    reason="proposal_candidate_same_horizon",
                )
            positive_rows = [row for row in candidate_rows if _advisory_is_positive(row)]
            if positive_rows:
                return _with_advisory_match(
                    sorted(positive_rows, key=_advisory_rank, reverse=True)[0],
                    key=key,
                    reason="proposal_candidate_positive_fallback",
                )
            return {}
        return _with_advisory_match(
            sorted(candidate_rows, key=_advisory_rank, reverse=True)[0],
            key=key,
            reason="strategy_candidate_legacy",
        )
    return {}


def _advisory_response_fields(
    advisory: Mapping[str, Any] | None,
    diagnostics: DiagnosticsConfig,
) -> dict[str, Any]:
    advisory = dict(advisory or {})
    enable_live_small = bool(getattr(diagnostics, "enable_live_small_from_quant_lab", False))
    decision = str(advisory.get("decision") or "").strip().upper()
    recommended_mode = str(advisory.get("recommended_mode") or "").strip().lower().replace("-", "_")
    advisory_fresh = _normalize_bool(advisory.get("advisory_fresh"))
    if advisory_fresh is None:
        advisory_fresh = True if advisory else False
    raw_max_notional = _normalize_float(advisory.get("max_live_notional_usdt"))
    max_notional = 0.0 if advisory and not advisory_fresh else raw_max_notional
    present = bool(advisory)
    negative = decision == "KILL"
    live_small = decision == "LIVE_SMALL_READY"
    max_ignored = bool(max_notional is not None and not (live_small and enable_live_small and advisory_fresh))
    if not present:
        response_action = "no_advisory"
    elif negative:
        response_action = "negative_advisory"
    elif recommended_mode == "paper" and not advisory_fresh:
        response_action = "stale_paper_display_only"
    elif recommended_mode == "shadow" and not advisory_fresh:
        response_action = "stale_shadow_display_only"
    elif recommended_mode == "paper":
        response_action = "paper_tracking"
    elif recommended_mode == "shadow":
        response_action = "shadow_tracking"
    elif recommended_mode in ADVISORY_DISPLAY_ONLY_RECOMMENDED_MODES:
        response_action = "research_display_only"
    elif live_small and not advisory_fresh:
        response_action = "stale_advisory_live_disabled"
    elif live_small and not enable_live_small:
        response_action = "ignored_live_small_disabled"
    else:
        response_action = "ignored_recommended_mode_not_paper_or_shadow"
    return {
        "advisory_present": present,
        "advisory_source": str(advisory.get("advisory_source") or ("missing" if not present else "")),
        "advisory_source_path": str(advisory.get("source_path") or ""),
        "advisory_fresh": bool(advisory_fresh),
        "advisory_age_sec": _normalize_float(advisory.get("advisory_age_sec")),
        "advisory_contract_match": bool(_normalize_bool(advisory.get("advisory_contract_match"))),
        "stale_advisory_used": bool(_normalize_bool(advisory.get("stale_advisory_used"))),
        "api_fallback_attempted": bool(_normalize_bool(advisory.get("api_fallback_attempted"))),
        "api_fallback_success": bool(_normalize_bool(advisory.get("api_fallback_success"))),
        "advisory_strategy_id": str(advisory.get("strategy_id") or ""),
        "advisory_strategy_candidate": str(advisory.get("strategy_candidate") or ""),
        "advisory_decision": decision,
        "advisory_recommended_mode": recommended_mode,
        "advisory_negative": negative,
        "advisory_response_action": response_action,
        "advisory_match_key": str(advisory.get("_match_key") or ""),
        "advisory_match_reason": str(advisory.get("_match_reason") or ""),
        "advisory_max_paper_notional_usdt": _normalize_float(advisory.get("max_paper_notional_usdt")),
        "advisory_max_live_notional_usdt": max_notional,
        "advisory_max_live_notional_usdt_ignored": max_ignored,
        "advisory_live_block_reasons": str(advisory.get("live_block_reasons") or advisory.get("advisory_reason") or ""),
        "enable_live_small_from_quant_lab": enable_live_small,
    }


def _advisory_summary_rows(
    advisory_rows: Iterable[Mapping[str, Any]],
    diagnostics: DiagnosticsConfig,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    materialized = list(advisory_rows)
    if not materialized:
        materialized = [{"advisory_source": "missing", "advisory_fresh": False, "advisory_contract_match": False}]
    for row in materialized:
        fields = _advisory_response_fields(row, diagnostics)
        out.append(
            {
                "source_path": row.get("source_path"),
                "advisory_source": fields["advisory_source"],
                "advisory_fresh": fields["advisory_fresh"],
                "freshness_status": row.get("freshness_status"),
                "stale_reason": row.get("stale_reason"),
                "advisory_age_sec": fields["advisory_age_sec"],
                "advisory_contract_match": fields["advisory_contract_match"],
                "stale_advisory_used": fields["stale_advisory_used"],
                "api_fallback_attempted": fields["api_fallback_attempted"],
                "api_fallback_success": fields["api_fallback_success"],
                "as_of_ts": row.get("as_of_ts"),
                "generated_at": row.get("generated_at"),
                "expires_at": row.get("expires_at"),
                "contract_version": row.get("contract_version"),
                "quant_lab_git_commit": row.get("quant_lab_git_commit"),
                "source_version": row.get("source_version"),
                "strategy_id": row.get("strategy_id"),
                "strategy_candidate": row.get("strategy_candidate"),
                "experiment_name": row.get("experiment_name"),
                "symbol": row.get("symbol"),
                "decision": row.get("decision"),
                "recommended_mode": row.get("recommended_mode"),
                "universe_type": row.get("universe_type"),
                "horizon_hours": row.get("horizon_hours"),
                "sample_count": row.get("sample_count"),
                "complete_sample_count": row.get("complete_sample_count"),
                "expanded_universe_maturity_state": row.get("expanded_universe_maturity_state"),
                "cost_source": row.get("cost_source"),
                "cost_source_quality": row.get("cost_source_quality"),
                "cost_bps": row.get("cost_bps"),
                "cost_model_version": row.get("cost_model_version"),
                "advisory_status": row.get("advisory_status"),
                "advisory_reason": row.get("advisory_reason"),
                "max_paper_notional_usdt": row.get("max_paper_notional_usdt"),
                "max_live_notional_usdt": fields["advisory_max_live_notional_usdt"],
                "live_block_reasons": row.get("live_block_reasons"),
                "would_block_if_enabled": row.get("would_block_if_enabled"),
                "would_enter": row.get("would_enter"),
                "no_sample_reason": row.get("no_sample_reason"),
                "enable_live_small_from_quant_lab": fields["enable_live_small_from_quant_lab"],
                "response_action": fields["advisory_response_action"],
                "negative_advisory": fields["advisory_negative"],
                "max_live_notional_usdt_ignored": fields["advisory_max_live_notional_usdt_ignored"],
            }
        )
    return out


def _live_symbol_set(cfg: AppConfig) -> set[str]:
    return {_symbol_text(symbol) for symbol in (getattr(cfg, "symbols", None) or []) if _symbol_text(symbol)}


def _expanded_universe_type(row: Mapping[str, Any]) -> str:
    return str(row.get("universe_type") or "").strip().lower().replace("-", "_")


def _expanded_would_enter(row: Mapping[str, Any]) -> bool:
    explicit = _normalize_bool(row.get("would_enter"))
    if explicit is not None:
        return bool(explicit)
    decision = str(row.get("decision") or "").strip().upper()
    mode = str(row.get("recommended_mode") or "").strip().lower().replace("-", "_")
    return bool(mode == "paper" and decision != "KILL")


def _expanded_no_sample_reason(row: Mapping[str, Any], fields: Mapping[str, Any]) -> str:
    for value in (
        row.get("no_sample_reason"),
        row.get("advisory_reason"),
        row.get("live_block_reasons"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    action = str(fields.get("advisory_response_action") or "").strip()
    if action == "negative_advisory":
        return "negative_advisory"
    if action == "paper_tracking":
        return "expanded_paper_tracking"
    if action == "shadow_tracking":
        return "expanded_shadow_tracking"
    return "expanded_universe_display_only"


def _expanded_universe_advisory_rows(
    advisory_rows: Iterable[Mapping[str, Any]],
    *,
    diagnostics: DiagnosticsConfig,
    cfg: AppConfig,
    run_id: str,
    asof_ts_ms: int,
) -> list[dict[str, Any]]:
    live_symbols = _live_symbol_set(cfg)
    ts_utc = _iso_from_ms(asof_ts_ms)
    out: list[dict[str, Any]] = []
    for row in advisory_rows:
        if _expanded_universe_type(row) != "expanded_paper":
            continue
        fields = _advisory_response_fields(row, diagnostics)
        symbol = _symbol_text(row.get("symbol"))
        response_action = str(fields.get("advisory_response_action") or "")
        paper_allowed = response_action == "paper_tracking"
        shadow_allowed = response_action == "shadow_tracking"
        out.append(
            {
                "run_id": run_id,
                "ts_utc": ts_utc,
                "source_path": row.get("source_path"),
                "advisory_source": fields["advisory_source"],
                "advisory_fresh": fields["advisory_fresh"],
                "advisory_age_sec": fields["advisory_age_sec"],
                "advisory_contract_match": fields["advisory_contract_match"],
                "stale_advisory_used": fields["stale_advisory_used"],
                "api_fallback_attempted": fields["api_fallback_attempted"],
                "api_fallback_success": fields["api_fallback_success"],
                "as_of_ts": row.get("as_of_ts"),
                "generated_at": row.get("generated_at"),
                "expires_at": row.get("expires_at"),
                "contract_version": row.get("contract_version"),
                "quant_lab_git_commit": row.get("quant_lab_git_commit"),
                "source_version": row.get("source_version"),
                "universe_type": "expanded_paper",
                "symbol": symbol,
                "symbol_in_live_universe": symbol in live_symbols,
                "live_symbols_unchanged": True,
                "strategy_id": row.get("strategy_id"),
                "strategy_candidate": row.get("strategy_candidate"),
                "experiment_name": row.get("experiment_name"),
                "decision": row.get("decision"),
                "recommended_mode": row.get("recommended_mode"),
                "horizon_hours": row.get("horizon_hours"),
                "sample_count": row.get("sample_count"),
                "complete_sample_count": row.get("complete_sample_count"),
                "expanded_universe_maturity_state": row.get("expanded_universe_maturity_state"),
                "cost_source": row.get("cost_source"),
                "cost_source_quality": row.get("cost_source_quality"),
                "cost_bps": row.get("cost_bps"),
                "cost_model_version": row.get("cost_model_version"),
                "response_action": response_action,
                "negative_advisory": fields["advisory_negative"],
                "paper_tracking_allowed": paper_allowed,
                "shadow_tracking_allowed": shadow_allowed,
                "max_paper_notional_usdt": row.get("max_paper_notional_usdt"),
                "max_live_notional_usdt": 0.0,
                "max_live_notional_usdt_ignored": True,
                "live_block_reasons": row.get("live_block_reasons"),
                "would_block_if_enabled": row.get("would_block_if_enabled"),
                "would_enter": row.get("would_enter"),
                "no_sample_reason": _expanded_no_sample_reason(row, fields),
                "advisory_reason": row.get("advisory_reason"),
                "live_order_effect": "read_only_no_live_order",
            }
        )
    return out


def _expanded_universe_paper_rows(expanded_rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in expanded_rows:
        response_action = str(row.get("response_action") or "")
        if response_action not in {"paper_tracking", "shadow_tracking", "negative_advisory"}:
            continue
        tracking_mode = "negative" if response_action == "negative_advisory" else str(row.get("recommended_mode") or "")
        would_enter = _expanded_would_enter(row)
        max_paper = _normalize_float(row.get("max_paper_notional_usdt"))
        out.append(
            {
                "run_id": row.get("run_id"),
                "ts_utc": row.get("ts_utc"),
                "paper_date": str(row.get("ts_utc") or "")[:10],
                "universe_type": "expanded_paper",
                "symbol": row.get("symbol"),
                "symbol_in_live_universe": row.get("symbol_in_live_universe"),
                "live_symbols_unchanged": True,
                "strategy_id": row.get("strategy_id"),
                "strategy_candidate": row.get("strategy_candidate"),
                "experiment_name": row.get("experiment_name"),
                "tracking_mode": tracking_mode,
                "decision": row.get("decision"),
                "recommended_mode": row.get("recommended_mode"),
                "response_action": response_action,
                "negative_advisory": row.get("negative_advisory"),
                "would_enter": would_enter,
                "would_size_usdt": max_paper if would_enter and max_paper is not None else 0.0,
                "max_paper_notional_usdt": max_paper,
                "max_live_notional_usdt_ignored": True,
                "no_sample_reason": row.get("no_sample_reason"),
                "advisory_source": row.get("advisory_source"),
                "advisory_source_path": row.get("source_path"),
                "advisory_fresh": row.get("advisory_fresh"),
                "advisory_contract_match": row.get("advisory_contract_match"),
                "live_block_reasons": row.get("live_block_reasons"),
                "live_order_effect": "read_only_no_live_order",
            }
        )
    return out


def _is_bottom_zone_advisory(row: Mapping[str, Any]) -> bool:
    keys = {
        str(row.get("strategy_id") or "").strip(),
        str(row.get("strategy_candidate") or "").strip(),
        str(row.get("experiment_name") or "").strip(),
    }
    normalized = {key.lower() for key in keys if key}
    return bool(
        BOTTOM_ZONE_PROBE_STRATEGY_ID.lower() in normalized
        or normalized.intersection(BOTTOM_ZONE_ADVISORY_CANDIDATES)
    )


def _bottom_zone_paper_records(
    advisory_rows: Iterable[Mapping[str, Any]],
    *,
    diagnostics: DiagnosticsConfig,
    audit: DecisionAudit,
    ts_utc: str,
    asof_ts_ms: int,
    rt_cost_bps: float,
    required_days: int,
    required_entry_days: int,
    required_coverage: float,
    allowed_cost_sources: set[str],
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    cached: dict[str, list[dict[str, float | int]]],
    top_of_book: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for advisory in advisory_rows:
        if not _is_bottom_zone_advisory(advisory):
            continue
        fields = _advisory_response_fields(advisory, diagnostics)
        response_action = str(fields.get("advisory_response_action") or "")
        if response_action not in {"paper_tracking", "shadow_tracking", "negative_advisory", "research_display_only", "stale_paper_display_only", "stale_shadow_display_only"}:
            continue
        symbol = _symbol_text(advisory.get("symbol"))
        if not symbol:
            continue
        would_enter = bool(
            response_action == "paper_tracking"
            and (
                _normalize_bool(advisory.get("would_enter")) is not False
                or str(advisory.get("decision") or "").strip().upper() == "PAPER_READY"
            )
        )
        entry_px = _entry_px(
            symbol=symbol,
            entry_ts_ms=asof_ts_ms,
            market_data_1h=market_data_1h,
            cache_dir=cache_dir,
            cached=cached,
        )
        quote_context = _quote_context(
            symbol=symbol,
            row=advisory,
            top_of_book=top_of_book,
            entry_px=entry_px,
        )
        estimated_cost = _normalize_float(
            advisory.get("estimated_cost_bps")
            or advisory.get("cost_bps")
            or advisory.get("selected_total_cost_bps")
        )
        if estimated_cost is None:
            estimated_cost = float(rt_cost_bps)
        max_paper = _normalize_float(advisory.get("max_paper_notional_usdt"))
        horizon = _parse_horizon_hours(advisory.get("horizon_hours")) or PRIMARY_HORIZON
        no_sample_reason = str(advisory.get("no_sample_reason") or "")
        if not no_sample_reason and not would_enter:
            no_sample_reason = response_action
        out.append(
            {
                "strategy_id": str(advisory.get("strategy_id") or BOTTOM_ZONE_PROBE_STRATEGY_ID),
                "experiment_name": str(advisory.get("experiment_name") or "v5.bottom_zone_probe_paper"),
                "enabled_shadow_only": True,
                "enable_live_experiment": False,
                "live_symbols_unchanged": True,
                "run_id": str(advisory.get("run_id") or getattr(audit, "run_id", "") or ""),
                "ts_utc": ts_utc,
                "entry_ts_ms": asof_ts_ms,
                "paper_date": ts_utc[:10],
                "symbol": symbol,
                "source_strategy_candidate": str(advisory.get("strategy_candidate") or "v5.bottom_zone_probe_paper"),
                "candidate_id": str(advisory.get("candidate_id") or f"bottom_zone_{symbol}_{getattr(audit, 'run_id', '')}"),
                "final_decision": "paper_advisory" if would_enter else "advisory_display",
                "no_sample_reason": "" if would_enter else no_sample_reason,
                "sol_candidate_present": True,
                "risk_level": str(advisory.get("risk_level") or ""),
                "original_block_reason": str(advisory.get("live_block_reasons") or advisory.get("advisory_reason") or no_sample_reason),
                "cooldown_active": False,
                "risk_off": False,
                "skip_reason": "" if would_enter else no_sample_reason,
                "entry_reason": "v5.bottom_zone_probe_paper",
                "experiment_reason": "bottom_zone_probe_paper_tracking",
                "would_enter": would_enter,
                "would_exit": False,
                "would_exit_time": _iso_from_ms(asof_ts_ms + horizon * 3600 * 1000) if asof_ts_ms > 0 and would_enter else "",
                "would_exit_rule": f"paper_time_horizon_{horizon}h" if would_enter else "",
                "expected_exit_horizon": f"{horizon}h" if would_enter else "",
                "would_size_notional": max_paper if would_enter and max_paper is not None else 0.0,
                "would_size_usdt": max_paper if would_enter and max_paper is not None else 0.0,
                "entry_px": entry_px if would_enter else None,
                "arrival_bid": quote_context.get("arrival_bid"),
                "arrival_ask": quote_context.get("arrival_ask"),
                "arrival_mid": quote_context.get("arrival_mid"),
                "estimated_spread_bps": quote_context.get("estimated_spread_bps"),
                "expected_order_type": quote_context.get("expected_order_type"),
                "estimated_fill_px": quote_context.get("estimated_fill_px") if would_enter else None,
                "final_score": _normalize_float(advisory.get("final_score")),
                "alpha6_score": _normalize_float(advisory.get("alpha6_score")),
                "alpha6_side": str(advisory.get("alpha6_side") or ""),
                "f4_volume_expansion": _normalize_float(advisory.get("f4_volume_expansion")),
                "f4_threshold": None,
                "f5_rsi_trend_confirm": _normalize_float(advisory.get("f5_rsi_trend_confirm")),
                "cost_source": str(advisory.get("cost_source") or "local_estimate"),
                "cost_source_quality": str(advisory.get("cost_source_quality") or advisory.get("cost_quality") or ""),
                "estimated_cost_bps": float(estimated_cost),
                "cost_model_version": str(advisory.get("cost_model_version") or ""),
                "cost_source_live_ready": _cost_source_live_ready(advisory, allowed_cost_sources),
                "slippage_covered": _slippage_observed(quote_context),
                "required_paper_days": required_days,
                "required_entry_days": required_entry_days,
                "required_slippage_coverage": required_coverage,
                "rt_cost_bps": rt_cost_bps,
                "primary_horizon_hours": horizon,
                "extra_live_block_reasons": list(BOTTOM_ZONE_PAPER_LIVE_BLOCK_REASONS),
                "live_order_effect": "read_only_no_live_order",
                "proposal_present": False,
                "proposal_source": "",
                **fields,
                "label_status": "pending" if would_enter else "heartbeat",
                "label_not_observable_reason": "",
            }
        )
    return out


def _is_alpha_factory_advisory(row: Mapping[str, Any]) -> bool:
    source_module = str(row.get("source_module") or "").strip().lower()
    candidate = str(row.get("strategy_candidate") or "").strip().lower()
    strategy_id = str(row.get("strategy_id") or "").strip().lower()
    experiment = str(row.get("experiment_name") or "").strip().lower()
    keys = {candidate, strategy_id, experiment}
    return bool(
        source_module == "alpha_factory"
        or candidate.startswith("v5.af.")
        or any(key in ALPHA_FACTORY_SECOND_STAGE_CANDIDATES for key in keys if key)
    )


def _alpha_factory_response_action(row: Mapping[str, Any]) -> str:
    decision = str(row.get("decision") or "").strip().upper()
    mode = str(row.get("recommended_mode") or "").strip().lower().replace("-", "_")
    fresh = _normalize_bool(row.get("advisory_fresh"))
    fresh = True if fresh is None else bool(fresh)
    if decision == "KILL":
        return "negative_advisory"
    if mode == "shadow" and not fresh:
        return "stale_shadow_display_only"
    if mode == "paper" and not fresh:
        return "stale_paper_display_only"
    if mode == "shadow":
        return "shadow_tracking"
    if mode == "paper":
        return "paper_tracking"
    return "display_only"


def _alpha_factory_stale_response_downgraded(row: Mapping[str, Any]) -> bool:
    decision = str(row.get("decision") or "").strip().upper()
    mode = str(row.get("recommended_mode") or "").strip().lower().replace("-", "_")
    fresh = _normalize_bool(row.get("advisory_fresh"))
    fresh = True if fresh is None else bool(fresh)
    return bool(decision != "KILL" and mode in {"paper", "shadow"} and not fresh)


def _alpha_factory_family(row: Mapping[str, Any]) -> str:
    text = " ".join(
        str(row.get(key) or "").strip().lower()
        for key in ("strategy_candidate", "strategy_id", "experiment_name")
    )
    if "futures_" in text or "future" in text:
        return "futures"
    if "exit_policy" in text or "probe_exit" in text or "strict_probe_exit" in text:
        return "exit_policy"
    if "expanded_" in text or "relative_strength" in text:
        return "expanded"
    if "pair_trade" in text:
        return "pair_trade"
    return "other"


def _alpha_factory_advisory_rows(
    advisory_rows: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    asof_ts_ms: int,
    source_health_rows: Iterable[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    ts_utc = _iso_from_ms(asof_ts_ms)
    source_health = next(iter(source_health_rows), {})
    selected_source = str(source_health.get("selected_source") or "")
    source_health_freshness_status = str(source_health.get("freshness_status") or "")
    if not source_health_freshness_status and source_health:
        source_health_freshness_status = (
            "stale" if _normalize_bool(source_health.get("selected_source_is_stale")) else "fresh"
        )
    out: list[dict[str, Any]] = []
    for row in advisory_rows:
        if not _is_alpha_factory_advisory(row):
            continue
        out.append(
            {
                "run_id": run_id,
                "ts_utc": ts_utc,
                "strategy_candidate": row.get("strategy_candidate") or row.get("strategy_id") or row.get("experiment_name"),
                "symbol": row.get("symbol"),
                "decision": row.get("decision"),
                "recommended_mode": row.get("recommended_mode"),
                "promotion_state": row.get("promotion_state"),
                "alpha_factory_score": row.get("alpha_factory_score"),
                "advisory_source": row.get("advisory_source"),
                "selected_source": selected_source or row.get("advisory_source"),
                "source_health_freshness_status": source_health_freshness_status,
                "advisory_fresh": row.get("advisory_fresh"),
                "advisory_age_sec": row.get("advisory_age_sec"),
                "stale_reason": row.get("stale_reason"),
                "stale_response_downgraded": _alpha_factory_stale_response_downgraded(row),
                "response_action": _alpha_factory_response_action(row),
                "max_live_notional_usdt_ignored": True,
                "live_order_effect": "read_only_no_live_order",
            }
        )
    return out


def _alpha_factory_family_summary_rows(
    alpha_rows: Iterable[Mapping[str, Any]],
    advisory_rows: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    asof_ts_ms: int,
) -> list[dict[str, Any]]:
    by_candidate: dict[str, Mapping[str, Any]] = {}
    for row in advisory_rows:
        if not _is_alpha_factory_advisory(row):
            continue
        for key in ("strategy_candidate", "strategy_id", "experiment_name"):
            value = str(row.get(key) or "").strip()
            if value:
                by_candidate[value] = row
    buckets: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in alpha_rows:
        original = by_candidate.get(str(row.get("strategy_candidate") or "")) or row
        buckets[_alpha_factory_family(original)].append(row)
    ts_utc = _iso_from_ms(asof_ts_ms)
    out: list[dict[str, Any]] = []
    for family, rows in sorted(buckets.items()):
        actions = Counter(str(row.get("response_action") or "") for row in rows)
        display_only_count = (
            actions.get("display_only", 0)
            + actions.get("stale_paper_display_only", 0)
            + actions.get("stale_shadow_display_only", 0)
        )
        candidates = sorted({str(row.get("strategy_candidate") or "") for row in rows if str(row.get("strategy_candidate") or "")})
        out.append(
            {
                "run_id": run_id,
                "ts_utc": ts_utc,
                "family": family,
                "row_count": len(rows),
                "display_only_count": display_only_count,
                "shadow_tracking_count": actions.get("shadow_tracking", 0),
                "paper_tracking_count": actions.get("paper_tracking", 0),
                "negative_advisory_count": actions.get("negative_advisory", 0),
                "max_live_notional_usdt_ignored": True,
                "live_order_effect": "read_only_no_live_order",
                "strategy_candidates": json.dumps(candidates, ensure_ascii=False),
            }
        )
    return out


def _symbol_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    raw_items: list[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, (list, tuple)):
            raw_items = list(parsed)
        else:
            cleaned = text.strip("[]")
            for old, new in ((";", ","), ("|", ","), ("\n", ",")):
                cleaned = cleaned.replace(old, new)
            raw_items = [item.strip().strip("'\"") for item in cleaned.split(",")]
    symbols = [_symbol_text(item) for item in raw_items]
    return [symbol for symbol in dict.fromkeys(symbols) if symbol]


def _symbol_list_from_row(row: Mapping[str, Any], names: Iterable[str]) -> list[str]:
    for name in names:
        symbols = _symbol_list(row.get(name))
        if symbols:
            return symbols
    return []


def _risk_on_symbol_list_from_row(row: Mapping[str, Any], names: Iterable[str]) -> list[str]:
    for name in names:
        symbols = [
            symbol
            for symbol in _symbol_list(row.get(name))
            if symbol != "MULTI" and "/" in symbol
        ]
        if symbols:
            return symbols
    return []


def _json_symbol_list(symbols: Iterable[str]) -> str:
    return json.dumps(list(dict.fromkeys(symbols)), ensure_ascii=False)


def _risk_on_symbols_csv_value(symbols: Iterable[str]) -> str:
    values = list(dict.fromkeys(symbols))
    return _json_symbol_list(values) if values else "not_observable"


def _actual_bought_symbols_for_run(run_path: Path, candidate_rows: Iterable[Mapping[str, Any]]) -> list[str]:
    bought: set[str] = set()
    trades_path = run_path / "trades.csv"
    if trades_path.is_file():
        try:
            with trades_path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    intent = str(row.get("intent") or "").strip().upper()
                    side = str(row.get("side") or "").strip().lower()
                    if intent in {"OPEN_LONG", "BUY", "OPEN"} or (not intent and side == "buy"):
                        symbol = _symbol_text(row.get("symbol"))
                        if symbol:
                            bought.add(symbol)
        except Exception:
            pass
    for row in candidate_rows:
        decision = str(row.get("final_decision") or "").strip().upper()
        if decision in {"OPEN_LONG", "BUY", "ALLOW_OPEN_LONG"}:
            symbol = _symbol_text(row.get("symbol"))
            if symbol:
                bought.add(symbol)
    return sorted(bought)


def _risk_on_multi_buy_strategy_key(row: Mapping[str, Any]) -> str:
    keys = {
        str(row.get("strategy_candidate") or "").strip().lower(),
        str(row.get("advisory_strategy_candidate") or "").strip().lower(),
        str(row.get("strategy_id") or "").strip().lower(),
        str(row.get("advisory_strategy_id") or "").strip().lower(),
        str(row.get("experiment_name") or "").strip().lower(),
        str(row.get("candidate") or "").strip().lower(),
        str(row.get("candidate_name") or "").strip().lower(),
        str(row.get("source_strategy_candidate") or "").strip().lower(),
        str(row.get("strategy") or "").strip().lower(),
        str(row.get("strategy_name") or "").strip().lower(),
    }
    for key in keys:
        if key in RISK_ON_MULTI_BUY_SHADOW_CANDIDATES:
            return key
    return ""


def _risk_on_multi_buy_advisory(row: Mapping[str, Any]) -> bool:
    return bool(_risk_on_multi_buy_strategy_key(row))


def _risk_on_multi_buy_top_k(row: Mapping[str, Any]) -> Optional[int]:
    explicit = _normalize_float(row.get("top_k") or row.get("k"))
    if explicit is not None and explicit > 0:
        return int(explicit)
    for key in (
        "strategy_candidate",
        "advisory_strategy_candidate",
        "strategy_id",
        "advisory_strategy_id",
        "experiment_name",
        "candidate",
        "candidate_name",
        "source_strategy_candidate",
        "strategy",
        "strategy_name",
    ):
        text = str(row.get(key) or "").strip().lower()
        marker = "risk_on_multi_buy_top"
        if marker not in text:
            continue
        tail = text.split(marker, 1)[1]
        digits = ""
        for char in tail:
            if char.isdigit():
                digits += char
                continue
            break
        if digits:
            return int(digits)
    return None


def _risk_on_multi_buy_detail_paths(*, run_path: Path, reports_dir: Path) -> list[Path]:
    raw_rel = Path("raw") / "reports" / "risk_on_multi_buy_shadow.csv"
    candidates = [
        reports_dir / "risk_on_multi_buy_shadow.csv",
        reports_dir / "quant_lab" / "risk_on_multi_buy_shadow.csv",
        reports_dir / "quant_lab_latest" / "risk_on_multi_buy_shadow.csv",
        reports_dir / "quant_lab_latest" / "reports" / "risk_on_multi_buy_shadow.csv",
        reports_dir / "quant_lab_latest" / raw_rel,
        reports_dir / "quant_lab" / "latest" / "reports" / "risk_on_multi_buy_shadow.csv",
        reports_dir / "quant_lab" / "latest" / raw_rel,
        reports_dir / raw_rel,
        reports_dir.parent / "reports" / "risk_on_multi_buy_shadow.csv",
        reports_dir.parent / raw_rel,
        reports_dir.parent / "reports" / "quant_lab" / "latest" / "reports" / "risk_on_multi_buy_shadow.csv",
        reports_dir.parent / "reports" / "quant_lab" / "latest" / raw_rel,
        run_path / "risk_on_multi_buy_shadow.csv",
        reports_dir / "quant_lab_latest_bundle.zip",
        reports_dir / "quant_lab_latest_bundle.tar.gz",
        reports_dir / "quant_lab" / "latest_bundle.zip",
        reports_dir / "quant_lab" / "latest_bundle.tar.gz",
        reports_dir.parent / "reports" / "quant_lab_latest_bundle.zip",
        reports_dir.parent / "reports" / "quant_lab_latest_bundle.tar.gz",
        reports_dir.parent / "reports" / "quant_lab" / "latest_bundle.zip",
        reports_dir.parent / "reports" / "quant_lab" / "latest_bundle.tar.gz",
        Path("/var/lib/v5-prod/raw/reports/risk_on_multi_buy_shadow.csv"),
        Path("/var/lib/v5-prod/reports/risk_on_multi_buy_shadow.csv"),
        Path("/var/lib/v5-prod/quant_lab_latest/reports/risk_on_multi_buy_shadow.csv"),
        Path("/var/lib/v5-prod/quant_lab_latest/raw/reports/risk_on_multi_buy_shadow.csv"),
        Path("/var/lib/v5-prod/quant_lab/latest/reports/risk_on_multi_buy_shadow.csv"),
        Path("/var/lib/v5-prod/quant_lab/latest/raw/reports/risk_on_multi_buy_shadow.csv"),
        Path("/var/lib/v5-prod/quant_lab_latest_bundle.zip"),
        Path("/var/lib/v5-prod/quant_lab_latest_bundle.tar.gz"),
    ]
    for pattern in (
        reports_dir / "quant_lab_latest_bundle*.zip",
        reports_dir / "quant_lab_latest_bundle*.tar.gz",
        reports_dir / "quant_lab_expert_pack*.zip",
        reports_dir / "quant_lab_expert_pack*.tar.gz",
        reports_dir.parent / "reports" / "quant_lab_latest_bundle*.zip",
        reports_dir.parent / "reports" / "quant_lab_latest_bundle*.tar.gz",
        reports_dir.parent / "reports" / "quant_lab_expert_pack*.zip",
        reports_dir.parent / "reports" / "quant_lab_expert_pack*.tar.gz",
        Path("/var/lib/v5-prod/quant_lab_latest_bundle*.zip"),
        Path("/var/lib/v5-prod/quant_lab_latest_bundle*.tar.gz"),
        Path("/var/lib/v5-prod/quant_lab_expert_pack*.zip"),
        Path("/var/lib/v5-prod/quant_lab_expert_pack*.tar.gz"),
    ):
        try:
            candidates.extend(
                sorted(
                    pattern.parent.glob(pattern.name),
                    key=lambda path: path.stat().st_mtime if path.exists() else 0,
                    reverse=True,
                )
            )
        except Exception:
            continue
    return list(dict.fromkeys(path.resolve() for path in candidates))


def _read_risk_on_multi_buy_detail_rows(*, run_path: Path, reports_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    for path in _risk_on_multi_buy_detail_paths(run_path=run_path, reports_dir=reports_dir):
        if path in seen_paths:
            continue
        seen_paths.add(path)
        if not path.is_file():
            continue
        rows.extend(_read_raw_csv_path(path, target_filename="risk_on_multi_buy_shadow.csv"))
    return rows


def _risk_on_multi_buy_detail_for(
    advisory_row: Mapping[str, Any],
    detail_rows: Iterable[Mapping[str, Any]],
    *,
    top_k: Optional[int],
) -> Optional[Mapping[str, Any]]:
    rows = list(detail_rows)
    strategy_key = _risk_on_multi_buy_strategy_key(advisory_row)
    if strategy_key:
        candidates = [row for row in rows if _risk_on_multi_buy_strategy_key(row) == strategy_key]
    else:
        candidates = []
    if not candidates and top_k is not None:
        candidates = [row for row in rows if _risk_on_multi_buy_top_k(row) == top_k]
    if not candidates:
        return None

    advisory_run_id = str(advisory_row.get("run_id") or "").strip()
    if advisory_run_id:
        same_run = [row for row in candidates if str(row.get("run_id") or "").strip() == advisory_run_id]
        if same_run:
            candidates = same_run

    latest_ts_ms = max((_risk_on_multi_buy_row_ts_ms(row) or -1) for row in candidates)
    if latest_ts_ms >= 0:
        candidates = [row for row in candidates if (_risk_on_multi_buy_row_ts_ms(row) or -1) == latest_ts_ms]
    else:
        latest_run_id = max((str(row.get("run_id") or "") for row in candidates), default="")
        if latest_run_id:
            candidates = [row for row in candidates if str(row.get("run_id") or "") == latest_run_id]
    return _merge_risk_on_multi_buy_detail_rows(candidates)


def _risk_on_multi_buy_row_ts_ms(row: Mapping[str, Any]) -> Optional[int]:
    decision_values = [
        _advisory_time_ms(row.get(name))
        for name in ("decision_ts", "decision_time", "ts_utc", "timestamp", "run_ts")
    ]
    decision_values = [value for value in decision_values if value is not None]
    if decision_values:
        return max(decision_values)
    fallback_values = [
        _advisory_time_ms(row.get(name))
        for name in ("sampled_at", "as_of_ts", "generated_at")
    ]
    fallback_values = [value for value in fallback_values if value is not None]
    return max(fallback_values) if fallback_values else None


def _merge_risk_on_multi_buy_detail_rows(rows: Iterable[Mapping[str, Any]]) -> Mapping[str, Any]:
    candidates = list(rows)
    if not candidates:
        return {}
    merged = dict(candidates[-1])
    selected: list[str] = []
    would_buy: list[str] = []
    for row in candidates:
        selected.extend(
            _risk_on_symbol_list_from_row(
                row,
                (
                    "selected_symbols",
                    "selected_symbol_list",
                    "top_symbols",
                    "top_n_symbols",
                    "symbols",
                    "would_buy_symbols",
                    "would_buy_symbol",
                    "symbol",
                ),
            )
        )
        would_buy.extend(
            _risk_on_symbol_list_from_row(
                row,
                (
                    "would_buy_symbols",
                    "would_buy_symbol",
                    "would_enter_symbols",
                    "would_open_symbols",
                    "paper_buy_symbols",
                    "selected_symbols",
                    "symbol",
                ),
            )
        )
    if selected:
        merged["selected_symbols"] = _risk_on_symbols_csv_value(selected)
    if would_buy:
        merged["would_buy_symbols"] = _risk_on_symbols_csv_value(would_buy)
    return merged


def _risk_on_multi_buy_shadow_rows(
    advisory_rows: Iterable[Mapping[str, Any]],
    *,
    detail_rows: Iterable[Mapping[str, Any]] = (),
    diagnostics: DiagnosticsConfig,
    run_id: str,
    asof_ts_ms: int,
    audit: DecisionAudit,
    run_path: Path,
    candidate_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    ts_utc = _iso_from_ms(asof_ts_ms)
    candidates = list(candidate_rows)
    actual_bought = _actual_bought_symbols_for_run(run_path, candidates)
    out: list[dict[str, Any]] = []
    for row in advisory_rows:
        if not _risk_on_multi_buy_advisory(row):
            continue
        top_k = _risk_on_multi_buy_top_k(row)
        fields = _advisory_response_fields(row, diagnostics)
        response_action = str(fields.get("advisory_response_action") or "")
        detail_row = _risk_on_multi_buy_detail_for(row, detail_rows, top_k=top_k)
        source_row = detail_row or row
        source_detail_available = detail_row is not None
        selected = _risk_on_symbol_list_from_row(
            source_row,
            (
                "selected_symbols",
                "selected_symbol_list",
                "top_symbols",
                "top_n_symbols",
                "symbols",
                "would_buy_symbols",
                "would_buy_symbol",
                "symbol",
            ),
        )
        would_buy = _risk_on_symbol_list_from_row(
            source_row,
            (
                "would_buy_symbols",
                "would_buy_symbol",
                "would_enter_symbols",
                "would_open_symbols",
                "paper_buy_symbols",
                "selected_symbols",
                "symbol",
            ),
        )
        if not selected and would_buy:
            selected = list(would_buy)
        if not would_buy and selected:
            would_buy = list(selected)
        if top_k is not None and top_k > 0:
            selected = selected[:top_k]
            would_buy = would_buy[:top_k]
        missed = [symbol for symbol in would_buy if symbol not in set(actual_bought)]
        current_regime = str(row.get("current_regime") or row.get("regime_state") or "").strip()
        if not current_regime:
            current_regime = str(getattr(audit, "regime_state", "") or getattr(audit, "risk_level", "") or "").strip()
        out.append(
            {
                "run_id": run_id,
                "ts_utc": ts_utc,
                "current_regime": current_regime,
                "top_k": top_k,
                "selected_symbols": _risk_on_symbols_csv_value(selected),
                "would_buy_symbols": _risk_on_symbols_csv_value(would_buy),
                "actual_bought_symbols": _json_symbol_list(actual_bought),
                "missed_symbols": _json_symbol_list(missed),
                "source_detail_available": bool(source_detail_available),
                "source_detail_missing_paths": ""
                if source_detail_available
                else "reports/risk_on_multi_buy_shadow.csv;raw/reports/risk_on_multi_buy_shadow.csv",
                "response_action": response_action or "display_only",
                "live_order_effect": "read_only_no_live_order",
            }
        )
    return out


def _record_key(record: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(record.get("strategy_id") or ""),
            str(record.get("run_id") or ""),
            str(record.get("ts_utc") or ""),
            str(record.get("symbol") or ""),
            str(record.get("candidate_id") or ""),
            str(record.get("source_strategy_candidate") or ""),
            str(record.get("original_block_reason") or ""),
        ]
    )


def _asof_ts_ms(audit: DecisionAudit, market_data_1h: Dict[str, MarketSeries]) -> int:
    for raw in (getattr(audit, "now_ts", None), getattr(audit, "window_end_ts", None)):
        parsed = _coerce_epoch_ms(raw)
        if parsed and parsed > 0:
            return int(parsed)
    return max(
        [0]
        + [
            int(_coerce_epoch_ms(max(getattr(series, "ts", []) or [0])) or 0)
            for series in (market_data_1h or {}).values()
        ]
    )


def _entry_px(
    *,
    symbol: str,
    entry_ts_ms: int,
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    cached: dict[str, list[dict[str, float | int]]],
) -> Optional[float]:
    rows = _series_for_symbol(
        symbol=symbol,
        cache_dir=cache_dir,
        market_data_1h=market_data_1h,
        cached=cached,
    )
    price = _find_close_at_or_after(rows, entry_ts_ms)
    if price is not None:
        return price
    series = market_data_1h.get(symbol)
    closes = getattr(series, "close", []) if series is not None else []
    if closes:
        value = _normalize_float(closes[-1])
        if value is not None and value > 0:
            return value
    return None


def _current_equity_usdt(audit: DecisionAudit) -> Optional[float]:
    budget = getattr(audit, "budget", None)
    if isinstance(budget, Mapping):
        value = _normalize_float(budget.get("current_equity_usdt"))
        if value is not None and value > 0:
            return value
    for attr in ("equity_usdt", "current_equity_usdt"):
        value = _normalize_float(getattr(audit, attr, None))
        if value is not None and value > 0:
            return value
    return None


def _would_size_notional(row: Mapping[str, Any], audit: DecisionAudit) -> Optional[float]:
    equity = _current_equity_usdt(audit)
    for key in ("target_weight_after_risk", "target_weight_raw", "target_w"):
        target_w = _normalize_float(row.get(key))
        if target_w is not None and target_w > 0 and equity is not None:
            return round(float(target_w) * float(equity), 8)
    return None


def _estimated_cost_bps(row: Mapping[str, Any], fallback_bps: float) -> float:
    for key in ("cost_bps", "selected_total_cost_bps", "estimated_cost_bps"):
        value = _normalize_float(row.get(key))
        if value is not None and value >= 0:
            return float(value)
    return float(fallback_bps)


def _cost_source_live_ready(row: Mapping[str, Any], allowed: set[str]) -> bool:
    source = str(row.get("cost_source") or "").strip().lower()
    return source in allowed


def _lookup_top_of_book(top_of_book: Mapping[str, Any] | None, symbol: str) -> Mapping[str, Any]:
    book = top_of_book or {}
    variants = {
        symbol,
        symbol.upper(),
        symbol.replace("/", "-"),
        symbol.replace("/", "-").upper(),
        symbol.replace("-", "/"),
        symbol.replace("-", "/").upper(),
    }
    for key in variants:
        value = book.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def _quote_context(
    *,
    symbol: str,
    row: Mapping[str, Any],
    top_of_book: Mapping[str, Any] | None,
    entry_px: Optional[float],
) -> dict[str, Any]:
    quote = _lookup_top_of_book(top_of_book, symbol)
    bid = _normalize_float(row.get("arrival_bid") or row.get("bid") or quote.get("bid"))
    ask = _normalize_float(row.get("arrival_ask") or row.get("ask") or quote.get("ask"))
    mid = _normalize_float(row.get("arrival_mid") or row.get("mid") or quote.get("mid"))
    if mid is None and bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (float(bid) + float(ask)) / 2.0
    spread_bps = _normalize_float(
        row.get("estimated_spread_bps")
        or row.get("spread_bps_at_decision")
        or row.get("spread_bps")
        or quote.get("spread_bps")
    )
    if spread_bps is None and bid is not None and ask is not None and mid and mid > 0:
        spread_bps = (float(ask) - float(bid)) / float(mid) * 10_000.0
    expected_order_type = str(
        row.get("expected_order_type")
        or row.get("order_type")
        or "paper_market_buy"
    )
    estimated_fill_px = _normalize_float(row.get("estimated_fill_px"))
    if estimated_fill_px is None:
        estimated_fill_px = ask if ask is not None and ask > 0 else entry_px
    return {
        "arrival_bid": bid,
        "arrival_ask": ask,
        "arrival_mid": mid,
        "estimated_spread_bps": round(float(spread_bps), 8) if spread_bps is not None else None,
        "expected_order_type": expected_order_type,
        "estimated_fill_px": estimated_fill_px,
    }


def _slippage_observed(row: Mapping[str, Any]) -> bool:
    return (
        (_normalize_float(row.get("arrival_mid")) or 0.0) > 0.0
        and _normalize_float(row.get("estimated_spread_bps")) is not None
    )


def _risk_level_for_row(row: Mapping[str, Any], audit: DecisionAudit) -> str:
    for value in (
        row.get("risk_level"),
        row.get("current_level"),
        getattr(audit, "risk_level", None),
        getattr(audit, "current_level", None),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _is_protect_level(value: Any) -> bool:
    return str(value or "").strip().upper().replace("-", "_") == "PROTECT"


def _is_alpha6_buy(value: Any) -> bool:
    return str(value or "").strip().lower().replace("_", "-") in {"buy", "long", "trend-buy", "trend_buy"}


def _is_risk_off_context(audit: DecisionAudit, row: Mapping[str, Any], risk_level: Any) -> bool:
    for key in ("risk_off", "is_risk_off", "risk_off_close_only"):
        if _truthy(row.get(key)):
            return True
    for value in (
        getattr(audit, "regime", None),
        getattr(audit, "regime_state", None),
        row.get("regime_state"),
        row.get("regime"),
        row.get("market_regime"),
        row.get("target_zero_reason"),
        risk_level,
    ):
        if _risk_off_text(value):
            return True
    return False


def _regime_text(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")


def _regime_for_row(row: Mapping[str, Any], audit: DecisionAudit) -> str:
    for value in (
        row.get("current_regime"),
        row.get("regime_state"),
        row.get("regime"),
        row.get("market_regime"),
        getattr(audit, "current_regime", None),
        getattr(audit, "regime_state", None),
        getattr(audit, "regime", None),
    ):
        text = _regime_text(value)
        if text:
            return text
    return ""


def _has_active_cooldown(row: Mapping[str, Any], *, asof_ts_ms: int) -> bool:
    for key in (
        "active_cooldown",
        "cooldown_active",
        "negative_expectancy_cooldown_active",
        "same_symbol_reentry_cooldown_active",
    ):
        if _truthy(row.get(key)):
            return True
    for key in ("remain_seconds", "cooldown_remaining_seconds", "cooldown_remain_seconds"):
        value = _normalize_float(row.get(key))
        if value is not None and value > 0.0:
            return True
    for key in ("cooldown_until_ms", "cooldown_until_ts_ms", "cooldown_until"):
        until_ms = _coerce_epoch_ms(row.get(key))
        if until_ms is not None and until_ms > int(asof_ts_ms or 0):
            return True
    return False


def _cost_context_for_symbol(
    *,
    symbol: str,
    candidate_rows: Iterable[Mapping[str, Any]],
    fallback_bps: float,
) -> dict[str, Any]:
    best: Mapping[str, Any] | None = None
    best_rank = -1
    quality_rank = {
        "actual_fills": 5,
        "mixed_actual_proxy": 4,
        "quant_lab_cached": 3,
        "public_spread_proxy": 2,
        "local_estimate": 1,
    }
    for row in candidate_rows:
        if not isinstance(row, Mapping) or _symbol_text(row.get("symbol")) != symbol:
            continue
        source = str(row.get("cost_source") or "").strip().lower()
        rank = quality_rank.get(source, 0)
        if rank > best_rank:
            best = row
            best_rank = rank
    if best is None:
        return {
            "cost_source": "local_estimate",
            "cost_source_quality": "local_estimate",
            "cost_model_version": "v5_local_paper_fallback",
            "estimated_cost_bps": float(fallback_bps),
        }
    return {
        "cost_source": str(best.get("cost_source") or "local_estimate"),
        "cost_source_quality": str(best.get("cost_source_quality") or best.get("cost_source") or "local_estimate"),
        "cost_model_version": str(best.get("cost_model_version") or ""),
        "estimated_cost_bps": _estimated_cost_bps(best, fallback_bps),
    }


def _strategy_source_or_reason_matches(row: Mapping[str, Any], spec: Mapping[str, Any]) -> bool:
    strategy = str(row.get("strategy_candidate") or "").strip()
    block_reason = str(row.get("block_reason") or "").strip()
    source_candidates = set(spec.get("source_strategy_candidates") or set())
    allowed_reasons = set(spec.get("allowed_block_reasons") or set())
    strategy_matches = bool(strategy and strategy in source_candidates)
    reason_matches = bool(allowed_reasons and block_reason in allowed_reasons)
    if source_candidates and allowed_reasons:
        return bool(strategy_matches or reason_matches)
    if source_candidates:
        return strategy_matches
    if allowed_reasons:
        return reason_matches
    return True


def _row_condition_diagnostics(
    row: Mapping[str, Any],
    *,
    spec: Mapping[str, Any],
    audit: DecisionAudit,
    asof_ts_ms: int,
    sol_candidate_present: bool = True,
) -> dict[str, Any]:
    risk_level = _risk_level_for_row(row, audit)
    alpha6_side = str(row.get("alpha6_side") or "").strip()
    alpha6_score = _normalize_float(row.get("alpha6_score"))
    f4 = _normalize_float(row.get("f4_volume_expansion"))
    cooldown_active = _has_active_cooldown(row, asof_ts_ms=asof_ts_ms)
    risk_off = _is_risk_off_context(audit, row, risk_level)
    current_regime = _regime_for_row(row, audit)
    original_block_reason = str(row.get("block_reason") or row.get("no_signal_reason") or "")
    return {
        "sol_candidate_present": bool(sol_candidate_present),
        "risk_level": risk_level,
        "current_regime": current_regime,
        "alpha6_score": alpha6_score,
        "alpha6_side": alpha6_side,
        "f4_volume_expansion": f4,
        "f4_threshold": _normalize_float(spec.get("min_f4_volume_expansion")),
        "f5_rsi_trend_confirm": _normalize_float(row.get("f5_rsi_trend_confirm")),
        "expected_edge_bps": _normalize_float(row.get("expected_edge_bps")),
        "required_edge_bps": _normalize_float(row.get("required_edge_bps")),
        "cost_gate_verified": _normalize_bool(row.get("cost_gate_verified")),
        "original_block_reason": original_block_reason,
        "cooldown_active": bool(cooldown_active),
        "risk_off": bool(risk_off),
        "cost_source": str(row.get("cost_source") or ""),
        "cost_source_quality": str(row.get("cost_source_quality") or ""),
        "cost_model_version": str(row.get("cost_model_version") or ""),
        "estimated_cost_bps": _estimated_cost_bps(row, 0.0),
    }


def _condition_block_reason(
    diagnostics: Mapping[str, Any],
    *,
    spec: Mapping[str, Any],
    source_or_reason_matched: bool,
) -> str:
    no_candidate_reason = _no_candidate_reason(spec)
    if not bool(diagnostics.get("sol_candidate_present")):
        return no_candidate_reason
    if bool(diagnostics.get("risk_off")):
        return "risk_off"
    risk_level = str(diagnostics.get("risk_level") or "").strip()
    if _spec_bool(spec, "require_protect_level", True) and risk_level and not _is_protect_level(risk_level):
        return "risk_not_protect"
    if _spec_bool(spec, "require_no_cooldown", True) and bool(diagnostics.get("cooldown_active")):
        return "cooldown_active"
    if _spec_bool(spec, "require_alpha6_buy", True) and not _is_alpha6_buy(diagnostics.get("alpha6_side")):
        if str(spec.get("strategy_id") or "") == ETH_F3_DOMINANT_STRATEGY_ID:
            return ETH_F3_ALPHA6_NOT_BUY_REASON
        return "alpha6_not_buy"
    min_alpha6 = _normalize_float(spec.get("min_alpha6_score"))
    alpha6_score = _normalize_float(diagnostics.get("alpha6_score"))
    if min_alpha6 is not None and (alpha6_score is None or alpha6_score < min_alpha6):
        return "alpha6_score_below_threshold"
    if _spec_bool(spec, "require_expected_edge_gt_required", False):
        expected = _normalize_float(diagnostics.get("expected_edge_bps"))
        required = _normalize_float(diagnostics.get("required_edge_bps"))
        if expected is None or required is None or expected <= required:
            return "edge_not_above_required"
    if _spec_bool(spec, "require_cost_gate_verified", False):
        if _normalize_bool(diagnostics.get("cost_gate_verified")) is not True:
            return "cost_gate_not_verified"
    allowed_regimes = {
        _regime_text(value)
        for value in (spec.get("allowed_current_regimes") or [])
        if _regime_text(value)
    }
    if allowed_regimes and _regime_text(diagnostics.get("current_regime")) not in allowed_regimes:
        return "regime_not_allowed"
    min_f4 = _normalize_float(spec.get("min_f4_volume_expansion"))
    f4 = _normalize_float(diagnostics.get("f4_volume_expansion"))
    if min_f4 is not None and (f4 is None or f4 < min_f4):
        return "f4_below_threshold"
    if not source_or_reason_matched:
        return no_candidate_reason
    return ""


def _row_qualifies(
    row: Mapping[str, Any],
    *,
    spec: Mapping[str, Any],
    audit: DecisionAudit,
    asof_ts_ms: int,
) -> tuple[bool, str, dict[str, Any]]:
    diagnostics = _row_condition_diagnostics(row, spec=spec, audit=audit, asof_ts_ms=asof_ts_ms)
    source_or_reason_matched = _strategy_source_or_reason_matches(row, spec)
    reason = _condition_block_reason(
        diagnostics,
        spec=spec,
        source_or_reason_matched=source_or_reason_matched,
    )
    return bool(source_or_reason_matched and not reason), reason, diagnostics


def _matches_strategy(
    row: Mapping[str, Any],
    spec: Mapping[str, Any],
    *,
    audit: DecisionAudit,
    asof_ts_ms: int,
) -> tuple[bool, str, dict[str, Any]]:
    strategy_symbol = _strategy_symbol(spec)
    if _symbol_text(row.get("symbol")) != strategy_symbol:
        return False, _no_candidate_reason(spec), {}
    decision = str(row.get("final_decision") or "").strip().upper()
    if decision in {"OPEN_LONG", "REBALANCE"}:
        diagnostics = _row_condition_diagnostics(row, spec=spec, audit=audit, asof_ts_ms=asof_ts_ms)
        return False, _no_candidate_reason(spec), diagnostics
    qualifies, reason, diagnostics = _row_qualifies(row, spec=spec, audit=audit, asof_ts_ms=asof_ts_ms)
    risk_level = str(diagnostics.get("risk_level") or "").strip()
    if _spec_bool(spec, "require_protect_level", True) and risk_level and not _is_protect_level(risk_level):
        qualifies = False
    if diagnostics.get("risk_off"):
        qualifies = False
    if _spec_bool(spec, "require_no_cooldown", True) and diagnostics.get("cooldown_active"):
        qualifies = False
    min_f4 = _normalize_float(spec.get("min_f4_volume_expansion"))
    f4 = _normalize_float(diagnostics.get("f4_volume_expansion"))
    if min_f4 is not None and (f4 is None or f4 < min_f4):
        qualifies = False
    return bool(qualifies), reason, diagnostics


def _expanded_paper_advisory_record(
    *,
    spec: Mapping[str, Any],
    advisory: Mapping[str, Any],
    advisory_fields: Mapping[str, Any],
    audit: DecisionAudit,
    ts_utc: str,
    asof_ts_ms: int,
    rt_cost_bps: float,
    required_days: int,
    required_entry_days: int,
    required_coverage: float,
    allowed_cost_sources: set[str],
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    cached: dict[str, list[dict[str, float | int]]],
    top_of_book: Mapping[str, Any] | None,
) -> Optional[dict[str, Any]]:
    if not _spec_bool(spec, "expanded_universe_paper_only", False):
        return None
    if not advisory:
        return None
    if _expanded_universe_type(advisory) != "expanded_paper":
        return None
    if str(advisory_fields.get("advisory_response_action") or "") != "paper_tracking":
        return None
    symbol = _strategy_symbol(spec)
    if _symbol_text(advisory.get("symbol")) != symbol:
        return None
    required_maturity = str(spec.get("required_expanded_universe_maturity_state") or "").strip().upper()
    maturity = str(
        advisory.get("expanded_universe_maturity_state")
        or advisory.get("maturity_state")
        or advisory.get("decision")
        or ""
    ).strip().upper()
    if required_maturity and maturity != required_maturity:
        return None
    if _spec_bool(spec, "require_zero_live_notional", False):
        live_notional = _normalize_float(advisory.get("max_live_notional_usdt"))
        if live_notional is not None and live_notional > 0.0:
            return None
    cost_source = str(advisory.get("cost_source") or advisory.get("latest_cost_source") or "local_estimate").strip()
    if _spec_bool(spec, "require_cost_source_not_global_default", False):
        if "global_default" in cost_source.lower():
            return None
    estimated_cost = _normalize_float(
        advisory.get("estimated_cost_bps")
        or advisory.get("cost_bps")
        or advisory.get("selected_total_cost_bps")
    )
    if estimated_cost is None:
        estimated_cost = float(rt_cost_bps)
    entry_px = _entry_px(
        symbol=symbol,
        entry_ts_ms=asof_ts_ms,
        market_data_1h=market_data_1h,
        cache_dir=cache_dir,
        cached=cached,
    )
    quote_context = _quote_context(
        symbol=symbol,
        row=advisory,
        top_of_book=top_of_book,
        entry_px=entry_px,
    )
    primary_horizon = _primary_horizon_for_spec(spec, DEFAULT_HORIZONS)
    would_size = _normalize_float(advisory.get("max_paper_notional_usdt"))
    return {
        "strategy_id": str(spec.get("strategy_id") or ""),
        "experiment_name": str(spec.get("experiment_name") or ""),
        "enabled_shadow_only": True,
        "enable_live_experiment": False,
        "live_symbols_unchanged": True,
        "run_id": str(getattr(audit, "run_id", "") or advisory.get("run_id") or ""),
        "ts_utc": ts_utc,
        "entry_ts_ms": asof_ts_ms,
        "paper_date": ts_utc[:10],
        "symbol": symbol,
        "source_strategy_candidate": str(advisory.get("strategy_candidate") or ""),
        "candidate_id": str(advisory.get("candidate_id") or f"expanded_paper_{spec.get('strategy_id')}_{getattr(audit, 'run_id', '')}"),
        "final_decision": "paper_advisory",
        "no_sample_reason": "",
        "sol_candidate_present": True,
        "risk_level": str(advisory.get("risk_level") or ""),
        "original_block_reason": str(advisory.get("live_block_reasons") or advisory.get("advisory_reason") or ""),
        "cooldown_active": False,
        "risk_off": False,
        "skip_reason": "",
        "entry_reason": str(spec.get("experiment_name") or "expanded_universe_paper"),
        "experiment_reason": "expanded_universe_paper_tracking",
        "would_enter": True,
        "would_exit": False,
        "would_exit_time": _iso_from_ms(asof_ts_ms + primary_horizon * 3600 * 1000) if asof_ts_ms > 0 else "",
        "would_exit_rule": f"paper_time_horizon_{primary_horizon}h",
        "expected_exit_horizon": f"{primary_horizon}h",
        "would_size_notional": would_size,
        "would_size_usdt": would_size,
        "entry_px": entry_px,
        "arrival_bid": quote_context.get("arrival_bid"),
        "arrival_ask": quote_context.get("arrival_ask"),
        "arrival_mid": quote_context.get("arrival_mid"),
        "estimated_spread_bps": quote_context.get("estimated_spread_bps"),
        "expected_order_type": quote_context.get("expected_order_type"),
        "estimated_fill_px": quote_context.get("estimated_fill_px"),
        "final_score": None,
        "alpha6_score": _normalize_float(advisory.get("alpha6_score")),
        "alpha6_side": str(advisory.get("alpha6_side") or ""),
        "f4_volume_expansion": _normalize_float(advisory.get("f4_volume_expansion")),
        "f4_threshold": None,
        "f5_rsi_trend_confirm": _normalize_float(advisory.get("f5_rsi_trend_confirm")),
        "cost_source": cost_source,
        "cost_source_quality": str(advisory.get("cost_source_quality") or advisory.get("cost_quality") or cost_source),
        "estimated_cost_bps": float(estimated_cost),
        "cost_model_version": str(advisory.get("cost_model_version") or ""),
        "cost_source_live_ready": _cost_source_live_ready({"cost_source": cost_source}, allowed_cost_sources),
        "slippage_covered": _slippage_observed(quote_context),
        "required_paper_days": required_days,
        "required_entry_days": required_entry_days,
        "required_slippage_coverage": required_coverage,
        "rt_cost_bps": rt_cost_bps,
        "primary_horizon_hours": primary_horizon,
        "extra_live_block_reasons": list(spec.get("extra_live_block_reasons") or []),
        "live_order_effect": "read_only_no_live_order",
        "proposal_present": False,
        "proposal_source": "",
        **advisory_fields,
        "label_status": "pending",
        "label_not_observable_reason": "",
    }


def _best_candidate_for_strategy(
    *,
    candidate_rows: Iterable[Mapping[str, Any]],
    spec: Mapping[str, Any],
    audit: DecisionAudit,
    asof_ts_ms: int,
) -> tuple[Optional[Mapping[str, Any]], dict[str, Any], str]:
    strategy_symbol = _strategy_symbol(spec)
    strategy_rows = [row for row in candidate_rows if isinstance(row, Mapping) and _symbol_text(row.get("symbol")) == strategy_symbol]
    no_candidate_reason = _no_candidate_reason(spec)
    if not strategy_rows:
        return None, {"sol_candidate_present": False}, no_candidate_reason
    ranked: list[tuple[int, float, Mapping[str, Any], dict[str, Any], str]] = []
    for row in strategy_rows:
        diagnostics = _row_condition_diagnostics(row, spec=spec, audit=audit, asof_ts_ms=asof_ts_ms)
        source_match = _strategy_source_or_reason_matches(row, spec)
        reason = _condition_block_reason(diagnostics, spec=spec, source_or_reason_matched=source_match)
        if not reason:
            reason = no_candidate_reason
        score = _normalize_float(row.get("final_score"))
        ranked.append((1 if source_match else 0, float(score or 0.0), row, diagnostics, reason))
    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    _match_rank, _score, row, diagnostics, reason = ranked[0]
    return row, diagnostics, reason


def _heartbeat_record(
    *,
    spec: Mapping[str, Any],
    audit: DecisionAudit,
    ts_utc: str,
    asof_ts_ms: int,
    rt_cost_bps: float,
    required_days: int,
    required_entry_days: int,
    required_coverage: float,
    cost_context: Mapping[str, Any],
    allowed_cost_sources: set[str],
    condition_diagnostics: Mapping[str, Any] | None = None,
    no_sample_reason: str = "no_sol_candidate",
    quote_context: Mapping[str, Any] | None = None,
    advisory_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    condition_diagnostics = dict(condition_diagnostics or {})
    quote_context = dict(quote_context or {})
    advisory_fields = dict(advisory_fields or {})
    symbol = _strategy_symbol(spec)
    cost_source = str(cost_context.get("cost_source") or "local_estimate")
    diagnostic_cost_source = str(condition_diagnostics.get("cost_source") or "")
    if diagnostic_cost_source:
        cost_source = diagnostic_cost_source
    row_for_cost = {"cost_source": cost_source}
    live_ready = _cost_source_live_ready(row_for_cost, allowed_cost_sources)
    original_block_reason = str(
        condition_diagnostics.get("original_block_reason")
        or no_sample_reason
        or "no_sol_candidate"
    )
    return {
        "strategy_id": str(spec.get("strategy_id") or ""),
        "experiment_name": str(spec.get("experiment_name") or ""),
        "enabled_shadow_only": True,
        "enable_live_experiment": False,
        "live_symbols_unchanged": True,
        "run_id": str(getattr(audit, "run_id", "") or ""),
        "ts_utc": ts_utc,
        "entry_ts_ms": asof_ts_ms,
        "paper_date": ts_utc[:10],
        "symbol": symbol,
        "source_strategy_candidate": "heartbeat",
        "candidate_id": f"heartbeat_{spec.get('strategy_id')}_{getattr(audit, 'run_id', '')}",
        "final_decision": "heartbeat",
        "no_sample_reason": no_sample_reason,
        "sol_candidate_present": bool(condition_diagnostics.get("sol_candidate_present", False)),
        "risk_level": str(condition_diagnostics.get("risk_level") or ""),
        "original_block_reason": original_block_reason,
        "cooldown_active": bool(condition_diagnostics.get("cooldown_active", False)),
        "risk_off": bool(condition_diagnostics.get("risk_off", False)),
        "skip_reason": no_sample_reason,
        "entry_reason": "paper_strategy_heartbeat",
        "experiment_reason": "paper_strategy_heartbeat",
        "would_enter": False,
        "would_exit": False,
        "would_exit_time": "",
        "would_exit_rule": "",
        "expected_exit_horizon": "",
        "would_size_notional": None,
        "would_size_usdt": None,
        "entry_px": None,
        "arrival_bid": quote_context.get("arrival_bid"),
        "arrival_ask": quote_context.get("arrival_ask"),
        "arrival_mid": quote_context.get("arrival_mid"),
        "estimated_spread_bps": quote_context.get("estimated_spread_bps"),
        "expected_order_type": quote_context.get("expected_order_type") or "paper_market_buy",
        "estimated_fill_px": quote_context.get("estimated_fill_px"),
        "final_score": None,
        "alpha6_score": condition_diagnostics.get("alpha6_score"),
        "alpha6_side": str(condition_diagnostics.get("alpha6_side") or ""),
        "f4_volume_expansion": condition_diagnostics.get("f4_volume_expansion"),
        "f4_threshold": condition_diagnostics.get("f4_threshold"),
        "f5_rsi_trend_confirm": condition_diagnostics.get("f5_rsi_trend_confirm"),
        "cost_source": cost_source,
        "cost_source_quality": str(
            condition_diagnostics.get("cost_source_quality")
            or cost_context.get("cost_source_quality")
            or cost_source
        ),
        "estimated_cost_bps": float(
            condition_diagnostics.get("estimated_cost_bps")
            or cost_context.get("estimated_cost_bps")
            or rt_cost_bps
        ),
        "cost_model_version": str(condition_diagnostics.get("cost_model_version") or cost_context.get("cost_model_version") or ""),
        "cost_source_live_ready": live_ready,
        "slippage_covered": _slippage_observed(quote_context),
        "required_paper_days": required_days,
        "required_entry_days": required_entry_days,
        "required_slippage_coverage": required_coverage,
        "rt_cost_bps": rt_cost_bps,
        "primary_horizon_hours": _parse_horizon_hours(spec.get("primary_horizon_hours")),
        "extra_live_block_reasons": list(spec.get("extra_live_block_reasons") or []),
        "live_order_effect": "read_only_no_live_order",
        "proposal_present": bool(spec.get("proposal_present", False)),
        "proposal_source": str(spec.get("proposal_source") or ""),
        **advisory_fields,
        "label_status": "heartbeat",
        "label_not_observable_reason": "",
    }


def _collect_candidates(
    *,
    candidate_rows: Iterable[Mapping[str, Any]],
    audit: DecisionAudit,
    cfg: AppConfig,
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    top_of_book: Mapping[str, Any] | None = None,
    advisory_by_strategy: Mapping[str, Mapping[str, Any]] | None = None,
    advisory_rows: Iterable[Mapping[str, Any]] | None = None,
    proposal_rows: Iterable[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    diagnostics = _diagnostics_cfg(cfg)
    enabled_shadow_only = bool(getattr(diagnostics, "paper_strategy_enabled_shadow_only", True))
    enable_live_experiment = bool(getattr(diagnostics, "paper_strategy_enable_live_experiment", False))
    if not enabled_shadow_only:
        return []
    horizons = _horizons(diagnostics)
    rt_cost_bps = float(getattr(diagnostics, "paper_strategy_rt_cost_bps", 30.0) or 30.0)
    required_days = int(getattr(diagnostics, "paper_strategy_required_paper_days", 14) or 14)
    required_entry_days = int(getattr(diagnostics, "paper_strategy_required_entry_days", 3) or 3)
    required_coverage = float(getattr(diagnostics, "paper_strategy_required_slippage_coverage", 0.8) or 0.8)
    allowed_cost_sources = {
        str(item or "").strip().lower()
        for item in (
            getattr(diagnostics, "paper_strategy_live_ready_cost_sources", None)
            or sorted(LIVE_SMALL_READY_COST_SOURCES)
        )
        if str(item or "").strip()
    }
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    ts_utc = _iso_from_ms(asof_ts_ms) if asof_ts_ms > 0 else ""
    cached: dict[str, list[dict[str, float | int]]] = {}
    records: list[dict[str, Any]] = []
    matched_strategy_ids: set[str] = set()
    specs = _strategy_configs_with_proposals(diagnostics, proposal_rows or [])
    for row in candidate_rows:
        if not isinstance(row, Mapping):
            continue
        for spec in specs:
            advisory = _advisory_for_spec(spec, advisory_by_strategy)
            advisory_fields = _advisory_response_fields(advisory, diagnostics)
            if bool(advisory_fields.get("advisory_negative")):
                continue
            matched, _no_sample_reason, condition_diagnostics = _matches_strategy(
                row,
                spec,
                audit=audit,
                asof_ts_ms=asof_ts_ms,
            )
            if not matched:
                continue
            matched_strategy_ids.add(str(spec.get("strategy_id") or ""))
            symbol = _symbol_text(row.get("symbol"))
            entry_px = _entry_px(
                symbol=symbol,
                entry_ts_ms=asof_ts_ms,
                market_data_1h=market_data_1h,
                cache_dir=cache_dir,
                cached=cached,
            )
            source_strategy = str(row.get("strategy_candidate") or "").strip()
            primary_horizon = _primary_horizon_for_spec(spec, horizons)
            would_size = _would_size_notional(row, audit)
            cost_source = str(row.get("cost_source") or "")
            live_ready = _cost_source_live_ready(row, allowed_cost_sources)
            estimated_cost = _estimated_cost_bps(row, rt_cost_bps)
            quote_context = _quote_context(
                symbol=symbol,
                row=row,
                top_of_book=top_of_book,
                entry_px=entry_px,
            )
            records.append(
                {
                    "strategy_id": str(spec.get("strategy_id") or ""),
                    "experiment_name": str(spec.get("experiment_name") or ""),
                    "enabled_shadow_only": enabled_shadow_only,
                    "enable_live_experiment": enable_live_experiment,
                    "live_symbols_unchanged": True,
                    "run_id": str(row.get("run_id") or getattr(audit, "run_id", "") or ""),
                    "ts_utc": ts_utc,
                    "entry_ts_ms": asof_ts_ms,
                    "paper_date": ts_utc[:10],
                    "symbol": symbol,
                    "source_strategy_candidate": source_strategy,
                    "candidate_id": str(row.get("candidate_id") or ""),
                    "final_decision": str(row.get("final_decision") or ""),
                    "no_sample_reason": "",
                    "sol_candidate_present": bool(condition_diagnostics.get("sol_candidate_present", True)),
                    "risk_level": str(condition_diagnostics.get("risk_level") or ""),
                    "original_block_reason": str(row.get("block_reason") or row.get("no_signal_reason") or ""),
                    "cooldown_active": bool(condition_diagnostics.get("cooldown_active", False)),
                    "risk_off": bool(condition_diagnostics.get("risk_off", False)),
                    "skip_reason": str(row.get("block_reason") or row.get("no_signal_reason") or ""),
                    "entry_reason": str(spec.get("experiment_name") or source_strategy or "sol_paper_strategy"),
                    "experiment_reason": "sol_paper_strategy_tracking",
                    "would_enter": True,
                    "would_exit": False,
                    "would_exit_time": _iso_from_ms(asof_ts_ms + primary_horizon * 3600 * 1000) if asof_ts_ms > 0 else "",
                    "would_exit_rule": f"paper_time_horizon_{primary_horizon}h",
                    "expected_exit_horizon": f"{primary_horizon}h",
                    "would_size_notional": would_size,
                    "would_size_usdt": would_size,
                    "entry_px": entry_px,
                    "arrival_bid": quote_context.get("arrival_bid"),
                    "arrival_ask": quote_context.get("arrival_ask"),
                    "arrival_mid": quote_context.get("arrival_mid"),
                    "estimated_spread_bps": quote_context.get("estimated_spread_bps"),
                    "expected_order_type": quote_context.get("expected_order_type"),
                    "estimated_fill_px": quote_context.get("estimated_fill_px"),
                    "final_score": _normalize_float(row.get("final_score")),
                    "alpha6_score": _normalize_float(row.get("alpha6_score")),
                    "alpha6_side": str(condition_diagnostics.get("alpha6_side") or row.get("alpha6_side") or ""),
                    "f4_volume_expansion": _normalize_float(row.get("f4_volume_expansion")),
                    "f4_threshold": condition_diagnostics.get("f4_threshold"),
                    "f5_rsi_trend_confirm": _normalize_float(row.get("f5_rsi_trend_confirm")),
                    "cost_source": cost_source,
                    "cost_source_quality": str(row.get("cost_source_quality") or ""),
                    "estimated_cost_bps": estimated_cost,
                    "cost_model_version": str(row.get("cost_model_version") or ""),
                    "cost_source_live_ready": live_ready,
                    "slippage_covered": _slippage_observed(quote_context),
                    "required_paper_days": required_days,
                    "required_entry_days": required_entry_days,
                    "required_slippage_coverage": required_coverage,
                    "rt_cost_bps": rt_cost_bps,
                    "primary_horizon_hours": primary_horizon,
                    "extra_live_block_reasons": list(spec.get("extra_live_block_reasons") or []),
                    "live_order_effect": "read_only_no_live_order",
                    "proposal_present": bool(spec.get("proposal_present", False)),
                    "proposal_source": str(spec.get("proposal_source") or ""),
                    **advisory_fields,
                    "label_status": "pending",
                    "label_not_observable_reason": "",
                }
            )
    for spec in specs:
        strategy_id = str(spec.get("strategy_id") or "")
        if strategy_id in matched_strategy_ids:
            continue
        strategy_symbol = _strategy_symbol(spec)
        cost_context = _cost_context_for_symbol(
            symbol=strategy_symbol,
            candidate_rows=candidate_rows,
            fallback_bps=rt_cost_bps,
        )
        advisory = _advisory_for_spec(spec, advisory_by_strategy)
        advisory_fields = _advisory_response_fields(advisory, diagnostics)
        expanded_record = _expanded_paper_advisory_record(
            spec=spec,
            advisory=advisory,
            advisory_fields=advisory_fields,
            audit=audit,
            ts_utc=ts_utc,
            asof_ts_ms=asof_ts_ms,
            rt_cost_bps=rt_cost_bps,
            required_days=required_days,
            required_entry_days=required_entry_days,
            required_coverage=required_coverage,
            allowed_cost_sources=allowed_cost_sources,
            market_data_1h=market_data_1h,
            cache_dir=cache_dir,
            cached=cached,
            top_of_book=top_of_book,
        )
        if expanded_record is not None:
            expanded_record["enabled_shadow_only"] = enabled_shadow_only
            expanded_record["enable_live_experiment"] = enable_live_experiment
            records.append(expanded_record)
            continue
        best_row, condition_diagnostics, no_sample_reason = _best_candidate_for_strategy(
            candidate_rows=candidate_rows,
            spec=spec,
            audit=audit,
            asof_ts_ms=asof_ts_ms,
        )
        if bool(advisory_fields.get("advisory_negative")):
            no_sample_reason = "quant_lab_advisory_kill"
        quote_context = _quote_context(
            symbol=strategy_symbol,
            row=best_row or {},
            top_of_book=top_of_book,
            entry_px=None,
        )
        heartbeat = _heartbeat_record(
            spec=spec,
            audit=audit,
            ts_utc=ts_utc,
            asof_ts_ms=asof_ts_ms,
            rt_cost_bps=rt_cost_bps,
            required_days=required_days,
            required_entry_days=required_entry_days,
            required_coverage=required_coverage,
            cost_context=cost_context,
            allowed_cost_sources=allowed_cost_sources,
            condition_diagnostics=condition_diagnostics,
            no_sample_reason=no_sample_reason,
            quote_context=quote_context,
            advisory_fields=advisory_fields,
        )
        heartbeat["enabled_shadow_only"] = enabled_shadow_only
        heartbeat["enable_live_experiment"] = enable_live_experiment
        records.append(heartbeat)
    records.extend(
        _bottom_zone_paper_records(
            advisory_rows or [],
            diagnostics=diagnostics,
            audit=audit,
            ts_utc=ts_utc,
            asof_ts_ms=asof_ts_ms,
            rt_cost_bps=rt_cost_bps,
            required_days=required_days,
            required_entry_days=required_entry_days,
            required_coverage=required_coverage,
            allowed_cost_sources=allowed_cost_sources,
            market_data_1h=market_data_1h,
            cache_dir=cache_dir,
            cached=cached,
            top_of_book=top_of_book,
        )
    )
    return records


def _sync_paper_fields(record: dict[str, Any], horizons: Iterable[int]) -> None:
    if not _paper_would_enter(record):
        reason = str(record.get("no_sample_reason") or record.get("skip_reason") or "no_sol_candidate")
        for horizon in horizons:
            h = int(horizon)
            record[f"paper_pnl_bps_{h}h"] = None
            record[f"paper_pnl_usdt_{h}h"] = None
            record[f"{HORIZON_PREFIX}{h}h_status"] = "heartbeat"
            record[f"{HORIZON_PREFIX}{h}h_reason"] = reason
        record["paper_pnl_bps"] = None
        record["paper_pnl_usdt"] = None
        record["would_exit"] = False
        record["label_status"] = "heartbeat"
        record["label_not_observable_reason"] = ""
        return

    size = _normalize_float(record.get("would_size_notional"))
    primary = _record_primary_horizon(record, horizons)
    for horizon in horizons:
        h = int(horizon)
        net_key = f"{HORIZON_PREFIX}{h}h_net_bps"
        status_key = f"{HORIZON_PREFIX}{h}h_status"
        net = _normalize_float(record.get(net_key))
        record[f"paper_pnl_bps_{h}h"] = net
        record[f"paper_pnl_usdt_{h}h"] = round(float(net) * float(size) / 10_000.0, 8) if net is not None and size is not None else None
        if net is not None:
            record[status_key] = "complete"
    primary_net = _normalize_float(record.get(f"paper_pnl_bps_{primary}h"))
    record["paper_pnl_bps"] = primary_net
    record["paper_pnl_usdt"] = (
        round(float(primary_net) * float(size) / 10_000.0, 8)
        if primary_net is not None and size is not None
        else None
    )
    primary_status = str(record.get(f"{HORIZON_PREFIX}{primary}h_status") or "")
    record["would_exit"] = primary_status == "complete"
    if primary_status == "complete":
        record["label_status"] = "complete"
        record["label_not_observable_reason"] = ""


def _row_for_csv(record: Mapping[str, Any], horizons: Iterable[int]) -> dict[str, Any]:
    fields = list(PAPER_RUN_FIELDS)
    for horizon in horizons:
        h = int(horizon)
        for field in (
            f"paper_pnl_bps_{h}h",
            f"paper_pnl_usdt_{h}h",
            f"{HORIZON_PREFIX}{h}h_status",
            f"{HORIZON_PREFIX}{h}h_reason",
        ):
            if field not in fields:
                fields.append(field)
    return {field: record.get(field) for field in fields}


def _cost_source_mix(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        source = str(row.get("cost_source") or "missing").strip().lower() or "missing"
        counts[source] += 1
    return dict(sorted(counts.items()))


def _coverage_ratio(rows: list[dict[str, Any]], predicate: Any) -> float:
    return round(
        float(sum(1 for row in rows if predicate(row))) / float(len(rows)),
        6,
    ) if rows else 0.0


def _reason_items(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [str(item or "").strip() for item in parsed if str(item or "").strip()]
    return [item.strip() for item in text.replace(",", ";").split(";") if item.strip()]


def _horizon_pnl_values(rows: Iterable[Mapping[str, Any]], horizon: int) -> list[float]:
    return [
        value
        for value in (
            _normalize_float(row.get(f"paper_pnl_bps_{horizon}h"))
            for row in rows
            if _paper_would_enter(row)
        )
        if value is not None
    ]


def _avg(values: list[float]) -> Optional[float]:
    return round(sum(values) / len(values), 6) if values else None


def _is_eth_f3_rows(rows: list[dict[str, Any]]) -> bool:
    strategy_ids = {str(row.get("strategy_id") or "") for row in rows}
    return ETH_F3_DOMINANT_STRATEGY_ID in strategy_ids


def _eth_f3_48h_stats(rows: list[dict[str, Any]]) -> tuple[int, Optional[float]]:
    values = _horizon_pnl_values(rows, 48)
    return len(values), _avg(values)


def _readiness_for_rows(
    rows: list[dict[str, Any]],
    *,
    required_days: int,
    required_entry_days: int,
    required_coverage: float,
    enable_live_experiment: bool,
    allowed_cost_sources: set[str],
) -> dict[str, Any]:
    paper_days = len({str(row.get("paper_date") or "") for row in rows if str(row.get("paper_date") or "")})
    entry_days = len(
        {
            str(row.get("paper_date") or "")
            for row in rows
            if str(row.get("paper_date") or "") and _paper_would_enter(row)
        }
    )
    cost_mix = _cost_source_mix(rows)
    has_live_ready_cost = any(source in allowed_cost_sources for source in cost_mix)
    has_global_default = "global_default" in cost_mix
    slippage_covered = [row for row in rows if _slippage_observed(row)]
    coverage = float(len(slippage_covered)) / float(len(rows)) if rows else 0.0
    coverage = round(coverage, 6)
    arrival_mid_coverage = _coverage_ratio(
        rows,
        lambda row: (_normalize_float(row.get("arrival_mid")) or 0.0) > 0.0,
    )
    spread_observation_coverage = _coverage_ratio(
        rows,
        lambda row: _normalize_float(row.get("estimated_spread_bps")) is not None,
    )
    reasons: list[str] = []
    if paper_days < int(required_days):
        reasons.append("no_paper_days")
    if entry_days < int(required_entry_days):
        reasons.append("insufficient_entry_days")
    if has_global_default:
        reasons.append("cost_source_global_default")
    if not has_live_ready_cost and coverage < float(required_coverage):
        reasons.append("cost_source_not_actual_or_mixed")
    if arrival_mid_coverage < float(required_coverage):
        reasons.append("arrival_mid_coverage_insufficient")
    if spread_observation_coverage < float(required_coverage):
        reasons.append("spread_observation_coverage_insufficient")
    if coverage < float(required_coverage):
        reasons.append("no_live_slippage_coverage")
    for row in rows:
        for reason in _reason_items(row.get("extra_live_block_reasons")):
            if reason not in reasons:
                reasons.append(reason)
    eth_f3_paper_only = _is_eth_f3_rows(rows)
    eth_f3_48h_complete_count, eth_f3_48h_avg = _eth_f3_48h_stats(rows) if eth_f3_paper_only else (0, None)
    keep_shadow = bool(eth_f3_paper_only and eth_f3_48h_avg is not None and eth_f3_48h_avg < 0.0)
    if eth_f3_paper_only:
        if "eth_f3_paper_only_no_live" not in reasons:
            reasons.append("eth_f3_paper_only_no_live")
        if eth_f3_48h_complete_count < ETH_F3_DOMINANT_MIN_48H_COMPLETE_COUNT:
            if "eth_f3_waiting_for_48h_complete_samples" not in reasons:
                reasons.append("eth_f3_waiting_for_48h_complete_samples")
        elif eth_f3_48h_avg is not None and eth_f3_48h_avg > 0.0:
            if "eth_f3_48h_positive_continue_paper" not in reasons:
                reasons.append("eth_f3_48h_positive_continue_paper")
        else:
            if "eth_f3_48h_not_positive_continue_paper" not in reasons:
                reasons.append("eth_f3_48h_not_positive_continue_paper")
        if keep_shadow and "eth_f3_negative_48h_paper_pnl" not in reasons:
            reasons.append("eth_f3_negative_48h_paper_pnl")
    rules_pass = not reasons
    if rules_pass and not enable_live_experiment:
        reasons.append("live_experiment_disabled")
    live_small_ready = bool(rules_pass and enable_live_experiment and not keep_shadow and not eth_f3_paper_only)
    if keep_shadow:
        status = "KEEP_SHADOW"
    elif eth_f3_paper_only:
        status = "PAPER_READY"
    else:
        status = "LIVE_SMALL_READY" if live_small_ready else "PAPER_READY"
    return {
        "live_small_ready": live_small_ready,
        "readiness_status": status,
        "live_block_reason": ";".join(reasons),
        "paper_days": paper_days,
        "entry_day_count": entry_days,
        "slippage_coverage": coverage,
        "slippage_covered_rows": len(slippage_covered),
        "arrival_mid_coverage": arrival_mid_coverage,
        "spread_observation_coverage": spread_observation_coverage,
        "cost_source_mix": json.dumps(cost_mix, sort_keys=True),
    }


def _annotate_readiness(
    records: list[dict[str, Any]],
    *,
    diagnostics: DiagnosticsConfig,
) -> list[dict[str, Any]]:
    required_days = int(getattr(diagnostics, "paper_strategy_required_paper_days", 14) or 14)
    required_entry_days = int(getattr(diagnostics, "paper_strategy_required_entry_days", 3) or 3)
    required_coverage = float(getattr(diagnostics, "paper_strategy_required_slippage_coverage", 0.8) or 0.8)
    enable_live_experiment = bool(getattr(diagnostics, "paper_strategy_enable_live_experiment", False))
    allowed_cost_sources = {
        str(item or "").strip().lower()
        for item in (
            getattr(diagnostics, "paper_strategy_live_ready_cost_sources", None)
            or sorted(LIVE_SMALL_READY_COST_SOURCES)
        )
        if str(item or "").strip()
    }
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(str(record.get("strategy_id") or ""), str(record.get("symbol") or ""))].append(record)
    for rows in grouped.values():
        readiness = _readiness_for_rows(
            rows,
            required_days=required_days,
            required_entry_days=required_entry_days,
            required_coverage=required_coverage,
            enable_live_experiment=enable_live_experiment,
            allowed_cost_sources=allowed_cost_sources,
        )
        for row in rows:
            row["live_small_ready"] = readiness["live_small_ready"]
            row["readiness_status"] = readiness["readiness_status"]
            row["live_block_reason"] = readiness["live_block_reason"]
            row["required_entry_days"] = row.get("required_entry_days") or required_entry_days
    return records


def _daily_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_strategy_days: dict[tuple[str, str], set[str]] = defaultdict(set)
    entry_days_by_strategy: dict[tuple[str, str], set[str]] = defaultdict(set)
    observed_days_by_horizon: dict[tuple[str, str, int], set[str]] = defaultdict(set)
    for record in records:
        strategy_id = str(record.get("strategy_id") or "")
        symbol = str(record.get("symbol") or "")
        paper_date = str(record.get("paper_date") or "")
        by_strategy_days[(strategy_id, symbol)].add(paper_date)
        if _paper_would_enter(record) and paper_date:
            entry_days_by_strategy[(strategy_id, symbol)].add(paper_date)
            for horizon in DEFAULT_HORIZONS:
                if _normalize_float(record.get(f"paper_pnl_bps_{horizon}h")) is not None:
                    observed_days_by_horizon[(strategy_id, symbol, horizon)].add(paper_date)
    buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        buckets[
            (
                str(record.get("paper_date") or ""),
                str(record.get("strategy_id") or ""),
                str(record.get("symbol") or ""),
            )
        ].append(record)
    out: list[dict[str, Any]] = []
    for (paper_date, strategy_id, symbol), rows in sorted(buckets.items()):
        entry_rows = [row for row in rows if _paper_would_enter(row)]
        values = [_normalize_float(row.get("paper_pnl_bps")) for row in entry_rows]
        usable = [value for value in values if value is not None]
        horizon_values: dict[str, list[float]] = {}
        horizon_avgs: dict[str, float] = {}
        horizon_all_usable: list[float] = []
        horizon_observed_counts: dict[str, int] = {}
        horizon_complete_counts: dict[str, int] = {}
        horizon_day_counts: dict[str, int] = {}
        horizon_win_rates: dict[str, float] = {}
        for horizon in DEFAULT_HORIZONS:
            key = f"{horizon}h"
            values_for_horizon = _horizon_pnl_values(entry_rows, horizon)
            horizon_values[key] = values_for_horizon
            if values_for_horizon:
                horizon_avgs[key] = round(sum(values_for_horizon) / len(values_for_horizon), 6)
                horizon_win_rates[key] = round(
                    sum(1 for value in values_for_horizon if float(value) > 0.0) / len(values_for_horizon),
                    6,
                )
            horizon_all_usable.extend(values_for_horizon)
            horizon_observed_counts[key] = len(values_for_horizon)
            status_key = f"{HORIZON_PREFIX}{horizon}h_status"
            pnl_key = f"paper_pnl_bps_{horizon}h"
            horizon_complete_counts[key] = sum(
                1
                for row in entry_rows
                if str(row.get(status_key) or "").strip().lower() == "complete"
                or _normalize_float(row.get(pnl_key)) is not None
            )
            horizon_day_counts[key] = len(
                {
                    date
                    for date in observed_days_by_horizon[(strategy_id, symbol, horizon)]
                    if date and date <= paper_date
                }
            )
        effective_usable = usable or horizon_all_usable
        pnl_usdt = [_normalize_float(row.get("paper_pnl_usdt")) for row in entry_rows]
        pnl_usdt_usable = [value for value in pnl_usdt if value is not None]
        entry_day_count = len(
            {
                date
                for date in entry_days_by_strategy[(strategy_id, symbol)]
                if date and date <= paper_date
            }
        )
        daily_row = {
            "paper_date": paper_date,
            "strategy_id": strategy_id,
            "experiment_name": rows[0].get("experiment_name"),
            "symbol": symbol,
            "entry_count": len(entry_rows),
            "entry_day_count": entry_day_count,
            "complete_count": sum(1 for row in entry_rows if str(row.get("label_status") or "") == "complete"),
            "pending_count": sum(1 for row in entry_rows if str(row.get("label_status") or "") == "pending"),
            "not_observable_count": sum(1 for row in entry_rows if str(row.get("label_status") or "") == "not_observable"),
            "avg_paper_pnl_bps": round(sum(effective_usable) / len(effective_usable), 6) if effective_usable else None,
            "avg_paper_pnl_bps_by_horizon": json.dumps(horizon_avgs, sort_keys=True),
            "complete_count_by_horizon": json.dumps(horizon_complete_counts, sort_keys=True),
            "win_rate_by_horizon": json.dumps(horizon_win_rates, sort_keys=True),
            "paper_pnl_observed_count_by_horizon": json.dumps(horizon_observed_counts, sort_keys=True),
            "paper_pnl_day_count_by_horizon": json.dumps(horizon_day_counts, sort_keys=True),
            "paper_pnl_usdt_sum": round(sum(pnl_usdt_usable), 8) if pnl_usdt_usable else None,
            "win_rate": round(sum(1 for value in effective_usable if float(value) > 0.0) / len(effective_usable), 6) if effective_usable else None,
            "paper_days_to_date": len(
                {date for date in by_strategy_days[(strategy_id, symbol)] if date and date <= paper_date}
            ),
        }
        for horizon in DEFAULT_HORIZONS:
            daily_row[f"avg_paper_pnl_bps_{horizon}h"] = horizon_avgs.get(f"{horizon}h")
        out.append(daily_row)
    return out


def _slippage_rows(records: list[dict[str, Any]], diagnostics: DiagnosticsConfig) -> list[dict[str, Any]]:
    required_days = int(getattr(diagnostics, "paper_strategy_required_paper_days", 14) or 14)
    required_entry_days = int(getattr(diagnostics, "paper_strategy_required_entry_days", 3) or 3)
    required_coverage = float(getattr(diagnostics, "paper_strategy_required_slippage_coverage", 0.8) or 0.8)
    enable_live_experiment = bool(getattr(diagnostics, "paper_strategy_enable_live_experiment", False))
    allowed_cost_sources = {
        str(item or "").strip().lower()
        for item in (
            getattr(diagnostics, "paper_strategy_live_ready_cost_sources", None)
            or sorted(LIVE_SMALL_READY_COST_SOURCES)
        )
        if str(item or "").strip()
    }
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        buckets[(str(record.get("strategy_id") or ""), str(record.get("symbol") or ""))].append(record)
    out: list[dict[str, Any]] = []
    for (strategy_id, symbol), rows in sorted(buckets.items()):
        readiness = _readiness_for_rows(
            rows,
            required_days=required_days,
            required_entry_days=required_entry_days,
            required_coverage=required_coverage,
            enable_live_experiment=enable_live_experiment,
            allowed_cost_sources=allowed_cost_sources,
        )
        latest = sorted(rows, key=lambda row: str(row.get("ts_utc") or ""))[-1] if rows else {}
        out.append(
            {
                "strategy_id": strategy_id,
                "experiment_name": latest.get("experiment_name"),
                "symbol": symbol,
                "paper_days": readiness["paper_days"],
                "required_paper_days": required_days,
                "required_entry_days": required_entry_days,
                "total_rows": len(rows),
                "slippage_covered_rows": readiness["slippage_covered_rows"],
                "slippage_coverage": readiness["slippage_coverage"],
                "arrival_mid_coverage": readiness["arrival_mid_coverage"],
                "spread_observation_coverage": readiness["spread_observation_coverage"],
                "cost_source_mix": readiness["cost_source_mix"],
                "required_slippage_coverage": required_coverage,
                "latest_cost_source": latest.get("cost_source"),
                "allowed_live_cost_sources": ",".join(sorted(allowed_cost_sources)),
                "live_small_ready": readiness["live_small_ready"],
                "readiness_status": readiness["readiness_status"],
                "live_block_reason": readiness["live_block_reason"],
            }
        )
    return out


def _bnb_paper_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        record
        for record in records
        if str(record.get("strategy_id") or "") in {
            BNB_F3_DOMINANT_STRATEGY_ID,
            BNB_RISK_ON_BUY_STRATEGY_ID,
        }
    ]


def update_sol_paper_strategy_tracker(
    *,
    run_dir: str | Path,
    audit: DecisionAudit,
    market_data_1h: Dict[str, MarketSeries],
    cfg: AppConfig,
    cache_dir: str | Path | None = None,
    ohlcv_provider: Any = None,
    top_of_book: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    diagnostics = _diagnostics_cfg(cfg)
    enabled = bool(getattr(diagnostics, "paper_strategy_tracking_enabled", True))
    if not enabled:
        return {"enabled": False, "new_records": 0, "total_records": 0}

    run_path = Path(run_dir)
    reports_dir = _resolve_reports_dir(run_path)
    labels_path = _labels_path(reports_dir)
    summaries_dir = _summaries_dir(reports_dir)
    cache_root = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parents[2] / "data" / "cache"
    horizons = _horizons(diagnostics)
    candidate_rows = _read_candidate_snapshot(run_path / "candidate_snapshot.csv")
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    advisory_result = _read_strategy_opportunity_advisory_result(
        run_path=run_path,
        reports_dir=reports_dir,
        diagnostics=diagnostics,
        cfg=cfg,
        run_id=str(getattr(audit, "run_id", "") or ""),
        now_ms=asof_ts_ms,
    )
    advisory_rows = _normalize_selected_advisory_rows(
        advisory_result.rows,
        advisory_result.source_health_rows,
    )
    proposal_rows = _read_paper_strategy_proposals(
        run_path=run_path,
        reports_dir=reports_dir,
        diagnostics=diagnostics,
    )
    expanded_advisory_rows = _expanded_universe_advisory_rows(
        advisory_rows,
        diagnostics=diagnostics,
        cfg=cfg,
        run_id=str(getattr(audit, "run_id", "") or ""),
        asof_ts_ms=asof_ts_ms,
    )
    expanded_paper_rows = _expanded_universe_paper_rows(expanded_advisory_rows)
    alpha_factory_rows = _alpha_factory_advisory_rows(
        advisory_rows,
        run_id=str(getattr(audit, "run_id", "") or ""),
        asof_ts_ms=asof_ts_ms,
        source_health_rows=advisory_result.source_health_rows,
    )
    alpha_factory_family_rows = _alpha_factory_family_summary_rows(
        alpha_factory_rows,
        advisory_rows,
        run_id=str(getattr(audit, "run_id", "") or ""),
        asof_ts_ms=asof_ts_ms,
    )
    risk_on_multi_buy_rows = _risk_on_multi_buy_shadow_rows(
        advisory_rows,
        detail_rows=_read_risk_on_multi_buy_detail_rows(run_path=run_path, reports_dir=reports_dir),
        diagnostics=diagnostics,
        run_id=str(getattr(audit, "run_id", "") or ""),
        asof_ts_ms=asof_ts_ms,
        audit=audit,
        run_path=run_path,
        candidate_rows=candidate_rows,
    )
    advisory_index = _advisory_by_strategy(advisory_rows)
    records_by_key = _load_existing_records(labels_path)
    new_records = _collect_candidates(
        candidate_rows=candidate_rows,
        audit=audit,
        cfg=cfg,
        market_data_1h=market_data_1h,
        cache_dir=cache_root,
        top_of_book=top_of_book,
        advisory_by_strategy=advisory_index,
        advisory_rows=advisory_rows,
        proposal_rows=proposal_rows,
    )
    inserted = 0
    for record in new_records:
        key = _record_key(record)
        if key not in records_by_key:
            records_by_key[key] = record
            inserted += 1
        else:
            existing = records_by_key[key]
            for preserve_key, value in record.items():
                if existing.get(preserve_key) in (None, "") and value not in (None, ""):
                    existing[preserve_key] = value

    eth_f3_alpha6_gate_rewrites = 0
    for record in records_by_key.values():
        if _apply_eth_f3_alpha6_entry_gate(record):
            eth_f3_alpha6_gate_rewrites += 1

    records = list(records_by_key.values())
    if ohlcv_provider is None:
        ohlcv_provider = _default_ohlcv_provider_for_cfg(cfg)
    if records:
        labelable_records = [record for record in records if _paper_would_enter(record)]
        _update_labels(
            records=labelable_records,
            cache_dir=cache_root,
            horizons=horizons,
            market_data_1h=market_data_1h,
            asof_ts_ms=asof_ts_ms,
            ohlcv_provider=ohlcv_provider,
        )
        for record in records:
            _sync_paper_fields(record, horizons)
        records = _annotate_readiness(records, diagnostics=diagnostics)
        records.sort(
            key=lambda row: (
                _record_entry_ts_ms(row),
                str(row.get("strategy_id") or ""),
                str(row.get("run_id") or ""),
                str(row.get("symbol") or ""),
                str(row.get("candidate_id") or ""),
            )
        )
        _write_records(labels_path, records)

    run_rows = [_row_for_csv(record, horizons) for record in records]
    fields = list(PAPER_RUN_FIELDS)
    for horizon in horizons:
        h = int(horizon)
        for field in (
            f"paper_pnl_bps_{h}h",
            f"paper_pnl_usdt_{h}h",
            f"{HORIZON_PREFIX}{h}h_status",
            f"{HORIZON_PREFIX}{h}h_reason",
        ):
            if field not in fields:
                fields.append(field)
    _write_csv(summaries_dir / "paper_strategy_runs.csv", run_rows, fields)
    _write_csv(summaries_dir / "paper_strategy_daily.csv", _daily_rows(records), PAPER_DAILY_FIELDS)
    _write_csv(summaries_dir / "paper_slippage_coverage.csv", _slippage_rows(records, diagnostics), PAPER_SLIPPAGE_FIELDS)
    bnb_records = _bnb_paper_records(records)
    _write_csv(
        summaries_dir / "bnb_paper_strategy_runs.csv",
        [_row_for_csv(record, horizons) for record in bnb_records],
        fields,
    )
    _write_csv(
        summaries_dir / "bnb_paper_strategy_daily.csv",
        _daily_rows(bnb_records),
        PAPER_DAILY_FIELDS,
    )
    _write_csv(
        summaries_dir / "strategy_opportunity_advisory_reader.csv",
        _advisory_summary_rows(advisory_rows, diagnostics),
        STRATEGY_ADVISORY_FIELDS,
    )
    _write_csv(
        summaries_dir / "strategy_opportunity_advisory_source_health.csv",
        advisory_result.source_health_rows,
        STRATEGY_ADVISORY_SOURCE_HEALTH_FIELDS,
    )
    _write_csv(
        summaries_dir / "expanded_universe_advisory_reader.csv",
        expanded_advisory_rows,
        EXPANDED_UNIVERSE_ADVISORY_FIELDS,
    )
    _write_csv(
        summaries_dir / "expanded_universe_paper_runs.csv",
        expanded_paper_rows,
        EXPANDED_UNIVERSE_PAPER_RUN_FIELDS,
    )
    _write_csv(
        summaries_dir / "alpha_factory_advisory_reader.csv",
        alpha_factory_rows,
        ALPHA_FACTORY_ADVISORY_FIELDS,
    )
    _write_csv(
        summaries_dir / "alpha_factory_family_summary.csv",
        alpha_factory_family_rows,
        ALPHA_FACTORY_FAMILY_SUMMARY_FIELDS,
    )
    _write_csv(
        summaries_dir / "risk_on_multi_buy_shadow.csv",
        risk_on_multi_buy_rows,
        RISK_ON_MULTI_BUY_SHADOW_FIELDS,
    )

    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "advisory_rows": int(len(advisory_rows)),
        "strategy_opportunity_advisory_source_health_rows": int(len(advisory_result.source_health_rows)),
        "expanded_universe_advisory_rows": int(len(expanded_advisory_rows)),
        "expanded_universe_paper_rows": int(len(expanded_paper_rows)),
        "alpha_factory_advisory_rows": int(len(alpha_factory_rows)),
        "alpha_factory_family_rows": int(len(alpha_factory_family_rows)),
        "risk_on_multi_buy_shadow_rows": int(len(risk_on_multi_buy_rows)),
        "bnb_paper_strategy_rows": int(len(bnb_records)),
        "proposal_rows": int(len(proposal_rows)),
        "eth_f3_alpha6_gate_rewrites": int(eth_f3_alpha6_gate_rewrites),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
