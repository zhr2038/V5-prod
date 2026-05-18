from __future__ import annotations

import csv
import json
from collections import defaultdict
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
DEFAULT_HORIZONS = [4, 8, 12, 24, 48, 72]
PRIMARY_HORIZON = 24
LIVE_SMALL_READY_COST_SOURCES = {"actual_fills", "mixed_actual_proxy"}

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
]

PAPER_RUN_FIELDS = [
    "strategy_id",
    "experiment_name",
    "enabled_shadow_only",
    "enable_live_experiment",
    "run_id",
    "ts_utc",
    "paper_date",
    "symbol",
    "source_strategy_candidate",
    "candidate_id",
    "final_decision",
    "original_block_reason",
    "entry_reason",
    "experiment_reason",
    "would_enter",
    "would_exit",
    "would_exit_time",
    "would_exit_rule",
    "would_size_notional",
    "would_size_usdt",
    "paper_pnl_bps",
    "paper_pnl_usdt",
    "entry_px",
    "final_score",
    "alpha6_score",
    "f4_volume_expansion",
    "f5_rsi_trend_confirm",
    "cost_source",
    "cost_source_quality",
    "estimated_cost_bps",
    "cost_model_version",
    "cost_source_live_ready",
    "slippage_covered",
    "required_paper_days",
    "required_slippage_coverage",
    "live_small_ready",
    "readiness_status",
    "live_block_reason",
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
    "complete_count",
    "pending_count",
    "not_observable_count",
    "avg_paper_pnl_bps",
    "paper_pnl_usdt_sum",
    "win_rate",
    "paper_days_to_date",
]

PAPER_SLIPPAGE_FIELDS = [
    "strategy_id",
    "experiment_name",
    "symbol",
    "paper_days",
    "required_paper_days",
    "total_rows",
    "slippage_covered_rows",
    "slippage_coverage",
    "required_slippage_coverage",
    "latest_cost_source",
    "allowed_live_cost_sources",
    "live_small_ready",
    "readiness_status",
    "live_block_reason",
]


def _diagnostics_cfg(cfg: Any) -> DiagnosticsConfig:
    diagnostics = getattr(cfg, "diagnostics", None)
    return diagnostics if diagnostics is not None else DiagnosticsConfig()


def _labels_path(reports_dir: Path) -> Path:
    return reports_dir / "sol_paper_strategy_labels.jsonl"


def _symbol_text(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "/")


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


def _read_candidate_snapshot(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return [dict(row) for row in csv.DictReader(fh) if row]
    except Exception:
        return []


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


def _matches_strategy(row: Mapping[str, Any], spec: Mapping[str, Any]) -> bool:
    if _symbol_text(row.get("symbol")) != SOL_SYMBOL:
        return False
    decision = str(row.get("final_decision") or "").strip().upper()
    if decision in {"OPEN_LONG", "REBALANCE"}:
        return False
    strategy = str(row.get("strategy_candidate") or "").strip()
    block_reason = str(row.get("block_reason") or "").strip()
    source_candidates = set(spec.get("source_strategy_candidates") or set())
    allowed_reasons = set(spec.get("allowed_block_reasons") or set())
    strategy_matches = bool(strategy and strategy in source_candidates)
    reason_matches = bool(allowed_reasons and block_reason in allowed_reasons)
    if source_candidates and allowed_reasons and not (strategy_matches or reason_matches):
        return False
    if source_candidates and not allowed_reasons and not strategy_matches:
        return False
    if allowed_reasons and not source_candidates and not reason_matches:
        return False
    min_f4 = _normalize_float(spec.get("min_f4_volume_expansion"))
    f4 = _normalize_float(row.get("f4_volume_expansion"))
    if min_f4 is not None and (f4 is None or f4 < min_f4):
        return False
    return True


def _heartbeat_record(
    *,
    spec: Mapping[str, Any],
    audit: DecisionAudit,
    ts_utc: str,
    asof_ts_ms: int,
    rt_cost_bps: float,
    required_days: int,
    required_coverage: float,
    cost_context: Mapping[str, Any],
    allowed_cost_sources: set[str],
) -> dict[str, Any]:
    cost_source = str(cost_context.get("cost_source") or "local_estimate")
    row_for_cost = {"cost_source": cost_source}
    live_ready = _cost_source_live_ready(row_for_cost, allowed_cost_sources)
    return {
        "strategy_id": str(spec.get("strategy_id") or ""),
        "experiment_name": str(spec.get("experiment_name") or ""),
        "enabled_shadow_only": True,
        "enable_live_experiment": False,
        "run_id": str(getattr(audit, "run_id", "") or ""),
        "ts_utc": ts_utc,
        "entry_ts_ms": asof_ts_ms,
        "paper_date": ts_utc[:10],
        "symbol": SOL_SYMBOL,
        "source_strategy_candidate": "heartbeat",
        "candidate_id": f"heartbeat_{spec.get('strategy_id')}_{getattr(audit, 'run_id', '')}",
        "final_decision": "heartbeat",
        "original_block_reason": "no_qualifying_candidate",
        "skip_reason": "no_qualifying_candidate",
        "entry_reason": "paper_strategy_heartbeat",
        "experiment_reason": "sol_paper_strategy_heartbeat",
        "would_enter": False,
        "would_exit": False,
        "would_exit_time": "",
        "would_exit_rule": "",
        "would_size_notional": None,
        "would_size_usdt": None,
        "entry_px": None,
        "final_score": None,
        "alpha6_score": None,
        "f4_volume_expansion": None,
        "f5_rsi_trend_confirm": None,
        "cost_source": cost_source,
        "cost_source_quality": str(cost_context.get("cost_source_quality") or cost_source),
        "estimated_cost_bps": float(cost_context.get("estimated_cost_bps") or rt_cost_bps),
        "cost_model_version": str(cost_context.get("cost_model_version") or ""),
        "cost_source_live_ready": live_ready,
        "slippage_covered": live_ready,
        "required_paper_days": required_days,
        "required_slippage_coverage": required_coverage,
        "rt_cost_bps": rt_cost_bps,
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
) -> list[dict[str, Any]]:
    diagnostics = _diagnostics_cfg(cfg)
    enabled_shadow_only = bool(getattr(diagnostics, "paper_strategy_enabled_shadow_only", True))
    enable_live_experiment = bool(getattr(diagnostics, "paper_strategy_enable_live_experiment", False))
    if not enabled_shadow_only:
        return []
    horizons = _horizons(diagnostics)
    rt_cost_bps = float(getattr(diagnostics, "paper_strategy_rt_cost_bps", 30.0) or 30.0)
    required_days = int(getattr(diagnostics, "paper_strategy_required_paper_days", 14) or 14)
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
    specs = _strategy_configs(diagnostics)
    for row in candidate_rows:
        if not isinstance(row, Mapping):
            continue
        for spec in specs:
            if not _matches_strategy(row, spec):
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
            primary_horizon = PRIMARY_HORIZON if PRIMARY_HORIZON in horizons else max(horizons)
            would_size = _would_size_notional(row, audit)
            cost_source = str(row.get("cost_source") or "")
            live_ready = _cost_source_live_ready(row, allowed_cost_sources)
            estimated_cost = _estimated_cost_bps(row, rt_cost_bps)
            records.append(
                {
                    "strategy_id": str(spec.get("strategy_id") or ""),
                    "experiment_name": str(spec.get("experiment_name") or ""),
                    "enabled_shadow_only": enabled_shadow_only,
                    "enable_live_experiment": enable_live_experiment,
                    "run_id": str(row.get("run_id") or getattr(audit, "run_id", "") or ""),
                    "ts_utc": ts_utc,
                    "entry_ts_ms": asof_ts_ms,
                    "paper_date": ts_utc[:10],
                    "symbol": symbol,
                    "source_strategy_candidate": source_strategy,
                    "candidate_id": str(row.get("candidate_id") or ""),
                    "final_decision": str(row.get("final_decision") or ""),
                    "original_block_reason": str(row.get("block_reason") or row.get("no_signal_reason") or ""),
                    "skip_reason": str(row.get("block_reason") or row.get("no_signal_reason") or ""),
                    "entry_reason": str(spec.get("experiment_name") or source_strategy or "sol_paper_strategy"),
                    "experiment_reason": "sol_paper_strategy_tracking",
                    "would_enter": True,
                    "would_exit": False,
                    "would_exit_time": _iso_from_ms(asof_ts_ms + primary_horizon * 3600 * 1000) if asof_ts_ms > 0 else "",
                    "would_exit_rule": f"paper_time_horizon_{primary_horizon}h",
                    "would_size_notional": would_size,
                    "would_size_usdt": would_size,
                    "entry_px": entry_px,
                    "final_score": _normalize_float(row.get("final_score")),
                    "alpha6_score": _normalize_float(row.get("alpha6_score")),
                    "f4_volume_expansion": _normalize_float(row.get("f4_volume_expansion")),
                    "f5_rsi_trend_confirm": _normalize_float(row.get("f5_rsi_trend_confirm")),
                    "cost_source": cost_source,
                    "cost_source_quality": str(row.get("cost_source_quality") or ""),
                    "estimated_cost_bps": estimated_cost,
                    "cost_model_version": str(row.get("cost_model_version") or ""),
                    "cost_source_live_ready": live_ready,
                    "slippage_covered": live_ready,
                    "required_paper_days": required_days,
                    "required_slippage_coverage": required_coverage,
                    "rt_cost_bps": rt_cost_bps,
                    "label_status": "pending",
                    "label_not_observable_reason": "",
                }
            )
    cost_context = _cost_context_for_symbol(
        symbol=SOL_SYMBOL,
        candidate_rows=candidate_rows,
        fallback_bps=rt_cost_bps,
    )
    for spec in specs:
        strategy_id = str(spec.get("strategy_id") or "")
        if strategy_id in matched_strategy_ids:
            continue
        heartbeat = _heartbeat_record(
            spec=spec,
            audit=audit,
            ts_utc=ts_utc,
            asof_ts_ms=asof_ts_ms,
            rt_cost_bps=rt_cost_bps,
            required_days=required_days,
            required_coverage=required_coverage,
            cost_context=cost_context,
            allowed_cost_sources=allowed_cost_sources,
        )
        heartbeat["enabled_shadow_only"] = enabled_shadow_only
        heartbeat["enable_live_experiment"] = enable_live_experiment
        records.append(heartbeat)
    return records


def _sync_paper_fields(record: dict[str, Any], horizons: Iterable[int]) -> None:
    if not bool(record.get("would_enter")):
        for horizon in horizons:
            h = int(horizon)
            record[f"paper_pnl_bps_{h}h"] = None
            record[f"paper_pnl_usdt_{h}h"] = None
            record[f"{HORIZON_PREFIX}{h}h_status"] = "heartbeat"
            record[f"{HORIZON_PREFIX}{h}h_reason"] = "no_qualifying_candidate"
        record["paper_pnl_bps"] = None
        record["paper_pnl_usdt"] = None
        record["would_exit"] = False
        record["label_status"] = "heartbeat"
        record["label_not_observable_reason"] = ""
        return

    size = _normalize_float(record.get("would_size_notional"))
    primary = PRIMARY_HORIZON if PRIMARY_HORIZON in set(int(h) for h in horizons) else max(int(h) for h in horizons)
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


def _readiness_for_rows(
    rows: list[dict[str, Any]],
    *,
    required_days: int,
    required_coverage: float,
    enable_live_experiment: bool,
    allowed_cost_sources: set[str],
) -> tuple[bool, str, str, int, float, int]:
    paper_days = len({str(row.get("paper_date") or "") for row in rows if str(row.get("paper_date") or "")})
    covered = [row for row in rows if _cost_source_live_ready(row, allowed_cost_sources)]
    coverage = float(len(covered)) / float(len(rows)) if rows else 0.0
    reasons: list[str] = []
    if paper_days < int(required_days):
        reasons.append("no_paper_days")
    if not covered:
        reasons.append("cost_source_not_actual_or_mixed")
    if coverage < float(required_coverage):
        reasons.append("no_live_slippage_coverage")
    rules_pass = not reasons
    if rules_pass and not enable_live_experiment:
        reasons.append("live_experiment_disabled")
    live_small_ready = bool(rules_pass and enable_live_experiment)
    status = "LIVE_SMALL_READY" if live_small_ready else "PAPER_READY"
    return live_small_ready, status, ";".join(reasons), paper_days, round(coverage, 6), len(covered)


def _annotate_readiness(
    records: list[dict[str, Any]],
    *,
    diagnostics: DiagnosticsConfig,
) -> list[dict[str, Any]]:
    required_days = int(getattr(diagnostics, "paper_strategy_required_paper_days", 14) or 14)
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
        live_small_ready, status, reason, _days, _coverage, _covered = _readiness_for_rows(
            rows,
            required_days=required_days,
            required_coverage=required_coverage,
            enable_live_experiment=enable_live_experiment,
            allowed_cost_sources=allowed_cost_sources,
        )
        for row in rows:
            row["live_small_ready"] = live_small_ready
            row["readiness_status"] = status
            row["live_block_reason"] = reason
    return records


def _daily_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_strategy_days: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in records:
        by_strategy_days[(str(record.get("strategy_id") or ""), str(record.get("symbol") or ""))].add(
            str(record.get("paper_date") or "")
        )
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
        entry_rows = [row for row in rows if bool(row.get("would_enter"))]
        values = [_normalize_float(row.get("paper_pnl_bps")) for row in entry_rows]
        usable = [value for value in values if value is not None]
        pnl_usdt = [_normalize_float(row.get("paper_pnl_usdt")) for row in entry_rows]
        pnl_usdt_usable = [value for value in pnl_usdt if value is not None]
        out.append(
            {
                "paper_date": paper_date,
                "strategy_id": strategy_id,
                "experiment_name": rows[0].get("experiment_name"),
                "symbol": symbol,
                "entry_count": len(entry_rows),
                "complete_count": sum(1 for row in entry_rows if str(row.get("label_status") or "") == "complete"),
                "pending_count": sum(1 for row in entry_rows if str(row.get("label_status") or "") == "pending"),
                "not_observable_count": sum(1 for row in entry_rows if str(row.get("label_status") or "") == "not_observable"),
                "avg_paper_pnl_bps": round(sum(usable) / len(usable), 6) if usable else None,
                "paper_pnl_usdt_sum": round(sum(pnl_usdt_usable), 8) if pnl_usdt_usable else None,
                "win_rate": round(sum(1 for value in usable if float(value) > 0.0) / len(usable), 6) if usable else None,
                "paper_days_to_date": len(
                    {date for date in by_strategy_days[(strategy_id, symbol)] if date and date <= paper_date}
                ),
            }
        )
    return out


def _slippage_rows(records: list[dict[str, Any]], diagnostics: DiagnosticsConfig) -> list[dict[str, Any]]:
    required_days = int(getattr(diagnostics, "paper_strategy_required_paper_days", 14) or 14)
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
        live_small_ready, status, reason, paper_days, coverage, covered_count = _readiness_for_rows(
            rows,
            required_days=required_days,
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
                "paper_days": paper_days,
                "required_paper_days": required_days,
                "total_rows": len(rows),
                "slippage_covered_rows": covered_count,
                "slippage_coverage": coverage,
                "required_slippage_coverage": required_coverage,
                "latest_cost_source": latest.get("cost_source"),
                "allowed_live_cost_sources": ",".join(sorted(allowed_cost_sources)),
                "live_small_ready": live_small_ready,
                "readiness_status": status,
                "live_block_reason": reason,
            }
        )
    return out


def update_sol_paper_strategy_tracker(
    *,
    run_dir: str | Path,
    audit: DecisionAudit,
    market_data_1h: Dict[str, MarketSeries],
    cfg: AppConfig,
    cache_dir: str | Path | None = None,
    ohlcv_provider: Any = None,
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
    records_by_key = _load_existing_records(labels_path)
    new_records = _collect_candidates(
        candidate_rows=candidate_rows,
        audit=audit,
        cfg=cfg,
        market_data_1h=market_data_1h,
        cache_dir=cache_root,
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

    records = list(records_by_key.values())
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    if ohlcv_provider is None:
        ohlcv_provider = _default_ohlcv_provider_for_cfg(cfg)
    if records:
        labelable_records = [record for record in records if bool(record.get("would_enter"))]
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

    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
