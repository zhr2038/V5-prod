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
    _label_horizons,
    _normalize_bool,
    _normalize_float,
    _normalize_horizons,
    _record_entry_ts_ms,
    _resolve_reports_dir,
    _series_for_symbol,
    _signal_factor,
    _summaries_dir,
    _update_labels,
    _write_csv,
    _write_records,
)


EXPERIMENT_NAME = "protect_sol_exception_v1"
SOL_SYMBOL = "SOL/USDT"
ALLOWED_BLOCK_REASONS = {
    "protect_entry_rsi_confirm_too_weak",
    "protect_entry_alpha6_score_too_low",
}
DEFAULT_HORIZONS = [4, 8, 12, 24, 48, 72]


def _diagnostics_cfg(cfg: Any) -> DiagnosticsConfig:
    diagnostics = getattr(cfg, "diagnostics", None)
    return diagnostics if diagnostics is not None else DiagnosticsConfig()


def _labels_path(reports_dir: Path) -> Path:
    return reports_dir / "protect_sol_exception_shadow_labels.jsonl"


def _symbol_text(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "/")


def _truthy(value: Any) -> bool:
    parsed = _normalize_bool(value)
    if parsed is not None:
        return bool(parsed)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


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


def _shadow_horizons(diagnostics: DiagnosticsConfig) -> list[int]:
    return _normalize_horizons(
        getattr(diagnostics, "protect_sol_exception_horizons_hours", None),
        DEFAULT_HORIZONS,
    )


def _weight_candidates(raw: Any, fallback: list[float]) -> list[float]:
    out: list[float] = []
    seen: set[float] = set()
    for item in raw or fallback:
        try:
            value = float(item)
        except Exception:
            continue
        if value < 0.0 or value > 1.0:
            continue
        key = round(value, 10)
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out or list(fallback)


def _labels_key(record: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(record.get("experiment_name") or EXPERIMENT_NAME),
            str(record.get("run_id") or ""),
            str(record.get("ts_utc") or ""),
            str(record.get("symbol") or ""),
            str(record.get("original_block_reason") or record.get("skip_reason") or ""),
            str(record.get("f3_weight_candidate") or ""),
            str(record.get("f4_weight_candidate") or ""),
        ]
    )


def _candidate_key(record: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(record.get("run_id") or ""),
            str(record.get("ts_utc") or ""),
            str(record.get("symbol") or ""),
            str(record.get("original_block_reason") or record.get("skip_reason") or ""),
        ]
    )


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
            records[_labels_key(payload)] = payload
    return records


def _target_explain_reason(item: Mapping[str, Any]) -> str:
    return str(item.get("router_reason") or item.get("blocked_reason") or item.get("reason") or "").strip()


def _strategy_signal_for_symbol(audit: DecisionAudit, symbol: str) -> Optional[Mapping[str, Any]]:
    for strategy in getattr(audit, "strategy_signals", []) or []:
        if str((strategy or {}).get("strategy") or "").strip() != "Alpha6Factor":
            continue
        for signal in (strategy or {}).get("signals", []) or []:
            if _symbol_text((signal or {}).get("symbol")) == symbol:
                return signal
    return None


def _score_from_signal(signal: Optional[Mapping[str, Any]]) -> Optional[float]:
    if not isinstance(signal, Mapping):
        return None
    return _normalize_float(signal.get("score"))


def _factor_from_item_or_signal(item: Mapping[str, Any], signal: Optional[Mapping[str, Any]], name: str) -> Optional[float]:
    value = _normalize_float(item.get(name))
    if value is not None:
        return value
    return _signal_factor(signal, name)


def _price_from_mapping(item: Mapping[str, Any]) -> Optional[float]:
    for key in ("entry_px", "latest_px", "current_px", "price", "px", "last_px"):
        value = _normalize_float(item.get(key))
        if value is not None and value > 0:
            return value
    return None


def _entry_px(
    *,
    item: Mapping[str, Any],
    symbol: str,
    entry_ts_ms: int,
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    cached: dict[str, list[dict[str, float | int]]],
) -> Optional[float]:
    price = _price_from_mapping(item)
    if price is not None:
        return price
    rows = _series_for_symbol(
        symbol=symbol,
        cache_dir=cache_dir,
        market_data_1h=market_data_1h,
        cached=cached,
    )
    return _find_close_at_or_after(rows, entry_ts_ms)


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


def _would_size_notional(item: Mapping[str, Any], audit: DecisionAudit) -> Optional[float]:
    for key in ("would_size_notional", "target_notional", "target_notional_usdt", "notional", "notional_usdt"):
        value = _normalize_float(item.get(key))
        if value is not None and value > 0:
            return value
    target_w = _normalize_float(item.get("target_w"))
    equity = _current_equity_usdt(audit)
    if target_w is not None and target_w > 0 and equity is not None:
        return float(target_w) * float(equity)
    return None


def _would_exit_time(entry_ts_ms: int, horizons: Iterable[int]) -> str:
    parts = []
    for horizon in horizons:
        h = int(horizon)
        parts.append(f"{h}h={_iso_from_ms(entry_ts_ms + h * 3600 * 1000)}")
    return ";".join(parts)


def _shadow_alpha6_score(
    *,
    live_score: Optional[float],
    signal: Optional[Mapping[str, Any]],
    current_weights: Mapping[str, Any],
    f3_weight: float,
    f4_weight: float,
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    f3_z = _signal_factor(signal, "f3_vol_adj_ret")
    f4_z = _signal_factor(signal, "f4_volume_expansion")
    current_f3 = _normalize_float(current_weights.get("f3_vol_adj_ret"))
    current_f4 = _normalize_float(current_weights.get("f4_volume_expansion"))
    if f3_z is None or f4_z is None or current_f3 is None or current_f4 is None:
        return (None, None, f3_z, f4_z)
    delta = (float(f3_weight) - current_f3) * float(f3_z) + (float(f4_weight) - current_f4) * float(f4_z)
    if live_score is None:
        return (None, delta, f3_z, f4_z)
    return (float(live_score) + delta, delta, f3_z, f4_z)


def _collect_shadow_candidates(
    *,
    audit: DecisionAudit,
    cfg: AppConfig,
    market_data_1h: Dict[str, MarketSeries],
    current_level: Optional[str],
    cache_dir: Path,
) -> list[dict[str, Any]]:
    diagnostics = _diagnostics_cfg(cfg)
    experiment_name = str(
        getattr(diagnostics, "protect_sol_exception_experiment_name", EXPERIMENT_NAME)
        or EXPERIMENT_NAME
    )
    enabled_shadow_only = bool(getattr(diagnostics, "protect_sol_exception_enabled_shadow_only", True))
    enable_live_experiment = bool(getattr(diagnostics, "protect_sol_exception_enable_live_experiment", False))
    if not enabled_shadow_only:
        return []

    horizons = _shadow_horizons(diagnostics)
    rt_cost_bps = float(getattr(diagnostics, "protect_sol_exception_rt_cost_bps", 30.0) or 30.0)
    min_f4 = float(getattr(diagnostics, "protect_sol_exception_min_f4_volume_expansion", 0.0) or 0.0)
    f3_candidates = _weight_candidates(
        getattr(diagnostics, "protect_sol_exception_f3_weight_candidates", None),
        [0.20, 0.25],
    )
    f4_candidates = _weight_candidates(
        getattr(diagnostics, "protect_sol_exception_f4_weight_candidates", None),
        [0.25, 0.30],
    )
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    ts_utc = _iso_from_ms(asof_ts_ms) if asof_ts_ms > 0 else ""
    cached: dict[str, list[dict[str, float | int]]] = {}
    records: list[dict[str, Any]] = []
    live_weights = getattr(audit, "effective_alpha6_weights", {}) or {}

    for item in getattr(audit, "target_execution_explain", []) or []:
        if not isinstance(item, Mapping):
            continue
        symbol = _symbol_text(item.get("symbol"))
        if symbol != SOL_SYMBOL:
            continue
        reason = _target_explain_reason(item)
        if reason not in ALLOWED_BLOCK_REASONS:
            continue
        level_text = str(item.get("current_level") or current_level or "").strip().upper()
        if level_text != "PROTECT":
            continue
        router_action = str(item.get("router_action") or item.get("action") or "").strip().lower()
        if router_action and router_action != "skip":
            continue
        final_score = _normalize_float(item.get("final_score"))
        high_score = _truthy(item.get("high_score_but_not_executed")) or (
            final_score is not None and final_score >= 0.80
        )
        if not high_score:
            continue

        signal = _strategy_signal_for_symbol(audit, symbol)
        f4_volume_expansion = _factor_from_item_or_signal(item, signal, "f4_volume_expansion")
        if f4_volume_expansion is None or f4_volume_expansion <= min_f4:
            continue

        entry_px = _entry_px(
            item=item,
            symbol=symbol,
            entry_ts_ms=asof_ts_ms,
            market_data_1h=market_data_1h,
            cache_dir=cache_dir,
            cached=cached,
        )
        alpha6_score = _normalize_float(item.get("alpha6_score"))
        if alpha6_score is None:
            alpha6_score = _score_from_signal(signal)
        target_w = _normalize_float(item.get("target_w"))
        would_size_notional = _would_size_notional(item, audit)
        base = {
            "experiment_name": experiment_name,
            "enabled_shadow_only": enabled_shadow_only,
            "enable_live_experiment": enable_live_experiment,
            "ts_utc": ts_utc,
            "entry_ts_ms": asof_ts_ms,
            "run_id": str(getattr(audit, "run_id", "") or ""),
            "symbol": symbol,
            "intended_side": "buy",
            "skip_reason": reason,
            "original_block_reason": reason,
            "experiment_reason": "sol_high_score_f4_positive_protect_exception_shadow",
            "would_enter": True,
            "would_size_notional": round(would_size_notional, 8) if would_size_notional is not None else None,
            "would_exit_time": _would_exit_time(asof_ts_ms, horizons) if asof_ts_ms > 0 else "",
            "entry_px": entry_px,
            "final_score": final_score,
            "target_w": target_w,
            "alpha6_score": alpha6_score,
            "trend_score": _normalize_float(item.get("trend_score")),
            "f3_vol_adj_ret": _factor_from_item_or_signal(item, signal, "f3_vol_adj_ret"),
            "f4_volume_expansion": f4_volume_expansion,
            "f5_rsi_trend_confirm": _factor_from_item_or_signal(item, signal, "f5_rsi_trend_confirm"),
            "current_level": level_text,
            "regime": str(item.get("regime") or getattr(audit, "regime", "") or ""),
            "rt_cost_bps": rt_cost_bps,
            "btc_leadership_relax_allowed": False,
            "alt_impulse_relax_allowed": False,
            "eth_relax_allowed": False,
            "label_status": "pending",
        }
        for f3_weight in f3_candidates:
            for f4_weight in f4_candidates:
                shadow_score, shadow_delta, f3_z, f4_z = _shadow_alpha6_score(
                    live_score=alpha6_score,
                    signal=signal,
                    current_weights=live_weights,
                    f3_weight=f3_weight,
                    f4_weight=f4_weight,
                )
                record = dict(base)
                record.update(
                    {
                        "f3_weight_candidate": round(float(f3_weight), 8),
                        "f4_weight_candidate": round(float(f4_weight), 8),
                        "f3_z_factor": f3_z,
                        "f4_z_factor": f4_z,
                        "shadow_alpha6_score_candidate": (
                            round(float(shadow_score), 8) if shadow_score is not None else None
                        ),
                        "shadow_alpha6_score_delta": (
                            round(float(shadow_delta), 8) if shadow_delta is not None else None
                        ),
                    }
                )
                records.append(record)
    return records


def _sync_would_pnl_fields(record: dict[str, Any], horizons: Iterable[int]) -> None:
    statuses: list[str] = []
    for horizon in horizons:
        h = int(horizon)
        net_key = f"{HORIZON_PREFIX}{h}h_net_bps"
        status_key = f"{HORIZON_PREFIX}{h}h_status"
        record[f"would_pnl_bps_{h}h"] = record.get(net_key)
        status = str(record.get(status_key) or "").strip()
        if _normalize_float(record.get(net_key)) is not None:
            status = "complete"
            record[status_key] = "complete"
        if status:
            statuses.append(status)
    if "complete" in statuses:
        record["label_status"] = "complete"
        record["label_not_observable_reason"] = ""
    elif "pending" in statuses:
        record["label_status"] = "pending"
        record["label_not_observable_reason"] = ""
    elif statuses and all(status == "not_observable" for status in statuses):
        record["label_status"] = "not_observable"
        reasons = [str(record.get(f"{HORIZON_PREFIX}{int(h)}h_reason") or "") for h in horizons]
        record["label_not_observable_reason"] = next((reason for reason in reasons if reason), "")


def _summary_fields(horizons: Iterable[int]) -> list[str]:
    horizon_fields: list[str] = []
    for horizon in horizons:
        h = int(horizon)
        horizon_fields.extend(
            [
                f"would_pnl_bps_{h}h",
                f"{HORIZON_PREFIX}{h}h_gross_bps",
                f"{HORIZON_PREFIX}{h}h_net_bps",
                f"{HORIZON_PREFIX}{h}h_would_have_won_net",
                f"{HORIZON_PREFIX}{h}h_status",
                f"{HORIZON_PREFIX}{h}h_reason",
            ]
        )
    return [
        "experiment_name",
        "enabled_shadow_only",
        "enable_live_experiment",
        "ts_utc",
        "run_id",
        "symbol",
        "intended_side",
        "would_enter",
        "would_size_notional",
        "would_exit_time",
        "entry_px",
        "original_block_reason",
        "experiment_reason",
        "final_score",
        "target_w",
        "alpha6_score",
        "trend_score",
        "f3_vol_adj_ret",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        "f3_weight_candidate",
        "f4_weight_candidate",
        "f3_z_factor",
        "f4_z_factor",
        "shadow_alpha6_score_candidate",
        "shadow_alpha6_score_delta",
        "btc_leadership_relax_allowed",
        "alt_impulse_relax_allowed",
        "eth_relax_allowed",
        "current_level",
        "regime",
        "rt_cost_bps",
        *horizon_fields,
        "label_status",
        "label_not_observable_reason",
    ]


def _aggregate_by_horizon(
    records: list[dict[str, Any]],
    *,
    horizons: list[int],
    min_samples: int,
    include_variant: bool,
) -> list[dict[str, Any]]:
    buckets: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        for horizon in horizons:
            key = [
                record.get("symbol") or "unknown",
                record.get("original_block_reason") or record.get("skip_reason") or "unknown",
                int(horizon),
            ]
            if include_variant:
                key.extend([record.get("f3_weight_candidate"), record.get("f4_weight_candidate")])
            buckets[tuple(key)].append(record)

    out: list[dict[str, Any]] = []
    for key, rows in sorted(buckets.items(), key=lambda item: item[0]):
        horizon = int(key[2])
        net_key = f"would_pnl_bps_{horizon}h"
        values = [_normalize_float(row.get(net_key)) for row in rows]
        usable = [value for value in values if value is not None]
        complete_candidate_keys = {
            _candidate_key(row)
            for row in rows
            if _normalize_float(row.get(net_key)) is not None
        }
        payload = {
            "symbol": key[0],
            "original_block_reason": key[1],
            "horizon_hours": horizon,
            "count": len(rows),
            "unique_candidate_count": len({_candidate_key(row) for row in rows}),
            "complete_count": len(usable),
            "complete_unique_candidate_count": len(complete_candidate_keys),
            "pending_count": sum(1 for row in rows if str(row.get(f"{HORIZON_PREFIX}{horizon}h_status") or "") == "pending"),
            "not_observable_count": sum(1 for row in rows if str(row.get(f"{HORIZON_PREFIX}{horizon}h_status") or "") == "not_observable"),
            "avg_would_pnl_bps": round(sum(usable) / len(usable), 6) if usable else None,
            "win_rate": round(sum(1 for value in usable if float(value) > 0.0) / len(usable), 6) if usable else None,
            "current_strategy_net_bps": 0.0,
            "better_than_current_strategy": bool(usable and (sum(usable) / len(usable)) > 0.0),
            "sample_warning": (
                f"insufficient_samples_min_{int(min_samples)}"
                if len(complete_candidate_keys) < int(min_samples)
                else ""
            ),
        }
        if include_variant:
            payload["f3_weight_candidate"] = key[3]
            payload["f4_weight_candidate"] = key[4]
        out.append(payload)
    return out


def update_protect_sol_exception_shadow_evaluator(
    *,
    run_dir: str | Path,
    audit: DecisionAudit,
    market_data_1h: Dict[str, MarketSeries],
    cfg: AppConfig,
    current_level: Optional[str],
    cache_dir: str | Path | None = None,
    ohlcv_provider: Any = None,
) -> dict[str, Any]:
    diagnostics = _diagnostics_cfg(cfg)
    enabled = bool(getattr(diagnostics, "protect_sol_exception_enabled_shadow_only", True))
    if not enabled:
        return {"enabled": False, "new_records": 0, "total_records": 0}

    reports_dir = _resolve_reports_dir(run_dir)
    labels_path = _labels_path(reports_dir)
    summaries_dir = _summaries_dir(reports_dir)
    cache_root = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parents[2] / "data" / "cache"
    horizons = _shadow_horizons(diagnostics)
    min_samples = int(getattr(diagnostics, "protect_sol_exception_min_complete_samples_warning", 5) or 5)

    records_by_key = _load_existing_records(labels_path)
    new_records = _collect_shadow_candidates(
        audit=audit,
        cfg=cfg,
        market_data_1h=market_data_1h,
        current_level=current_level,
        cache_dir=cache_root,
    )
    inserted = 0
    for record in new_records:
        key = _labels_key(record)
        if key not in records_by_key:
            inserted += 1
            records_by_key[key] = record
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
        _update_labels(
            records=records,
            cache_dir=cache_root,
            horizons=horizons,
            market_data_1h=market_data_1h,
            asof_ts_ms=asof_ts_ms,
            ohlcv_provider=ohlcv_provider,
        )
        for record in records:
            _sync_would_pnl_fields(record, horizons)
        records.sort(
            key=lambda row: (
                _record_entry_ts_ms(row),
                str(row.get("run_id") or ""),
                str(row.get("symbol") or ""),
                str(row.get("original_block_reason") or ""),
                float(row.get("f3_weight_candidate") or 0.0),
                float(row.get("f4_weight_candidate") or 0.0),
            )
        )
        _write_records(labels_path, records)

    csv_rows = []
    for row in records:
        payload = dict(row)
        payload.pop("entry_ts_ms", None)
        csv_rows.append(payload)
    _write_csv(summaries_dir / "protect_sol_exception_shadow_outcomes.csv", csv_rows, _summary_fields(horizons))
    _write_csv(
        summaries_dir / "protect_sol_exception_shadow_outcomes_by_symbol_reason_horizon.csv",
        _aggregate_by_horizon(records, horizons=horizons, min_samples=min_samples, include_variant=False),
        [
            "symbol",
            "original_block_reason",
            "horizon_hours",
            "count",
            "unique_candidate_count",
            "complete_count",
            "complete_unique_candidate_count",
            "pending_count",
            "not_observable_count",
            "avg_would_pnl_bps",
            "win_rate",
            "current_strategy_net_bps",
            "better_than_current_strategy",
            "sample_warning",
        ],
    )
    _write_csv(
        summaries_dir / "protect_sol_exception_factor_weight_shadow.csv",
        _aggregate_by_horizon(records, horizons=horizons, min_samples=min_samples, include_variant=True),
        [
            "symbol",
            "original_block_reason",
            "horizon_hours",
            "f3_weight_candidate",
            "f4_weight_candidate",
            "count",
            "unique_candidate_count",
            "complete_count",
            "complete_unique_candidate_count",
            "pending_count",
            "not_observable_count",
            "avg_would_pnl_bps",
            "win_rate",
            "current_strategy_net_bps",
            "better_than_current_strategy",
            "sample_warning",
        ],
    )
    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
