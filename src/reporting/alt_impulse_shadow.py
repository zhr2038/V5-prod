from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from configs.schema import AppConfig, DiagnosticsConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.skipped_candidate_tracker import (
    HORIZON_PREFIX,
    _aggregate_records_by_fields,
    _aggregate_records_by_horizon,
    _coerce_epoch_ms,
    _default_ohlcv_provider_for_cfg,
    _find_close_at_or_after,
    _iso_from_ms,
    _load_existing_records,
    _label_horizons,
    _merge_series,
    _normalize_bool,
    _normalize_float,
    _record_entry_ts_ms,
    _record_key,
    _resolve_reports_dir,
    _series_for_symbol,
    _summaries_dir,
    _update_labels,
    _write_csv,
    _write_records,
)


ALT_IMPULSE_SHADOW_REASONS = {
    "protect_entry_trend_only",
    "protect_entry_no_alpha6_confirmation",
    "protect_entry_alpha6_score_too_low",
}
ALT_IMPULSE_SHADOW_DECISION_REASON = "alt_impulse_regime_dependent_shadow_only"
ALT_IMPULSE_SHADOW_STATUS_FIELDS = [
    "shadow_decision",
    "alpha_discovery_board_status",
    "paper_ready_allowed",
    "live_ready_allowed",
    "shadow_decision_reason",
]
ALT_IMPULSE_READINESS_SCHEMA_VERSION = "v5.alt_impulse_shadow_readiness.v1"
ALT_IMPULSE_READINESS_FIELDS = [
    "symbol",
    "ready_for_live_probe",
    "blocking_reasons",
    "sample_count",
    "recent_sample_count",
    "avg_24h_net_bps",
    "avg_48h_net_bps",
    "win_rate_24h",
    "win_rate_48h",
    "bnb_high_score_blocked_24h_avg_net_bps",
    "bnb_negative_expectancy_bps",
]
ALT_IMPULSE_READINESS_THRESHOLDS = {
    "min_sample_count": 30,
    "min_recent_7d_sample_count": 10,
    "min_avg_24h_net_bps": 80.0,
    "min_win_rate_24h": 0.60,
    "min_avg_48h_net_bps": 50.0,
    "recent_window_days": 7,
}


def _diagnostics_cfg(cfg: Any) -> DiagnosticsConfig:
    diagnostics = getattr(cfg, "diagnostics", None)
    if diagnostics is None:
        return DiagnosticsConfig()
    return diagnostics


def _labels_path(reports_dir: Path) -> Path:
    return reports_dir / "alt_impulse_shadow_labels.jsonl"


def _asof_ts_ms(audit: DecisionAudit, market_data_1h: Dict[str, MarketSeries]) -> int:
    for raw in (
        getattr(audit, "now_ts", None),
        getattr(audit, "window_end_ts", None),
    ):
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


def _find_close_at_or_before(series: list[dict[str, float | int]], target_ts_ms: int) -> Optional[float]:
    out: Optional[float] = None
    for row in series:
        ts_ms = int(row.get("timestamp_ms") or 0)
        if ts_ms > int(target_ts_ms):
            break
        close = _normalize_float(row.get("close"))
        if close is not None:
            out = close
    return out


def _series(
    *,
    symbol: str,
    cache_dir: Path,
    market_data_1h: Dict[str, MarketSeries],
    cached: dict[str, list[dict[str, float | int]]],
) -> list[dict[str, float | int]]:
    return _series_for_symbol(
        symbol=symbol,
        cache_dir=cache_dir,
        market_data_1h=market_data_1h,
        cached=cached,
    )


def _four_hour_return_bps(
    *,
    symbol: str,
    entry_ts_ms: int,
    cache_dir: Path,
    market_data_1h: Dict[str, MarketSeries],
    cached: dict[str, list[dict[str, float | int]]],
) -> Optional[float]:
    rows = _series(symbol=symbol, cache_dir=cache_dir, market_data_1h=market_data_1h, cached=cached)
    if entry_ts_ms <= 0:
        return None
    start_px = _find_close_at_or_before(rows, entry_ts_ms - 4 * 3600 * 1000)
    end_px = _find_close_at_or_before(rows, entry_ts_ms)
    if start_px is None or start_px <= 0 or end_px is None or end_px <= 0:
        return None
    return ((float(end_px) / float(start_px)) - 1.0) * 10_000.0


def _entry_px(
    *,
    item: Mapping[str, Any],
    symbol: str,
    entry_ts_ms: int,
    cache_dir: Path,
    market_data_1h: Dict[str, MarketSeries],
    cached: dict[str, list[dict[str, float | int]]],
) -> Optional[float]:
    for key in ("entry_px", "price", "px", "latest_px", "last_px", "current_px"):
        value = _normalize_float(item.get(key))
        if value is not None and value > 0:
            return value
    rows = _series(symbol=symbol, cache_dir=cache_dir, market_data_1h=market_data_1h, cached=cached)
    return _find_close_at_or_after(rows, entry_ts_ms)


def _bool_config(value: Any, default: bool) -> bool:
    parsed = _normalize_bool(value)
    return bool(default if parsed is None else parsed)


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "value"):
        value = getattr(value, "value")
    if hasattr(value, "name") and not isinstance(value, str):
        value = getattr(value, "name")
    return str(value or "").strip()


def _audit_text(audit: DecisionAudit, names: tuple[str, ...]) -> str:
    for name in names:
        value = getattr(audit, name, None)
        if value is None:
            continue
        if name in {"regime", "market_regime"} and hasattr(value, "state"):
            text = _text_value(getattr(value, "state", None))
        else:
            text = _text_value(value)
        if text:
            return text
    return ""


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = _text_value(value)
        if text:
            return text
    return default


def _btc_trend_state(item: Mapping[str, Any], btc_4h_ret_bps: Optional[float]) -> str:
    explicit = _first_text(item.get("btc_trend_state"), item.get("btc_state"))
    if explicit:
        return explicit
    if btc_4h_ret_bps is None:
        return "not_observable"
    if float(btc_4h_ret_bps) > 0.0:
        return "positive_4h"
    if float(btc_4h_ret_bps) < 0.0:
        return "negative_4h"
    return "flat_4h"


def _shadow_symbols(diagnostics: DiagnosticsConfig) -> set[str]:
    return {
        str(symbol or "").strip().upper()
        for symbol in (getattr(diagnostics, "alt_impulse_shadow_symbols", None) or ["ETH/USDT", "SOL/USDT", "BNB/USDT"])
        if str(symbol or "").strip()
    }


def _whitelist_symbols(cfg: AppConfig, shadow_symbols: set[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for symbol in ["BTC/USDT", *(getattr(cfg, "symbols", None) or []), *sorted(shadow_symbols)]:
        text = str(symbol or "").strip().upper()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _collect_shadow_candidates(
    *,
    audit: DecisionAudit,
    cfg: AppConfig,
    market_data_1h: Dict[str, MarketSeries],
    current_level: Optional[str],
    cache_dir: Path,
) -> list[dict[str, Any]]:
    diagnostics = _diagnostics_cfg(cfg)
    shadow_symbols = _shadow_symbols(diagnostics)
    min_final_score = float(getattr(diagnostics, "alt_impulse_shadow_min_final_score", 0.80) or 0.80)
    min_trend_score = float(getattr(diagnostics, "alt_impulse_shadow_min_trend_score", 0.80) or 0.80)
    require_btc_positive = _bool_config(getattr(diagnostics, "alt_impulse_shadow_require_btc_positive_4h", True), True)
    require_positive_count = int(getattr(diagnostics, "alt_impulse_shadow_require_broad_market_positive_count", 2) or 0)
    rt_cost_bps = float(getattr(diagnostics, "alt_impulse_shadow_rt_cost_bps", 30.0) or 30.0)
    whitelist_symbols = _whitelist_symbols(cfg, shadow_symbols)
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    ts_utc = _iso_from_ms(asof_ts_ms) if asof_ts_ms > 0 else ""
    cached: dict[str, list[dict[str, float | int]]] = {}
    records: list[dict[str, Any]] = []
    captured: set[str] = set()

    for item in getattr(audit, "target_execution_explain", []) or []:
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol not in shadow_symbols:
            continue
        reason = str(item.get("router_reason") or item.get("blocked_reason") or "").strip()
        if reason not in ALT_IMPULSE_SHADOW_REASONS:
            continue
        router_action = str(item.get("router_action") or "").strip().lower()
        if router_action and router_action != "skip":
            continue
        level = str(item.get("current_level") or current_level or "").strip().upper()
        if level != "PROTECT":
            continue
        final_score = _normalize_float(item.get("final_score"))
        target_w = _normalize_float(item.get("target_w"))
        trend_score = _normalize_float(item.get("trend_score"))
        if (target_w is None or target_w <= 0.0) and (final_score is None or final_score < min_final_score):
            continue
        if trend_score is None or trend_score < min_trend_score:
            continue

        btc_4h_ret_bps = _four_hour_return_bps(
            symbol="BTC/USDT",
            entry_ts_ms=asof_ts_ms,
            cache_dir=cache_dir,
            market_data_1h=market_data_1h,
            cached=cached,
        )
        if require_btc_positive and (btc_4h_ret_bps is None or btc_4h_ret_bps <= 0.0):
            continue
        positive_count = 0
        for whitelist_symbol in whitelist_symbols:
            ret_bps = _four_hour_return_bps(
                symbol=whitelist_symbol,
                entry_ts_ms=asof_ts_ms,
                cache_dir=cache_dir,
                market_data_1h=market_data_1h,
                cached=cached,
            )
            if ret_bps is not None and ret_bps > 0.0:
                positive_count += 1
        if require_positive_count > 0 and positive_count < require_positive_count:
            continue

        regime_state = _first_text(
            item.get("regime_state"),
            item.get("regime"),
            _audit_text(audit, ("regime_state", "regime", "market_regime")),
            default="UNKNOWN",
        )
        risk_level = _first_text(
            item.get("risk_level"),
            item.get("current_level"),
            current_level,
            _audit_text(audit, ("risk_level", "current_level")),
            default=level,
        )
        funding_state = _first_text(
            item.get("funding_state"),
            item.get("funding_state_hint"),
            _audit_text(audit, ("funding_state", "funding_state_hint")),
            default="not_observable",
        )
        volatility_bucket = _first_text(
            item.get("volatility_bucket"),
            item.get("vol_bucket"),
            item.get("volatility_state"),
            _audit_text(audit, ("volatility_bucket", "vol_bucket", "volatility_state")),
            default="not_observable",
        )
        px = _entry_px(
            item=item,
            symbol=symbol,
            entry_ts_ms=asof_ts_ms,
            cache_dir=cache_dir,
            market_data_1h=market_data_1h,
            cached=cached,
        )
        record = {
            "ts_utc": ts_utc,
            "entry_ts_ms": asof_ts_ms,
            "run_id": str(getattr(audit, "run_id", "") or ""),
            "symbol": symbol,
            "entry_px": px,
            "final_score": final_score,
            "trend_score": trend_score,
            "trend_side": str(item.get("trend_side") or "").strip().lower() or None,
            "alpha6_score": _normalize_float(item.get("alpha6_score")),
            "alpha6_side": str(item.get("alpha6_side") or "").strip().lower() or None,
            "f4_volume_expansion": _normalize_float(item.get("f4_volume_expansion")),
            "f5_rsi_trend_confirm": _normalize_float(item.get("f5_rsi_trend_confirm")),
            "skip_reason": reason,
            "btc_4h_ret_bps": round(float(btc_4h_ret_bps), 6) if btc_4h_ret_bps is not None else None,
            "btc_trend_state": _btc_trend_state(item, btc_4h_ret_bps),
            "whitelist_positive_4h_count": positive_count,
            "broad_market_positive_count": positive_count,
            "regime": regime_state,
            "regime_state": regime_state,
            "current_level": level,
            "risk_level": risk_level,
            "funding_state": funding_state,
            "volatility_bucket": volatility_bucket,
            "shadow_decision": "REGIME_SHADOW",
            "alpha_discovery_board_status": "REGIME_SHADOW",
            "paper_ready_allowed": False,
            "live_ready_allowed": False,
            "shadow_decision_reason": ALT_IMPULSE_SHADOW_DECISION_REASON,
            "rt_cost_bps": rt_cost_bps,
            "label_status": "pending",
        }
        key = _record_key(record)
        if key in captured:
            continue
        captured.add(key)
        records.append(record)
    return records


def _summary_fields(horizons: list[int]) -> list[str]:
    horizon_fields: list[str] = []
    for horizon in horizons:
        h = int(horizon)
        horizon_fields.extend(
            [
                f"{HORIZON_PREFIX}{h}h_gross_bps",
                f"{HORIZON_PREFIX}{h}h_net_bps",
                f"{HORIZON_PREFIX}{h}h_would_have_won_net",
                f"{HORIZON_PREFIX}{h}h_status",
                f"{HORIZON_PREFIX}{h}h_reason",
            ]
        )
    return [
        "ts_utc",
        "run_id",
        "symbol",
        "entry_px",
        "final_score",
        "trend_score",
        "trend_side",
        "alpha6_score",
        "alpha6_side",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        "skip_reason",
        "btc_4h_ret_bps",
        "btc_trend_state",
        "whitelist_positive_4h_count",
        "broad_market_positive_count",
        "regime",
        "regime_state",
        "current_level",
        "risk_level",
        "funding_state",
        "volatility_bucket",
        *ALT_IMPULSE_SHADOW_STATUS_FIELDS,
        "rt_cost_bps",
        *horizon_fields,
        "label_status",
        "label_not_observable_reason",
    ]


def _summary_shadow_decision(row: Mapping[str, Any]) -> str:
    try:
        complete_count = int(float(row.get("complete_count") or 0))
    except Exception:
        complete_count = 0
    return "REGIME_SHADOW" if complete_count > 0 else "KEEP_SHADOW"


def _with_shadow_status(row: dict[str, Any], *, decision: str | None = None) -> dict[str, Any]:
    normalized = str(decision or row.get("shadow_decision") or "").strip().upper()
    if normalized not in {"REGIME_SHADOW", "KEEP_SHADOW"}:
        normalized = _summary_shadow_decision(row)
    row["shadow_decision"] = normalized
    row["alpha_discovery_board_status"] = normalized
    row["paper_ready_allowed"] = False
    row["live_ready_allowed"] = False
    row["shadow_decision_reason"] = ALT_IMPULSE_SHADOW_DECISION_REASON
    return row


def _with_shadow_status_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_with_shadow_status(dict(row)) for row in rows]


def _symbol_key(value: Any) -> str:
    return str(value or "").strip().replace("-", "/").upper()


def _net_values(records: list[Mapping[str, Any]], horizon: int) -> list[float]:
    key = f"{HORIZON_PREFIX}{int(horizon)}h_net_bps"
    values = [_normalize_float(row.get(key)) for row in records]
    return [float(value) for value in values if value is not None]


def _avg(values: list[float]) -> Optional[float]:
    return round(sum(values) / len(values), 6) if values else None


def _win_rate(values: list[float]) -> Optional[float]:
    return round(sum(1 for value in values if float(value) > 0.0) / len(values), 6) if values else None


def _readiness_ts_utc(asof_ts_ms: int) -> str:
    if int(asof_ts_ms or 0) > 0:
        return _iso_from_ms(int(asof_ts_ms))
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _symbol_negative_expectancy_bps(negative_expectancy_state: Mapping[str, Any] | None, symbol: str) -> Optional[float]:
    if not isinstance(negative_expectancy_state, Mapping):
        return None
    wanted = _symbol_key(symbol)
    sections: list[Any] = [
        negative_expectancy_state.get("stats"),
        negative_expectancy_state.get("symbols"),
        negative_expectancy_state,
    ]
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        for raw_symbol, payload in section.items():
            if _symbol_key(raw_symbol) != wanted or not isinstance(payload, Mapping):
                continue
            value = _normalize_float(payload.get("net_expectancy_bps"))
            if value is None:
                value = _normalize_float(payload.get("expectancy_bps"))
            return value
    return None


def _high_score_24h_avg_for_symbol(high_score_blocked_rows: list[Mapping[str, Any]] | None, symbol: str) -> Optional[float]:
    if not high_score_blocked_rows:
        return None
    wanted = _symbol_key(symbol)
    values = [
        _normalize_float(row.get(f"{HORIZON_PREFIX}24h_net_bps"))
        for row in high_score_blocked_rows
        if _symbol_key(row.get("symbol")) == wanted
    ]
    usable = [float(value) for value in values if value is not None]
    return _avg(usable)


def build_alt_impulse_shadow_readiness(
    records: list[Mapping[str, Any]],
    *,
    asof_ts_ms: int,
    high_score_blocked_rows: list[Mapping[str, Any]] | None = None,
    negative_expectancy_state: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    thresholds = dict(ALT_IMPULSE_READINESS_THRESHOLDS)
    recent_cutoff_ms = int(asof_ts_ms or 0) - int(thresholds["recent_window_days"]) * 24 * 3600 * 1000
    symbols = sorted({_symbol_key(row.get("symbol")) for row in records if _symbol_key(row.get("symbol"))})
    by_symbol: list[dict[str, Any]] = []

    for symbol in symbols:
        rows = [row for row in records if _symbol_key(row.get("symbol")) == symbol]
        recent_rows = [
            row
            for row in rows
            if int(_record_entry_ts_ms(row) or 0) >= recent_cutoff_ms
        ] if int(asof_ts_ms or 0) > 0 else []
        net_24h = _net_values(rows, 24)
        net_48h = _net_values(rows, 48)
        avg_24h = _avg(net_24h)
        avg_48h = _avg(net_48h)
        win_24h = _win_rate(net_24h)
        win_48h = _win_rate(net_48h)
        blocking: list[str] = []

        if len(rows) < int(thresholds["min_sample_count"]):
            blocking.append("sample_count_lt_30")
        if len(recent_rows) < int(thresholds["min_recent_7d_sample_count"]):
            blocking.append("recent_7d_sample_count_lt_10")
        if avg_24h is None:
            blocking.append("avg_24h_not_observable")
        elif avg_24h <= float(thresholds["min_avg_24h_net_bps"]):
            blocking.append("avg_24h_net_bps_lte_80")
        if win_24h is None:
            blocking.append("win_rate_24h_not_observable")
        elif win_24h <= float(thresholds["min_win_rate_24h"]):
            blocking.append("win_rate_24h_lte_0_60")
        if avg_48h is None:
            blocking.append("avg_48h_not_observable")
        elif avg_48h <= float(thresholds["min_avg_48h_net_bps"]):
            blocking.append("avg_48h_net_bps_lte_50")

        bnb_high_score_avg = None
        bnb_negexp = None
        if symbol == "BNB/USDT":
            bnb_high_score_avg = _high_score_24h_avg_for_symbol(high_score_blocked_rows, symbol)
            bnb_negexp = _symbol_negative_expectancy_bps(negative_expectancy_state, symbol)
            if bnb_high_score_avg is None:
                blocking.append("bnb_high_score_blocked_24h_not_observable")
            elif bnb_high_score_avg <= 0.0:
                blocking.append("bnb_high_score_blocked_24h_avg_lte_0")
            if bnb_negexp is None:
                blocking.append("bnb_negative_expectancy_not_observable")
            elif bnb_negexp < 0.0:
                blocking.append("bnb_negative_expectancy_lt_0")

        by_symbol.append(
            {
                "symbol": symbol,
                "ready_for_live_probe": not blocking,
                "blocking_reasons": ",".join(blocking),
                "sample_count": len(rows),
                "recent_sample_count": len(recent_rows),
                "avg_24h_net_bps": avg_24h,
                "avg_48h_net_bps": avg_48h,
                "win_rate_24h": win_24h,
                "win_rate_48h": win_48h,
                "bnb_high_score_blocked_24h_avg_net_bps": bnb_high_score_avg,
                "bnb_negative_expectancy_bps": bnb_negexp,
            }
        )

    overall_24h = _net_values(records, 24)
    overall_48h = _net_values(records, 48)
    overall_recent = [
        row
        for row in records
        if int(_record_entry_ts_ms(row) or 0) >= recent_cutoff_ms
    ] if int(asof_ts_ms or 0) > 0 else []
    ready_symbols = [row["symbol"] for row in by_symbol if bool(row.get("ready_for_live_probe"))]
    overall_blocking = [] if ready_symbols else ["no_symbol_ready_for_live_probe"]
    if len(records) < int(thresholds["min_sample_count"]):
        overall_blocking.append("overall_sample_count_lt_30")
    if len(overall_recent) < int(thresholds["min_recent_7d_sample_count"]):
        overall_blocking.append("overall_recent_7d_sample_count_lt_10")

    summary = {
        "schema_version": ALT_IMPULSE_READINESS_SCHEMA_VERSION,
        "generated_ts_utc": _readiness_ts_utc(asof_ts_ms),
        "ready_for_live_probe": bool(ready_symbols),
        "ready_symbols": ready_symbols,
        "blocking_reasons": overall_blocking,
        "sample_count": len(records),
        "recent_sample_count": len(overall_recent),
        "avg_24h_net_bps": _avg(overall_24h),
        "avg_48h_net_bps": _avg(overall_48h),
        "win_rate_24h": _win_rate(overall_24h),
        "win_rate_48h": _win_rate(overall_48h),
        "thresholds": thresholds,
        "by_symbol": by_symbol,
    }
    return summary, by_symbol


def _write_readiness_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_optional_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except (OSError, csv.Error):
        return []


def _alt_global_not_observable_reason(record: Mapping[str, Any], horizons: list[int]) -> str:
    reasons: list[str] = []
    for horizon in horizons:
        h = int(horizon)
        if str(record.get(f"{HORIZON_PREFIX}{h}h_status") or "").strip() != "not_observable":
            continue
        reason = str(record.get(f"{HORIZON_PREFIX}{h}h_reason") or "").strip()
        if reason:
            reasons.append(reason)
    for preferred in ("missing_entry_px", "missing_market_data", "missing_future_px"):
        if preferred in reasons:
            return preferred
    return reasons[0] if reasons else ""


def _normalize_label_status_reason(record: dict[str, Any], horizons: list[int]) -> None:
    statuses: list[str] = []
    for horizon in horizons:
        h = int(horizon)
        status_key = f"{HORIZON_PREFIX}{h}h_status"
        reason_key = f"{HORIZON_PREFIX}{h}h_reason"
        net_key = f"{HORIZON_PREFIX}{h}h_net_bps"
        status = str(record.get(status_key) or "").strip()
        if _normalize_float(record.get(net_key)) is not None:
            status = "complete"
            record[status_key] = "complete"
            record[reason_key] = ""
        if status:
            statuses.append(status)

    entry_px = _normalize_float(record.get("entry_px"))
    entry_px_observed = entry_px is not None and entry_px > 0

    if "complete" in statuses:
        record["label_status"] = "complete"
        record["label_not_observable_reason"] = ""
        return

    if "pending" in statuses:
        record["label_status"] = "pending"
        record["label_not_observable_reason"] = ""
        return

    if statuses and all(status == "not_observable" for status in statuses):
        reason = _alt_global_not_observable_reason(record, horizons)
        if entry_px_observed and reason == "missing_entry_px":
            reason = ""
        record["label_status"] = "not_observable"
        record["label_not_observable_reason"] = reason
        return

    if entry_px_observed and str(record.get("label_not_observable_reason") or "").strip() == "missing_entry_px":
        record["label_not_observable_reason"] = ""


def _aggregate_records_by_symbol_regime_horizon(
    records: list[dict[str, Any]],
    *,
    horizons: list[int],
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        key = (
            str(record.get("symbol") or "unknown"),
            str(record.get("regime_state") or record.get("regime") or "UNKNOWN"),
        )
        buckets.setdefault(key, []).append(record)
    out: list[dict[str, Any]] = []
    for (symbol, regime_state), rows in sorted(buckets.items(), key=lambda item: item[0]):
        for horizon_row in _aggregate_records_by_horizon(rows, horizons=horizons):
            payload = dict(horizon_row)
            payload["symbol"] = symbol
            payload["regime_state"] = regime_state
            out.append(_with_shadow_status(payload))
    return out


def update_alt_impulse_shadow_evaluator(
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
    if not bool(getattr(diagnostics, "alt_impulse_shadow_enabled", True)):
        return {"enabled": False, "new_records": 0, "total_records": 0}

    reports_dir = _resolve_reports_dir(run_dir)
    labels_path = _labels_path(reports_dir)
    summaries_dir = _summaries_dir(reports_dir)
    cache_root = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parents[2] / "data" / "cache"
    horizons = _label_horizons(diagnostics)

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
        key = _record_key(record)
        if key not in records_by_key:
            inserted += 1
            records_by_key[key] = record
        else:
            existing = records_by_key[key]
            for preserve_key in (
                "entry_px",
                "final_score",
                "trend_score",
                "trend_side",
                "alpha6_score",
                "alpha6_side",
                "f4_volume_expansion",
                "f5_rsi_trend_confirm",
                "btc_4h_ret_bps",
                "btc_trend_state",
                "whitelist_positive_4h_count",
                "broad_market_positive_count",
                "regime",
                "regime_state",
                "current_level",
                "risk_level",
                "funding_state",
                "volatility_bucket",
                "shadow_decision",
                "alpha_discovery_board_status",
                "paper_ready_allowed",
                "live_ready_allowed",
                "shadow_decision_reason",
                "rt_cost_bps",
            ):
                if existing.get(preserve_key) in (None, "") and record.get(preserve_key) not in (None, ""):
                    existing[preserve_key] = record.get(preserve_key)

    records = list(records_by_key.values())
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    if records:
        if ohlcv_provider is None:
            ohlcv_provider = _default_ohlcv_provider_for_cfg(cfg)
        _update_labels(
            records=records,
            cache_dir=cache_root,
            horizons=horizons,
            market_data_1h=market_data_1h,
            asof_ts_ms=asof_ts_ms,
            ohlcv_provider=ohlcv_provider,
        )
        for record in records:
            _normalize_label_status_reason(record, horizons)
            _with_shadow_status(record, decision="REGIME_SHADOW")
        records.sort(key=lambda row: (_record_entry_ts_ms(row), str(row.get("run_id") or ""), str(row.get("symbol") or ""), str(row.get("skip_reason") or "")))
        _write_records(labels_path, records)

    fields = _summary_fields(horizons)
    csv_rows = []
    for row in records:
        payload = dict(row)
        payload.pop("entry_ts_ms", None)
        csv_rows.append(payload)
    _write_csv(summaries_dir / "alt_impulse_shadow_outcomes.csv", csv_rows, fields)
    _write_csv(
        summaries_dir / "alt_impulse_shadow_outcomes_by_symbol.csv",
        _with_shadow_status_rows(_aggregate_records_by_fields(records, key_fields=["symbol", "skip_reason"], horizons=horizons)),
        [
            "symbol",
            "skip_reason",
            *ALT_IMPULSE_SHADOW_STATUS_FIELDS,
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            *[f"avg_{int(h)}h_net_bps" for h in horizons],
            *[f"win_rate_{int(h)}h" for h in horizons],
        ],
    )
    _write_csv(
        summaries_dir / "alt_impulse_shadow_outcomes_by_reason.csv",
        _with_shadow_status_rows(_aggregate_records_by_fields(records, key_fields=["skip_reason"], horizons=horizons)),
        [
            "skip_reason",
            *ALT_IMPULSE_SHADOW_STATUS_FIELDS,
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            *[f"avg_{int(h)}h_net_bps" for h in horizons],
            *[f"win_rate_{int(h)}h" for h in horizons],
        ],
    )
    _write_csv(
        summaries_dir / "alt_impulse_shadow_by_regime.csv",
        _with_shadow_status_rows(_aggregate_records_by_fields(records, key_fields=["regime_state"], horizons=horizons)),
        [
            "regime_state",
            *ALT_IMPULSE_SHADOW_STATUS_FIELDS,
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            *[f"avg_{int(h)}h_net_bps" for h in horizons],
            *[f"win_rate_{int(h)}h" for h in horizons],
        ],
    )
    _write_csv(
        summaries_dir / "alt_impulse_shadow_by_symbol_regime_horizon.csv",
        _aggregate_records_by_symbol_regime_horizon(records, horizons=horizons),
        [
            "symbol",
            "regime_state",
            "horizon_hours",
            *ALT_IMPULSE_SHADOW_STATUS_FIELDS,
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            "avg_net_bps",
            "win_rate",
        ],
    )
    _write_csv(
        summaries_dir / "alt_impulse_shadow_outcomes_by_horizon.csv",
        _with_shadow_status_rows(_aggregate_records_by_horizon(records, horizons=horizons)),
        [
            "horizon_hours",
            *ALT_IMPULSE_SHADOW_STATUS_FIELDS,
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            "avg_net_bps",
            "win_rate",
        ],
    )
    readiness_summary, readiness_by_symbol = build_alt_impulse_shadow_readiness(
        records,
        asof_ts_ms=asof_ts_ms,
        high_score_blocked_rows=_read_optional_csv_rows(summaries_dir / "high_score_blocked_outcomes.csv"),
        negative_expectancy_state=getattr(audit, "negative_expectancy_state", None),
    )
    _write_readiness_json(summaries_dir / "alt_impulse_shadow_readiness.json", readiness_summary)
    _write_csv(
        summaries_dir / "alt_impulse_shadow_readiness_by_symbol.csv",
        readiness_by_symbol,
        ALT_IMPULSE_READINESS_FIELDS,
    )
    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
