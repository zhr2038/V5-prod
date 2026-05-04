from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from configs.schema import AppConfig, DiagnosticsConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit
from src.reporting.skipped_candidate_tracker import (
    HORIZON_PREFIX,
    _aggregate_records_by_fields,
    _coerce_epoch_ms,
    _default_ohlcv_provider_for_cfg,
    _find_close_at_or_after,
    _iso_from_ms,
    _load_existing_records,
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
ALT_IMPULSE_SHADOW_HORIZONS = [4, 8, 12, 24]


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
            "whitelist_positive_4h_count": positive_count,
            "regime": str(item.get("regime") or getattr(audit, "regime", "") or ""),
            "current_level": level,
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
        "whitelist_positive_4h_count",
        "regime",
        "current_level",
        "rt_cost_bps",
        *horizon_fields,
        "label_status",
        "label_not_observable_reason",
    ]


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
    horizons = list(ALT_IMPULSE_SHADOW_HORIZONS)

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
                "whitelist_positive_4h_count",
                "regime",
                "current_level",
                "rt_cost_bps",
            ):
                if existing.get(preserve_key) in (None, "") and record.get(preserve_key) not in (None, ""):
                    existing[preserve_key] = record.get(preserve_key)

    records = list(records_by_key.values())
    if records:
        asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
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
        _aggregate_records_by_fields(records, key_fields=["symbol", "skip_reason"], horizons=horizons),
        [
            "symbol",
            "skip_reason",
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            "avg_4h_net_bps",
            "avg_8h_net_bps",
            "avg_12h_net_bps",
            "avg_24h_net_bps",
            "win_rate_4h",
            "win_rate_8h",
            "win_rate_12h",
            "win_rate_24h",
        ],
    )
    _write_csv(
        summaries_dir / "alt_impulse_shadow_outcomes_by_reason.csv",
        _aggregate_records_by_fields(records, key_fields=["skip_reason"], horizons=horizons),
        [
            "skip_reason",
            "count",
            "pending_count",
            "not_observable_count",
            "complete_count",
            "avg_4h_net_bps",
            "avg_8h_net_bps",
            "avg_12h_net_bps",
            "avg_24h_net_bps",
            "win_rate_4h",
            "win_rate_8h",
            "win_rate_12h",
            "win_rate_24h",
        ],
    )
    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
