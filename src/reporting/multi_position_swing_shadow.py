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
    ONE_HOUR_MS,
    _coerce_epoch_ms,
    _default_ohlcv_provider_for_cfg,
    _ensure_provider_series,
    _find_close_at_or_after,
    _iso_from_ms,
    _load_cache_ohlcv,
    _normalize_float,
    _normalize_horizons,
    _record_entry_ts_ms,
    _resolve_reports_dir,
    _series_for_symbol,
    _summaries_dir,
    _write_csv,
    _write_records,
)


DEFAULT_SWING_SHADOW_SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
DEFAULT_SWING_SHADOW_HORIZONS = [24, 48, 72]
SHADOW_MODE_ALL_CANDIDATES = "all_candidates"
SHADOW_MODE_PROTECT_RECOVERY_RULES = "protect_recovery_rules"
SHADOW_MODES = [SHADOW_MODE_ALL_CANDIDATES, SHADOW_MODE_PROTECT_RECOVERY_RULES]
NEGATIVE_EXPECTANCY_HARD_REASONS = {
    "negative_expectancy_cooldown",
    "negative_expectancy_open_block",
    "negative_expectancy_fast_fail_open_block",
    "protect_negative_expectancy_short_cycle_block",
}


def _diagnostics_cfg(cfg: Any) -> DiagnosticsConfig:
    diagnostics = getattr(cfg, "diagnostics", None)
    return diagnostics if diagnostics is not None else DiagnosticsConfig()


def _labels_path(reports_dir: Path) -> Path:
    return reports_dir / "multi_position_swing_shadow_labels.jsonl"


def _swing_shadow_horizons(diagnostics: Any) -> list[int]:
    return _normalize_horizons(
        getattr(diagnostics, "multi_position_swing_shadow_horizons_hours", None),
        DEFAULT_SWING_SHADOW_HORIZONS,
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


def _symbol_text(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "/")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _shadow_key(record: Mapping[str, Any]) -> str:
    symbols = _parse_symbols(record.get("symbols"))
    return "|".join(
        [
            str(record.get("shadow_mode") or SHADOW_MODE_ALL_CANDIDATES),
            str(record.get("run_id") or ""),
            str(record.get("ts_utc") or ""),
            str(record.get("k") or ""),
            ",".join(symbols),
        ]
    )


def _parse_symbols(value: Any) -> list[str]:
    raw = _json_loads(value, value)
    if isinstance(raw, str):
        raw = [part.strip() for part in raw.split(",")]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        symbol = _symbol_text(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


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
            key = _shadow_key(payload)
            if key:
                records[key] = _merge_record(records.get(key), payload)
    return records


def _status_rank(value: Any) -> int:
    text = str(value or "").strip()
    if text == "complete":
        return 3
    if text == "pending":
        return 2
    if text == "not_observable":
        return 1
    return 0


def _merge_record(existing: Optional[dict[str, Any]], incoming: Mapping[str, Any]) -> dict[str, Any]:
    if existing is None:
        return dict(incoming)
    if _status_rank(incoming.get("label_status")) > _status_rank(existing.get("label_status")):
        base = dict(incoming)
        other = existing
    else:
        base = dict(existing)
        other = incoming
    for key, value in dict(other).items():
        if base.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
            base[key] = value
    return base


def _strategy_signal_map(audit: DecisionAudit) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for strategy in getattr(audit, "strategy_signals", []) or []:
        for signal in (strategy or {}).get("signals", []) or []:
            symbol = _symbol_text((signal or {}).get("symbol"))
            if symbol and symbol not in out:
                out[symbol] = dict(signal or {})
    return out


def _target_explain_map(audit: DecisionAudit) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in getattr(audit, "target_execution_explain", []) or []:
        if not isinstance(item, Mapping):
            continue
        symbol = _symbol_text(item.get("symbol"))
        if symbol:
            out[symbol] = dict(item)
    return out


def _router_reasons_by_symbol(audit: DecisionAudit) -> dict[str, set[str]]:
    out: dict[str, set[str]] = defaultdict(set)
    for item in getattr(audit, "router_decisions", []) or []:
        if not isinstance(item, Mapping):
            continue
        symbol = _symbol_text(item.get("symbol"))
        reason = str(item.get("reason") or "").strip()
        if symbol and reason:
            out[symbol].add(reason)
    return out


def _price_from_mapping(item: Mapping[str, Any]) -> Optional[float]:
    for key in ("entry_px", "latest_px", "current_px", "price", "px", "last_px"):
        value = _normalize_float(item.get(key))
        if value is not None and value > 0:
            return value
    return None


def _find_close_at_or_before(series: list[dict[str, float | int]], target_ts_ms: int) -> Optional[float]:
    out: Optional[float] = None
    for row in series:
        ts_ms = int(row.get("timestamp_ms") or 0)
        if ts_ms > int(target_ts_ms):
            break
        close = _normalize_float(row.get("close"))
        if close is not None and close > 0:
            out = close
    return out


def _four_hour_return_bps(
    *,
    symbol: str,
    asof_ts_ms: int,
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    cached: dict[str, list[dict[str, float | int]]],
) -> Optional[float]:
    if asof_ts_ms <= 0:
        return None
    rows = _series_for_symbol(
        symbol=symbol,
        cache_dir=cache_dir,
        market_data_1h=market_data_1h,
        cached=cached,
    )
    start_px = _find_close_at_or_before(rows, asof_ts_ms - 4 * ONE_HOUR_MS)
    end_px = _find_close_at_or_before(rows, asof_ts_ms)
    if start_px is None or start_px <= 0 or end_px is None or end_px <= 0:
        return None
    return ((float(end_px) / float(start_px)) - 1.0) * 10_000.0


def _entry_px(
    *,
    symbol: str,
    entry_ts_ms: int,
    market_data_1h: Dict[str, MarketSeries],
    cache_dir: Path,
    cached: dict[str, list[dict[str, float | int]]],
    candidates: Iterable[Mapping[str, Any]],
) -> Optional[float]:
    for candidate in candidates:
        price = _price_from_mapping(candidate)
        if price is not None:
            return price
    rows = _series_for_symbol(
        symbol=symbol,
        cache_dir=cache_dir,
        market_data_1h=market_data_1h,
        cached=cached,
    )
    return _find_close_at_or_after(rows, entry_ts_ms)


def _is_risk_off(*, audit: DecisionAudit, current_level: Optional[str]) -> bool:
    values = [getattr(audit, "regime", None), getattr(audit, "current_level", None), current_level]
    for value in values:
        text = str(value or "").strip().lower().replace("_", "-")
        if text in {"risk-off", "riskoff"}:
            return True
    return False


def _protect_recovery_allowed_symbols(cfg: AppConfig) -> list[str]:
    execution = getattr(cfg, "execution", None)
    raw = getattr(execution, "protect_recovery_allowed_symbols", None) or ["BTC/USDT", "SOL/USDT", "ETH/USDT"]
    out: list[str] = []
    seen: set[str] = set()
    for symbol in raw:
        text = _symbol_text(symbol)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _negative_expectancy_entry(audit: DecisionAudit, symbol: str) -> dict[str, Any]:
    state = getattr(audit, "negative_expectancy_state", {}) or {}
    if not isinstance(state, Mapping):
        return {}
    wanted = _symbol_text(symbol)
    for section_name in ("stats", "symbols"):
        section = state.get(section_name)
        if not isinstance(section, Mapping):
            continue
        for raw_symbol, entry in section.items():
            if _symbol_text(raw_symbol) == wanted and isinstance(entry, Mapping):
                return dict(entry)
    for raw_symbol, entry in state.items():
        if _symbol_text(raw_symbol) == wanted and isinstance(entry, Mapping):
            return dict(entry)
    return {}


def _negative_expectancy_bps(entry: Mapping[str, Any], *, fast_fail: bool = False) -> Optional[float]:
    keys = (
        ("fast_fail_net_expectancy_bps", "fast_fail_expectancy_bps", "net_expectancy_bps")
        if fast_fail
        else ("net_expectancy_bps", "expectancy_bps")
    )
    for key in keys:
        value = _normalize_float(entry.get(key))
        if value is not None:
            return value
    return None


def _symbol_has_negative_expectancy(audit: DecisionAudit, symbol: str, router_reasons: set[str]) -> bool:
    if router_reasons & NEGATIVE_EXPECTANCY_HARD_REASONS:
        return True
    entry = _negative_expectancy_entry(audit, symbol)
    if not entry:
        return False
    closed_cycles = _normalize_float(entry.get("closed_cycles")) or 0.0
    fast_fail_cycles = _normalize_float(entry.get("fast_fail_closed_cycles")) or 0.0
    net_bps = _negative_expectancy_bps(entry)
    fast_fail_bps = _negative_expectancy_bps(entry, fast_fail=True)
    if closed_cycles > 0 and net_bps is not None and net_bps < 0:
        return True
    if fast_fail_cycles > 0 and fast_fail_bps is not None and fast_fail_bps < 0:
        return True
    return False


def _alpha6_confirmed_for_swing(*, cfg: AppConfig, alpha6_side: str, alpha6_score: Optional[float], f4: Optional[float], f5: Optional[float]) -> bool:
    execution = getattr(cfg, "execution", None)
    min_alpha6 = float(getattr(execution, "swing_min_alpha6_score", 0.50) or 0.50)
    min_f5 = float(getattr(execution, "swing_min_f5_rsi", 0.30) or 0.30)
    min_f4 = float(getattr(execution, "swing_min_f4_volume", 0.0) or 0.0)
    if str(alpha6_side or "").strip().lower() != "buy":
        return False
    if alpha6_score is None or alpha6_score < min_alpha6:
        return False
    if f4 is None or f4 < min_f4:
        return False
    if f5 is None or f5 < min_f5:
        return False
    return True


def _collect_candidates(
    *,
    audit: DecisionAudit,
    cfg: AppConfig,
    market_data_1h: Dict[str, MarketSeries],
    current_level: Optional[str],
    cache_dir: Path,
    shadow_mode: str,
) -> list[dict[str, Any]]:
    diagnostics = _diagnostics_cfg(cfg)
    mode = str(shadow_mode or SHADOW_MODE_ALL_CANDIDATES)
    if mode == SHADOW_MODE_PROTECT_RECOVERY_RULES:
        allowed_symbols = set(_protect_recovery_allowed_symbols(cfg))
    else:
        allowed_symbols = {
            _symbol_text(symbol)
            for symbol in (
                getattr(diagnostics, "multi_position_swing_shadow_symbols", None)
                or DEFAULT_SWING_SHADOW_SYMBOLS
            )
        }
    min_score = float(getattr(diagnostics, "multi_position_swing_shadow_min_final_score", 0.30) or 0.30)
    if _is_risk_off(audit=audit, current_level=current_level):
        return []
    if mode == SHADOW_MODE_PROTECT_RECOVERY_RULES:
        execution = getattr(cfg, "execution", None)
        require_market_context = bool(getattr(execution, "protect_recovery_require_market_context", True))
        min_positive = int(getattr(execution, "protect_recovery_min_positive_whitelist_4h_count", 3) or 0)
        cached_for_context: dict[str, list[dict[str, float | int]]] = {}
        asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
        positive_count = 0
        for symbol in allowed_symbols:
            ret_bps = _four_hour_return_bps(
                symbol=symbol,
                asof_ts_ms=asof_ts_ms,
                market_data_1h=market_data_1h,
                cache_dir=cache_dir,
                cached=cached_for_context,
            )
            if ret_bps is not None and ret_bps > 0:
                positive_count += 1
        if require_market_context and positive_count < min_positive:
            return []

    explain_by_symbol = _target_explain_map(audit)
    signal_by_symbol = _strategy_signal_map(audit)
    router_reasons = _router_reasons_by_symbol(audit)
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    cached: dict[str, list[dict[str, float | int]]] = {}
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    for idx, row in enumerate(getattr(audit, "top_scores", []) or []):
        if not isinstance(row, Mapping):
            continue
        symbol = _symbol_text(row.get("symbol"))
        if symbol not in allowed_symbols or symbol in seen:
            continue
        final_score = _normalize_float(row.get("final_score"))
        if final_score is None:
            final_score = _normalize_float(row.get("score"))
        if final_score is None:
            final_score = _normalize_float(row.get("display_score"))
        if final_score is None or final_score < min_score:
            continue
        reasons = router_reasons.get(symbol, set())
        has_negative_expectancy = _symbol_has_negative_expectancy(audit, symbol, reasons)
        if mode == SHADOW_MODE_ALL_CANDIDATES and reasons & NEGATIVE_EXPECTANCY_HARD_REASONS:
            continue
        if (
            mode == SHADOW_MODE_PROTECT_RECOVERY_RULES
            and bool(getattr(getattr(cfg, "execution", None), "protect_recovery_disallow_symbols_with_negative_expectancy", True))
            and has_negative_expectancy
        ):
            continue
        explain = explain_by_symbol.get(symbol, {})
        signal = signal_by_symbol.get(symbol, {})
        px = _entry_px(
            symbol=symbol,
            entry_ts_ms=asof_ts_ms,
            market_data_1h=market_data_1h,
            cache_dir=cache_dir,
            cached=cached,
            candidates=(explain, row, signal),
        )
        selected_rank = _normalize_float(row.get("rank"))
        if selected_rank is None:
            selected_rank = _normalize_float(explain.get("selected_rank"))
        alpha6_score = _normalize_float(explain.get("alpha6_score"))
        alpha6_side = str(explain.get("alpha6_side") or signal.get("side") or "").strip().lower()
        f4 = _normalize_float(explain.get("f4_volume_expansion"))
        f5 = _normalize_float(explain.get("f5_rsi_trend_confirm"))
        alpha6_confirmed = _alpha6_confirmed_for_swing(
            cfg=cfg,
            alpha6_side=alpha6_side,
            alpha6_score=alpha6_score,
            f4=f4,
            f5=f5,
        )
        out.append(
            {
                "symbol": symbol,
                "final_score": float(final_score),
                "selected_rank": int(selected_rank) if selected_rank is not None else idx + 1,
                "entry_px": float(px) if px is not None and px > 0 else None,
                "target_w": _normalize_float(explain.get("target_w")),
                "router_action": str(explain.get("router_action") or "").strip().lower() or None,
                "router_reason": str(explain.get("router_reason") or "").strip() or None,
                "trend_score": _normalize_float(explain.get("trend_score")),
                "alpha6_score": alpha6_score,
                "alpha6_side": alpha6_side or None,
                "f4_volume_expansion": f4,
                "f5_rsi_trend_confirm": f5,
                "entry_support": "alpha6_confirmed" if alpha6_confirmed else "score",
                "negative_expectancy_excluded": bool(has_negative_expectancy),
            }
        )
        seen.add(symbol)

    if mode == SHADOW_MODE_PROTECT_RECOVERY_RULES:
        return sorted(
            out,
            key=lambda item: (
                0 if item.get("entry_support") == "alpha6_confirmed" else 1,
                -float(item.get("final_score") or 0.0),
                int(item.get("selected_rank") or 999),
            ),
        )
    return sorted(out, key=lambda item: (-float(item.get("final_score") or 0.0), int(item.get("selected_rank") or 999)))


def _collect_shadow_records(
    *,
    audit: DecisionAudit,
    cfg: AppConfig,
    market_data_1h: Dict[str, MarketSeries],
    current_level: Optional[str],
    cache_dir: Path,
) -> list[dict[str, Any]]:
    diagnostics = _diagnostics_cfg(cfg)
    rt_cost_bps = float(getattr(diagnostics, "multi_position_swing_shadow_rt_cost_bps", 30.0) or 30.0)
    asof_ts_ms = _asof_ts_ms(audit, market_data_1h)
    ts_utc = _iso_from_ms(asof_ts_ms) if asof_ts_ms > 0 else ""
    records: list[dict[str, Any]] = []
    for shadow_mode in SHADOW_MODES:
        candidates = _collect_candidates(
            audit=audit,
            cfg=cfg,
            market_data_1h=market_data_1h,
            current_level=current_level,
            cache_dir=cache_dir,
            shadow_mode=shadow_mode,
        )
        if not candidates:
            continue
        for k in range(1, min(3, len(candidates)) + 1):
            selected = candidates[:k]
            symbols = [item["symbol"] for item in selected]
            record = {
                "ts_utc": ts_utc,
                "entry_ts_ms": asof_ts_ms,
                "run_id": str(getattr(audit, "run_id", "") or ""),
                "shadow_mode": shadow_mode,
                "k": k,
                "symbols": symbols,
                "equal_weight": round(1.0 / float(k), 8),
                "entry_px": {
                    item["symbol"]: item.get("entry_px")
                    for item in selected
                },
                "final_score": {
                    item["symbol"]: item.get("final_score")
                    for item in selected
                },
                "selected_rank": {
                    item["symbol"]: item.get("selected_rank")
                    for item in selected
                },
                "entry_support": {
                    item["symbol"]: item.get("entry_support")
                    for item in selected
                },
                "rt_cost_bps": rt_cost_bps,
                "label_status": "pending",
            }
            records.append(record)
    return records


def _ensure_symbol_series(
    *,
    symbol: str,
    entry_ts_ms: int,
    horizon_ms: int,
    cache_dir: Path,
    market_data_1h: Dict[str, MarketSeries],
    cached: dict[str, list[dict[str, float | int]]],
    ohlcv_provider: Any,
    fetched_windows: set[tuple[str, int, int]],
) -> list[dict[str, float | int]]:
    rows = _series_for_symbol(
        symbol=symbol,
        cache_dir=cache_dir,
        market_data_1h=market_data_1h,
        cached=cached,
    )
    if _find_close_at_or_after(rows, entry_ts_ms + horizon_ms) is not None:
        return rows
    return _ensure_provider_series(
        symbol=symbol,
        entry_ts_ms=entry_ts_ms,
        end_ts_ms=entry_ts_ms + horizon_ms,
        provider=ohlcv_provider,
        cached=cached,
        fetched_windows=fetched_windows,
    )


def _update_labels(
    *,
    records: list[dict[str, Any]],
    cache_dir: Path,
    horizons: list[int],
    market_data_1h: Dict[str, MarketSeries],
    asof_ts_ms: int,
    ohlcv_provider: Any,
) -> None:
    cached: dict[str, list[dict[str, float | int]]] = {}
    fetched_windows: set[tuple[str, int, int]] = set()
    for record in records:
        symbols = _parse_symbols(record.get("symbols"))
        entry_px_by_symbol = _json_loads(record.get("entry_px"), {})
        if not isinstance(entry_px_by_symbol, dict):
            entry_px_by_symbol = {}
        entry_ts_ms = _record_entry_ts_ms(record)
        rt_cost_bps = float(_normalize_float(record.get("rt_cost_bps")) or 0.0)
        horizon_statuses: list[str] = []
        for horizon in horizons:
            h = int(horizon)
            target_ts_ms = int(entry_ts_ms) + h * ONE_HOUR_MS
            status_key = f"{HORIZON_PREFIX}{h}h_status"
            reason_key = f"{HORIZON_PREFIX}{h}h_reason"
            avg_key = f"{HORIZON_PREFIX}{h}h_portfolio_avg_net_bps"
            worst_key = f"{HORIZON_PREFIX}{h}h_worst_symbol_net_bps"
            win_key = f"{HORIZON_PREFIX}{h}h_win_count"
            symbol_key = f"{HORIZON_PREFIX}{h}h_symbol_net_bps"
            if int(asof_ts_ms or 0) < target_ts_ms:
                if record.get(status_key) == "complete" and record.get(avg_key) is not None:
                    horizon_statuses.append("complete")
                    continue
                record[avg_key] = None
                record[worst_key] = None
                record[win_key] = None
                record[symbol_key] = {}
                record[status_key] = "pending"
                record[reason_key] = f"awaiting_horizon_until_{_iso_from_ms(target_ts_ms)}"
                horizon_statuses.append("pending")
                continue

            symbol_net: dict[str, float] = {}
            missing: list[str] = []
            for symbol in symbols:
                entry_px = _normalize_float(entry_px_by_symbol.get(symbol))
                if entry_px is None or entry_px <= 0:
                    missing.append(f"{symbol}:missing_entry_px")
                    continue
                rows = _ensure_symbol_series(
                    symbol=symbol,
                    entry_ts_ms=entry_ts_ms,
                    horizon_ms=h * ONE_HOUR_MS,
                    cache_dir=cache_dir,
                    market_data_1h=market_data_1h,
                    cached=cached,
                    ohlcv_provider=ohlcv_provider,
                    fetched_windows=fetched_windows,
                )
                future_close = _find_close_at_or_after(rows, target_ts_ms)
                if future_close is None or future_close <= 0:
                    missing.append(f"{symbol}:missing_future_px")
                    continue
                gross_bps = (float(future_close) / float(entry_px) - 1.0) * 10_000.0
                symbol_net[symbol] = round(float(gross_bps) - rt_cost_bps, 6)
            if missing or len(symbol_net) != len(symbols):
                record[avg_key] = None
                record[worst_key] = None
                record[win_key] = None
                record[symbol_key] = symbol_net
                record[status_key] = "not_observable"
                record[reason_key] = ";".join(missing) or "missing_market_data"
                horizon_statuses.append("not_observable")
                continue
            values = list(symbol_net.values())
            record[symbol_key] = symbol_net
            record[avg_key] = round(sum(values) / len(values), 6)
            record[worst_key] = round(min(values), 6)
            record[win_key] = int(sum(1 for value in values if value > 0.0))
            record[status_key] = "complete"
            record[reason_key] = ""
            horizon_statuses.append("complete")

        if "complete" in horizon_statuses:
            record["label_status"] = "complete"
        elif "not_observable" in horizon_statuses:
            record["label_status"] = "not_observable"
        else:
            record["label_status"] = "pending"


def _csv_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        row = dict(record)
        row.pop("entry_ts_ms", None)
        for key in ("symbols", "entry_px", "final_score", "selected_rank", "entry_support"):
            if isinstance(row.get(key), (dict, list)):
                row[key] = _json_dumps(row[key])
        for key, value in list(row.items()):
            if key.endswith("_symbol_net_bps") and isinstance(value, dict):
                row[key] = _json_dumps(value)
        rows.append(row)
    return rows


def _fields(horizons: list[int]) -> list[str]:
    horizon_fields: list[str] = []
    for horizon in horizons:
        h = int(horizon)
        horizon_fields.extend(
            [
                f"{HORIZON_PREFIX}{h}h_status",
                f"{HORIZON_PREFIX}{h}h_portfolio_avg_net_bps",
                f"{HORIZON_PREFIX}{h}h_worst_symbol_net_bps",
                f"{HORIZON_PREFIX}{h}h_win_count",
                f"{HORIZON_PREFIX}{h}h_symbol_net_bps",
                f"{HORIZON_PREFIX}{h}h_reason",
            ]
        )
    return [
        "ts_utc",
        "run_id",
        "shadow_mode",
        "k",
        "symbols",
        "equal_weight",
        "entry_px",
        "final_score",
        "selected_rank",
        "entry_support",
        "rt_cost_bps",
        *horizon_fields,
        "label_status",
    ]


def _aggregate_by_k(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        try:
            grouped[(str(record.get("shadow_mode") or SHADOW_MODE_ALL_CANDIDATES), int(record.get("k") or 0))].append(record)
        except Exception:
            continue
    out: list[dict[str, Any]] = []
    for (shadow_mode, k), rows in sorted(grouped.items()):
        payload: dict[str, Any] = {"shadow_mode": shadow_mode, "k": k, "count": len(rows)}
        for horizon in DEFAULT_SWING_SHADOW_HORIZONS:
            values = [
                _normalize_float(row.get(f"{HORIZON_PREFIX}{horizon}h_portfolio_avg_net_bps"))
                for row in rows
            ]
            usable = [value for value in values if value is not None]
            payload[f"avg_{horizon}h_net_bps"] = round(sum(usable) / len(usable), 6) if usable else None
        values_24h = [
            _normalize_float(row.get(f"{HORIZON_PREFIX}24h_portfolio_avg_net_bps"))
            for row in rows
        ]
        usable_24h = [value for value in values_24h if value is not None]
        worst_24h = [
            _normalize_float(row.get(f"{HORIZON_PREFIX}24h_worst_symbol_net_bps"))
            for row in rows
        ]
        usable_worst_24h = [value for value in worst_24h if value is not None]
        payload["win_rate"] = (
            round(sum(1 for value in usable_24h if value > 0.0) / len(usable_24h), 6)
            if usable_24h
            else None
        )
        payload["worst_avg"] = (
            round(sum(usable_worst_24h) / len(usable_worst_24h), 6)
            if usable_worst_24h
            else None
        )
        out.append(payload)
    return out


def _aggregate_by_symbol(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        for symbol in _parse_symbols(record.get("symbols")):
            grouped[(str(record.get("shadow_mode") or SHADOW_MODE_ALL_CANDIDATES), symbol)].append(record)
    out: list[dict[str, Any]] = []
    for (shadow_mode, symbol), rows in sorted(grouped.items()):
        payload: dict[str, Any] = {"shadow_mode": shadow_mode, "symbol": symbol, "count": len(rows)}
        for horizon in DEFAULT_SWING_SHADOW_HORIZONS:
            values = []
            for row in rows:
                per_symbol = _json_loads(row.get(f"{HORIZON_PREFIX}{horizon}h_symbol_net_bps"), {})
                if isinstance(per_symbol, dict):
                    value = _normalize_float(per_symbol.get(symbol))
                    if value is not None:
                        values.append(value)
            payload[f"avg_{horizon}h_net_bps"] = round(sum(values) / len(values), 6) if values else None
            payload[f"win_rate_{horizon}h"] = (
                round(sum(1 for value in values if value > 0.0) / len(values), 6)
                if values
                else None
            )
        out.append(payload)
    return out


def update_multi_position_swing_shadow_evaluator(
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
    if not bool(getattr(diagnostics, "multi_position_swing_shadow_enabled", True)):
        return {"enabled": False, "new_records": 0, "total_records": 0}

    reports_dir = _resolve_reports_dir(run_dir)
    labels_path = _labels_path(reports_dir)
    summaries_dir = _summaries_dir(reports_dir)
    cache_root = Path(cache_dir) if cache_dir is not None else Path(__file__).resolve().parents[2] / "data" / "cache"
    horizons = _swing_shadow_horizons(diagnostics)

    records_by_key = _load_existing_records(labels_path)
    new_records = _collect_shadow_records(
        audit=audit,
        cfg=cfg,
        market_data_1h=market_data_1h,
        current_level=current_level,
        cache_dir=cache_root,
    )
    inserted = 0
    for record in new_records:
        key = _shadow_key(record)
        if key not in records_by_key:
            inserted += 1
            records_by_key[key] = record
        else:
            records_by_key[key] = _merge_record(records_by_key[key], record)

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
        records.sort(key=lambda row: (_record_entry_ts_ms(row), str(row.get("run_id") or ""), str(row.get("shadow_mode") or SHADOW_MODE_ALL_CANDIDATES), int(row.get("k") or 0)))
        _write_records(labels_path, records)

    fields = _fields(horizons)
    _write_csv(summaries_dir / "multi_position_swing_shadow_outcomes.csv", _csv_rows(records), fields)
    _write_csv(
        summaries_dir / "multi_position_swing_shadow_by_k.csv",
        _aggregate_by_k(records),
        [
            "shadow_mode",
            "k",
            "count",
            "avg_24h_net_bps",
            "avg_48h_net_bps",
            "avg_72h_net_bps",
            "win_rate",
            "worst_avg",
        ],
    )
    _write_csv(
        summaries_dir / "multi_position_swing_shadow_by_symbol.csv",
        _aggregate_by_symbol(records),
        [
            "shadow_mode",
            "symbol",
            "count",
            "avg_24h_net_bps",
            "avg_48h_net_bps",
            "avg_72h_net_bps",
            "win_rate_24h",
            "win_rate_48h",
            "win_rate_72h",
        ],
    )
    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
