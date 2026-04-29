from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from configs.schema import AppConfig, DiagnosticsConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ONE_HOUR_MS = 3600 * 1000
FOCUS_SKIP_REASONS = {
    "protect_entry_trend_only",
    "protect_entry_no_alpha6_confirmation",
    "protect_entry_alpha6_rsi_confirm_negative",
    "protect_entry_alpha6_score_too_low",
    "protect_entry_volume_confirm_negative",
    "protect_entry_rsi_confirm_too_weak",
    "cost_aware_edge",
    "target_zero_no_order",
    "risk_off_pos_mult_zero",
    "all_scores_below_threshold",
    "hold_current_no_valid_replacement",
}
BTC_LEADERSHIP_PROBE_SKIP_PREFIX = "btc_leadership_probe_"
HORIZON_PREFIX = "label_"
BTC_LEADERSHIP_PROBE_FIELDS = [
    "rolling_high",
    "breakout_buffer_bps",
    "breakout_met",
    "min_alpha6_score",
    "min_f4_volume",
    "min_f5_rsi",
    "negative_expectancy_bypassed",
    "closed_cycles",
    "net_expectancy_bps",
]
BTC_LEADERSHIP_PROBE_LABEL_KEY_FIELDS = ("run_id", "ts_utc", "symbol", "skip_reason")
BTC_LEADERSHIP_PROBE_NOT_OBSERVABLE_SKIP_REASONS = {
    "btc_leadership_probe_not_flat",
    "btc_leadership_probe_cooldown",
}


def _diagnostics_cfg(cfg: Any) -> DiagnosticsConfig:
    diagnostics = getattr(cfg, "diagnostics", None)
    if diagnostics is None:
        return DiagnosticsConfig()
    return diagnostics


def _iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_int(value: Any) -> Optional[int]:
    parsed = _normalize_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except Exception:
        return None


def _normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _is_btc_leadership_probe_skip_reason(reason: str) -> bool:
    return str(reason or "").startswith(BTC_LEADERSHIP_PROBE_SKIP_PREFIX)


def _resolve_reports_dir(run_dir: str | Path) -> Path:
    path = Path(run_dir)
    return path if path.name == "reports" else path.resolve().parent.parent


def _coerce_epoch_ms(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except Exception:
        return None
    if value <= 0:
        return None
    if value < 10_000_000_000:
        value *= 1000.0
    return int(value)


def _skipped_candidate_labels_path(reports_dir: Path) -> Path:
    return reports_dir / "skipped_candidate_labels.jsonl"


def _summaries_dir(reports_dir: Path) -> Path:
    return reports_dir / "summaries"


def _cache_file_epoch(path: Path, *, prefix: str) -> float:
    suffix = path.stem[len(prefix):] if path.stem.startswith(prefix) else path.stem

    hourly_match = re.search(r"(20\d{6}_\d{2})$", suffix)
    if hourly_match:
        try:
            return datetime.strptime(hourly_match.group(1), "%Y%m%d_%H").timestamp()
        except Exception:
            pass

    date_tokens = re.findall(r"(20\d{2}-\d{2}-\d{2}|20\d{6})", suffix)
    if date_tokens:
        token = date_tokens[-1]
        try:
            fmt = "%Y-%m-%d" if "-" in token else "%Y%m%d"
            return datetime.strptime(token, fmt).timestamp()
        except Exception:
            pass

    return path.stat().st_mtime


def _parse_timestamp_to_ms(raw: Any) -> Optional[int]:
    text = str(raw or "").strip()
    if not text:
        return None
    numeric_ts = _coerce_epoch_ms(text)
    if numeric_ts is not None:
        return numeric_ts
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _record_entry_ts_ms(record: Mapping[str, Any]) -> int:
    entry_ts_ms = _coerce_epoch_ms(record.get("entry_ts_ms"))
    ts_utc_ms = _parse_timestamp_to_ms(record.get("ts_utc"))
    if entry_ts_ms is not None:
        return int(entry_ts_ms)
    return int(ts_utc_ms or 0)


def _entry_ts_mismatch_reason(record: Mapping[str, Any]) -> str:
    entry_ts_ms = _coerce_epoch_ms(record.get("entry_ts_ms"))
    ts_utc_ms = _parse_timestamp_to_ms(record.get("ts_utc"))
    if entry_ts_ms is None or ts_utc_ms is None:
        return ""
    if abs(int(entry_ts_ms) - int(ts_utc_ms)) <= 1000:
        return ""
    return (
        "entry_ts_ms_ts_utc_mismatch"
        f"; entry_ts_ms={int(entry_ts_ms)}"
        f"; ts_utc_ms={int(ts_utc_ms)}"
    )


def _load_cache_ohlcv(cache_dir: Path, symbol: str) -> list[dict[str, float | int]]:
    prefix = str(symbol or "").replace("/", "_").replace("-", "_").strip()
    rows: dict[int, dict[str, float | int]] = {}
    for path in sorted(cache_dir.glob(f"{prefix}_1H_*.csv"), key=lambda path: _cache_file_epoch(path, prefix=f"{prefix}_1H_")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    ts_ms = _parse_timestamp_to_ms(row.get("timestamp"))
                    close = _normalize_float(row.get("close"))
                    if ts_ms is None or close is None:
                        continue
                    rows[ts_ms] = {"timestamp_ms": ts_ms, "close": close}
        except Exception:
            continue
    return [rows[key] for key in sorted(rows.keys())]


def _series_from_market_data(series: Optional[MarketSeries]) -> list[dict[str, float | int]]:
    if series is None:
        return []
    rows: dict[int, dict[str, float | int]] = {}
    for ts_raw, close_raw in zip(getattr(series, "ts", []) or [], getattr(series, "close", []) or []):
        ts_ms = _coerce_epoch_ms(ts_raw)
        close = _normalize_float(close_raw)
        if ts_ms is None or close is None:
            continue
        rows[ts_ms] = {"timestamp_ms": ts_ms, "close": close}
    return [rows[key] for key in sorted(rows.keys())]


def _merge_series(*parts: list[dict[str, float | int]]) -> list[dict[str, float | int]]:
    merged: dict[int, dict[str, float | int]] = {}
    for rows in parts:
        for row in rows:
            ts_ms = int(row.get("timestamp_ms") or 0)
            close = _normalize_float(row.get("close"))
            if ts_ms <= 0 or close is None:
                continue
            merged[ts_ms] = {"timestamp_ms": ts_ms, "close": close}
    return [merged[key] for key in sorted(merged.keys())]


def _find_close_at_or_after(series: list[dict[str, float | int]], target_ts_ms: int) -> Optional[float]:
    for row in series:
        ts_ms = int(row.get("timestamp_ms") or 0)
        if ts_ms >= int(target_ts_ms):
            value = _normalize_float(row.get("close"))
            if value is not None:
                return value
    return None


def _latest_series_ts(series: list[dict[str, float | int]]) -> int:
    if not series:
        return 0
    return int(series[-1].get("timestamp_ms") or 0)


def _build_strategy_signal_lookup(audit: DecisionAudit) -> Dict[str, Dict[str, Dict[str, Any]]]:
    lookup: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for entry in (audit.strategy_signals or []):
        strategy = str((entry or {}).get("strategy", "") or "").strip()
        if not strategy:
            continue
        per_symbol: Dict[str, Dict[str, Any]] = {}
        for signal in (entry or {}).get("signals", []) or []:
            symbol = str((signal or {}).get("symbol", "") or "").strip()
            if symbol:
                per_symbol[symbol] = dict(signal or {})
        lookup[strategy] = per_symbol
    return lookup


def _signal_score(signal: Optional[Mapping[str, Any]]) -> Optional[float]:
    if not isinstance(signal, Mapping):
        return None
    for key in ("score", "raw_score"):
        value = _normalize_float(signal.get(key))
        if value is not None:
            return value
    return None


def _signal_factor(signal: Optional[Mapping[str, Any]], name: str) -> Optional[float]:
    if not isinstance(signal, Mapping):
        return None
    metadata = signal.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    for bucket in ("z_factors", "raw_factors"):
        values = metadata.get(bucket)
        if not isinstance(values, Mapping):
            continue
        value = _normalize_float(values.get(name))
        if value is not None:
            return value
    return None


def _record_key(record: Mapping[str, Any]) -> str:
    if _is_btc_leadership_probe_skip_reason(str(record.get("skip_reason") or "")):
        return btc_leadership_probe_label_key(record)
    return "|".join(
        [
            str(record.get("run_id") or ""),
            str(record.get("symbol") or ""),
            str(record.get("skip_reason") or ""),
            str(record.get("intended_side") or ""),
            str(_record_entry_ts_ms(record) or ""),
        ]
    )


def btc_leadership_probe_label_key(record: Mapping[str, Any]) -> str:
    ts_utc = str(record.get("ts_utc") or "").strip()
    if not ts_utc:
        entry_ts_ms = _record_entry_ts_ms(record)
        ts_utc = _iso_from_ms(entry_ts_ms) if entry_ts_ms > 0 else ""
    return "|".join(
        [
            str(record.get("run_id") or ""),
            ts_utc,
            str(record.get("symbol") or "BTC/USDT"),
            str(record.get("skip_reason") or ""),
        ]
    )


def _label_status_priority(status: Any) -> int:
    text = str(status or "").strip()
    if text == "complete":
        return 3
    if text == "pending":
        return 2
    if text == "not_observable":
        return 1
    return 0


def _merge_record(existing: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    incoming_dict = dict(incoming)
    if _label_status_priority(incoming_dict.get("label_status")) > _label_status_priority(existing.get("label_status")):
        base = incoming_dict
        other = existing
    else:
        base = dict(existing)
        other = incoming_dict
    for key, value in other.items():
        if base.get(key) in (None, "") and value not in (None, ""):
            base[key] = value
    return base


def _load_existing_records(path: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            key = _record_key(payload)
            if key in records:
                records[key] = _merge_record(records[key], payload)
            else:
                records[key] = payload
    return records


def _write_records(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record, ensure_ascii=False, sort_keys=True) for record in records]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def _aggregate_records(records: list[dict[str, Any]], *, key_field: str, horizons: list[int]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        buckets[str(record.get(key_field) or "unknown")].append(record)

    out: list[dict[str, Any]] = []
    for bucket_key, rows in sorted(buckets.items(), key=lambda item: item[0]):
        payload: dict[str, Any] = {
            key_field: bucket_key,
            "count": len(rows),
            "pending_count": sum(1 for row in rows if str(row.get("label_status") or "") == "pending"),
            "not_observable_count": sum(1 for row in rows if str(row.get("label_status") or "") == "not_observable"),
            "complete_count": sum(1 for row in rows if str(row.get("label_status") or "") == "complete"),
        }
        for horizon in horizons:
            net_key = f"{HORIZON_PREFIX}{int(horizon)}h_net_bps"
            values = [_normalize_float(row.get(net_key)) for row in rows]
            usable = [value for value in values if value is not None]
            payload[f"avg_{int(horizon)}h_net_bps"] = round(sum(usable) / len(usable), 6) if usable else None
            payload[f"win_rate_{int(horizon)}h"] = round(
                sum(1 for value in usable if float(value) > 0.0) / len(usable),
                6,
            ) if usable else None
        out.append(payload)
    return out


def _build_record(
    *,
    audit: DecisionAudit,
    cfg: AppConfig,
    current_level: Optional[str],
    strategy_lookup: Dict[str, Dict[str, Dict[str, Any]]],
    top_scores: Dict[str, float],
    symbol: str,
    skip_reason: str,
    intended_side: str,
    target_w: Optional[float],
    entry_px: Optional[float],
    entry_ts_ms: int,
    router_decision: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    router_decision = dict(router_decision or {})
    trend_signal = (strategy_lookup.get("TrendFollowing") or {}).get(symbol)
    alpha6_signal = (strategy_lookup.get("Alpha6Factor") or {}).get(symbol)

    score = _normalize_float(router_decision.get("score"))
    if score is None:
        score = top_scores.get(symbol)

    alpha6_score = _normalize_float(router_decision.get("alpha6_score"))
    if alpha6_score is None:
        alpha6_score = _normalize_float(router_decision.get("actual_alpha6_score"))
    if alpha6_score is None:
        alpha6_score = _signal_score(alpha6_signal)

    trend_score = _normalize_float(router_decision.get("trend_score"))
    if trend_score is None:
        trend_score = _signal_score(trend_signal)

    f4_volume_expansion = _normalize_float(router_decision.get("f4_volume_expansion"))
    if f4_volume_expansion is None:
        f4_volume_expansion = _normalize_float(router_decision.get("actual_f4_volume"))
    if f4_volume_expansion is None:
        f4_volume_expansion = _signal_factor(alpha6_signal, "f4_volume_expansion")

    f5_rsi_trend_confirm = _normalize_float(router_decision.get("f5_rsi_trend_confirm"))
    if f5_rsi_trend_confirm is None:
        f5_rsi_trend_confirm = _normalize_float(router_decision.get("actual_f5_rsi"))
    if f5_rsi_trend_confirm is None:
        f5_rsi_trend_confirm = _signal_factor(alpha6_signal, "f5_rsi_trend_confirm")

    required_score = _normalize_float(router_decision.get("required_score"))
    if required_score is None and skip_reason == "all_scores_below_threshold":
        required_score = float(getattr(cfg.alpha, "min_score_threshold", 0.0) or 0.0)
    if required_score is None and skip_reason == "protect_entry_alpha6_score_too_low":
        required_score = float(getattr(cfg.execution, "protect_entry_alpha6_min_score", 0.0) or 0.0)

    min_alpha6_score = _normalize_float(router_decision.get("min_alpha6_score"))
    if min_alpha6_score is None and _is_btc_leadership_probe_skip_reason(skip_reason):
        min_alpha6_score = float(getattr(cfg.execution, "btc_leadership_probe_min_alpha6_score", 0.0) or 0.0)
    min_f4_volume = _normalize_float(router_decision.get("min_f4_volume"))
    if min_f4_volume is None and _is_btc_leadership_probe_skip_reason(skip_reason):
        min_f4_volume = float(getattr(cfg.execution, "btc_leadership_probe_min_f4_volume", 0.0) or 0.0)
    min_f5_rsi = _normalize_float(router_decision.get("min_f5_rsi"))
    if min_f5_rsi is None and _is_btc_leadership_probe_skip_reason(skip_reason):
        min_f5_rsi = float(getattr(cfg.execution, "btc_leadership_probe_min_f5_rsi", 0.0) or 0.0)

    negative_expectancy_bypassed = _normalize_bool(router_decision.get("negative_expectancy_bypassed"))
    if negative_expectancy_bypassed is None:
        negative_expectancy_bypassed = _normalize_bool(router_decision.get("bypassed_negative_expectancy"))
    if negative_expectancy_bypassed is None and _is_btc_leadership_probe_skip_reason(skip_reason):
        negative_expectancy_bypassed = False

    net_expectancy_bps = _normalize_float(router_decision.get("net_expectancy_bps"))
    if net_expectancy_bps is None:
        net_expectancy_bps = _normalize_float(router_decision.get("expectancy_bps"))
    if net_expectancy_bps is None:
        net_expectancy_bps = _normalize_float(router_decision.get("fast_fail_expectancy_bps"))

    diagnostics = _diagnostics_cfg(cfg)
    record = {
        "ts_utc": _iso_from_ms(entry_ts_ms),
        "run_id": str(audit.run_id),
        "symbol": str(symbol),
        "intended_side": str(intended_side),
        "skip_reason": str(skip_reason),
        "score": score,
        "required_score": required_score,
        "regime": str(audit.regime or "Unknown"),
        "current_level": str(current_level or router_decision.get("current_level") or ""),
        "target_w": _normalize_float(target_w),
        "alpha6_score": alpha6_score,
        "trend_score": trend_score,
        "f4_volume_expansion": f4_volume_expansion,
        "f5_rsi_trend_confirm": f5_rsi_trend_confirm,
        "rolling_high": _normalize_float(router_decision.get("rolling_high")),
        "breakout_buffer_bps": _normalize_float(router_decision.get("breakout_buffer_bps")),
        "breakout_met": _normalize_bool(router_decision.get("breakout_met")),
        "min_alpha6_score": min_alpha6_score,
        "min_f4_volume": min_f4_volume,
        "min_f5_rsi": min_f5_rsi,
        "negative_expectancy_bypassed": negative_expectancy_bypassed,
        "closed_cycles": _normalize_int(router_decision.get("closed_cycles")),
        "net_expectancy_bps": net_expectancy_bps,
        "entry_px": _normalize_float(entry_px),
        "rt_cost_bps": float(getattr(diagnostics, "skipped_candidate_roundtrip_cost_bps", 30.0) or 30.0),
        "entry_ts_ms": int(entry_ts_ms),
        "label_status": "pending",
        "label_not_observable_reason": "",
    }
    for horizon in getattr(diagnostics, "skipped_candidate_horizons_hours", [4, 8, 12, 24]) or [4, 8, 12, 24]:
        h = int(horizon)
        record[f"{HORIZON_PREFIX}{h}h_gross_bps"] = None
        record[f"{HORIZON_PREFIX}{h}h_net_bps"] = None
        record[f"{HORIZON_PREFIX}{h}h_would_have_won_net"] = None
        record[f"{HORIZON_PREFIX}{h}h_status"] = "pending"
        record[f"{HORIZON_PREFIX}{h}h_reason"] = ""
    return record


def _collect_skipped_candidates(
    *,
    audit: DecisionAudit,
    cfg: AppConfig,
    market_data_1h: Dict[str, MarketSeries],
    current_level: Optional[str],
) -> list[dict[str, Any]]:
    strategy_lookup = _build_strategy_signal_lookup(audit)
    top_scores = {
        str(item.get("symbol") or ""): float(item.get("score") or 0.0)
        for item in (audit.top_scores or [])
        if str(item.get("symbol") or "").strip()
    }
    captured_keys: set[Any] = set()
    records: list[dict[str, Any]] = []

    def _entry_context(symbol: str, router_decision: Mapping[str, Any] | None) -> tuple[Optional[float], int]:
        decision = router_decision or {}
        px = _normalize_float(decision.get("px"))
        if px is None:
            for px_key in ("entry_px", "latest_px", "last_px", "signal_price"):
                px = _normalize_float(decision.get(px_key))
                if px is not None:
                    break
        ts_ms = 0
        series = market_data_1h.get(symbol)
        if series and getattr(series, "close", None) and getattr(series, "ts", None):
            latest_idx = None
            latest_ts = 0
            for idx, raw_ts in enumerate(series.ts):
                candidate_ts = int(_coerce_epoch_ms(raw_ts) or 0)
                if candidate_ts >= latest_ts:
                    latest_ts = candidate_ts
                    latest_idx = idx
            if latest_idx is not None:
                ts_ms = latest_ts
                if px is None and latest_idx < len(series.close):
                    try:
                        px = float(series.close[latest_idx])
                    except Exception:
                        px = None
        if ts_ms <= 0:
            for ts_key in ("ts_ms", "timestamp_ms", "timestamp", "ts", "now_ts"):
                ts_ms = int(_coerce_epoch_ms(decision.get(ts_key)) or 0)
                if ts_ms > 0:
                    break
        if ts_ms <= 0:
            ts_ms = int(_coerce_epoch_ms(getattr(audit, "now_ts", 0)) or 0)
        return px, ts_ms

    for rd in (audit.router_decisions or []):
        if str(rd.get("action") or "").lower() != "skip":
            continue
        symbol = str(rd.get("symbol") or rd.get("held_symbol") or "").strip()
        reason = str(rd.get("reason") or "").strip()
        is_btc_probe_skip = _is_btc_leadership_probe_skip_reason(reason)
        if is_btc_probe_skip and not symbol:
            symbol = "BTC/USDT"
        if not symbol:
            continue
        if reason == "target_zero_no_order":
            zero_reason = str(rd.get("target_zero_reason") or "").strip()
            reason = zero_reason if zero_reason == "risk_off_pos_mult_zero" else "target_zero_no_order"
        is_btc_probe_skip = _is_btc_leadership_probe_skip_reason(reason)
        if reason not in FOCUS_SKIP_REASONS and not is_btc_probe_skip:
            continue
        entry_px, entry_ts_ms = _entry_context(symbol, rd)
        if entry_ts_ms <= 0 and is_btc_probe_skip:
            entry_ts_ms = int(_coerce_epoch_ms(getattr(audit, "now_ts", 0)) or 0)
        if entry_ts_ms <= 0:
            continue
        intended_side = "buy" if is_btc_probe_skip else ("hold" if reason == "hold_current_no_valid_replacement" else "buy")
        target_w = rd.get("effective_target_w", rd.get("target_w", (audit.targets_post_risk or {}).get(symbol)))
        record = _build_record(
            audit=audit,
            cfg=cfg,
            current_level=current_level,
            strategy_lookup=strategy_lookup,
            top_scores=top_scores,
            symbol=symbol,
            skip_reason=reason,
            intended_side=intended_side,
            target_w=target_w,
            entry_px=entry_px,
            entry_ts_ms=entry_ts_ms,
            router_decision=rd,
        )
        key = _record_key(record) if is_btc_probe_skip else "|".join([symbol, reason])
        if key in captured_keys:
            continue
        captured_keys.add(key)
        records.append(record)

    if int((audit.counts or {}).get("selected", 0) or 0) == 0 and audit.top_scores:
        threshold = float(getattr(cfg.alpha, "min_score_threshold", 0.0) or 0.0)
        for item in (audit.top_scores or []):
            symbol = str(item.get("symbol") or "").strip()
            if not symbol:
                continue
            try:
                score = float(item.get("score") or 0.0)
            except Exception:
                continue
            if score >= threshold:
                continue
            key = (symbol, "all_scores_below_threshold")
            if key in captured_keys:
                continue
            captured_keys.add(key)
            entry_px, entry_ts_ms = _entry_context(symbol, None)
            if entry_ts_ms <= 0:
                continue
            records.append(
                _build_record(
                    audit=audit,
                    cfg=cfg,
                    current_level=current_level,
                    strategy_lookup=strategy_lookup,
                    top_scores=top_scores,
                    symbol=symbol,
                    skip_reason="all_scores_below_threshold",
                    intended_side="buy",
                    target_w=(audit.targets_post_risk or {}).get(symbol),
                    entry_px=entry_px,
                    entry_ts_ms=entry_ts_ms,
                    router_decision={"score": score, "required_score": threshold},
                )
            )
    return records


def _label_direction(record: Mapping[str, Any]) -> float:
    side = str(record.get("intended_side") or "").lower()
    return -1.0 if side == "sell" else 1.0


def _series_for_symbol(
    *,
    symbol: str,
    cache_dir: Path,
    market_data_1h: Dict[str, MarketSeries],
    cached: dict[str, list[dict[str, float | int]]],
) -> list[dict[str, float | int]]:
    if symbol not in cached:
        cached[symbol] = _merge_series(
            _load_cache_ohlcv(cache_dir, symbol),
            _series_from_market_data(market_data_1h.get(symbol)),
        )
    return cached[symbol]


def _fetch_provider_ohlcv(
    provider: Any,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, float | int]]:
    if provider is None or not hasattr(provider, "fetch_ohlcv"):
        return []
    span_ms = max(int(end_ms) - int(start_ms), ONE_HOUR_MS)
    limit = max(8, int(span_ms // ONE_HOUR_MS) + 4)
    try:
        series_map = provider.fetch_ohlcv(
            symbols=[symbol],
            timeframe="1h",
            limit=limit,
            end_ts_ms=int(end_ms) + ONE_HOUR_MS,
        )
    except TypeError:
        try:
            series_map = provider.fetch_ohlcv([symbol], "1h", limit)
        except Exception:
            return []
    except Exception:
        return []
    if not isinstance(series_map, Mapping):
        return []
    series = series_map.get(symbol)
    if series is None:
        alt_symbol = str(symbol or "").replace("/", "-")
        series = series_map.get(alt_symbol)
    rows = _series_from_market_data(series)
    return [
        row
        for row in rows
        if int(row.get("timestamp_ms") or 0) >= int(start_ms) - ONE_HOUR_MS
        and int(row.get("timestamp_ms") or 0) <= int(end_ms) + ONE_HOUR_MS
    ]


def _ensure_provider_series(
    *,
    symbol: str,
    entry_ts_ms: int,
    end_ts_ms: int,
    provider: Any,
    cached: dict[str, list[dict[str, float | int]]],
    fetched_windows: dict[str, tuple[int, int]],
) -> list[dict[str, float | int]]:
    if provider is None:
        return cached.get(symbol, [])
    requested_start = int(entry_ts_ms)
    requested_end = int(end_ts_ms)
    fetched_start, fetched_end = fetched_windows.get(symbol, (0, 0))
    if fetched_start and fetched_end and fetched_start <= requested_start and fetched_end >= requested_end:
        return cached.get(symbol, [])

    fetch_start = min([value for value in (fetched_start, requested_start) if int(value) > 0], default=requested_start)
    fetch_end = max(int(fetched_end or 0), requested_end)
    fetched_rows = _fetch_provider_ohlcv(
        provider,
        symbol,
        start_ms=fetch_start,
        end_ms=fetch_end,
    )
    fetched_windows[symbol] = (fetch_start, fetch_end)
    if fetched_rows:
        cached[symbol] = _merge_series(cached.get(symbol, []), fetched_rows)
    return cached.get(symbol, [])


def _default_ohlcv_provider_for_cfg(cfg: AppConfig) -> Any:
    mode = str(getattr(getattr(cfg, "execution", None), "mode", "") or "").lower()
    if mode != "live":
        return None
    try:
        from src.data.okx_ccxt_provider import OKXCCXTProvider
    except Exception:
        return None
    try:
        return OKXCCXTProvider(rate_limit=True)
    except Exception:
        return None


def _update_labels(
    *,
    records: list[dict[str, Any]],
    cache_dir: Path,
    horizons: list[int],
    market_data_1h: Dict[str, MarketSeries],
    asof_ts_ms: int,
    ohlcv_provider: Any = None,
) -> None:
    series_cache: dict[str, list[dict[str, float | int]]] = {}
    fetched_windows: dict[str, tuple[int, int]] = {}
    for record in records:
        symbol = str(record.get("symbol") or "")
        series = _series_for_symbol(
            symbol=symbol,
            cache_dir=cache_dir,
            market_data_1h=market_data_1h,
            cached=series_cache,
        )
        entry_px = _normalize_float(record.get("entry_px"))
        entry_ts_ms = _record_entry_ts_ms(record)
        ts_mismatch_reason = _entry_ts_mismatch_reason(record)
        if entry_ts_ms > 0:
            record["entry_ts_ms"] = int(entry_ts_ms)
            if not ts_mismatch_reason:
                record["ts_utc"] = _iso_from_ms(entry_ts_ms)
        rt_cost_bps = float(record.get("rt_cost_bps") or 0.0)
        if entry_px is None or entry_px <= 0 or entry_ts_ms <= 0 or ts_mismatch_reason:
            skip_reason = str(record.get("skip_reason") or "")
            if (
                entry_px is None or entry_px <= 0
            ) and skip_reason in BTC_LEADERSHIP_PROBE_NOT_OBSERVABLE_SKIP_REASONS:
                not_observable_reason = f"{skip_reason}_entry_px_not_observable"
            elif ts_mismatch_reason:
                not_observable_reason = ts_mismatch_reason
            elif entry_ts_ms <= 0:
                not_observable_reason = "missing_entry_ts"
            else:
                not_observable_reason = "missing_entry_px"
            for horizon in horizons:
                h = int(horizon)
                record[f"{HORIZON_PREFIX}{h}h_status"] = "not_observable"
                record[f"{HORIZON_PREFIX}{h}h_reason"] = not_observable_reason
            record["label_status"] = "not_observable"
            record["label_not_observable_reason"] = not_observable_reason
            continue

        latest_ts = _latest_series_ts(series)
        horizon_statuses: list[str] = []
        for horizon in horizons:
            h = int(horizon)
            target_ts_ms = entry_ts_ms + h * 3600 * 1000
            gross_key = f"{HORIZON_PREFIX}{h}h_gross_bps"
            net_key = f"{HORIZON_PREFIX}{h}h_net_bps"
            win_key = f"{HORIZON_PREFIX}{h}h_would_have_won_net"
            status_key = f"{HORIZON_PREFIX}{h}h_status"
            reason_key = f"{HORIZON_PREFIX}{h}h_reason"

            if int(asof_ts_ms or 0) < target_ts_ms:
                if record.get(status_key) == "complete" and record.get(net_key) is not None:
                    horizon_statuses.append("complete")
                    continue
                record[gross_key] = None
                record[net_key] = None
                record[win_key] = None
                record[status_key] = "pending"
                record[reason_key] = f"awaiting_horizon_until_{_iso_from_ms(target_ts_ms)}"
                horizon_statuses.append("pending")
                continue

            future_close = _find_close_at_or_after(series, target_ts_ms)
            if future_close is None:
                series = _ensure_provider_series(
                    symbol=symbol,
                    entry_ts_ms=entry_ts_ms,
                    end_ts_ms=int(asof_ts_ms or target_ts_ms),
                    provider=ohlcv_provider,
                    cached=series_cache,
                    fetched_windows=fetched_windows,
                )
                latest_ts = _latest_series_ts(series)
                future_close = _find_close_at_or_after(series, target_ts_ms)

            if future_close is not None:
                gross_forward_bps = ((float(future_close) / float(entry_px)) - 1.0) * 10_000.0
                net_forward_bps = float(gross_forward_bps) - float(rt_cost_bps)
                record[gross_key] = round(gross_forward_bps, 6)
                record[net_key] = round(net_forward_bps, 6)
                record[win_key] = bool(net_forward_bps > 0.0)
                record[status_key] = "complete"
                record[reason_key] = ""
                horizon_statuses.append("complete")
                continue

            record[gross_key] = None
            record[net_key] = None
            record[win_key] = None
            record[status_key] = "not_observable"
            record[reason_key] = (
                f"missing_price_at_or_after_{_iso_from_ms(target_ts_ms)}"
                f"; latest_available={_iso_from_ms(latest_ts) if latest_ts > 0 else 'none'}"
            )
            horizon_statuses.append("not_observable")

        if "complete" in horizon_statuses:
            record["label_status"] = "complete"
        elif "not_observable" in horizon_statuses:
            record["label_status"] = "not_observable"
        else:
            record["label_status"] = "pending"


def update_skipped_candidate_tracker(
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
    if not bool(getattr(diagnostics, "skipped_candidate_label_enabled", True)):
        return {"enabled": False, "new_records": 0, "total_records": 0}

    reports_dir = _resolve_reports_dir(run_dir)
    labels_path = _skipped_candidate_labels_path(reports_dir)
    summaries_dir = _summaries_dir(reports_dir)
    horizons = [int(value) for value in (getattr(diagnostics, "skipped_candidate_horizons_hours", [4, 8, 12, 24]) or [4, 8, 12, 24])]
    cache_root = Path(cache_dir) if cache_dir is not None else (PROJECT_ROOT / "data" / "cache")

    records_by_key = _load_existing_records(labels_path)
    new_records = _collect_skipped_candidates(
        audit=audit,
        cfg=cfg,
        market_data_1h=market_data_1h,
        current_level=current_level,
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
                "score",
                "required_score",
                "alpha6_score",
                "trend_score",
                "f4_volume_expansion",
                "f5_rsi_trend_confirm",
                "target_w",
                "current_level",
                *BTC_LEADERSHIP_PROBE_FIELDS,
            ):
                if existing.get(preserve_key) in (None, "") and record.get(preserve_key) not in (None, ""):
                    existing[preserve_key] = record.get(preserve_key)

    records = list(records_by_key.values())
    asof_ts_ms = int(_coerce_epoch_ms(getattr(audit, "now_ts", 0)) or 0)
    if asof_ts_ms <= 0:
        asof_ts_ms = max(
            [0]
            + [
                int(_coerce_epoch_ms(max(getattr(series, "ts", []) or [0])) or 0)
                for series in (market_data_1h or {}).values()
            ]
        )
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

    horizon_fields = []
    for h in horizons:
        horizon_fields.extend(
            [
                f"{HORIZON_PREFIX}{int(h)}h_gross_bps",
                f"{HORIZON_PREFIX}{int(h)}h_net_bps",
                f"{HORIZON_PREFIX}{int(h)}h_would_have_won_net",
                f"{HORIZON_PREFIX}{int(h)}h_status",
                f"{HORIZON_PREFIX}{int(h)}h_reason",
            ]
        )
    full_rows = []
    for row in records:
        payload = dict(row)
        payload.pop("entry_ts_ms", None)
        full_rows.append(payload)

    base_fields = [
        "ts_utc",
        "run_id",
        "symbol",
        "intended_side",
        "skip_reason",
        "score",
        "required_score",
        "regime",
        "current_level",
        "target_w",
        "alpha6_score",
        "trend_score",
        "f4_volume_expansion",
        "f5_rsi_trend_confirm",
        *BTC_LEADERSHIP_PROBE_FIELDS,
        "entry_px",
        "rt_cost_bps",
        *horizon_fields,
        "label_status",
        "label_not_observable_reason",
    ]
    _write_csv(summaries_dir / "skipped_candidate_outcomes.csv", full_rows, base_fields)

    by_reason = _aggregate_records(records, key_field="skip_reason", horizons=horizons)
    by_reason_fields = [
        "skip_reason",
        "count",
        "pending_count",
        "not_observable_count",
        "complete_count",
        *[f"avg_{int(h)}h_net_bps" for h in horizons],
        *[f"win_rate_{int(h)}h" for h in horizons],
    ]
    _write_csv(summaries_dir / "skipped_candidate_outcomes_by_reason.csv", by_reason, by_reason_fields)

    by_symbol = _aggregate_records(records, key_field="symbol", horizons=horizons)
    by_symbol_fields = [
        "symbol",
        "count",
        "pending_count",
        "not_observable_count",
        "complete_count",
        *[f"avg_{int(h)}h_net_bps" for h in horizons],
        *[f"win_rate_{int(h)}h" for h in horizons],
    ]
    _write_csv(summaries_dir / "skipped_candidate_outcomes_by_symbol.csv", by_symbol, by_symbol_fields)

    btc_probe_rows = [
        row
        for row in full_rows
        if _is_btc_leadership_probe_skip_reason(str(row.get("skip_reason") or ""))
    ]
    _write_csv(summaries_dir / "btc_leadership_probe_blocked_outcomes.csv", btc_probe_rows, base_fields)

    btc_probe_by_reason = _aggregate_records(
        [
            row
            for row in records
            if _is_btc_leadership_probe_skip_reason(str(row.get("skip_reason") or ""))
        ],
        key_field="skip_reason",
        horizons=horizons,
    )
    _write_csv(
        summaries_dir / "btc_leadership_probe_blocked_outcomes_by_reason.csv",
        btc_probe_by_reason,
        by_reason_fields,
    )

    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
