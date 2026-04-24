from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.reporting.decision_audit import DecisionAudit


PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
HORIZON_PREFIX = "label_"


def _iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _resolve_reports_dir(run_dir: str | Path) -> Path:
    path = Path(run_dir)
    return path if path.name == "reports" else path.resolve().parent.parent


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


def _load_cache_ohlcv(cache_dir: Path, symbol: str) -> list[dict[str, float | int]]:
    prefix = str(symbol or "").replace("/", "_").replace("-", "_").strip()
    rows: dict[int, dict[str, float | int]] = {}
    for path in sorted(cache_dir.glob(f"{prefix}_1H_*.csv"), key=lambda path: _cache_file_epoch(path, prefix=f"{prefix}_1H_")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    ts_raw = row.get("timestamp")
                    close_raw = row.get("close")
                    if ts_raw is None or close_raw in (None, ""):
                        continue
                    try:
                        ts_ms = int(datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")).timestamp() * 1000)
                        close = float(close_raw)
                    except Exception:
                        continue
                    rows[ts_ms] = {"timestamp_ms": ts_ms, "close": close}
        except Exception:
            continue
    return [rows[key] for key in sorted(rows.keys())]


def _find_close_at_or_after(series: list[dict[str, float | int]], target_ts_ms: int) -> Optional[float]:
    for row in series:
        ts_ms = int(row.get("timestamp_ms") or 0)
        if ts_ms >= int(target_ts_ms):
            value = _normalize_float(row.get("close"))
            if value is not None:
                return value
    return None


def _latest_cache_ts(series: list[dict[str, float | int]]) -> int:
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
    return "|".join(
        [
            str(record.get("run_id") or ""),
            str(record.get("symbol") or ""),
            str(record.get("skip_reason") or ""),
            str(record.get("intended_side") or ""),
            str(record.get("entry_ts_ms") or ""),
        ]
    )


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
        if not isinstance(payload, dict):
            continue
        records[_record_key(payload)] = payload
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
            label_key = f"{HORIZON_PREFIX}{int(horizon)}h_net_bps"
            values = [_normalize_float(row.get(label_key)) for row in rows]
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
        alpha6_score = _signal_score(alpha6_signal)

    trend_score = _normalize_float(router_decision.get("trend_score"))
    if trend_score is None:
        trend_score = _signal_score(trend_signal)

    f4_volume_expansion = _normalize_float(router_decision.get("f4_volume_expansion"))
    if f4_volume_expansion is None:
        f4_volume_expansion = _signal_factor(alpha6_signal, "f4_volume_expansion")

    f5_rsi_trend_confirm = _normalize_float(router_decision.get("f5_rsi_trend_confirm"))
    if f5_rsi_trend_confirm is None:
        f5_rsi_trend_confirm = _signal_factor(alpha6_signal, "f5_rsi_trend_confirm")

    required_score = _normalize_float(router_decision.get("required_score"))
    if required_score is None and skip_reason == "all_scores_below_threshold":
        required_score = float(getattr(cfg.alpha, "min_score_threshold", 0.0) or 0.0)
    if required_score is None and skip_reason == "protect_entry_alpha6_score_too_low":
        required_score = float(getattr(cfg.execution, "protect_entry_alpha6_min_score", 0.0) or 0.0)

    return {
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
        "entry_px": _normalize_float(entry_px),
        "rt_cost_bps": float(getattr(cfg.diagnostics, "skipped_candidate_roundtrip_cost_bps", 30.0) or 30.0),
        "entry_ts_ms": int(entry_ts_ms),
        "label_status": "pending",
    }


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
    captured_keys: set[tuple[str, str]] = set()
    records: list[dict[str, Any]] = []

    def _entry_context(symbol: str, router_decision: Mapping[str, Any] | None) -> tuple[Optional[float], int]:
        decision = router_decision or {}
        px = _normalize_float(decision.get("px"))
        ts_ms = 0
        series = market_data_1h.get(symbol)
        if series and getattr(series, "close", None) and getattr(series, "ts", None):
            latest_idx = None
            latest_ts = 0
            for idx, raw_ts in enumerate(series.ts):
                try:
                    candidate_ts = int(raw_ts or 0)
                except Exception:
                    continue
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
        return px, ts_ms

    for rd in (audit.router_decisions or []):
        if str(rd.get("action") or "").lower() != "skip":
            continue
        symbol = str(rd.get("symbol") or rd.get("held_symbol") or "").strip()
        if not symbol:
            continue
        reason = str(rd.get("reason") or "").strip()
        if reason == "target_zero_no_order":
            zero_reason = str(rd.get("target_zero_reason") or "").strip()
            reason = zero_reason if zero_reason == "risk_off_pos_mult_zero" else "target_zero_no_order"
        if reason not in FOCUS_SKIP_REASONS:
            continue
        key = (symbol, reason)
        if key in captured_keys:
            continue
        captured_keys.add(key)
        entry_px, entry_ts_ms = _entry_context(symbol, rd)
        if entry_ts_ms <= 0:
            continue
        intended_side = "hold" if reason == "hold_current_no_valid_replacement" else "buy"
        target_w = rd.get("target_w", (audit.targets_post_risk or {}).get(symbol))
        records.append(
            _build_record(
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
        )

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


def _update_labels(
    *,
    records: list[dict[str, Any]],
    cache_dir: Path,
    horizons: list[int],
) -> None:
    series_cache: dict[str, list[dict[str, float | int]]] = {}
    for record in records:
        symbol = str(record.get("symbol") or "")
        if symbol not in series_cache:
            series_cache[symbol] = _load_cache_ohlcv(cache_dir, symbol)

        series = series_cache[symbol]
        entry_px = _normalize_float(record.get("entry_px"))
        entry_ts_ms = int(record.get("entry_ts_ms") or 0)
        rt_cost_bps = float(record.get("rt_cost_bps") or 0.0)
        if entry_px is None or entry_px <= 0 or entry_ts_ms <= 0:
            record["label_status"] = "not_observable"
            continue

        latest_ts = _latest_cache_ts(series)
        status = "complete"
        for horizon in horizons:
            target_ts_ms = entry_ts_ms + int(horizon) * 3600 * 1000
            future_close = _find_close_at_or_after(series, target_ts_ms)
            label_key = f"{HORIZON_PREFIX}{int(horizon)}h_net_bps"
            if future_close is None:
                record[label_key] = None
                if latest_ts < target_ts_ms:
                    status = "pending"
                else:
                    status = "not_observable" if status != "pending" else status
                continue

            gross_forward_bps = (float(future_close) / float(entry_px) - 1.0) * 10_000.0
            record[label_key] = round(float(gross_forward_bps) - float(rt_cost_bps), 6)
        record["label_status"] = status


def update_skipped_candidate_tracker(
    *,
    run_dir: str | Path,
    audit: DecisionAudit,
    market_data_1h: Dict[str, MarketSeries],
    cfg: AppConfig,
    current_level: Optional[str],
    cache_dir: str | Path | None = None,
) -> dict[str, Any]:
    if not bool(getattr(cfg.diagnostics, "skipped_candidate_label_enabled", True)):
        return {"enabled": False, "new_records": 0, "total_records": 0}

    reports_dir = _resolve_reports_dir(run_dir)
    labels_path = _skipped_candidate_labels_path(reports_dir)
    summaries_dir = _summaries_dir(reports_dir)
    horizons = [int(value) for value in (getattr(cfg.diagnostics, "skipped_candidate_horizons_hours", [4, 8, 12, 24]) or [4, 8, 12, 24])]
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

    records = list(records_by_key.values())
    _update_labels(records=records, cache_dir=cache_root, horizons=horizons)
    records.sort(key=lambda row: (int(row.get("entry_ts_ms") or 0), str(row.get("run_id") or ""), str(row.get("symbol") or ""), str(row.get("skip_reason") or "")))
    _write_records(labels_path, records)

    full_rows = []
    for row in records:
        payload = dict(row)
        payload.pop("entry_ts_ms", None)
        full_rows.append(payload)
    horizon_fields = [f"{HORIZON_PREFIX}{int(h)}h_net_bps" for h in horizons]
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
        "entry_px",
        "rt_cost_bps",
        *horizon_fields,
        "label_status",
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

    return {
        "enabled": True,
        "new_records": int(inserted),
        "total_records": int(len(records)),
        "labels_path": str(labels_path),
        "summaries_dir": str(summaries_dir),
    }
