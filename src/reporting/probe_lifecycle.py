from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional


PROBE_TYPES = {"market_impulse_probe", "btc_leadership_probe"}

ROUNDTRIP_FIELDS = [
    "open_time_utc",
    "close_time_utc",
    "hold_minutes",
    "symbol",
    "qty",
    "entry_px",
    "exit_px",
    "gross_pnl_usdt",
    "net_pnl_usdt",
    "gross_bps",
    "net_bps",
    "fee_usdt",
    "entry_reason",
    "exit_reason",
    "probe_type",
    "open_run_id",
    "close_run_id",
]

PROBE_LIFECYCLE_FIELDS = [
    "probe_type",
    "entry_ts",
    "entry_px",
    "exit_ts",
    "exit_px",
    "exit_reason",
    "gross_bps",
    "net_bps",
    "remaining_value_usdt",
    "state_still_present_after_close",
    "symbol",
    "hold_minutes",
    "fee_usdt",
]


@dataclass
class TradeEvent:
    ts: datetime
    run_id: str
    source_file: str
    row_number: int
    symbol: str
    side: str
    intent: str
    qty: float
    price: float
    notional_usdt: float
    fee_usdt: float
    entry_reason: Optional[str]
    exit_reason: Optional[str]
    probe_type: Optional[str]


@dataclass
class OpenLot:
    event: TradeEvent
    qty_remaining: float
    fee_remaining: float


def _parse_ts(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _norm_symbol(value: Any) -> str:
    return str(value or "").strip().replace("-", "/").upper()


def _norm_side(value: Any) -> str:
    return str(value or "").strip().lower()


def _reason_from_mapping(payload: Mapping[str, Any], *, is_exit: bool) -> Optional[str]:
    keys = (
        ("exit_reason", "source_reason", "reason")
        if is_exit
        else ("entry_reason", "source_reason", "reason")
    )
    for key in keys:
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    return None


def _probe_type_from_mapping(payload: Mapping[str, Any], reason: Optional[str]) -> Optional[str]:
    raw = str(payload.get("probe_type") or "").strip()
    if raw in PROBE_TYPES:
        return raw
    if reason in PROBE_TYPES:
        return reason
    for probe_type in PROBE_TYPES:
        if bool(payload.get(probe_type, False)):
            return probe_type
    return None


def _audit_run_id(run_dir: Path, payload: Mapping[str, Any]) -> str:
    return str(payload.get("run_id") or run_dir.name)


def _load_audit_reason_maps(recent_runs_dir: Path) -> Dict[tuple[str, str, str, str], Dict[str, Any]]:
    out: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for audit_path in sorted(recent_runs_dir.glob("*/decision_audit.json")):
        run_dir = audit_path.parent
        try:
            payload = json.loads(audit_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        run_ids = {run_dir.name, _audit_run_id(run_dir, payload)}
        for decision in payload.get("router_decisions") or []:
            if not isinstance(decision, dict):
                continue
            if str(decision.get("action") or "").strip().lower() not in {"create", "fill", ""}:
                continue
            symbol = _norm_symbol(decision.get("symbol"))
            if not symbol:
                continue
            side = _norm_side(decision.get("side"))
            intent = str(decision.get("intent") or "").strip().upper()
            if not side:
                continue
            is_exit = side == "sell" or intent == "CLOSE_LONG"
            reason = _reason_from_mapping(decision, is_exit=is_exit)
            probe_type = _probe_type_from_mapping(decision, reason)
            info = {
                "reason": reason,
                "probe_type": probe_type,
                "decision": decision,
            }
            for run_id in run_ids:
                out[(run_id, symbol, side, intent)] = info
                out[(run_id, symbol, side, "")] = info
    return out


def _trade_reason_info(
    row: Mapping[str, Any],
    *,
    run_id: str,
    symbol: str,
    side: str,
    intent: str,
    audit_reason_maps: Mapping[tuple[str, str, str, str], Dict[str, Any]],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    is_exit = side == "sell" or intent == "CLOSE_LONG"
    row_reason = _reason_from_mapping(row, is_exit=is_exit)
    row_probe_type = _probe_type_from_mapping(row, row_reason)
    audit_info = (
        audit_reason_maps.get((run_id, symbol, side, intent))
        or audit_reason_maps.get((run_id, symbol, side, ""))
        or {}
    )
    audit_reason = audit_info.get("reason")
    audit_probe_type = audit_info.get("probe_type")
    reason = row_reason or audit_reason
    probe_type = row_probe_type or audit_probe_type or _probe_type_from_mapping(row, reason)
    if is_exit:
        return None, str(reason or "sell"), probe_type
    return str(reason or "normal"), None, probe_type


def _load_trades(recent_runs_dir: Path, *, hours: int = 72, asof: Optional[datetime] = None) -> List[TradeEvent]:
    audit_reason_maps = _load_audit_reason_maps(recent_runs_dir)
    events: List[TradeEvent] = []
    for trade_path in sorted(recent_runs_dir.glob("*/trades.csv")):
        run_dir = trade_path.parent
        try:
            with trade_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row_number, row in enumerate(reader, start=1):
                    ts = _parse_ts(row.get("ts") or row.get("timestamp") or row.get("time"))
                    symbol = _norm_symbol(row.get("symbol") or row.get("inst_id"))
                    side = _norm_side(row.get("side"))
                    qty = _to_float(row.get("qty") or row.get("fill_sz"))
                    price = _to_float(row.get("price") or row.get("fill_px"))
                    if ts is None or not symbol or side not in {"buy", "sell"} or qty <= 0 or price <= 0:
                        continue
                    intent = str(row.get("intent") or "").strip().upper()
                    run_id = str(row.get("run_id") or run_dir.name)
                    notional = _to_float(row.get("notional_usdt"), qty * price)
                    fee_usdt = abs(_to_float(row.get("fee_usdt") or row.get("fee")))
                    entry_reason, exit_reason, probe_type = _trade_reason_info(
                        row,
                        run_id=run_id,
                        symbol=symbol,
                        side=side,
                        intent=intent,
                        audit_reason_maps=audit_reason_maps,
                    )
                    events.append(
                        TradeEvent(
                            ts=ts,
                            run_id=run_id,
                            source_file=str(trade_path.as_posix()),
                            row_number=row_number,
                            symbol=symbol,
                            side=side,
                            intent=intent,
                            qty=qty,
                            price=price,
                            notional_usdt=notional,
                            fee_usdt=fee_usdt,
                            entry_reason=entry_reason,
                            exit_reason=exit_reason,
                            probe_type=probe_type,
                        )
                    )
        except OSError:
            continue

    if not events:
        return []
    effective_asof = asof or max(event.ts for event in events)
    cutoff = effective_asof - timedelta(hours=int(hours))
    return sorted((event for event in events if event.ts >= cutoff), key=lambda event: (event.ts, event.row_number))


def _round(value: float, digits: int = 8) -> float:
    return round(float(value), digits)


def _state_symbols(bundle_root: Path) -> set[str]:
    symbols: set[str] = set()
    state_dir = bundle_root / "raw" / "state"
    for name in (
        "profit_taking_state.json",
        "highest_px_state.json",
        "stop_loss_state.json",
        "fixed_stop_loss_state.json",
    ):
        path = state_dir / name
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            symbols.update(_norm_symbol(symbol) for symbol in payload.keys())
    return {symbol for symbol in symbols if symbol}


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _bps_stats(rows: List[Mapping[str, Any]], field: str) -> Any:
    values = [_to_float(row.get(field)) for row in rows if str(row.get(field) or "").strip()]
    if not values:
        return {"min": "not_observable", "max": "not_observable", "avg": "not_observable"}
    return {
        "min": _round(min(values), 6),
        "max": _round(max(values), 6),
        "avg": _round(sum(values) / len(values), 6),
    }


def _load_json(path: Path, default: Any) -> Any:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj
    except Exception:
        return default


def _merge_issue(payload: Dict[str, Any], issue: Dict[str, Any]) -> None:
    issues = payload.setdefault("issues", [])
    if not isinstance(issues, list):
        issues = []
        payload["issues"] = issues
    key = (issue.get("severity"), issue.get("code"), json.dumps(issue.get("evidence", {}), sort_keys=True))
    for existing in issues:
        if not isinstance(existing, dict):
            continue
        existing_key = (
            existing.get("severity"),
            existing.get("code"),
            json.dumps(existing.get("evidence", {}), sort_keys=True),
        )
        if existing_key == key:
            return
    issues.append(issue)


def _refresh_issue_counts(payload: Dict[str, Any]) -> None:
    issues = [issue for issue in payload.get("issues", []) if isinstance(issue, dict)]
    payload["high_issue_count"] = sum(1 for issue in issues if issue.get("severity") == "high")
    payload["medium_issue_count"] = sum(1 for issue in issues if issue.get("severity") == "medium")
    payload["warning_count"] = sum(1 for issue in issues if issue.get("severity") == "warning")


def build_probe_lifecycle_reports(
    bundle_root: str | Path,
    *,
    hours: int = 72,
    asof: Optional[datetime] = None,
) -> Dict[str, Any]:
    root = Path(bundle_root)
    recent_runs_dir = root / "raw" / "recent_runs"
    summaries_dir = root / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)

    events = _load_trades(recent_runs_dir, hours=hours, asof=asof)
    open_lots: Dict[str, Deque[OpenLot]] = defaultdict(deque)
    roundtrips: List[Dict[str, Any]] = []
    unmatched_closes: List[TradeEvent] = []

    for event in events:
        if event.side == "buy" and event.intent in {"", "OPEN_LONG"}:
            open_lots[event.symbol].append(
                OpenLot(
                    event=event,
                    qty_remaining=event.qty,
                    fee_remaining=event.fee_usdt,
                )
            )
            continue
        if event.side != "sell" or event.intent not in {"", "CLOSE_LONG"}:
            continue
        close_qty_remaining = event.qty
        close_fee_remaining = event.fee_usdt
        matched_any = False
        while close_qty_remaining > 1e-12 and open_lots[event.symbol]:
            lot = open_lots[event.symbol][0]
            pair_qty = min(lot.qty_remaining, close_qty_remaining)
            if pair_qty <= 0:
                open_lots[event.symbol].popleft()
                continue
            entry_fee = lot.fee_remaining * (pair_qty / lot.qty_remaining) if lot.qty_remaining > 0 else 0.0
            exit_fee = close_fee_remaining * (pair_qty / close_qty_remaining) if close_qty_remaining > 0 else 0.0
            entry_notional = pair_qty * lot.event.price
            exit_notional = pair_qty * event.price
            gross_pnl = exit_notional - entry_notional
            fee_usdt = abs(entry_fee) + abs(exit_fee)
            net_pnl = gross_pnl - fee_usdt
            gross_bps = gross_pnl / entry_notional * 10000.0 if entry_notional > 0 else 0.0
            net_bps = net_pnl / entry_notional * 10000.0 if entry_notional > 0 else 0.0
            entry_reason = lot.event.entry_reason or "normal"
            exit_reason = event.exit_reason or "sell"
            probe_type = lot.event.probe_type or event.probe_type
            if probe_type is None and entry_reason in PROBE_TYPES:
                probe_type = entry_reason
            if probe_type is None:
                probe_type = "normal"
            roundtrips.append(
                {
                    "open_time_utc": _fmt_ts(lot.event.ts),
                    "close_time_utc": _fmt_ts(event.ts),
                    "hold_minutes": _round((event.ts - lot.event.ts).total_seconds() / 60.0, 3),
                    "symbol": event.symbol,
                    "qty": _round(pair_qty, 12),
                    "entry_px": _round(lot.event.price, 8),
                    "exit_px": _round(event.price, 8),
                    "gross_pnl_usdt": _round(gross_pnl, 10),
                    "net_pnl_usdt": _round(net_pnl, 10),
                    "gross_bps": _round(gross_bps, 6),
                    "net_bps": _round(net_bps, 6),
                    "fee_usdt": _round(fee_usdt, 10),
                    "entry_reason": entry_reason,
                    "exit_reason": exit_reason,
                    "probe_type": probe_type,
                    "open_run_id": lot.event.run_id,
                    "close_run_id": event.run_id,
                }
            )
            matched_any = True
            lot.qty_remaining -= pair_qty
            lot.fee_remaining -= entry_fee
            close_qty_remaining -= pair_qty
            close_fee_remaining -= exit_fee
            if lot.qty_remaining <= 1e-12:
                open_lots[event.symbol].popleft()
        if not matched_any:
            unmatched_closes.append(event)

    state_symbols = _state_symbols(root)
    remaining_by_symbol = {
        symbol: sum(lot.qty_remaining for lot in lots)
        for symbol, lots in open_lots.items()
    }
    probe_rows: List[Dict[str, Any]] = []
    for row in roundtrips:
        if row.get("probe_type") not in PROBE_TYPES:
            continue
        symbol = str(row.get("symbol") or "")
        remaining_qty = float(remaining_by_symbol.get(symbol, 0.0) or 0.0)
        remaining_value = remaining_qty * _to_float(row.get("exit_px"))
        probe_rows.append(
            {
                "probe_type": row.get("probe_type"),
                "entry_ts": row.get("open_time_utc"),
                "entry_px": row.get("entry_px"),
                "exit_ts": row.get("close_time_utc"),
                "exit_px": row.get("exit_px"),
                "exit_reason": row.get("exit_reason"),
                "gross_bps": row.get("gross_bps"),
                "net_bps": row.get("net_bps"),
                "remaining_value_usdt": _round(remaining_value, 10),
                "state_still_present_after_close": symbol in state_symbols,
                "symbol": symbol,
                "hold_minutes": row.get("hold_minutes"),
                "fee_usdt": row.get("fee_usdt"),
            }
        )

    _write_csv(summaries_dir / "trades_roundtrips.csv", roundtrips, ROUNDTRIP_FIELDS)
    _write_csv(summaries_dir / "probe_lifecycle_audit.csv", probe_rows, PROBE_LIFECYCLE_FIELDS)

    window_summary_path = summaries_dir / "window_summary.json"
    window_summary = _load_json(window_summary_path, {})
    if not isinstance(window_summary, dict):
        window_summary = {}
    window_summary["trade_rows"] = len(events)
    window_summary["roundtrip_rows"] = len(roundtrips)
    window_summary["probe_lifecycle_rows"] = len(probe_rows)
    window_summary["probe_trade_gross_bps"] = _bps_stats(probe_rows, "gross_bps")
    window_summary["probe_trade_net_bps"] = _bps_stats(probe_rows, "net_bps")
    window_summary_path.write_text(json.dumps(window_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    issue_path = summaries_dir / "issues_to_fix.json"
    issues_payload = _load_json(issue_path, {})
    if not isinstance(issues_payload, dict):
        issues_payload = {}
    symbols_with_buy = {event.symbol for event in events if event.side == "buy"}
    symbols_with_sell = {event.symbol for event in events if event.side == "sell"}
    symbols_with_roundtrip = {str(row.get("symbol") or "") for row in roundtrips}
    for symbol in sorted((symbols_with_buy & symbols_with_sell) - symbols_with_roundtrip):
        _merge_issue(
            issues_payload,
            {
                "severity": "high",
                "code": "raw_trades_buy_sell_not_paired",
                "message": "raw/recent_runs trades contain both buy and sell for symbol but no FIFO roundtrip was produced.",
                "evidence": {"symbol": symbol},
            },
        )
    for event in unmatched_closes:
        if event.symbol in symbols_with_buy:
            _merge_issue(
                issues_payload,
                {
                    "severity": "high",
                    "code": "raw_trade_close_unmatched",
                    "message": "CLOSE_LONG/sell trade could not be matched to an earlier OPEN_LONG/buy trade.",
                    "evidence": {
                        "symbol": event.symbol,
                        "run_id": event.run_id,
                        "ts": _fmt_ts(event.ts),
                    },
                },
            )
    probe_entry_seen = any(
        event.entry_reason in PROBE_TYPES or event.probe_type in PROBE_TYPES
        for event in events
        if event.side == "buy"
    )
    probe_exit_seen = any(
        str(event.exit_reason or "").startswith("probe_") or event.exit_reason == "market_impulse_probe_time_stop"
        for event in events
        if event.side == "sell"
    )
    if probe_entry_seen and probe_exit_seen and not probe_rows:
        _merge_issue(
            issues_payload,
            {
                "severity": "high",
                "code": "probe_trades_not_paired",
                "message": "Probe buy/sell trades were observable but no probe lifecycle audit row was produced.",
                "evidence": {"probe_entry_seen": True, "probe_exit_seen": True},
            },
        )
    _refresh_issue_counts(issues_payload)
    issue_path.write_text(json.dumps(issues_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "trade_rows": len(events),
        "roundtrip_rows": len(roundtrips),
        "probe_lifecycle_rows": len(probe_rows),
        "unmatched_close_rows": len(unmatched_closes),
        "roundtrips_path": str(summaries_dir / "trades_roundtrips.csv"),
        "probe_lifecycle_path": str(summaries_dir / "probe_lifecycle_audit.csv"),
        "issues_path": str(issue_path),
    }
