from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


CANDIDATE_SNAPSHOT_SCHEMA_VERSION = "v5.candidate_snapshot.v1"

CANDIDATE_SNAPSHOT_FIELDS = (
    "candidate_id",
    "run_id",
    "ts_utc",
    "symbol",
    "regime_state",
    "risk_level",
    "current_position",
    "current_weight",
    "target_weight_raw",
    "target_weight_after_risk",
    "final_score",
    "rank",
    "f1_mom_5d",
    "f2_mom_20d",
    "f3_vol_adj_ret",
    "f4_volume_expansion",
    "f5_rsi_trend_confirm",
    "alpha6_score",
    "alpha6_side",
    "ml_score",
    "mean_reversion_score",
    "expected_edge_bps",
    "required_edge_bps",
    "cost_bps",
    "cost_source",
    "eligible_before_filters",
    "final_decision",
    "block_reason",
    "strategy_candidate",
)


def candidate_id_for(run_id: str, symbol: str, strategy_candidate: str) -> str:
    material = "|".join(
        [
            str(run_id or "").strip(),
            str(symbol or "").strip().upper(),
            str(strategy_candidate or "portfolio").strip(),
        ]
    )
    return "cand_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def build_candidate_snapshot_rows(
    *,
    run_id: str,
    ts_utc: str,
    symbols: Iterable[str],
    audit: Any,
    regime_state: Any = None,
    risk_level: Any = None,
    positions: Iterable[Any] = (),
    prices: Mapping[str, Any] | None = None,
    equity_usdt: Any = None,
    target_weights_raw: Mapping[str, Any] | None = None,
    target_weights_after_risk: Mapping[str, Any] | None = None,
    orders: Iterable[Any] = (),
) -> list[dict[str, Any]]:
    prices = dict(prices or {})
    target_weights_raw = dict(target_weights_raw or getattr(audit, "targets_pre_risk", {}) or {})
    target_weights_after_risk = dict(
        target_weights_after_risk or getattr(audit, "targets_post_risk", {}) or {}
    )
    top_scores = _symbol_map(getattr(audit, "top_scores", []) or [])
    explain_rows = _symbol_map(getattr(audit, "target_execution_explain", []) or [])
    router_decisions = _router_decisions_by_symbol(getattr(audit, "router_decisions", []) or [])
    strategy_lookup = _strategy_signal_lookup(getattr(audit, "strategy_signals", []) or [])
    order_lookup = _orders_by_symbol(orders)
    position_lookup = _positions_by_symbol(positions)

    all_symbols = _ordered_symbols(
        symbols,
        top_scores.keys(),
        explain_rows.keys(),
        target_weights_raw.keys(),
        target_weights_after_risk.keys(),
        router_decisions.keys(),
        order_lookup.keys(),
        position_lookup.keys(),
    )

    rows: list[dict[str, Any]] = []
    for symbol in all_symbols:
        top = top_scores.get(symbol, {})
        explain = explain_rows.get(symbol, {})
        strategy_signals = strategy_lookup.get(symbol, {})
        alpha6 = strategy_signals.get("Alpha6Factor", {})
        mean_reversion = strategy_signals.get("MeanReversion", {})
        order = order_lookup.get(symbol)
        qlab = _order_quant_lab_meta(order)
        position = position_lookup.get(symbol)
        current_position = _float_or_none(getattr(position, "qty", None))
        current_weight = _current_weight(position, prices.get(symbol), equity_usdt)
        target_raw = _float_or_none(target_weights_raw.get(symbol))
        target_after = _float_or_none(target_weights_after_risk.get(symbol))
        block_reason = _block_reason(router_decisions.get(symbol, []))
        final_decision = _final_decision(
            order=order,
            block_reason=block_reason,
            target_weight=target_after,
            current_weight=current_weight,
        )
        strategy_candidate = _strategy_candidate(order, top, strategy_signals)
        expected_edge = _first_float(
            _nested_get(order, ("meta", "expected_edge_bps")),
            _nested_get(qlab, ("expected_edge_bps",)),
            _nested_get(top, ("expected_edge_bps",)),
            _nested_get(explain, ("expected_edge_bps",)),
            _nested_get(alpha6, ("metadata", "expected_edge_bps")),
        )
        required_edge = _first_float(
            _nested_get(qlab, ("required_edge_bps",)),
            _nested_get(qlab, ("min_required_edge_bps",)),
            _nested_get(order, ("meta", "required_edge_bps")),
        )
        cost_bps = _first_float(
            _nested_get(qlab, ("effective_total_cost_bps",)),
            _nested_get(qlab, ("selected_total_cost_bps",)),
            _nested_get(qlab, ("total_cost_bps",)),
            _nested_get(qlab, ("cost_bps",)),
        )

        row = {
            "candidate_id": candidate_id_for(run_id, symbol, strategy_candidate),
            "run_id": run_id,
            "ts_utc": ts_utc,
            "symbol": symbol,
            "regime_state": _string_or_none(regime_state),
            "risk_level": _string_or_none(risk_level),
            "current_position": current_position,
            "current_weight": current_weight,
            "target_weight_raw": target_raw,
            "target_weight_after_risk": target_after,
            "final_score": _first_float(top.get("final_score"), top.get("score"), explain.get("final_score")),
            "rank": _first(top.get("rank"), top.get("selected_rank"), explain.get("selected_rank")),
            "f1_mom_5d": _first_float(
                _find_nested(top, "f1_mom_5d"),
                _find_nested(alpha6, "f1_mom_5d"),
                _find_nested(explain, "f1_mom_5d"),
            ),
            "f2_mom_20d": _first_float(
                _find_nested(top, "f2_mom_20d"),
                _find_nested(alpha6, "f2_mom_20d"),
                _find_nested(explain, "f2_mom_20d"),
            ),
            "f3_vol_adj_ret": _first_float(
                _find_nested(top, "f3_vol_adj_ret"),
                _find_nested(alpha6, "f3_vol_adj_ret"),
                _find_nested(explain, "f3_vol_adj_ret"),
            ),
            "f4_volume_expansion": _first_float(
                explain.get("f4_volume_expansion"),
                _find_nested(top, "f4_volume_expansion"),
                _find_nested(alpha6, "f4_volume_expansion"),
            ),
            "f5_rsi_trend_confirm": _first_float(
                explain.get("f5_rsi_trend_confirm"),
                _find_nested(top, "f5_rsi_trend_confirm"),
                _find_nested(alpha6, "f5_rsi_trend_confirm"),
            ),
            "alpha6_score": _first_float(
                explain.get("alpha6_score"),
                alpha6.get("score"),
                alpha6.get("raw_score"),
                _find_nested(top, "alpha6_score"),
            ),
            "alpha6_side": _first(
                explain.get("alpha6_side"),
                alpha6.get("side"),
                alpha6.get("direction"),
                _find_nested(top, "alpha6_side"),
            ),
            "ml_score": _first_float(
                top.get("ml_score"),
                top.get("ml_overlay_score"),
                top.get("ml_pred_raw"),
                top.get("ml_pred_zscore"),
                _find_nested(top, "ml_overlay_score"),
            ),
            "mean_reversion_score": _first_float(mean_reversion.get("score"), mean_reversion.get("raw_score")),
            "expected_edge_bps": expected_edge,
            "required_edge_bps": required_edge,
            "cost_bps": cost_bps,
            "cost_source": _first(
                _nested_get(qlab, ("cost_source",)),
                _nested_get(qlab, ("source",)),
                _nested_get(qlab, ("fallback_level",)),
            ),
            "eligible_before_filters": _bool_str(
                symbol in top_scores
                or abs(float(target_raw or 0.0)) > 0.0
                or abs(float(target_after or 0.0)) > 0.0
                or bool(router_decisions.get(symbol))
                or order is not None
            ),
            "final_decision": final_decision,
            "block_reason": block_reason,
            "strategy_candidate": strategy_candidate,
        }
        rows.append(row)
    return rows


def write_candidate_snapshot(
    *,
    run_dir: str | Path,
    reports_dir: str | Path | None,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    run_path = Path(run_dir)
    _write_csv(run_path / "candidate_snapshot.csv", rows)
    if reports_dir is not None:
        _append_csv(Path(reports_dir) / "candidate_snapshot.csv", rows)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANDIDATE_SNAPSHOT_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow(_format_row(row))


def _append_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANDIDATE_SNAPSHOT_FIELDS))
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(_format_row(row))


def _format_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {field: _csv_value(row.get(field)) for field in CANDIDATE_SNAPSHOT_FIELDS}


def _csv_value(value: Any) -> Any:
    if value is None or value == "":
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _ordered_symbols(*groups: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for value in group or []:
            symbol = str(value or "").strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            out.append(symbol)
    return out


def _symbol_map(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            out[symbol] = dict(row)
    return out


def _router_decisions_by_symbol(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            out.setdefault(symbol, []).append(dict(row))
    return out


def _strategy_signal_lookup(entries: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for entry in entries or []:
        if not isinstance(entry, Mapping):
            continue
        strategy = str(entry.get("strategy") or "").strip()
        if not strategy:
            continue
        for signal in entry.get("signals", []) or []:
            if not isinstance(signal, Mapping):
                continue
            symbol = str(signal.get("symbol") or "").strip()
            if not symbol:
                continue
            out.setdefault(symbol, {})[strategy] = dict(signal)
    return out


def _orders_by_symbol(orders: Iterable[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for order in orders or []:
        symbol = str(getattr(order, "symbol", "") or "").strip()
        if symbol:
            out[symbol] = order
    return out


def _positions_by_symbol(positions: Iterable[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for position in positions or []:
        symbol = str(getattr(position, "symbol", "") or "").strip()
        if symbol:
            out[symbol] = position
    return out


def _current_weight(position: Any, price: Any, equity_usdt: Any) -> Optional[float]:
    qty = _float_or_none(getattr(position, "qty", None))
    px = _float_or_none(price)
    equity = _float_or_none(equity_usdt)
    if qty is None or px is None or equity is None or equity <= 0:
        return None
    return float(qty) * float(px) / float(equity)


def _order_quant_lab_meta(order: Any) -> Mapping[str, Any]:
    meta = getattr(order, "meta", None)
    if isinstance(meta, Mapping):
        qlab = meta.get("quant_lab")
        if isinstance(qlab, Mapping):
            return qlab
    return {}


def _block_reason(decisions: Sequence[Mapping[str, Any]]) -> Optional[str]:
    for decision in decisions or []:
        action = str(decision.get("action") or "").strip().lower()
        reason = str(decision.get("reason") or "").strip()
        if reason and action in {"skip", "blocked", "reject", "filter", "filtered"}:
            return reason
    for decision in decisions or []:
        reason = str(decision.get("reason") or "").strip()
        if reason:
            return reason
    return None


def _final_decision(
    *,
    order: Any,
    block_reason: Optional[str],
    target_weight: Optional[float],
    current_weight: Optional[float],
) -> str:
    if order is not None:
        intent = str(getattr(order, "intent", "") or "").strip()
        side = str(getattr(order, "side", "") or "").strip()
        return intent or (f"order_{side}" if side else "order")
    if block_reason:
        return "blocked"
    tw = float(target_weight or 0.0)
    cw = float(current_weight or 0.0)
    if abs(tw) > 0:
        return "target_no_order"
    if abs(cw) > 0:
        return "held_no_order"
    return "no_order"


def _strategy_candidate(order: Any, top: Mapping[str, Any], strategy_signals: Mapping[str, Mapping[str, Any]]) -> str:
    meta = getattr(order, "meta", None)
    if isinstance(meta, Mapping):
        for key in ("strategy_candidate", "strategy_id", "strategy", "source"):
            value = meta.get(key)
            if value not in (None, ""):
                return str(value)
    for key in ("strategy_candidate", "strategy", "source"):
        value = top.get(key)
        if value not in (None, ""):
            return str(value)
    if "Alpha6Factor" in strategy_signals:
        return "Alpha6Factor"
    if strategy_signals:
        return sorted(strategy_signals.keys())[0]
    return "portfolio"


def _nested_get(obj: Any, path: Sequence[str]) -> Any:
    cur = obj
    for key in path:
        if isinstance(cur, Mapping):
            cur = cur.get(key)
        else:
            cur = getattr(cur, key, None)
        if cur is None:
            return None
    return cur


def _find_nested(obj: Any, key: str) -> Any:
    if isinstance(obj, Mapping):
        if key in obj and obj.get(key) not in (None, ""):
            return obj.get(key)
        for value in obj.values():
            found = _find_nested(value, key)
            if found not in (None, ""):
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _find_nested(value, key)
            if found not in (None, ""):
                return found
    return None


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _first_float(*values: Any) -> Optional[float]:
    for value in values:
        number = _float_or_none(value)
        if number is not None:
            return number
    return None


def _float_or_none(value: Any) -> Optional[float]:
    if value in (None, "", "null", "not_observable"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "value"):
        value = value.value
    text = str(value).strip()
    return text or None


def _bool_str(value: bool) -> str:
    return "true" if bool(value) else "false"
