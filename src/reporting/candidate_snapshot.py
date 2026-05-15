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
    "selected_total_cost_bps",
    "cost_source",
    "cost_model_version",
    "cost_gate_verified",
    "would_block_by_cost",
    "cost_reason",
    "eligible_before_filters",
    "final_decision",
    "block_reason",
    "no_signal_reason",
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


def candidate_snapshot_cost_defaults(cfg: Any) -> dict[str, Any]:
    execution = getattr(cfg, "execution", cfg)
    quant_lab = getattr(cfg, "quant_lab", cfg)

    local_cost = _float_or_none(getattr(execution, "cost_aware_roundtrip_cost_bps", None))
    source_detail = "execution.cost_aware_roundtrip_cost_bps"
    if local_cost is None:
        fee_bps = _float_or_none(getattr(execution, "fee_bps", None)) or 0.0
        slippage_bps = _float_or_none(getattr(execution, "slippage_bps", None)) or 0.0
        local_cost = 2.0 * (fee_bps + slippage_bps)
        source_detail = "execution.roundtrip_fee_slippage"

    min_floor = _float_or_none(getattr(quant_lab, "min_cost_bps_floor", None)) or 0.0
    local_cost = max(float(local_cost or 0.0), float(min_floor or 0.0))
    return {
        "local_cost_bps": local_cost,
        "local_cost_source_detail": source_detail,
        "local_cost_model_version": f"v5_local_{source_detail}",
        "cost_min_edge_multiplier": _float_or_none(getattr(quant_lab, "cost_min_edge_multiplier", None)) or 1.5,
        "min_cost_bps_floor": min_floor,
        "score_proxy_floor": _float_or_none(getattr(execution, "cost_aware_min_score_floor", None)) or 0.0,
        "score_per_bps": _float_or_none(getattr(execution, "cost_aware_score_per_bps", None)),
    }


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
    local_cost_bps: Any = None,
    local_cost_source_detail: str | None = None,
    local_cost_model_version: str | None = None,
    cost_min_edge_multiplier: Any = 1.5,
    min_cost_bps_floor: Any = 0.0,
    score_proxy_floor: Any = None,
    score_per_bps: Any = None,
    no_signal_reasons: Mapping[str, Any] | None = None,
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
    quant_lab_costs = _quant_lab_cost_lookup(getattr(audit, "quant_lab", {}) or {})
    no_signal_reasons = dict(no_signal_reasons or {})
    order_lookup = _orders_by_symbol(orders)
    position_lookup = _positions_by_symbol(positions)
    local_cost_value = _first_float(local_cost_bps, 22.0)
    min_cost_floor_value = _first_float(min_cost_bps_floor, 0.0) or 0.0
    if local_cost_value is not None:
        local_cost_value = max(float(local_cost_value), float(min_cost_floor_value))
    multiplier = _first_float(cost_min_edge_multiplier, 1.5) or 1.5

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
        qlab = _merge_mappings(
            _lookup_symbol_mapping(quant_lab_costs, symbol),
            _first_mapping(top.get("quant_lab"), top.get("cost_estimate"), top.get("cost")),
            _first_mapping(explain.get("quant_lab"), explain.get("cost_estimate"), explain.get("cost")),
            _order_quant_lab_meta(order),
        )
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
        final_score = _first_float(top.get("final_score"), top.get("score"), explain.get("final_score"))
        alpha6_score = _first_float(
            explain.get("alpha6_score"),
            alpha6.get("score"),
            alpha6.get("raw_score"),
            _find_nested(top, "alpha6_score"),
        )
        expected_edge = _first_float(
            _nested_get(order, ("meta", "expected_edge_bps")),
            _nested_get(order, ("meta", "expected_net_bps")),
            _nested_get(order, ("meta", "expected_net_edge_bps")),
            _nested_get(qlab, ("expected_edge_bps",)),
            _nested_get(qlab, ("expected_net_bps",)),
            _nested_get(top, ("expected_edge_bps",)),
            _nested_get(top, ("expected_net_bps",)),
            _nested_get(explain, ("expected_edge_bps",)),
            _nested_get(explain, ("expected_net_bps",)),
            _nested_get(alpha6, ("metadata", "expected_edge_bps")),
        )
        if expected_edge is None:
            expected_edge = _score_proxy_expected_edge(
                final_score=final_score,
                alpha6_score=alpha6_score,
                score_floor=score_proxy_floor,
                score_per_bps=score_per_bps,
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
        selected_total_cost_bps = _first_float(
            _nested_get(qlab, ("selected_total_cost_bps",)),
            _nested_get(qlab, ("total_cost_bps",)),
            _nested_get(qlab, ("cost_bps",)),
            _nested_get(qlab, ("effective_total_cost_bps",)),
        )
        qlab_has_cost = _mapping_has_any(
            qlab,
            (
                "effective_total_cost_bps",
                "selected_total_cost_bps",
                "total_cost_bps",
                "cost_bps",
                "cost_source",
                "source",
                "cost_model_version",
            ),
        )
        used_local_cost = False
        if cost_bps is None:
            cost_bps = local_cost_value
            used_local_cost = True
        if selected_total_cost_bps is None:
            selected_total_cost_bps = cost_bps
        if required_edge is None and cost_bps is not None:
            required_edge = float(cost_bps) * float(multiplier)
        cost_source = _normalized_cost_source(
            _first(
                _nested_get(qlab, ("cost_source",)),
                _nested_get(qlab, ("source",)),
                _nested_get(qlab, ("fallback_level",)),
            ),
            qlab_has_cost=qlab_has_cost,
            used_local_cost=used_local_cost,
        )
        cost_model_version = _first(
            _nested_get(qlab, ("cost_model_version",)),
            _nested_get(qlab, ("cost_contract_version",)),
        )
        if not cost_model_version and used_local_cost:
            cost_model_version = local_cost_model_version or "v5_local_cost_estimate"
        cost_gate_verified = _first_bool(
            _nested_get(qlab, ("cost_gate_verified",)),
            _nested_get(qlab, ("cost_gate_passed",)),
        )
        if cost_gate_verified is None:
            cost_gate_verified = False if used_local_cost else bool(qlab_has_cost and required_edge is not None)
        would_block_by_cost = _first_bool(
            _nested_get(qlab, ("would_block_by_cost",)),
            _nested_get(qlab, ("would_filter_by_cost",)),
            _nested_get(qlab, ("would_filter",)),
            _nested_get(qlab, ("filtered",)),
        )
        if would_block_by_cost is None:
            would_block_by_cost = bool(
                expected_edge is not None and required_edge is not None and float(expected_edge) < float(required_edge)
            )
        cost_reason = _first(
            _nested_get(qlab, ("filter_reason",)),
            _nested_get(qlab, ("reason",)),
            _nested_get(qlab, ("diagnosis",)),
            _nested_get(qlab, ("warning",)),
            _nested_get(qlab, ("fallback_reason",)),
        )
        if not cost_reason:
            cost_reason = (
                f"local_cost_estimate_no_quant_lab_cost:{local_cost_source_detail or 'default_roundtrip_cost'}"
                if used_local_cost
                else "quant_lab_cost_estimate"
            )
        no_signal_reason = _no_signal_reason(
            symbol=symbol,
            final_decision=final_decision,
            block_reason=block_reason,
            order=order,
            top=top,
            strategy_signals=strategy_signals,
            target_raw=target_raw,
            target_after=target_after,
            no_signal_reasons=no_signal_reasons,
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
            "final_score": final_score,
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
            "alpha6_score": alpha6_score,
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
            "selected_total_cost_bps": selected_total_cost_bps,
            "cost_source": cost_source,
            "cost_model_version": cost_model_version,
            "cost_gate_verified": bool(cost_gate_verified),
            "would_block_by_cost": bool(would_block_by_cost),
            "cost_reason": cost_reason,
            "eligible_before_filters": _bool_str(
                symbol in top_scores
                or abs(float(target_raw or 0.0)) > 0.0
                or abs(float(target_after or 0.0)) > 0.0
                or bool(router_decisions.get(symbol))
                or order is not None
            ),
            "final_decision": final_decision,
            "block_reason": block_reason,
            "no_signal_reason": no_signal_reason,
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
    if exists and _csv_header(path) != list(CANDIDATE_SNAPSHOT_FIELDS):
        _rewrite_csv_with_current_schema(path, rows)
        return
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANDIDATE_SNAPSHOT_FIELDS))
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(_format_row(row))


def _csv_header(path: Path) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            return next(reader, [])
    except Exception:
        return []


def _rewrite_csv_with_current_schema(path: Path, new_rows: Sequence[Mapping[str, Any]]) -> None:
    existing_rows: list[dict[str, Any]] = []
    if path.exists() and path.stat().st_size > 0:
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                existing_rows = [dict(row) for row in csv.DictReader(fh) if row]
        except Exception:
            existing_rows = []
    fallback_cost = _first_float(
        *[row.get("cost_bps") for row in new_rows],
        *[row.get("selected_total_cost_bps") for row in new_rows],
        22.0,
    )
    fallback_model = _first(
        *[row.get("cost_model_version") for row in new_rows],
        "v5_local_legacy_candidate_snapshot_rewrite",
    )
    tmp = path.with_name(f"{path.name}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CANDIDATE_SNAPSHOT_FIELDS))
        writer.writeheader()
        for row in [
            *[_backfill_legacy_cost_fields(row, fallback_cost, fallback_model) for row in existing_rows],
            *list(new_rows),
        ]:
            writer.writerow(_format_row(row))
    tmp.replace(path)


def _backfill_legacy_cost_fields(
    row: Mapping[str, Any],
    fallback_cost_bps: Any,
    fallback_model: Any,
) -> dict[str, Any]:
    out = dict(row)
    if not _missing_value(out.get("cost_source")):
        return out
    cost_bps = _first_float(
        out.get("cost_bps"),
        out.get("selected_total_cost_bps"),
        out.get("total_cost_bps"),
        out.get("effective_total_cost_bps"),
        fallback_cost_bps,
    )
    out["cost_source"] = "local_estimate"
    out["cost_bps"] = cost_bps
    out["selected_total_cost_bps"] = _first_float(out.get("selected_total_cost_bps"), cost_bps)
    out["cost_model_version"] = _first(out.get("cost_model_version"), fallback_model)
    out["required_edge_bps"] = _first_float(out.get("required_edge_bps"), out.get("min_required_edge_bps"), (cost_bps or 0.0) * 1.5)
    out["cost_gate_verified"] = False
    out["would_block_by_cost"] = False
    out["cost_reason"] = _first(out.get("cost_reason"), "legacy_candidate_snapshot_schema_backfilled_local_estimate")
    return out


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


def _quant_lab_cost_lookup(quant_lab: Any) -> dict[str, dict[str, Any]]:
    payload = _as_mapping(quant_lab)
    rows: list[Mapping[str, Any]] = []
    cost_estimates = payload.get("cost_estimates")
    if isinstance(cost_estimates, list):
        rows.extend(row for row in cost_estimates if isinstance(row, Mapping))
    events_tail = payload.get("events_tail")
    if isinstance(events_tail, list):
        for event in events_tail:
            if not isinstance(event, Mapping):
                continue
            event_type = str(event.get("event_type") or event.get("type") or "").strip()
            if event_type in {"cost_estimate", "quant_lab_cost_estimate"}:
                rows.append(event)

    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol_values = (
            row.get("symbol"),
            row.get("request_symbol"),
            row.get("normalized_symbol"),
            row.get("response_symbol"),
        )
        aliases: set[str] = set()
        for value in symbol_values:
            aliases.update(_symbol_aliases(value))
        if not aliases:
            continue
        normalized_row = dict(row)
        for alias in aliases:
            out[alias] = _merge_mappings(out.get(alias, {}), normalized_row)
    return out


def _lookup_symbol_mapping(rows: Mapping[str, Mapping[str, Any]], symbol: Any) -> dict[str, Any]:
    for alias in _symbol_aliases(symbol):
        row = rows.get(alias)
        if isinstance(row, Mapping):
            return dict(row)
    return {}


def _symbol_aliases(symbol: Any) -> set[str]:
    text = str(symbol or "").strip()
    if not text:
        return set()
    upper = text.upper()
    aliases = {text, upper}
    aliases.add(upper.replace("-", "/"))
    aliases.add(upper.replace("/", "-"))
    aliases.add(upper.replace("_", "/"))
    return {alias for alias in aliases if alias}


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_mapping(*values: Any) -> dict[str, Any]:
    for value in values:
        mapping = _as_mapping(value)
        if mapping:
            return mapping
    return {}


def _merge_mappings(*items: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        for key, value in item.items():
            if value in (None, ""):
                continue
            merged[str(key)] = value
    return merged


def _mapping_has_any(item: Mapping[str, Any], keys: Iterable[str]) -> bool:
    return any(item.get(key) not in (None, "") for key in keys)


def _score_proxy_expected_edge(
    *,
    final_score: Any,
    alpha6_score: Any,
    score_floor: Any,
    score_per_bps: Any,
) -> Optional[float]:
    per_bps = _float_or_none(score_per_bps)
    if per_bps is None or per_bps <= 0:
        return None
    floor = _float_or_none(score_floor) or 0.0
    score = _first_float(final_score, alpha6_score)
    if score is None:
        return None
    return max(0.0, float(score) - float(floor)) / float(per_bps)


def _normalized_cost_source(value: Any, *, qlab_has_cost: bool, used_local_cost: bool) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if used_local_cost or lowered in {"local_only", "permission_only_skip_cost", "local"}:
        return "local_estimate"
    if text:
        return text
    if qlab_has_cost:
        return "quant_lab_cached"
    return "local_estimate"


def _no_signal_reason(
    *,
    symbol: str,
    final_decision: str,
    block_reason: Optional[str],
    order: Any,
    top: Mapping[str, Any],
    strategy_signals: Mapping[str, Mapping[str, Any]],
    target_raw: Optional[float],
    target_after: Optional[float],
    no_signal_reasons: Mapping[str, Any],
) -> Optional[str]:
    if block_reason or order is not None:
        return None
    explicit = _lookup_symbol_value(no_signal_reasons, symbol)
    if explicit not in (None, ""):
        return str(explicit)
    decision = str(final_decision or "").strip()
    if decision == "no_order":
        has_candidate = bool(top) or bool(strategy_signals)
        if has_candidate:
            return "candidate_not_selected_no_order"
        if abs(float(target_raw or 0.0)) == 0.0 and abs(float(target_after or 0.0)) == 0.0:
            return "no_signal"
        return "target_zero_no_order"
    if decision == "target_no_order":
        return "target_positive_no_order_created"
    if decision == "held_no_order":
        return "held_position_no_new_order"
    return None


def _lookup_symbol_value(rows: Mapping[str, Any], symbol: Any) -> Any:
    for alias in _symbol_aliases(symbol):
        value = rows.get(alias)
        if value not in (None, ""):
            return value
    return None


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


def _first_bool(*values: Any) -> Optional[bool]:
    for value in values:
        if _missing_value(value):
            continue
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return None


def _float_or_none(value: Any) -> Optional[float]:
    if _missing_value(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _missing_value(value: Any) -> bool:
    return value in (None, "", "null", "not_observable")


def _string_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "value"):
        value = value.value
    text = str(value).strip()
    return text or None


def _bool_str(value: bool) -> str:
    return "true" if bool(value) else "false"
