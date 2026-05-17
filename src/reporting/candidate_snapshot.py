from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


CANDIDATE_SNAPSHOT_SCHEMA_VERSION = "v5.candidate_snapshot.v2"

SPECIFIC_STRATEGY_CANDIDATES = {
    "btc_leadership_probe_strict",
    "btc_leadership_alpha6_low_blocked",
    "btc_leadership_f5_low_blocked",
    "btc_leadership_no_breakout_blocked",
    "sol_protect_rsi_weak_exception",
    "sol_protect_alpha6_low_exception",
    "portfolio_alpha6_factor",
    "portfolio_trend_following",
    "f3_dominant_entry",
    "f4_volume_swing",
}

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
    "expected_edge_source",
    "required_edge_bps",
    "cost_bps",
    "selected_total_cost_bps",
    "cost_source",
    "cost_source_quality",
    "degraded_cost_model",
    "candidate_cost_trusted",
    "cost_resolution_reason",
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

DEFAULT_SYMBOL_COST_TABLE_FILENAMES = (
    "summaries/quant_lab_cost_usage.csv",
    "quant_lab_cost_usage.csv",
    "symbol_cost_table.csv",
    "quant_lab_symbol_costs.csv",
    "quant_lab_latest_symbol_costs.csv",
    "latest_symbol_cost_table.csv",
    "latest_symbol_costs.csv",
    "symbol_cost_table.jsonl",
    "quant_lab_symbol_costs.jsonl",
    "latest_symbol_costs.jsonl",
    "symbol_cost_table.json",
    "quant_lab_symbol_costs.json",
    "latest_symbol_costs.json",
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


def load_quant_lab_cost_cache(path: str | Path | None, *, max_rows: int = 5000) -> dict[str, dict[str, Any]]:
    if path in (None, ""):
        return {}
    cache_path = Path(path)
    if not cache_path.is_file():
        return {}
    rows: list[dict[str, Any]] = []
    try:
        with cache_path.open("r", encoding="utf-8") as fh:
            lines = fh.readlines()
    except Exception:
        return {}
    for line in lines[-max_rows:]:
        text = str(line or "").strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except Exception:
            continue
        if not isinstance(obj, Mapping):
            continue
        if not _row_has_cost_fields(obj):
            continue
        rows.append(dict(obj))
    return _cost_lookup_from_rows(rows, force_cached=True, source_label="quant_lab_cached")


def candidate_snapshot_symbol_cost_table_paths(reports_dir: str | Path | None) -> list[Path]:
    if reports_dir in (None, ""):
        return []
    root = Path(reports_dir)
    return [root / name for name in DEFAULT_SYMBOL_COST_TABLE_FILENAMES]


def load_latest_symbol_cost_table(
    paths: str | Path | Iterable[str | Path] | None,
    *,
    max_rows_per_file: int = 5000,
) -> dict[str, dict[str, Any]]:
    if paths in (None, ""):
        return {}
    if isinstance(paths, (str, Path)):
        candidate_paths = [Path(paths)]
    else:
        candidate_paths = [Path(path) for path in paths or []]

    rows: list[dict[str, Any]] = []
    for path in candidate_paths:
        rows.extend(_read_symbol_cost_rows(path, max_rows=max_rows_per_file))
    return _cost_lookup_from_rows(rows, force_cached=True, source_label="latest_symbol_cost_table")


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
    symbol_cost_table: Mapping[str, Mapping[str, Any]] | None = None,
    quant_lab_cost_cache: Mapping[str, Mapping[str, Any]] | None = None,
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
    symbol_cost_table = dict(symbol_cost_table or {})
    quant_lab_cost_cache = dict(quant_lab_cost_cache or {})
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
        cached_symbol_cost = _lookup_symbol_mapping(quant_lab_cost_cache, symbol)
        table_symbol_cost = _lookup_symbol_mapping(symbol_cost_table, symbol)
        current_run_symbol_cost = _lookup_symbol_mapping(quant_lab_costs, symbol)
        candidate_cost_meta = _merge_mappings(
            _first_mapping(top.get("quant_lab"), top.get("cost_estimate"), top.get("cost")),
            _first_mapping(explain.get("quant_lab"), explain.get("cost_estimate"), explain.get("cost")),
        )
        order_cost_meta = _order_quant_lab_meta(order)
        preferred_cost = _preferred_cost_mapping(
            order_cost_meta,
            current_run_symbol_cost,
            cached_symbol_cost,
            table_symbol_cost,
            candidate_cost_meta,
        )
        qlab = _merge_cost_context(
            preferred_cost,
            candidate_cost_meta,
            table_symbol_cost,
            cached_symbol_cost,
            current_run_symbol_cost,
            order_cost_meta,
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
        final_score = _first_float(top.get("final_score"), top.get("score"), explain.get("final_score"))
        alpha6_score = _first_float(
            explain.get("alpha6_score"),
            alpha6.get("score"),
            alpha6.get("raw_score"),
            _find_nested(top, "alpha6_score"),
        )
        f1_mom_5d = _first_float(
            _find_nested(top, "f1_mom_5d"),
            _find_nested(alpha6, "f1_mom_5d"),
            _find_nested(explain, "f1_mom_5d"),
        )
        f2_mom_20d = _first_float(
            _find_nested(top, "f2_mom_20d"),
            _find_nested(alpha6, "f2_mom_20d"),
            _find_nested(explain, "f2_mom_20d"),
        )
        f3_vol_adj_ret = _first_float(
            _find_nested(top, "f3_vol_adj_ret"),
            _find_nested(alpha6, "f3_vol_adj_ret"),
            _find_nested(explain, "f3_vol_adj_ret"),
        )
        f4_volume_expansion = _first_float(
            explain.get("f4_volume_expansion"),
            _find_nested(top, "f4_volume_expansion"),
            _find_nested(alpha6, "f4_volume_expansion"),
        )
        f5_rsi_trend_confirm = _first_float(
            explain.get("f5_rsi_trend_confirm"),
            _find_nested(top, "f5_rsi_trend_confirm"),
            _find_nested(alpha6, "f5_rsi_trend_confirm"),
        )
        alpha6_side = _first(
            explain.get("alpha6_side"),
            alpha6.get("side"),
            alpha6.get("direction"),
            _find_nested(top, "alpha6_side"),
        )
        strategy_candidate = _strategy_candidate(
            symbol=symbol,
            order=order,
            top=top,
            explain=explain,
            strategy_signals=strategy_signals,
            block_reason=block_reason,
            final_decision=final_decision,
            f1_mom_5d=f1_mom_5d,
            f2_mom_20d=f2_mom_20d,
            f3_vol_adj_ret=f3_vol_adj_ret,
            f4_volume_expansion=f4_volume_expansion,
            f5_rsi_trend_confirm=f5_rsi_trend_confirm,
            alpha6_score=alpha6_score,
            alpha6_side=alpha6_side,
        )
        expected_edge, expected_edge_source = _first_float_with_source(
            (_nested_get(order, ("meta", "expected_edge_bps")), _nested_get(order, ("meta", "expected_edge_source")) or "order.meta.expected_edge_bps"),
            (_nested_get(order, ("meta", "expected_net_bps")), _nested_get(order, ("meta", "expected_edge_source")) or "order.meta.expected_net_bps"),
            (_nested_get(order, ("meta", "expected_net_edge_bps")), _nested_get(order, ("meta", "expected_edge_source")) or "order.meta.expected_net_edge_bps"),
            (_nested_get(qlab, ("expected_edge_bps",)), _nested_get(qlab, ("expected_edge_source")) or "quant_lab.expected_edge_bps"),
            (_nested_get(qlab, ("expected_net_bps",)), _nested_get(qlab, ("expected_edge_source")) or "quant_lab.expected_net_bps"),
            (_nested_get(top, ("expected_edge_bps",)), _nested_get(top, ("expected_edge_source")) or "top_scores.expected_edge_bps"),
            (_nested_get(top, ("expected_net_bps",)), _nested_get(top, ("expected_edge_source")) or "top_scores.expected_net_bps"),
            (_nested_get(explain, ("expected_edge_bps",)), _nested_get(explain, ("expected_edge_source")) or "target_execution_explain.expected_edge_bps"),
            (_nested_get(explain, ("expected_net_bps",)), _nested_get(explain, ("expected_edge_source")) or "target_execution_explain.expected_net_bps"),
            (_nested_get(alpha6, ("metadata", "expected_edge_bps")), _nested_get(alpha6, ("metadata", "expected_edge_source")) or "Alpha6Factor.metadata.expected_edge_bps"),
        )
        if expected_edge is None:
            expected_edge = _score_proxy_expected_edge(
                final_score=final_score,
                alpha6_score=alpha6_score,
                score_floor=score_proxy_floor,
                score_per_bps=score_per_bps,
            )
            if expected_edge is not None:
                expected_edge_source = "score_proxy"
        if expected_edge is None:
            expected_edge = 0.0
            expected_edge_source = "not_available"
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
        cost_absence_reason = _cost_absence_reason(order=order, final_decision=final_decision)
        cost_source = _normalized_cost_source(
            _first(
                _nested_get(qlab, ("cost_source",)),
                _nested_get(qlab, ("source",)),
                _nested_get(qlab, ("fallback_level",)),
            ),
            qlab_has_cost=qlab_has_cost,
            used_local_cost=used_local_cost,
            absence_reason=cost_absence_reason,
        )
        cost_source_quality = _cost_source_quality(cost_source, qlab=qlab, used_local_cost=used_local_cost)
        cost_model_version = _first(
            _nested_get(qlab, ("cost_model_version",)),
            _nested_get(qlab, ("cost_contract_version",)),
        )
        if not cost_model_version and used_local_cost:
            cost_model_version = local_cost_model_version or "v5_local_cost_estimate"
        degraded_cost_model = _is_degraded_cost_model(cost_source, qlab=qlab, cost_model_version=cost_model_version)
        candidate_cost_trusted = _candidate_cost_trusted(
            cost_source=cost_source,
            degraded_cost_model=degraded_cost_model,
            used_local_cost=used_local_cost,
            qlab=qlab,
        )
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
                cost_absence_reason
                if used_local_cost
                else "quant_lab_cost_estimate"
            )
        if degraded_cost_model:
            cost_reason = "global_default_cost"
        cost_resolution_reason = _cost_resolution_reason(
            qlab=qlab,
            cost_source=cost_source,
            used_local_cost=used_local_cost,
            degraded_cost_model=degraded_cost_model,
            absence_reason=cost_absence_reason,
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
            "f1_mom_5d": f1_mom_5d,
            "f2_mom_20d": f2_mom_20d,
            "f3_vol_adj_ret": f3_vol_adj_ret,
            "f4_volume_expansion": f4_volume_expansion,
            "f5_rsi_trend_confirm": f5_rsi_trend_confirm,
            "alpha6_score": alpha6_score,
            "alpha6_side": alpha6_side,
            "ml_score": _first_float(
                top.get("ml_score"),
                top.get("ml_overlay_score"),
                top.get("ml_pred_raw"),
                top.get("ml_pred_zscore"),
                _find_nested(top, "ml_overlay_score"),
            ),
            "mean_reversion_score": _first_float(mean_reversion.get("score"), mean_reversion.get("raw_score")),
            "expected_edge_bps": expected_edge,
            "expected_edge_source": expected_edge_source,
            "required_edge_bps": required_edge,
            "cost_bps": cost_bps,
            "selected_total_cost_bps": selected_total_cost_bps,
            "cost_source": cost_source,
            "cost_source_quality": cost_source_quality,
            "degraded_cost_model": bool(degraded_cost_model),
            "candidate_cost_trusted": bool(candidate_cost_trusted),
            "cost_resolution_reason": cost_resolution_reason,
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
    out["expected_edge_bps"] = _first_float(out.get("expected_edge_bps"), 0.0)
    out["expected_edge_source"] = _first(out.get("expected_edge_source"), "not_available")
    out["cost_source_quality"] = _first(out.get("cost_source_quality"), "local_estimate")
    out["degraded_cost_model"] = _first(out.get("degraded_cost_model"), False)
    out["candidate_cost_trusted"] = _first(out.get("candidate_cost_trusted"), False)
    out["cost_resolution_reason"] = _first(out.get("cost_resolution_reason"), "legacy_candidate_snapshot_schema_backfilled_local_estimate")
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

    return _cost_lookup_from_rows(rows, force_cached=False, source_label="current_run_quant_lab_cost")


def _cost_lookup_from_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    force_cached: bool,
    source_label: str | None = None,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    priority_by_alias: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not _row_has_cost_fields(row):
            continue
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
        if force_cached:
            normalized_row.setdefault("cached_cost_estimate", True)
            if _missing_value(normalized_row.get("cost_source")) and _missing_value(normalized_row.get("source")):
                normalized_row["cost_source"] = "quant_lab_cached"
        if source_label and _missing_value(normalized_row.get("cost_resolution_reason")):
            normalized_row["cost_resolution_reason"] = (
                _global_default_resolution_reason(normalized_row)
                if _mapping_is_degraded_cost_model(normalized_row)
                else f"{source_label}_symbol_cost"
            )
        row_priority = _cost_mapping_priority(normalized_row)
        for alias in aliases:
            current_priority = priority_by_alias.get(alias, -1)
            if row_priority < current_priority:
                continue
            priority_by_alias[alias] = row_priority
            out[alias] = _merge_mappings(out.get(alias, {}), normalized_row)
    return out


def _preferred_cost_mapping(*items: Mapping[str, Any]) -> dict[str, Any]:
    best: dict[str, Any] = {}
    best_priority = -1
    for item in items:
        if not isinstance(item, Mapping) or not _row_has_cost_fields(item):
            continue
        priority = _cost_mapping_priority(item)
        if priority <= best_priority:
            continue
        best_priority = priority
        best = dict(item)
    return best


def _cost_mapping_priority(item: Mapping[str, Any]) -> int:
    if not _row_has_cost_fields(item):
        return 0
    if _mapping_is_degraded_cost_model(item):
        return 1
    return 2


def _merge_cost_context(preferred_cost: Mapping[str, Any], *items: Mapping[str, Any]) -> dict[str, Any]:
    merged = _merge_mappings(*items)
    if preferred_cost:
        merged = _merge_mappings(merged, preferred_cost)
    if preferred_cost and not _mapping_is_degraded_cost_model(preferred_cost):
        _drop_stale_degraded_cost_markers(merged, preferred_cost)
    if preferred_cost and _mapping_is_degraded_cost_model(preferred_cost):
        merged["cost_resolution_reason"] = _global_default_resolution_reason(merged)
    return merged


def _drop_stale_degraded_cost_markers(merged: dict[str, Any], preferred_cost: Mapping[str, Any]) -> None:
    for key in ("fallback_level", "degraded_cost_model"):
        if key not in preferred_cost:
            merged.pop(key, None)


def _read_symbol_cost_rows(path: Path, *, max_rows: int) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            with path.open("r", encoding="utf-8", newline="") as fh:
                rows = [dict(row) for row in csv.DictReader(fh) if row]
            return [row for row in rows[-max_rows:] if _row_has_cost_fields(row)]
        if suffix == ".jsonl":
            with path.open("r", encoding="utf-8") as fh:
                lines = fh.readlines()
            rows: list[dict[str, Any]] = []
            for line in lines[-max_rows:]:
                text = str(line or "").strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except Exception:
                    continue
                rows.extend(_extract_symbol_cost_rows(obj))
            return [row for row in rows[-max_rows:] if _row_has_cost_fields(row)]
        if suffix == ".json":
            with path.open("r", encoding="utf-8") as fh:
                obj = json.load(fh)
            rows = _extract_symbol_cost_rows(obj)
            return [row for row in rows[-max_rows:] if _row_has_cost_fields(row)]
    except Exception:
        return []
    return []


def _extract_symbol_cost_rows(obj: Any) -> list[dict[str, Any]]:
    if isinstance(obj, list):
        return [dict(row) for row in obj if isinstance(row, Mapping)]
    if not isinstance(obj, Mapping):
        return []
    if _row_has_cost_fields(obj) and any(obj.get(key) not in (None, "") for key in ("symbol", "request_symbol", "normalized_symbol", "response_symbol")):
        return [dict(obj)]
    for key in ("rows", "data", "costs", "symbols", "symbol_costs", "latest_symbol_costs"):
        value = obj.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
        if isinstance(value, Mapping):
            return _symbol_mapping_cost_rows(value)
    return _symbol_mapping_cost_rows(obj)


def _symbol_mapping_cost_rows(rows_by_symbol: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol, value in rows_by_symbol.items():
        if not isinstance(value, Mapping):
            continue
        row = dict(value)
        row.setdefault("symbol", symbol)
        rows.append(row)
    return rows


def _row_has_cost_fields(row: Mapping[str, Any]) -> bool:
    return _mapping_has_any(
        row,
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


def _first_float_with_source(*items: tuple[Any, Any]) -> tuple[Optional[float], Optional[str]]:
    for value, source in items:
        number = _float_or_none(value)
        if number is not None:
            source_text = str(source or "").strip() or None
            return number, source_text
    return None, None


def _normalized_cost_source(
    value: Any,
    *,
    qlab_has_cost: bool,
    used_local_cost: bool,
    absence_reason: str | None = None,
) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if used_local_cost or lowered in {"local_only", "permission_only_skip_cost", "local"}:
        return "local_estimate"
    if text:
        return text
    if qlab_has_cost:
        return "quant_lab_cached"
    return "local_estimate"


def _cost_source_quality(cost_source: Any, *, qlab: Mapping[str, Any], used_local_cost: bool) -> str:
    source = str(cost_source or "").strip().lower()
    if _is_degraded_cost_model(
        cost_source,
        qlab=qlab,
        cost_model_version=_first(_nested_get(qlab, ("cost_model_version",)), _nested_get(qlab, ("cost_contract_version",))),
    ):
        return "global_default_degraded"
    if used_local_cost or source == "local_estimate":
        return "local_estimate"
    if source == "mixed_actual_proxy":
        return "mixed_actual_proxy"
    if source in {"public_spread_proxy", "public_proxy"}:
        return "public_proxy"
    if bool(_first_bool(_nested_get(qlab, ("cached_cost_estimate",)), _nested_get(qlab, ("from_cache",)))):
        return "quant_lab_cached"
    if source == "quant_lab_cached":
        return "quant_lab_cached"
    if source:
        return "quant_lab_symbol_estimate"
    return "local_estimate"


def _mapping_is_degraded_cost_model(item: Mapping[str, Any]) -> bool:
    return _is_degraded_cost_model(
        _first(item.get("cost_source"), item.get("source")),
        qlab=item,
        cost_model_version=_first(item.get("cost_model_version"), item.get("cost_contract_version")),
    )


def _is_degraded_cost_model(cost_source: Any, *, qlab: Mapping[str, Any], cost_model_version: Any) -> bool:
    source = str(cost_source or _nested_get(qlab, ("source",)) or _nested_get(qlab, ("cost_source",)) or "").strip().lower()
    fallback_level = str(_nested_get(qlab, ("fallback_level",)) or "").strip().upper()
    version = str(cost_model_version or "").strip().lower()
    return bool(
        _first_bool(_nested_get(qlab, ("degraded_cost_model",)))
        or source == "global_default"
        or fallback_level == "GLOBAL_DEFAULT"
        or version == "global_default_v0"
    )


def _candidate_cost_trusted(
    *,
    cost_source: Any,
    degraded_cost_model: bool,
    used_local_cost: bool,
    qlab: Mapping[str, Any],
) -> bool:
    if degraded_cost_model or used_local_cost:
        return False
    source = str(cost_source or "").strip().lower()
    if source in {"", "local_estimate", "cost_not_requested_no_order", "global_default"}:
        return False
    return bool(
        source in {"quant_lab_cached", "mixed_actual_proxy", "public_spread_proxy", "public_proxy"}
        or _first_bool(_nested_get(qlab, ("cached_cost_estimate",)), _nested_get(qlab, ("from_cache",)))
        or bool(str(_nested_get(qlab, ("cost_model_version",)) or "").strip())
    )


def _cost_resolution_reason(
    *,
    qlab: Mapping[str, Any],
    cost_source: Any,
    used_local_cost: bool,
    degraded_cost_model: bool,
    absence_reason: str,
) -> str:
    if degraded_cost_model:
        return _global_default_resolution_reason(qlab)
    explicit = _first(
        _nested_get(qlab, ("cost_resolution_reason",)),
        _nested_get(qlab, ("resolution_reason",)),
    )
    if explicit:
        return str(explicit)
    if used_local_cost:
        return absence_reason
    source = str(cost_source or "").strip().lower()
    if source == "quant_lab_cached" or bool(_first_bool(_nested_get(qlab, ("cached_cost_estimate",)), _nested_get(qlab, ("from_cache",)))):
        return "quant_lab_cached_symbol_cost"
    if source in {"mixed_actual_proxy", "public_spread_proxy", "public_proxy"}:
        return "symbol_level_cost_estimate"
    if source:
        return "quant_lab_symbol_cost"
    return absence_reason


def _global_default_resolution_reason(qlab: Mapping[str, Any]) -> str:
    explicit = str(
        _nested_get(qlab, ("cost_resolution_reason",))
        or _nested_get(qlab, ("resolution_reason",))
        or ""
    ).strip()
    if explicit in {"symbol_missing", "cache_missing", "service_unavailable"}:
        return explicit
    service_fields = (
        _nested_get(qlab, ("error_type",)),
        _nested_get(qlab, ("error_message_short",)),
        _nested_get(qlab, ("warning",)),
        _nested_get(qlab, ("status_code",)),
    )
    success = _first_bool(_nested_get(qlab, ("success",)))
    if success is False or any(not _missing_value(value) for value in service_fields):
        return "service_unavailable"
    reason_text = " ".join(
        str(value or "").strip().lower()
        for value in (
            _nested_get(qlab, ("fallback_reason",)),
            _nested_get(qlab, ("reason",)),
            _nested_get(qlab, ("diagnosis",)),
            _nested_get(qlab, ("warning",)),
        )
        if not _missing_value(value)
    )
    if "service" in reason_text or "timeout" in reason_text or "unavailable" in reason_text:
        return "service_unavailable"
    if "symbol" in reason_text and "missing" in reason_text:
        return "symbol_missing"
    return "cache_missing"


def _cost_absence_reason(*, order: Any, final_decision: str) -> str:
    if order is not None:
        side = str(getattr(order, "side", "") or "").strip().lower()
        intent = str(getattr(order, "intent", "") or "").strip().upper()
        if side == "sell" or intent == "CLOSE_LONG":
            return "cost_not_requested_management_only"
        return "cost_not_available"
    if str(final_decision or "").strip() == "held_no_order":
        return "cost_not_requested_management_only"
    return "cost_not_requested_no_order"


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


def _strategy_candidate(
    *,
    symbol: str,
    order: Any,
    top: Mapping[str, Any],
    explain: Mapping[str, Any],
    strategy_signals: Mapping[str, Mapping[str, Any]],
    block_reason: Optional[str],
    final_decision: str,
    f1_mom_5d: Any,
    f2_mom_20d: Any,
    f3_vol_adj_ret: Any,
    f4_volume_expansion: Any,
    f5_rsi_trend_confirm: Any,
    alpha6_score: Any,
    alpha6_side: Any,
) -> str:
    reason = _first(
        block_reason,
        _nested_get(order, ("meta", "reason")),
        _nested_get(order, ("meta", "entry_reason")),
        _nested_get(order, ("meta", "probe_type")),
        explain.get("router_reason"),
        explain.get("blocked_reason"),
        explain.get("high_score_block_category"),
        top.get("reason"),
    )
    reason_l = str(reason or "").strip().lower()
    symbol_u = str(symbol or "").strip().upper()

    explicit = _specific_candidate_from_metadata(order, top)
    if explicit is not None:
        return explicit

    if symbol_u in {"BTC/USDT", "BTC-USDT"} and "btc_leadership_probe" in reason_l:
        if "alpha6" in reason_l and ("low" in reason_l or "score" in reason_l):
            return "btc_leadership_alpha6_low_blocked"
        if "f5" in reason_l or "rsi" in reason_l:
            return "btc_leadership_f5_low_blocked"
        if "breakout" in reason_l or "no_alpha6_buy" in reason_l:
            return "btc_leadership_no_breakout_blocked"
        return "btc_leadership_probe_strict"

    if symbol_u in {"SOL/USDT", "SOL-USDT"}:
        if "protect_entry_alpha6_score_too_low" in reason_l or "alpha6_low" in reason_l:
            return "sol_protect_alpha6_low_exception"
        if (
            "protect_entry_rsi_confirm_too_weak" in reason_l
            or "protect_entry_no_alpha6_confirmation" in reason_l
            or "f5_rsi" in reason_l
        ):
            return "sol_protect_rsi_weak_exception"

    dominant_factor = str(
        _first(
            top.get("dominant_factor"),
            explain.get("dominant_factor"),
            _find_nested(top, "dominant_factor"),
            _dominant_factor(
                {
                    "f1_mom_5d": f1_mom_5d,
                    "f2_mom_20d": f2_mom_20d,
                    "f3_vol_adj_ret": f3_vol_adj_ret,
                    "f4_volume_expansion": f4_volume_expansion,
                    "f5_rsi_trend_confirm": f5_rsi_trend_confirm,
                }
            ),
        )
        or ""
    )
    if dominant_factor == "f3_vol_adj_ret":
        return "f3_dominant_entry"

    f4 = _float_or_none(f4_volume_expansion)
    if f4 is not None and f4 >= 1.0:
        return "f4_volume_swing"
    if "volume" in reason_l or "f4" in reason_l or "swing" in reason_l:
        return "f4_volume_swing"

    if _has_strategy_name(top, strategy_signals, "TrendFollowing"):
        return "portfolio_trend_following"
    if _has_strategy_name(top, strategy_signals, "Alpha6Factor") or alpha6_score is not None or alpha6_side:
        return "portfolio_alpha6_factor"

    if str(final_decision or "").strip().upper() in {"OPEN_LONG", "REBALANCE"}:
        return "portfolio_alpha6_factor"
    return "portfolio_trend_following"


def _specific_candidate_from_metadata(order: Any, top: Mapping[str, Any]) -> Optional[str]:
    meta = getattr(order, "meta", None)
    if isinstance(meta, Mapping):
        for key in ("strategy_candidate", "strategy_id", "strategy", "source"):
            value = meta.get(key)
            candidate = _normalize_strategy_candidate(value)
            if candidate is not None:
                return candidate
    for key in ("strategy_candidate", "strategy", "source"):
        value = top.get(key)
        candidate = _normalize_strategy_candidate(value)
        if candidate is not None:
            return candidate
    return None


def _normalize_strategy_candidate(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    if text in SPECIFIC_STRATEGY_CANDIDATES:
        return text
    lowered = text.lower()
    if lowered in {"alpha6factor", "alpha6", "portfolio_alpha6", "portfolio"}:
        return "portfolio_alpha6_factor"
    if lowered in {"trendfollowing", "trend_following", "trend", "portfolio_trend"}:
        return "portfolio_trend_following"
    if "btc_leadership" in lowered:
        return "btc_leadership_probe_strict"
    if "f3" in lowered and "vol" in lowered:
        return "f3_dominant_entry"
    if "f4" in lowered or "volume" in lowered:
        return "f4_volume_swing"
    return None


def _dominant_factor(values: Mapping[str, Any]) -> Optional[str]:
    scored: list[tuple[str, float]] = []
    for key, value in values.items():
        number = _float_or_none(value)
        if number is not None:
            scored.append((key, abs(float(number))))
    if not scored:
        return None
    return max(scored, key=lambda item: item[1])[0]


def _has_strategy_name(
    top: Mapping[str, Any],
    strategy_signals: Mapping[str, Mapping[str, Any]],
    strategy_name: str,
) -> bool:
    if strategy_name in strategy_signals:
        return True
    target = strategy_name.lower()
    for key in ("strategy_candidate", "strategy", "source"):
        value = str(top.get(key) or "").strip().lower()
        if value == target:
            return True
    return False


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
