"""Run the shared participation policy on genuinely observed hourly quotes.

This module owns a separate virtual portfolio, never production orders/funds.
Historical replay cannot be written into its forward cohort.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from src.reporting.participation_store import ParticipationStore
from src.strategy import participation_policy as policy

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FACTORS = ("f1_mom_5d", "f2_mom_20d", "f3_vol_adj_ret", "f4_volume_expansion",
           "f5_rsi_trend_confirm", "f6_sentiment")


def _get(obj, key, default=None):
    return obj.get(key, default) if isinstance(obj, Mapping) else getattr(obj, key, default)


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def _epoch(value):
    number = _number(value)
    if number is not None:
        return number / 1000 if number > 10_000_000_000 else number
    if isinstance(value, str):
        try:
            stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return stamp.timestamp() if stamp.tzinfo is not None else None
        except ValueError:
            pass
    return None


def _save_report(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".participation-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, allow_nan=False, ensure_ascii=True, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def operational_block(reports_dir: Path, now: float) -> str | None:
    for name in ("kill_switch.json", "reconcile_status.json", "ledger_status.json"):
        try:
            payload = json.loads((reports_dir / name).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return name + ":unobservable"
        if name == "kill_switch.json":
            if payload.get("enabled") is not False:
                return "kill_switch_enabled_or_unknown"
        else:
            stamp = _epoch(payload.get("ts_ms"))
            if payload.get("ok") is not True or stamp is None or not 0 <= now - stamp <= 900:
                return name + ":failed_stale_or_future"
    return None


def build_snapshot(*, market_data, top_of_book, audit, config, now, block=None, costs=None, negative_state=None):
    end = _epoch(_get(audit, "window_end_ts"))
    if end is None or end % 3600 or end > now:
        raise ValueError("participation requires an observable completed decision hour")
    factors = _get(audit, "alpha_factor_snapshot", {}) or {}
    weights = _get(audit, "effective_alpha6_weights", {}) or {}
    routes = _get(audit, "router_decisions", []) or []
    qlab = _get(audit, "quant_lab", {}) or {}
    costs = costs or {}
    if qlab.get("permission_gate_enforced") and qlab.get("final_permission") not in ("ALLOW", "allow"):
        block = "enforced_quant_lab_permission"
    symbols = {}
    errors = {}
    for symbol in config["symbols"]:
        series = market_data.get(symbol)
        quote = top_of_book.get(symbol, top_of_book.get(symbol.replace("/", "-"), {})) or {}
        row = {"quote": {"bid": _number(quote.get("bid")), "ask": _number(quote.get("ask")),
                         "ts": _epoch(quote.get("timestamp", quote.get("ts", quote.get("quote_ts"))))},
               "cost_bps": 2 * (config["fee_bps"] + config["slippage_bps"])}
        row["cost_source"] = "explicit_policy_cost_assumption"
        cost_row = costs.get(symbol, {})
        observed_costs = [_number(cost_row.get(key)) for key in (
            "selected_entry_gate_cost_bps", "roundtrip_all_in_cost_bps", "selected_total_cost_bps")]
        observed_costs = [value for value in observed_costs if value is not None and value >= 0]
        if observed_costs:
            row["cost_bps"] = max(row["cost_bps"], *observed_costs)
            row["cost_source"] = "max_policy_assumption_and_cached_symbol_cost"
            row["cost_model_version"] = cost_row.get("cost_model_version")
        row["cost_trusted_for_live"] = False
        try:
            if series is None:
                raise ValueError("missing_symbol_market_data")
            columns = [_get(series, name, []) for name in ("ts", "open", "high", "low", "close", "volume")]
            if not columns[0] or len({len(column) for column in columns}) != 1:
                raise ValueError("incomplete_market_columns")
            bars = []
            for ts, o, high, low, close, volume in zip(*columns):
                opened = _epoch(ts)
                if opened is None:
                    raise ValueError("invalid_bar_timestamp")
                closed = opened + 3600
                if closed <= end:
                    bars.append({"bar_ts": closed, "open": o, "high": high, "low": low,
                                 "close": close, "volume": volume, "confirmed": True})
            if len(bars) < 60 or bars[-1]["bar_ts"] != end:
                raise ValueError("missing_latest_closed_bar_or_warmup")
            row.update(policy.market_features(bars))
            snapshot = factors.get(symbol, {})
            z = snapshot.get("z_factors", {})
            values = {factor: _number(z.get(factor)) for factor in FACTORS}
            resolved = {factor: _number(weights.get(factor)) for factor in FACTORS}
            absolute = _number(snapshot.get("raw_factors", {}).get("alpha6_final_score"))
            if absolute is None or any(v is None for v in [*values.values(), *resolved.values()]):
                raise ValueError("factor_provenance_unobservable")
            reconstructed = sum(values[key] * resolved[key] for key in FACTORS)
            if abs(reconstructed - absolute) > 1e-8:
                raise ValueError("factor_reconstruction_mismatch")
            row["rank_score"] = sum(values[key] * resolved[key] for key in FACTORS if key not in config["disabled_factors"])
            for route in routes:
                if route.get("symbol") != symbol or route.get("action") != "skip":
                    continue
                reason = str(route.get("reason", ""))
                if ("negative_expectancy" in reason and "no_closed" not in reason) or any(term in reason for term in ("kill_switch", "reconcile_failed", "ledger_failed")):
                    row["operational_block"] = reason
            cooldown = (negative_state or {}).get("symbols", {}).get(symbol, {})
            cooldown_until = _epoch(cooldown.get("cooldown_until_ms"))
            if cooldown_until is not None and cooldown_until > now:
                row["operational_block"] = "negative_expectancy_cooldown"
        except (ValueError, TypeError, KeyError) as exc:
            errors[symbol] = str(exc)
            row["operational_block"] = str(exc)
        symbols[symbol] = row
    return {"now_ts": now, "bar_ts": int(end), "regime": str(_get(audit, "regime", "Unknown")),
            "operational_block": block, "symbols": symbols, "data_errors": errors}


def process_observation(*, snapshot, config, store, identity, source_run_id):
    """Single atomic transition. Duplicate/older bars never execute again."""
    now, bar = snapshot["now_ts"], snapshot["bar_ts"]
    with store.transaction() as connection:
        state = store.load(connection, identity)
        if state is None:
            state = {**policy.new_state(config), "cohort_started_at": now,
                     "last_observed_ts": None, "last_observed_bar_ts": None,
                     "closed_trade_count": 0, "entry_count": 0, "net_realized_pnl_usdt": 0.0}
        last_bar = state.get("last_observed_bar_ts")
        if last_bar is not None and bar <= last_bar:
            saved = connection.execute("SELECT event FROM decisions ORDER BY bar_ts DESC LIMIT 1").fetchone()
            if saved is None:
                raise ValueError("portfolio has no committed observation event")
            return {**json.loads(saved[0]), "status": "duplicate_or_older_bar",
                    "requested_bar_ts": bar, "last_observed_bar_ts": last_bar}
        if state.get("last_observed_ts") is not None and now <= state["last_observed_ts"]:
            raise ValueError("forward observations must be strictly chronological")
        gap = last_bar is not None and bar != last_bar + config["bar_seconds"]
        state = policy.mark_to_market(state, snapshot, config)
        execution, closed = None, None
        pending = state.get("pending")
        if pending:
            if gap and pending["action"] == "entry_intent":
                execution = {"action": "cancel", "reason": "missing_forward_observation"}
            else:
                execution = policy.check_pending(pending, snapshot, state, config)
            if execution["action"] == "fill":
                state, closed = policy.apply_fill(state, execution, config)
                state["entry_count"] += int(execution["side"] == "buy")
            elif execution["action"] == "cancel":
                state["pending"] = None
        state = policy.mark_to_market(state, snapshot, config)
        if state.get("pending"):
            decision = {"action": "hold", "reason": "pending_intent"}
        else:
            decision = policy.decide(snapshot, state, config)
            if decision["action"] in ("entry_intent", "exit_intent"):
                state["pending"] = decision
        if closed:
            state["closed_trade_count"] += 1
            state["net_realized_pnl_usdt"] += closed["net_pnl_usdt"]
        state["last_observed_bar_ts"], state["last_observed_ts"] = bar, now
        event = {"schema_version": "v5.participation_forward_observation.v1", "status": "observed",
                 "observed_ts": now, "bar_ts": bar, "source_run_id": source_run_id,
                 "cohort_started_at": state["cohort_started_at"], "identity": identity,
                 "policy_hash": policy.policy_hash(config), "mode": "forward_paper",
                 "live_order_effect": "none", "live_promotion_allowed": False,
                 "historical_backfill": False, "observation_gap": gap,
                 "snapshot": snapshot, "decision": decision, "execution": execution,
                 "closed_trade": closed, "portfolio": state,
                 "sampling": "hourly_observed_quotes; intrahour_stop_fills_not_observable"}
        store.save(connection, identity=identity, state=state, event=event)
        return event


def update_participation_runtime(*, cfg, market_data_1h, top_of_book, audit,
                                 run_dir, reports_dir, now=None):
    settings = _get(cfg, "participation")
    if not settings or not _get(settings, "enabled", False):
        return {"enabled": False}
    if _get(settings, "mode") != "forward_paper":
        raise ValueError("participation runtime requires forward_paper")
    now = datetime.now(timezone.utc).timestamp() if now is None else float(now)
    end = _epoch(_get(audit, "window_end_ts"))
    if end is None or not 0 <= now - end <= _get(settings, "max_signal_age_seconds", 900):
        raise ValueError("stale or future run cannot create prospective observations")
    policy_path = Path(_get(settings, "policy_path"))
    if not policy_path.is_absolute():
        policy_path = PROJECT_ROOT / policy_path
    config = json.loads(policy_path.read_text(encoding="utf-8"))
    policy.validate_policy(config)
    state_path = Path(_get(settings, "state_path"))
    if not state_path.is_absolute():
        state_path = PROJECT_ROOT / state_path
    state_path = state_path.resolve()
    allowed_state_root = (Path(reports_dir) / "participation").resolve()
    if allowed_state_root not in state_path.parents or state_path.suffix != ".sqlite":
        raise ValueError("participation state must be an isolated .sqlite file under runtime reports/participation")
    from src.reporting.candidate_snapshot import (
        candidate_snapshot_symbol_cost_table_paths, load_latest_symbol_cost_table,
    )
    costs = load_latest_symbol_cost_table(candidate_snapshot_symbol_cost_table_paths(reports_dir))
    negative_state = {}
    negative_path = Path(reports_dir) / "negative_expectancy_cooldown.json"
    if negative_path.is_file():
        negative_state = json.loads(negative_path.read_text(encoding="utf-8"))
    # Bind executable decision/accounting code, not only parameter names.
    source_hashes = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
                     for name, path in (("policy", policy.__file__), ("runtime", __file__),
                                        ("store", Path(__file__).with_name("participation_store.py")))}
    identity = hashlib.sha256(json.dumps({"policy": policy.policy_hash(config),
                                        "code": source_hashes}, sort_keys=True).encode()).hexdigest()
    snapshot = build_snapshot(market_data=market_data_1h, top_of_book=top_of_book, audit=audit,
                              config=config, now=now, block=operational_block(Path(reports_dir), now),
                              costs=costs, negative_state=negative_state)
    event = process_observation(snapshot=snapshot, config=config, store=ParticipationStore(state_path),
                                identity=identity, source_run_id=str(_get(audit, "run_id", "")))
    event["source_hashes"] = source_hashes
    _save_report(Path(run_dir) / "participation_forward.json", event)
    _save_report(Path(reports_dir) / "participation" / "latest.json", event)
    return {"enabled": True, "status": event["status"], "decision": event.get("decision"),
            "entry_count": event.get("portfolio", {}).get("entry_count"),
            "closed_trades": event.get("portfolio", {}).get("closed_trade_count"),
            "state_path": str(state_path), "live_order_effect": "none"}
