"""Recorded-quote replay of the exact forward participation decision kernel."""
from __future__ import annotations

import collections
import copy
import statistics

from src.research.factor_ablation import ablate_records
from src.research.rally_reentry_validation import (
    candidate_values, epoch, prepare_market, prepare_records,
)
from src.strategy.participation_policy import (
    apply_fill, check_pending, decide, mark_to_market, market_feature_series,
    new_state, policy_hash, validate_policy,
)


def prepare_inputs(history: dict, market: dict) -> tuple[dict, dict, dict]:
    bars, old_features = prepare_market(market)
    records, quality = prepare_records(history, bars)
    if history.get("missing") or quality["price_mismatches"]:
        raise ValueError("missing or inconsistent immutable source evidence")
    records, quality["factor_removal"] = ablate_records(records, ["f4_volume_expansion"], old_features)
    features = {symbol: market_feature_series([
        {"bar_ts": stamp, "open": bar.open, "high": bar.high, "low": bar.low,
         "close": bar.close, "volume": bar.volume} for stamp, bar in sorted(rows.items())
    ]) for symbol, rows in bars.items()}
    return records, features, quality


def recorded_snapshot(stamp: int, record: dict, features: dict) -> dict:
    times = [epoch(row.get("ts_utc")) for row in record["candidates"].values()]
    now = max([stamp, *[value for value in times if value is not None]])
    snapshot = {"now_ts": now, "regime": record["audit"].get("regime"), "symbols": {}}
    for symbol, candidate in record["candidates"].items():
        values = candidate_values(record, symbol)
        blocked = []
        for route in record["audit"].get("router_decisions", []):
            reason = route.get("reason", "")
            if route.get("symbol") == symbol and route.get("action") == "skip" and (
                ("negative_expectancy" in reason and "no_closed" not in reason)
                or any(term in reason for term in ("kill_switch", "reconcile_failed", "ledger_failed"))
            ):
                blocked.append(reason)
        snapshot["symbols"][symbol] = {
            **features[symbol][stamp], "rank_score": values["relative"], "cost_bps": values["cost_bps"],
            "operational_block": ";".join(blocked), "run_id": record["run_id"],
            "quote": {"bid": _float(candidate.get("arrival_bid")), "ask": _float(candidate.get("arrival_ask")),
                      "ts": epoch(candidate.get("quote_ts"))},
        }
    return snapshot


def _float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def replay(records: dict, features: dict, config: dict, start: int, end: int) -> dict:
    """Every fill follows an intent at an earlier observation; no forced close."""
    validate_policy(config)
    if not start < end:
        raise ValueError("invalid replay window")
    expected = range(start, end + 1, int(config["bar_seconds"]))
    if any(stamp not in records for stamp in expected):
        raise ValueError("replay cannot skip missing recorded observations")
    state = new_state(config)
    trades, curve, fills = [], [], []
    reasons, candidate_reasons = collections.Counter(), collections.Counter()
    for stamp in expected:
        snapshot = recorded_snapshot(stamp, records[stamp], features)
        state = mark_to_market(state, snapshot, config)
        if state["pending"]:
            plan = check_pending(state["pending"], snapshot, state, config)
            reasons[plan["action"] + ":" + plan["reason"]] += 1
            if plan["action"] == "fill":
                state, closed = apply_fill(state, plan, config)
                fills.append(plan)
                if closed:
                    trades.append(closed)
                state = mark_to_market(state, snapshot, config)
            elif plan["action"] == "cancel":
                state["pending"] = None
        if state["pending"] is None:
            intent = decide(snapshot, state, config)
            reasons[intent["action"] + ":" + intent["reason"]] += 1
            candidate_reasons.update(intent.get("candidate_reasons", {}).values())
            if intent["action"] in ("entry_intent", "exit_intent"):
                state["pending"] = intent
        equity = state["equity_usdt"]
        position = state["position"]
        gross = 0
        if position:
            row = snapshot["symbols"][position["symbol"]]
            gross = position["qty"] * row["close"] / equity
        curve.append({"bar_ts": stamp, "now_ts": snapshot["now_ts"], "cash_usdt": state["cash_usdt"],
                      "equity_usdt": equity, "drawdown_fraction": 1 - equity / state["peak_equity_usdt"],
                      "gross_weight": gross, "halted": state["halted"],
                      "valuation_status": state["valuation_status"], "valuation_valid": state["valuation_valid"]})
    positive = sum(max(0, t["net_pnl_usdt"]) for t in trades)
    negative = -sum(min(0, t["net_pnl_usdt"]) for t in trades)
    realized = sum(t["net_pnl_usdt"] for t in trades)
    last_marked_pnl = state["equity_usdt"] - config["initial_cash_usdt"]
    marked_pnl = last_marked_pnl if state["valuation_valid"] else None
    metrics = {
        "start_ts": start, "end_ts": end, "policy_hash": policy_hash(config),
        "stop_mode": config["stop_mode"], "roundtrip_fee_reserve_bps": config["fee_bps"] * 2,
        "roundtrip_slippage_reserve_bps": config["slippage_bps"] * 2,
        "roundtrip_cost_reserve_bps": 2 * (config["fee_bps"] + config["slippage_bps"]),
        "net_liquidation_pnl_usdt": marked_pnl, "realized_net_pnl_usdt": realized,
        "unrealized_net_pnl_usdt": marked_pnl - realized if marked_pnl is not None else None,
        "return_pct": marked_pnl / config["initial_cash_usdt"] * 100 if marked_pnl is not None else None,
        "terminal_valuation_valid": state["valuation_valid"], "terminal_valuation_status": state["valuation_status"],
        "last_valuation_quote_ts": state["last_valuation_quote_ts"], "last_known_mark_pnl_usdt": last_marked_pnl,
        "max_drawdown_pct": max(row["drawdown_fraction"] for row in curve) * 100,
        "closed_trades": len(trades), "wins": sum(t["net_pnl_usdt"] > 0 for t in trades),
        "profit_factor": positive / negative if negative else None,
        "explicit_fee_cost_usdt": sum(t["cost_usdt"] for t in trades) + (state["position"]["entry_fee_usdt"] if state["position"] else 0),
        "mean_hold_hours": statistics.mean(t["hold_hours"] for t in trades) if trades else None,
        "mean_gross_weight_pct": statistics.mean(row["gross_weight"] for row in curve) * 100,
        "calendar_days_with_entries": len({int(f["fill_ts"]) // 86400 for f in fills if f["side"] == "buy"}),
        "maximum_planned_entry_loss_fraction": max((f["planned_loss_usdt"] / f["entry_equity_usdt"] for f in fills if f["side"] == "buy"), default=0),
        "largest_win_share": max((t["net_pnl_usdt"] for t in trades), default=0) / positive if positive else None,
        "open_positions_at_end": int(state["position"] is not None),
        "trial_halted": state["halted"], "reasons": dict(reasons), "candidate_reasons": dict(candidate_reasons),
    }
    if abs(sum(state["daily_realized_pnl"].values()) - realized) > 1e-8:
        raise AssertionError("realized ledger identity failed")
    if any(f["quote_ts"] <= f["decision_ts"] or f["fill_ts"] < f["quote_ts"] for f in fills):
        raise AssertionError("fill used non-causal quote")
    return {"metrics": metrics, "trades": trades, "fills": fills, "equity_curve": curve,
            "final_state": copy.deepcopy(state)}
