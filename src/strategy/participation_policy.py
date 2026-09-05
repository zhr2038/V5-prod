"""Pure participation decisions and accounting shared by replay and forward.

No files, clock, network, exchange client or order submission is accessed here.
All prices are supplied observations; momentum is not predicted future edge.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
from collections.abc import Mapping, Sequence

STATE_SCHEMA = "v5.participation_state.v1"
STOP_MODES = ("legacy_net_120bps", "price_120bps", "atr14_2x")


def _number(value):
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None
    return value if math.isfinite(value) else None


def policy_hash(config: Mapping) -> str:
    return hashlib.sha256(json.dumps(dict(config), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def validate_policy(config: Mapping) -> None:
    if config.get("schema_version") != "v5.participation_policy.v1":
        raise ValueError("unknown participation policy schema")
    if config.get("mode") != "research_shadow" or config.get("live_promotion_allowed") is not False:
        raise ValueError("participation policy only supports research_shadow")
    if config.get("stop_mode") not in STOP_MODES:
        raise ValueError("unknown stop mode")
    if config.get("maximum_positions") != 1:
        raise ValueError("exactly one shared portfolio position is supported")
    required = ("initial_cash_usdt", "target_weight", "maximum_gross_weight", "minimum_order_notional_usdt",
                "minimum_executable_buffer", "maximum_trade_risk_fraction", "maximum_daily_realized_loss_usdt",
                "maximum_trial_drawdown_fraction", "daily_turnover_ratio", "same_symbol_cooldown_hours",
                "two_losses_cooldown_hours", "maximum_holding_hours", "minimum_4h_momentum_bps",
                "momentum_cost_multiplier", "price_stop_bps", "atr_multiple", "atr_stop_floor_bps",
                "atr_stop_cap_bps", "maximum_signal_premium_fraction", "maximum_spread_bps",
                "maximum_quote_age_seconds", "maximum_bar_age_seconds", "maximum_pending_age_seconds",
                "bar_seconds", "maximum_volume_participation")
    allowed = set(required) | {"schema_version", "policy_id", "frozen_at_utc", "mode", "live_promotion_allowed",
                               "symbols", "disabled_factors", "stop_mode", "comparison_stop_modes", "maximum_positions",
                               "roundtrip_cost_scenarios_bps", "fee_bps", "slippage_bps"}
    if set(config) - allowed:
        raise ValueError("unknown policy settings:" + ",".join(sorted(set(config) - allowed)))
    for key in required:
        if _number(config.get(key)) is None or float(config[key]) <= 0:
            raise ValueError(f"invalid policy setting:{key}")
    for key in ("fee_bps", "slippage_bps"):
        if _number(config.get(key)) is None or not 0 <= config[key] < 10000:
            raise ValueError(f"invalid policy setting:{key}")
    if not 0 < config["target_weight"] <= config["maximum_gross_weight"] <= 1:
        raise ValueError("invalid gross exposure budget")
    if not 0 < config["maximum_trade_risk_fraction"] <= config["maximum_trial_drawdown_fraction"] < 1:
        raise ValueError("invalid loss budget")
    if not 0 < config["atr_stop_floor_bps"] <= config["atr_stop_cap_bps"] < 10000:
        raise ValueError("invalid ATR stop bounds")
    if config["bar_seconds"] != 3600 or config["minimum_executable_buffer"] < 1:
        raise ValueError("hourly bars and a non-reducing executable minimum are required")
    if len(set(config.get("symbols", []))) != 4 or config.get("disabled_factors") != ["f4_volume_expansion"]:
        raise ValueError("explicit four-symbol universe and frozen F4 removal required")


def new_state(config: Mapping) -> dict:
    validate_policy(config)
    cash = float(config["initial_cash_usdt"])
    return {"schema_version": STATE_SCHEMA, "policy_hash": policy_hash(config), "cash_usdt": cash,
            "equity_usdt": cash, "peak_equity_usdt": cash, "position": None, "pending": None,
            "cooldown_until": {}, "loss_streak": {}, "daily_realized_pnl": {}, "daily_turnover": {},
            "halted": False, "last_entry_bar_ts": None, "last_exit_bar_ts": None,
            "last_fill_ts": None, "last_fill_id": None, "valuation_valid": True,
            "valuation_status": "flat_cash", "last_valuation_quote_ts": None}


def market_feature_series(closed_bars: Sequence[Mapping], *, bar_seconds: int = 3600) -> dict:
    """Input rows use closed-bar end timestamps, increasing without gaps."""
    output, closes, averages, true_ranges = {}, [], [], []
    previous_ts = None
    average = None
    for row in closed_bars:
        values = {key: _number(row.get(key, row.get("end") if key == "bar_ts" else None)) for key in ("bar_ts", "open", "high", "low", "close", "volume")}
        if any(value is None for value in values.values()) or row.get("confirmed", True) is not True:
            raise ValueError("unobservable or unconfirmed closed bar")
        ts, o, high, low, close, volume = (values[key] for key in ("bar_ts", "open", "high", "low", "close", "volume"))
        if min(o, high, low, close) <= 0 or volume < 0 or high < max(o, low, close) or low > min(o, close):
            raise ValueError("invalid OHLCV")
        if ts % bar_seconds or (previous_ts is not None and ts != previous_ts + bar_seconds):
            raise ValueError("duplicate, reversed or missing closed bar")
        tr = max(high - low, abs(high - closes[-1]), abs(low - closes[-1])) if closes else high - low
        true_ranges.append(tr)
        average = close if average is None else close * 2 / 21 + average * 19 / 21
        output[int(ts)] = {"bar_ts": int(ts), "close": close, "ema20": average,
                           "ema20_4h_ago": averages[-4] if len(averages) >= 4 else None,
                           "ret4_bps": (close / closes[-4] - 1) * 10000 if len(closes) >= 4 else None,
                           "atr14": sum(true_ranges[-14:]) / 14 if len(true_ranges) >= 14 else None,
                           "volume": volume, "history_bars": len(closes) + 1}
        closes.append(close)
        averages.append(average)
        previous_ts = ts
    return output


def market_features(closed_bars: Sequence[Mapping], *, bar_seconds: int = 3600) -> dict:
    rows = market_feature_series(closed_bars, bar_seconds=bar_seconds)
    return rows[max(rows)] if rows else {}


def quote_status(row: Mapping, now: float, config: Mapping) -> str | None:
    quote = row.get("quote") or {}
    bid, ask, ts = (_number(quote.get(key)) for key in ("bid", "ask", "ts"))
    if bid is None or ask is None or ts is None or min(bid, ask) <= 0 or ask < bid:
        return "quote_unobservable"
    if ts > now:
        return "future_quote"
    if now - ts > config["maximum_quote_age_seconds"]:
        return "stale_quote"
    return None


def mark_to_market(state: Mapping, snapshot: Mapping, config: Mapping) -> dict:
    result = copy.deepcopy(dict(state))
    position = state.get("position")
    if position:
        row = snapshot.get("symbols", {}).get(position["symbol"], {})
        unavailable = quote_status(row, snapshot["now_ts"], config)
        if unavailable:
            result["valuation_valid"] = False
            result["valuation_status"] = unavailable
            return result
        bid = row["quote"]["bid"] * (1 - config["slippage_bps"] / 10000)
        equity = state["cash_usdt"] + position["qty"] * bid * (1 - config["fee_bps"] / 10000)
        result["last_valuation_quote_ts"] = row["quote"]["ts"]
        result["valuation_status"] = "observed_quote"
    else:
        equity = state["cash_usdt"]
        result["valuation_status"] = "flat_cash"
    result["valuation_valid"] = True
    result["equity_usdt"] = equity
    result["peak_equity_usdt"] = max(float(state["peak_equity_usdt"]), equity)
    if 1 - equity / result["peak_equity_usdt"] >= config["maximum_trial_drawdown_fraction"]:
        result["halted"] = True
    return result


def _spread(row: Mapping) -> float:
    quote = row["quote"]
    return (quote["ask"] - quote["bid"]) / ((quote["ask"] + quote["bid"]) / 2) * 10000


def direction_block(row: Mapping, config: Mapping) -> str | None:
    """Same direction predicate at selection and subsequent execution quote."""
    values = {key: _number(row.get(key)) for key in ("close", "ema20", "ema20_4h_ago", "ret4_bps", "cost_bps")}
    if any(value is None for value in values.values()) or values["cost_bps"] < 0 or min(values["close"], values["ema20"], values["ema20_4h_ago"]) <= 0:
        return "direction_or_cost_unobservable"
    if values["close"] <= values["ema20"]:
        return "price_below_ema20"
    if values["ema20"] <= values["ema20_4h_ago"]:
        return "ema20_slope_not_positive"
    cost = max(values["cost_bps"], 2 * (config["fee_bps"] + config["slippage_bps"]) + _spread(row))
    if values["ret4_bps"] < max(config["minimum_4h_momentum_bps"], cost * config["momentum_cost_multiplier"]):
        return "momentum_below_cost_buffer"
    return None


def _stop_distance(row: Mapping, config: Mapping) -> float:
    if config["stop_mode"] != "atr14_2x":
        return config["price_stop_bps"]
    atr = _number(row.get("atr14"))
    if atr is None or atr <= 0:
        raise ValueError("ATR stop requires observable positive ATR14")
    return min(config["atr_stop_cap_bps"], max(config["atr_stop_floor_bps"], atr / row["close"] * 10000 * config["atr_multiple"]))


def _entry_plan(row: Mapping, state: Mapping, config: Mapping, *, now: float, stop_distance: float | None = None) -> dict:
    fee, slip = config["fee_bps"] / 10000, config["slippage_bps"] / 10000
    price = row["quote"]["ask"] * (1 + slip)
    distance = _stop_distance(row, config) if stop_distance is None else stop_distance
    stop = price * (1 - distance / 10000)
    if config["stop_mode"] == "legacy_net_120bps":
        stop = price * (1 - distance / 10000) * (1 + fee) / ((1 - fee) * (1 - slip))
    if stop <= 0 or stop >= price:
        return {"blocked": "cost_consumes_stop_distance"}
    loss_per_notional = (1 + fee) - (stop / price) * (1 - slip) * (1 - fee)
    equity = float(state["equity_usdt"])
    day = str(int(now) // 86400)
    remaining_daily_loss = max(0., config["maximum_daily_realized_loss_usdt"] + float(state.get("daily_realized_pnl", {}).get(day, 0)))
    remaining_trial_loss = max(0., equity - state["peak_equity_usdt"] * (1 - config["maximum_trial_drawdown_fraction"]))
    loss_budget = min(equity * config["maximum_trade_risk_fraction"], remaining_daily_loss, remaining_trial_loss)
    minimum = config["minimum_order_notional_usdt"] * config["minimum_executable_buffer"]
    desired = max(equity * config["target_weight"], minimum)
    cap = min(equity * config["maximum_gross_weight"], state["cash_usdt"] / (1 + fee),
              loss_budget / loss_per_notional)
    if cap + 1e-10 < minimum:
        return {"blocked": "minimum_notional_exceeds_risk_budget"}
    notional = min(desired, cap)
    volume = _number(row.get("volume"))
    if volume is None or notional > volume * row["close"] * config["maximum_volume_participation"]:
        return {"blocked": "volume_participation"}
    return {"price": price, "fee_bps": config["fee_bps"], "notional_usdt": notional,
            "stop_price": stop, "stop_distance_bps": distance,
            "planned_loss_usdt": notional * loss_per_notional,
            "risk_budget_usdt": loss_budget, "entry_equity_usdt": equity,
            "remaining_daily_loss_usdt": remaining_daily_loss,
            "remaining_trial_loss_usdt": remaining_trial_loss}


def decide(snapshot: Mapping, state: Mapping, config: Mapping) -> dict:
    """Create an intent only. It cannot fill at the decision observation."""
    validate_policy(config)
    if state.get("schema_version") != STATE_SCHEMA or state.get("policy_hash") != policy_hash(config):
        raise ValueError("state schema or policy hash mismatch")
    now = _number(snapshot.get("now_ts"))
    if now is None:
        raise ValueError("finite explicit now_ts is required")
    marked = mark_to_market(state, snapshot, config)
    base = {"action": "hold", "reason": "no_eligible_candidate", "decision_ts": now,
            "policy_hash": policy_hash(config)}
    position = state.get("position")
    if position:
        row = snapshot.get("symbols", {}).get(position["symbol"], {})
        reason = None
        if marked.get("halted"):
            reason = "trial_drawdown_stop"
        elif snapshot.get("regime") == "Risk-Off":
            reason = "risk_off"
        elif now - position["entry_ts"] >= config["maximum_holding_hours"] * 3600:
            reason = "time_stop"
        elif not quote_status(row, now, config) and row["quote"]["bid"] <= position["stop_price"]:
            reason = "hard_stop"
        else:
            ts = _number(row.get("bar_ts"))
            close, ema = _number(row.get("close")), _number(row.get("ema20"))
            if ts is not None and position["entry_ts"] < ts <= now and now - ts <= config["maximum_bar_age_seconds"] and close is not None and ema is not None and close < ema:
                reason = "ema20_trend_exit"
        if reason:
            return {**base, "action": "exit_intent", "reason": reason, "symbol": position["symbol"],
                    "qty": position["qty"], "bar_ts": row.get("bar_ts"), "reference_px": row.get("close")}
        return {**base, "reason": "position_open"}
    if state.get("pending"):
        return {**base, "reason": "pending_intent"}
    if marked.get("halted"):
        return {**base, "reason": "trial_halted"}
    if snapshot.get("operational_block"):
        return {**base, "reason": "operational_block"}
    if snapshot.get("regime") not in ("Trending", "Sideways"):
        return {**base, "reason": "risk_off_or_unknown"}
    day = str(int(now) // 86400)
    if float(state.get("daily_realized_pnl", {}).get(day, 0)) <= -config["maximum_daily_realized_loss_usdt"]:
        return {**base, "reason": "daily_loss_limit"}
    ready, reasons = [], {}
    for symbol in config["symbols"]:
        row = snapshot.get("symbols", {}).get(symbol, {})
        ts = _number(row.get("bar_ts"))
        reason = quote_status(row, now, config)
        if row.get("operational_block"):
            reason = "recorded_hard_gate"
        elif ts is None or ts > now or now - ts > config["maximum_bar_age_seconds"]:
            reason = "bar_unobservable_future_or_stale"
        elif ts in (state.get("last_exit_bar_ts"), state.get("last_entry_bar_ts")):
            reason = "same_bar_reentry"
        elif now < float(state.get("cooldown_until", {}).get(symbol, 0)):
            reason = "same_symbol_cooldown"
        values = {key: _number(row.get(key)) for key in ("close", "ema20", "ema20_4h_ago", "ret4_bps", "rank_score", "cost_bps")}
        if reason is None and (any(v is None for v in values.values()) or values["cost_bps"] < 0):
            reason = "direction_rank_or_cost_unobservable"
        if reason is None and _spread(row) > config["maximum_spread_bps"]:
            reason = "spread_too_wide"
        if reason is None:
            cost = max(values["cost_bps"], 2 * (config["fee_bps"] + config["slippage_bps"]) + _spread(row))
            reason = direction_block(row, config)
        if reason is None:
            if config["stop_mode"] == "atr14_2x" and (_number(row.get("atr14")) is None or row["atr14"] <= 0):
                reasons[symbol] = "atr_unobservable"
                continue
            plan = _entry_plan(row, marked, config, now=now)
            reason = plan.get("blocked")
            if reason is None and state.get("daily_turnover", {}).get(day, 0) + 2 * plan["notional_usdt"] > marked["equity_usdt"] * config["daily_turnover_ratio"]:
                reason = "daily_turnover_budget"
            if reason is None:
                ready.append({**base, **plan, "action": "entry_intent", "reason": "absolute_direction_then_relative_rank",
                              "symbol": symbol, "bar_ts": ts, "reference_px": values["close"],
                              "rank_score": values["rank_score"], "cost_bps": cost})
        reasons[symbol] = reason or "eligible"
    if not ready:
        return {**base, "candidate_reasons": reasons}
    chosen = sorted(ready, key=lambda row: (-row["rank_score"], row["symbol"]))[0]
    return {**chosen, "candidate_reasons": reasons}


def check_pending(intent: Mapping, snapshot: Mapping, state: Mapping, config: Mapping) -> dict:
    """Plan a fill at a strictly later quote; caller persists/appends it."""
    now = _number(snapshot.get("now_ts"))
    if now is None or _number(intent.get("decision_ts")) is None:
        raise ValueError("pending execution requires finite explicit timestamps")
    if state.get("schema_version") != STATE_SCHEMA or state.get("policy_hash") != policy_hash(config):
        raise ValueError("pending state schema or policy hash mismatch")
    row = snapshot.get("symbols", {}).get(intent.get("symbol"), {})
    action = intent.get("action")
    if action not in ("entry_intent", "exit_intent"):
        raise ValueError("unknown pending action")
    base = {"action": "wait", "reason": "awaiting_subsequent_quote"}
    if intent.get("policy_hash") != policy_hash(config):
        raise ValueError("pending intent policy hash mismatch")
    if action == "entry_intent" and now - intent["decision_ts"] > config["maximum_pending_age_seconds"]:
        return {"action": "cancel", "reason": "entry_intent_expired"}
    bad_quote = quote_status(row, now, config)
    if bad_quote or row["quote"]["ts"] <= intent["decision_ts"]:
        return {**base, "reason": bad_quote or base["reason"]}
    marked = mark_to_market(state, snapshot, config)
    if action == "exit_intent":
        position = state.get("position")
        if not position or position["symbol"] != intent["symbol"]:
            return {"action": "cancel", "reason": "position_no_longer_open"}
        plan = {"price": row["quote"]["bid"] * (1 - config["slippage_bps"] / 10000),
                "fee_bps": config["fee_bps"], "qty": position["qty"]}
    else:
        reason = None
        if now - intent["decision_ts"] > config["maximum_pending_age_seconds"]:
            reason = "entry_intent_expired"
        elif state.get("position") or marked.get("halted"):
            reason = "position_open_or_trial_halted"
        elif snapshot.get("regime") not in ("Trending", "Sideways"):
            reason = "execution_regime_unobservable_or_risk_off"
        elif snapshot.get("operational_block") or row.get("operational_block"):
            reason = "execution_operational_block"
        elif _number(row.get("cost_bps")) is None or row["cost_bps"] < 0:
            reason = "execution_cost_unobservable"
        elif _number(row.get("bar_ts")) is None or row["bar_ts"] > now or now - row["bar_ts"] > config["maximum_bar_age_seconds"]:
            reason = "execution_bar_unobservable_future_or_stale"
        elif direction_block(row, config):
            reason = {"direction_or_cost_unobservable": "execution_direction_unobservable",
                      "momentum_below_cost_buffer": "execution_momentum_invalidated"}.get(direction_block(row, config), "execution_direction_invalidated")
        elif _spread(row) > config["maximum_spread_bps"]:
            reason = "execution_spread_too_wide"
        elif row["quote"]["ask"] / intent["reference_px"] - 1 > config["maximum_signal_premium_fraction"]:
            reason = "entry_price_premium"
        elif now < state.get("cooldown_until", {}).get(intent["symbol"], 0):
            reason = "same_symbol_cooldown"
        day = str(int(now) // 86400)
        if float(state.get("daily_realized_pnl", {}).get(day, 0)) <= -config["maximum_daily_realized_loss_usdt"]:
            reason = "daily_loss_limit"
        if reason:
            return {"action": "cancel", "reason": reason}
        plan = _entry_plan(row, marked, config, now=now, stop_distance=intent["stop_distance_bps"])
        if plan.get("blocked"):
            return {"action": "cancel", "reason": plan["blocked"]}
        if state.get("daily_turnover", {}).get(day, 0) + 2 * plan["notional_usdt"] > marked["equity_usdt"] * config["daily_turnover_ratio"]:
            return {"action": "cancel", "reason": "daily_turnover_budget"}
    identity = f"{intent['policy_hash']}|{intent['action']}|{intent['symbol']}|{intent['decision_ts']}|{row['quote']['ts']}"
    return {**intent, **plan, "action": "fill", "side": "buy" if action == "entry_intent" else "sell",
            "fill_ts": now, "quote_ts": row["quote"]["ts"], "fill_bar_ts": row.get("bar_ts"),
            "fill_id": hashlib.sha256(identity.encode()).hexdigest()}


def apply_fill(state: Mapping, fill: Mapping, config: Mapping) -> tuple[dict, dict | None]:
    """Return new state plus a closed trade (or None). Does not mutate inputs."""
    if fill.get("action") != "fill" or fill.get("policy_hash") != policy_hash(config):
        raise ValueError("only a policy-bound fill plan may update accounting")
    if fill.get("side") not in ("buy", "sell") or _number(fill.get("price")) is None or fill["price"] <= 0:
        raise ValueError("invalid fill side or price")
    if _number(fill.get("fee_bps")) is None or not 0 <= fill["fee_bps"] < 10000:
        raise ValueError("invalid fill fee")
    if fill["fee_bps"] != config["fee_bps"]:
        raise ValueError("fill fee does not match bound policy")
    if any(_number(fill.get(key)) is None for key in ("fill_ts", "decision_ts", "quote_ts")) or not 0 < fill["decision_ts"] < fill["quote_ts"] <= fill["fill_ts"]:
        raise ValueError("fill requires a finite strictly subsequent quote timestamp")
    if not isinstance(fill.get("fill_id"), str) or len(fill["fill_id"]) != 64:
        raise ValueError("fill identity is required")
    if state.get("schema_version") != STATE_SCHEMA or state.get("policy_hash") != policy_hash(config):
        raise ValueError("fill state schema or policy hash mismatch")
    if fill.get("fill_id") == state.get("last_fill_id"):
        return copy.deepcopy(dict(state)), None
    if state.get("last_fill_ts") is not None and fill["fill_ts"] <= state["last_fill_ts"]:
        raise ValueError("fills must be strictly chronological")
    result = copy.deepcopy(dict(state))
    day = str(int(fill["fill_ts"]) // 86400)
    fee = float(fill["fee_bps"]) / 10000
    closed = None
    if fill["side"] == "buy":
        if result.get("position"):
            raise ValueError("shared portfolio already has a position")
        notional = float(fill["notional_usdt"])
        if not math.isfinite(notional) or notional <= 0 or any(_number(fill.get(key)) is None for key in ("planned_loss_usdt", "stop_price")):
            raise ValueError("invalid entry size or risk")
        if fill["planned_loss_usdt"] <= 0 or not 0 < fill["stop_price"] < fill["price"]:
            raise ValueError("invalid entry stop or risk")
        spent = notional * (1 + fee)
        if spent > result["cash_usdt"] + 1e-9 or notional > result["equity_usdt"] * config["maximum_gross_weight"] + 1e-9:
            raise ValueError("fill exceeds portfolio cash or exposure")
        if fill["planned_loss_usdt"] > result["equity_usdt"] * config["maximum_trade_risk_fraction"] + 1e-9:
            raise ValueError("fill exceeds planned trade loss budget")
        result["cash_usdt"] -= spent
        result["position"] = {"symbol": fill["symbol"], "qty": notional / fill["price"],
                              "entry_price": fill["price"], "entry_ts": fill["fill_ts"],
                              "signal_ts": fill["decision_ts"], "entry_bar_ts": fill["bar_ts"],
                              "entry_notional_usdt": notional, "entry_fee_usdt": notional * fee,
                              "cash_spent": spent, "stop_price": fill["stop_price"],
                              "planned_loss_usdt": fill["planned_loss_usdt"], "policy_hash": fill["policy_hash"]}
        result["last_entry_bar_ts"] = fill.get("fill_bar_ts", fill["bar_ts"])
    else:
        position = result.get("position")
        if not position or position["symbol"] != fill["symbol"] or _number(fill.get("qty")) is None or abs(fill["qty"] - position["qty"]) > 1e-9:
            raise ValueError("close does not match open portfolio quantity")
        notional = position["qty"] * fill["price"]
        proceeds = notional * (1 - fee)
        pnl = proceeds - position["cash_spent"]
        result["cash_usdt"] += proceeds
        result["daily_realized_pnl"][day] = result["daily_realized_pnl"].get(day, 0) + pnl
        streak = result["loss_streak"].get(fill["symbol"], 0) + 1 if pnl < 0 else 0
        cooldown = config["two_losses_cooldown_hours"] if streak >= 2 else config["same_symbol_cooldown_hours"]
        result["loss_streak"][fill["symbol"]] = 0 if streak >= 2 else streak
        result["cooldown_until"][fill["symbol"]] = fill["fill_ts"] + cooldown * 3600
        closed = {**position, "exit_ts": fill["fill_ts"], "exit_price": fill["price"],
                  "exit_fee_usdt": notional * fee, "net_pnl_usdt": pnl,
                  "cost_usdt": position["entry_fee_usdt"] + notional * fee,
                  "cost_scope": "explicit_fees_only_spread_and_slippage_embedded_in_prices",
                  "net_bps": pnl / position["cash_spent"] * 10000,
                  "hold_hours": (fill["fill_ts"] - position["entry_ts"]) / 3600,
                  "exit_reason": fill["reason"]}
        result["position"] = None
        result["last_exit_bar_ts"] = fill.get("fill_bar_ts", fill.get("bar_ts"))
        result["equity_usdt"] = result["cash_usdt"]
    result["daily_turnover"][day] = result["daily_turnover"].get(day, 0) + notional
    result["pending"] = None
    result["last_fill_id"], result["last_fill_ts"] = fill["fill_id"], fill["fill_ts"]
    return result, closed
