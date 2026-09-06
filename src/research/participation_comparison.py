"""Independent portfolio adapter around the existing participation decision kernel."""
from __future__ import annotations

import copy
import hashlib
import json

from src.research.quote_portfolio import QuotePortfolio, serializable
from src.research.reference_contract import reference_use
from src.strategy import participation_policy as policy


class ParticipationComparison:
    def __init__(self, config, *, use_reference=False, reference_contract=None):
        self.config = copy.deepcopy(config)
        policy.validate_policy(config)
        self.state = policy.new_state(config)
        self.book = QuotePortfolio(initial_cash=config["initial_cash_usdt"], fee_bps=config["fee_bps"],
                                   slippage_bps=config["slippage_bps"], maximum_quote_age_seconds=config["maximum_quote_age_seconds"])
        self.use_reference = use_reference
        self.reference_contract = copy.deepcopy(reference_contract)
        self.last_observed = None
        self.last_bar = None
        self.events = []

    def _sync(self, snapshot):
        mark = self.book.mark(snapshot["symbols"], now=snapshot["now_ts"])
        self.state["cash_usdt"] = float(self.book.cash)
        self.state["equity_usdt"] = float(mark["equity_usdt"])
        self.state["peak_equity_usdt"] = float(self.book.peak)
        if not self.state["halted"] and float(mark["drawdown_fraction"]) >= self.config["maximum_trial_drawdown_fraction"]:
            self.state["halted"] = True
            self.state["halted_reason"] = {"reason": "economic_equity_trial_drawdown", "observed_at": snapshot["now_ts"],
                                           "drawdown_fraction": float(mark["drawdown_fraction"])}
        mark["trial_halted"] = self.state["halted"]
        mark["trial_stop_reason"] = self.state.get("halted_reason")
        active = [value for symbol, value in self.book.positions.items()
                  if not (value.get("management_status") == "residual_after_exit" and symbol in mark["dust_positions"])]
        if len(active) > 1:
            raise ValueError("participation comparison exceeded its single-position mandate")
        if active:
            value = active[0]
            meta = value["metadata"]
            self.state["position"] = {"symbol": value["symbol"], "qty": float(value["qty"]),
                                      "entry_ts": float(value["entry_ts"]), "entry_price": float(meta["entry_price"]),
                                      "cash_spent": float(value["cash_cost"]), "entry_fee_usdt": float(value["entry_fee_usdt"]),
                                      "entry_bar_ts": meta["entry_bar_ts"], "stop_price": meta["stop_price"],
                                      "policy_hash": policy.policy_hash(self.config)}
        else:
            self.state["position"] = None
        return mark

    def _reference_defer(self, reference, now):
        return reference_use(reference, now, self.reference_contract)["defer"]

    def observe(self, snapshot, *, references=None, allow_new_signal=True):
        now = snapshot["now_ts"]
        if self.last_observed is not None and now <= self.last_observed:
            raise ValueError("comparison observations must be strictly chronological")
        self._sync(snapshot)
        pending = self.state["pending"]
        execution, decision = None, None
        if pending:
            execution = policy.check_pending(pending, snapshot, self.state, self.config)
            if execution["action"] == "fill":
                symbol = pending["symbol"]
                trade_intent = {"intent_id": execution["fill_id"], "symbol": symbol, "side": execution["side"],
                                "decision_ts": pending["decision_ts"], "notional_usdt": execution.get("notional_usdt", execution.get("qty", 0) * execution["price"]),
                                "quantity": execution.get("qty") if execution["side"] == "sell" else None,
                                "reason": pending["reason"], "metadata": {"entry_price": execution["price"],
                                "entry_bar_ts": pending["bar_ts"], "stop_price": execution.get("stop_price")}}
                try:
                    fill = self.book.fill(trade_intent, snapshot["symbols"][symbol], now=now)
                    day = str(int(now) // 86400)
                    self.state["daily_turnover"][day] = self.state["daily_turnover"].get(day, 0) + float(fill["notional_usdt"])
                    if fill["side"] == "buy":
                        self.state["last_entry_bar_ts"] = snapshot["bar_ts"]
                    else:
                        pnl = float(fill["realized_pnl_usdt"])
                        self.state["daily_realized_pnl"][day] = self.state["daily_realized_pnl"].get(day, 0) + pnl
                        self.state["last_exit_bar_ts"] = snapshot["bar_ts"]
                        streak = self.state["loss_streak"].get(symbol, 0) + 1 if pnl < 0 else 0
                        hours = self.config["two_losses_cooldown_hours"] if streak >= 2 else self.config["same_symbol_cooldown_hours"]
                        self.state["loss_streak"][symbol] = 0 if streak >= 2 else streak
                        self.state["cooldown_until"][symbol] = now + hours * 3600
                    execution = serializable(fill)
                    self.state["pending"] = None
                except ValueError as exc:
                    execution = {"action": "execution_constraint_rejected", "reason": str(exc)}
                    if pending["action"] == "entry_intent":
                        self.state["pending"] = None
            elif execution["action"] == "cancel":
                self.state["pending"] = None
        self._sync(snapshot)
        if self.state["pending"] is None and (self.state["position"] or (allow_new_signal and self.last_bar != snapshot["bar_ts"])):
            decision = policy.decide(snapshot, self.state, self.config)
            if decision["action"] == "entry_intent" and self.use_reference:
                reference = (references or {}).get(decision["symbol"])
                if self._reference_defer(reference, now):
                    decision = {**decision, "action": "reference_deferred_entry", "reference_advice_id": reference.get("advice_id"),
                                "reference_use": reference_use(reference, now, self.reference_contract)}
            if decision["action"] in {"entry_intent", "exit_intent"}:
                self.state["pending"] = decision
        self.last_observed = now
        if allow_new_signal:
            self.last_bar = snapshot["bar_ts"]
        event = {"observed_at": now, "bar_ts": snapshot["bar_ts"], "decision": decision, "execution": execution,
                 "snapshot_hash": hashlib.sha256(json.dumps(snapshot, sort_keys=True, allow_nan=False).encode()).hexdigest(),
                 "portfolio": serializable(self._sync(snapshot)), "live_order_effect": "none"}
        self.events.append(event)
        return copy.deepcopy(event)
