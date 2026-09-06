"""Shared observable-quote execution for independent research portfolios.

There is no exchange client here. Quantity steps and fee currencies are explicit
inputs, and all residual quantities remain in the position ledger.
"""
from __future__ import annotations

import copy
import re
from decimal import Decimal, ROUND_DOWN


def number(value):
    if isinstance(value, bool) or value is None:
        raise ValueError("numeric portfolio input required")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("finite portfolio input required")
    return result


def rounded(value, step):
    step = number(step)
    if step <= 0:
        raise ValueError("positive quantity step required")
    return (number(value) / step).to_integral_value(rounding=ROUND_DOWN) * step


class QuotePortfolio:
    def __init__(self, *, initial_cash, fee_bps, slippage_bps, maximum_quote_age_seconds=120):
        self.initial_cash = self.cash = number(initial_cash)
        self.fee = number(fee_bps) / 10000
        self.slippage = number(slippage_bps) / 10000
        self.maximum_age = number(maximum_quote_age_seconds)
        if self.cash <= 0 or not 0 <= self.fee < 1 or not 0 <= self.slippage < 1 or self.maximum_age <= 0:
            raise ValueError("invalid portfolio assumptions")
        self.positions, self.fills, self.closed_lots = {}, [], []
        self.filled_ids = set()
        self.peak = self.cash
        self.last_fill_ts = None

    def _market(self, row, now):
        quote = row["quote"]
        bid, ask, ts = (number(quote[key]) for key in ("bid", "ask", "ts"))
        if min(bid, ask) <= 0 or bid > ask or not 0 <= number(now) - ts <= self.maximum_age:
            raise ValueError("invalid_stale_or_future_quote")
        instrument = row["instrument"]
        if not re.fullmatch(r"[a-f0-9]{64}", str(instrument.get("source_hash", ""))):
            raise ValueError("instrument_source_hash_required")
        step, minimum = number(instrument["lot_size"]), number(instrument["minimum_qty"])
        notional = number(instrument["minimum_notional_usdt"])
        if instrument.get("minimum_notional_source") == "common_research_policy":
            raise ValueError("research_entry_floor_is_not_an_exchange_specification")
        if min(step, minimum) <= 0 or notional < 0:
            raise ValueError("invalid_instrument_constraints")
        observed = number(instrument["observed_ts"])
        if observed > number(now) or observed <= 0:
            raise ValueError("instrument_not_yet_observed")
        return bid, ask, ts, step, minimum, notional

    def fill(self, intent, row, *, now):
        key = intent["intent_id"]
        if key in self.filled_ids:
            return None
        bid, ask, ts, step, minimum, minimum_notional = self._market(row, now)
        if not number(intent["decision_ts"]) < ts <= number(now):
            raise ValueError("fill_requires_subsequent_observable_quote")
        if self.last_fill_ts is not None and number(now) < self.last_fill_ts:
            raise ValueError("out_of_order_fill")
        side, symbol = intent["side"], intent["symbol"]
        base, quote_ccy = symbol.split("/")
        currency = row["instrument"]["buy_fee_currency" if side == "buy" else "sell_fee_currency"]
        if quote_ccy != "USDT" or currency not in {base, quote_ccy} or side not in {"buy", "sell"}:
            raise ValueError("unsupported_fee_currency_or_side")
        price = ask * (1 + self.slippage) if side == "buy" else bid * (1 - self.slippage)
        requested = number(intent["notional_usdt"])
        if requested <= 0:
            raise ValueError("invalid_order_notional")
        quantity = requested / price
        if side == "sell" and intent.get("quantity") is not None:
            quantity = number(intent["quantity"])
        position = self.positions.get(symbol)
        if side == "buy":
            affordable = self.cash / (price * (1 + self.fee if currency == quote_ccy else 1))
            quantity = min(quantity, affordable)
        else:
            if not position:
                raise ValueError("sell_without_position")
            available = position["qty"] / (1 + self.fee if currency == base else 1)
            quantity = min(quantity, available)
        quantity = rounded(quantity, step)
        notional = quantity * price
        if quantity < minimum or notional < minimum_notional:
            raise ValueError("below_minimum_executable_size")
        if side == "buy" and notional < number(row.get("entry_minimum_notional_usdt", 0)):
            raise ValueError("below_research_entry_minimum")
        fee_amount = (quantity if currency == base else notional) * self.fee
        fee_usdt = quantity * price * self.fee
        realized = Decimal(0)
        if side == "buy":
            received = quantity - (fee_amount if currency == base else 0)
            spent = notional + (fee_amount if currency == quote_ccy else 0)
            if received <= 0 or spent > self.cash:
                raise ValueError("invalid_cash_or_received_quantity")
            self.cash -= spent
            if position is None:
                position = {"symbol": symbol, "qty": Decimal(0), "cash_cost": Decimal(0),
                            "entry_ts": number(now), "entry_fee_usdt": Decimal(0), "metadata": copy.deepcopy(intent.get("metadata", {}))}
                self.positions[symbol] = position
            elif position.get("management_status") == "residual_after_exit":
                # A new executable campaign starts now; old dust and its cost are retained.
                position["entry_ts"] = number(now)
                position["metadata"] = {**copy.deepcopy(intent.get("metadata", {})), "carried_dust_cost_usdt": str(position["cash_cost"])}
                position["entry_fee_usdt"] = Decimal(0)
            position["management_status"] = "active"
            position["qty"] += received
            position["cash_cost"] += spent
            position["entry_fee_usdt"] += fee_usdt
            position["entry_price"] = position["cash_cost"] / position["qty"]
        else:
            consumed = quantity + (fee_amount if currency == base else 0)
            proceeds = notional - (fee_amount if currency == quote_ccy else 0)
            allocated_cost = position["cash_cost"] * consumed / position["qty"]
            realized = proceeds - allocated_cost
            self.cash += proceeds
            position["qty"] -= consumed
            position["cash_cost"] -= allocated_cost
            residual = rounded(position["qty"], step) < minimum or rounded(position["qty"], step) * bid < minimum_notional
            # Only an actually executed exit may leave an explicit residual. A price
            # decline below an entry budget never silently removes an active holding.
            position["management_status"] = "residual_after_exit" if residual else "active"
            self.closed_lots.append({"symbol": symbol, "entry_ts": position["entry_ts"], "exit_ts": number(now),
                                     "consumed_qty": consumed, "allocated_cost_usdt": allocated_cost,
                                     "net_pnl_usdt": realized, "reason": intent.get("reason"),
                                     "campaign_closed": residual})
            if position["qty"] == 0:
                del self.positions[symbol]
        fill = {"intent_id": key, "symbol": symbol, "side": side, "decision_ts": intent["decision_ts"],
                "observed_at": number(now), "quote_ts": ts, "price": price, "quantity": quantity,
                "notional_usdt": notional, "fee_currency": currency, "fee_amount": fee_amount,
                "fee_usdt": fee_usdt, "realized_pnl_usdt": realized,
                "instrument_source_hash": row["instrument"].get("source_hash"), "live_order_effect": "none"}
        self.fills.append(fill)
        self.filled_ids.add(key)
        self.last_fill_ts = number(now)
        return copy.deepcopy(fill)

    def mark(self, rows, *, now):
        liquidation, economic_value, cost_basis, gross_exposure = (Decimal(0) for _ in range(4))
        dust, restricted = {}, {}
        for symbol, position in self.positions.items():
            row = rows[symbol]
            bid, _ask, _ts, step, minimum, min_notional = self._market(row, now)
            price = bid * (1 - self.slippage)
            base, quote_ccy = symbol.split("/")
            currency = row["instrument"]["sell_fee_currency"]
            if currency not in {base, quote_ccy}:
                raise ValueError("unsupported_liquidation_fee_currency")
            available = position["qty"] / (1 + self.fee if currency == base else 1)
            qty = rounded(available, step)
            net_full_value = available * price * (1 - self.fee if currency == quote_ccy else 1)
            proceeds = qty * price
            reason = None
            if qty < minimum or proceeds < min_notional:
                reason = "below_exchange_minimum_quantity" if qty < minimum else "below_verified_exchange_notional"
                proceeds = Decimal(0)
                dust[symbol] = position["qty"]
                executable_consumed = Decimal(0)
            elif currency == quote_ccy:
                proceeds *= 1 - self.fee
                executable_consumed = qty
            else:
                executable_consumed = qty * (1 + self.fee)
            remaining = max(Decimal(0), position["qty"] - executable_consumed)
            if remaining:
                restricted[symbol] = {"quantity": remaining, "market_value_usdt": remaining * bid,
                                      "estimated_net_value_usdt": net_full_value - proceeds,
                                      "cost_basis_usdt": position["cash_cost"] * remaining / position["qty"],
                                      "reason": reason or "quantity_step_residual",
                                      "management_status": position.get("management_status", "active")}
            liquidation += proceeds
            economic_value += net_full_value
            cost_basis += position["cash_cost"]
            gross_exposure += position["qty"] * bid
        equity = self.cash + economic_value
        self.peak = max(self.peak, equity)
        return {"cash_usdt": self.cash, "equity_usdt": equity, "net_equity_increment_usdt": equity - self.initial_cash,
                "economic_equity_usdt": equity, "asset_equity_usdt": self.cash + gross_exposure,
                "asset_market_value_usdt": gross_exposure,
                "liquidation_value_usdt": liquidation, "immediately_executable_equity_usdt": self.cash + liquidation,
                "restricted_residual_value_usdt": economic_value - liquidation,
                "restricted_positions": restricted, "unrealized_pnl_usdt": economic_value - cost_basis,
                "gross_exposure_usdt": gross_exposure, "drawdown_fraction": 1 - equity / self.peak,
                "dust_positions": dust, "positions": copy.deepcopy(self.positions),
                "fee_usdt": sum((fill["fee_usdt"] for fill in self.fills), Decimal(0)),
                "valuation": "observed_bid_economic_value_net_of_model_costs; executable_liquidation_and_residual_separate"}

    def checkpoint(self):
        return serializable({"initial_cash": self.initial_cash, "cash": self.cash,
                             "fee_bps": self.fee * 10000, "slippage_bps": self.slippage * 10000,
                             "maximum_quote_age_seconds": self.maximum_age, "positions": self.positions,
                             "fills": self.fills, "closed_lots": self.closed_lots,
                             "peak": self.peak, "last_fill_ts": self.last_fill_ts})

    @classmethod
    def restore(cls, value):
        book = cls(initial_cash=value["initial_cash"], fee_bps=value["fee_bps"],
                   slippage_bps=value["slippage_bps"], maximum_quote_age_seconds=value["maximum_quote_age_seconds"])
        book.cash, book.peak = number(value["cash"]), number(value["peak"])
        book.positions = copy.deepcopy(value["positions"])
        for position in book.positions.values():
            for key in ("qty", "cash_cost", "entry_ts", "entry_fee_usdt", "entry_price", "highest_px"):
                if key in position:
                    position[key] = number(position[key])
        book.fills = copy.deepcopy(value["fills"])
        for fill in book.fills:
            for key in ("observed_at", "quote_ts", "price", "quantity", "notional_usdt", "fee_amount", "fee_usdt", "realized_pnl_usdt"):
                fill[key] = number(fill[key])
        book.closed_lots = copy.deepcopy(value["closed_lots"])
        for lot in book.closed_lots:
            for key in ("entry_ts", "exit_ts", "consumed_qty", "allocated_cost_usdt", "net_pnl_usdt"):
                lot[key] = number(lot[key])
        book.filled_ids = {fill["intent_id"] for fill in book.fills}
        book.last_fill_ts = number(value["last_fill_ts"]) if value["last_fill_ts"] is not None else None
        return book


def serializable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: serializable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [serializable(item) for item in value]
    return value
