"""Read-only strategy attribution against account inventory, including external bills.

Fills price trades; bills corroborate inventory and cash. Neither strategy CSVs
nor an old unmatched buy can override an observed account balance. Unknown cost
is retained explicitly. This module never writes an account or submits orders.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from decimal import Decimal, InvalidOperation
from pathlib import Path


ZERO = Decimal(0)
EPS = Decimal("1e-12")


def dec(value):
    if value is None or isinstance(value, bool):
        raise ValueError("missing_or_boolean_number")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid_number") from exc
    if not result.is_finite():
        raise ValueError("non_finite_number")
    return result


def obj(value):
    try:
        result = json.loads(value or "{}") if not isinstance(value, dict) else value
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def read_rows(path: Path, table: str):
    assert table in {"fills", "bills"}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as con:
        con.row_factory = sqlite3.Row
        return [dict(row) for row in con.execute(f"SELECT * FROM {table} ORDER BY ts_ms")]


def attribute_inventory(*, fills, bills, since_ms, allowed_symbols, metadata, eligible):
    by_symbol = {}
    symbols = {str(r["inst_id"]).replace("-", "/") for r in fills if str(r.get("inst_id", "")).endswith("-USDT")}
    if allowed_symbols is not None:
        symbols &= allowed_symbols
    by_clid, by_oid = metadata
    digest = hashlib.sha256(json.dumps([fills, bills], sort_keys=True, default=str).encode()).hexdigest()

    for symbol in sorted(symbols):
        base, quote = symbol.split("/")
        inst = symbol.replace("/", "-")
        relevant_fills = [r for r in fills if r["inst_id"] == inst]
        base_bills = [r for r in bills if r.get("ccy") == base]
        bill_index = {}
        for bill in bills:
            raw = obj(bill.get("raw_json"))
            key = (bill.get("inst_id"), str(raw.get("tradeId") or ""), bill.get("ccy"))
            if str(bill.get("type")) == "2" and key[1]:
                bill_index.setdefault(key, []).append(bill)
        events, used = [], set()
        for fill in relevant_fills:
            key = (inst, str(fill.get("trade_id") or ""))
            bases = bill_index.get((*key, base), [])
            quotes = bill_index.get((*key, quote), [])
            bill = bases[0] if len(bases) == 1 else None
            if bill is not None:
                used.add(bill["bill_id"])
            events.append({"ts": int(bill["ts_ms"] if bill else fill["ts_ms"]), "fill": fill,
                           "bill": bill, "quote_bill": quotes[0] if len(quotes) == 1 else None})
        events.extend({"ts": int(b["ts_ms"]), "bill": b} for b in base_bills if b["bill_id"] not in used)
        events.sort(key=lambda e: (e["ts"], str((e.get("bill") or {}).get("bill_id", ""))))
        lots, adjustments, issues, cycles = [], [], [], {}
        external_disposals = 0

        def issue(reason, ts):
            target = issues if ts >= since_ms else adjustments
            target.append({"ts_ms": ts, "reason": reason})

        for event in events:
            ts, bill, fill = event["ts"], event.get("bill"), event.get("fill")
            trade_ok, cash, px, is_strategy = False, ZERO, ZERO, False
            meta, delta = {}, ZERO
            if bill:
                try:
                    delta, after = dec(bill.get("bal_chg")), dec(bill.get("bal"))
                    before = after - delta
                    if min(before, after) < -EPS:
                        raise ValueError("negative_spot_balance")
                    total = sum((lot["qty"] for lot in lots), ZERO)
                    if abs(total - before) > EPS:
                        # Evidence boundary, not deletion of history: preserve every
                        # displaced lot/cost and represent the observed balance as unknown.
                        adjustments.append({"ts_ms": ts, "reason": "inventory_balance_boundary",
                                            "observed_before_qty": str(before),
                                            "prior_lots": [{k: str(v) for k, v in lot.items() if k in {"qty", "cost", "oid"}} for lot in lots]})
                        lots = ([{"qty": before, "cost": None, "px": ZERO, "ts": ts,
                                  "oid": "unknown", "eligible": False, "verified": False, "meta": {}}]
                                if before > ZERO else [])
                except ValueError as exc:
                    issue(str(exc), ts)
                    # A malformed anchor cannot establish a verified trade basis.
                    bill = None
            if fill:
                clid, oid = str(fill.get("cl_ord_id") or ""), str(fill.get("ord_id") or "")
                meta = dict(by_clid.get(clid) or {})
                meta.update(by_oid.get(oid) or {})
                is_strategy = (bool(meta) or clid.startswith("V5")) and eligible(meta | {"cl_ord_id": clid})
                try:
                    qty, px, fee = dec(fill.get("fill_sz")), dec(fill.get("fill_px")), dec(fill.get("fee"))
                    side, ccy = fill.get("side"), fill.get("fee_ccy")
                    if qty <= 0 or px <= 0 or side not in {"buy", "sell"}:
                        raise ValueError("invalid_fill")
                    if ccy not in {base, quote} and fee != 0:
                        raise ValueError("unsupported_fee_currency")
                    expected_delta = (qty if side == "buy" else -qty) + (fee if ccy == base else ZERO)
                    cash = (-qty * px if side == "buy" else qty * px) + (fee if ccy == quote else ZERO)
                    if bill is None:
                        delta = expected_delta
                        raise ValueError("missing_trade_bill")
                    cash_bill = event.get("quote_bill")
                    if cash_bill is None:
                        raise ValueError("missing_trade_bill")
                    fill_time = obj(fill.get("raw_json")).get("fillTime")
                    # Record-generation ts can lag execution. Compare the actual
                    # fillTime only when supplied by both matched exchange records.
                    times_conflict = any(
                        fill_time and obj(b.get("raw_json")).get("fillTime")
                        and dec(fill_time) != dec(obj(b.get("raw_json"))["fillTime"])
                        for b in (bill, cash_bill))
                    if (str(bill.get("ord_id") or "") != oid or str(cash_bill.get("ord_id") or "") != oid
                            or times_conflict
                            or abs(delta - expected_delta) > EPS or abs(dec(cash_bill.get("bal_chg")) - cash) > EPS):
                        raise ValueError("fill_bill_mismatch")
                    if (side == "buy" and (delta <= 0 or cash >= 0)) or (side == "sell" and (delta >= 0 or cash <= 0)):
                        raise ValueError("invalid_trade_cash_direction")
                    trade_ok = True
                except ValueError as exc:
                    if is_strategy:
                        issue(str(exc), ts)
            if delta > ZERO:
                lots.append({"qty": delta, "cost": -cash if trade_ok and cash < 0 else None, "px": px,
                             "ts": ts, "oid": str((fill or {}).get("ord_id") or "unknown"),
                             "eligible": is_strategy, "verified": trade_ok, "meta": meta})
                continue
            if delta >= ZERO:
                continue
            remaining, disposed = -delta, -delta
            if not is_strategy:
                external_disposals += 1
            while remaining > ZERO and lots:
                lot = lots[0]
                take = min(lot["qty"], remaining)
                fraction = take / lot["qty"]
                cost = lot["cost"] * fraction if lot["cost"] is not None else None
                if fill and is_strategy and ts >= since_ms:
                    if trade_ok and lot["verified"] and lot["eligible"] and cost is not None:
                        entry_id, exit_id = lot["oid"], str(fill.get("ord_id") or fill["trade_id"])
                        key = (entry_id, exit_id)
                        cycle = cycles.setdefault(key, {"entry_order_id": entry_id, "exit_order_id": exit_id,
                            "entry_ts_ms": lot["ts"], "exit_ts_ms": ts, "qty": ZERO, "cost": ZERO,
                            "gross_pnl": ZERO, "net_pnl": ZERO, "meta": dict(lot["meta"]) | meta})
                        cycle["entry_ts_ms"] = min(cycle["entry_ts_ms"], lot["ts"])
                        cycle["exit_ts_ms"] = max(cycle["exit_ts_ms"], ts)
                        cycle["qty"] += take
                        cycle["cost"] += cost
                        cycle["gross_pnl"] += (px - lot["px"]) * take
                        cycle["net_pnl"] += cash * take / disposed - cost
                    else:
                        issue("unverified_or_non_strategy_entry", ts)
                remaining -= take
                lot["qty"] -= take
                if cost is not None:
                    lot["cost"] -= cost
                if lot["qty"] <= ZERO:
                    lots.pop(0)
            if remaining > ZERO and is_strategy:
                issue("missing_entry_inventory", ts)
        by_symbol[symbol] = {
            "cycles": list(cycles.values()), "issues": issues, "inventory_adjustments": adjustments,
            "inventory_remaining_qty": str(sum((lot["qty"] for lot in lots), ZERO)),
            "inventory_remaining_known_cost_usdt": str(sum((lot["cost"] or ZERO for lot in lots), ZERO)),
            "inventory_unknown_cost_qty": str(sum((lot["qty"] for lot in lots if lot["cost"] is None), ZERO)),
            "external_inventory_disposals": external_disposals, "input_sha256": digest,
        }
    return by_symbol
