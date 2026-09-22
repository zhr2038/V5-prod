"""Read-only execution-cost evidence from real fills and pre-submit order quotes.

This report never updates a strategy cost model or the exchange. A fill is not an
independent trade: partial fills are also counted by their linked order identity.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sqlite3
from typing import Any

SCHEMA = "v5.live_cost_evidence.v1"
MAX_QUOTE_AGE_MS = 10_000
MAX_FILL_DELAY_MS = 60_000


def number(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool) or str(value).strip() == "":
        return None
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def obj(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def iso_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def _expected(meta: dict) -> tuple[Decimal | None, str | None]:
    qlab = obj(meta.get("quant_lab"))
    for key in ("one_way_all_in_cost_bps",):
        value = number(qlab.get(key))
        if value is not None and value >= 0:
            return value, f"order.quant_lab.{key}"
    for key in ("roundtrip_all_in_cost_bps", "local_cost_bps"):
        value = number(qlab.get(key))
        if value is not None and value >= 0:
            return value / 2, f"order.quant_lab.{key}/2_symmetric_assumption"
    return None, None


def fill_evidence(fill: dict, order: dict | None) -> dict:
    """Positive shortfall is a cost, negative is price improvement; no abs()."""
    reasons: list[str] = []
    # main.py's pre-route private fills fetch uses a distinct provenance tag.
    if fill.get("source") not in {"fills", "fills_pre_route", "fills-history", "fills_history"}:
        reasons.append("UNVERIFIED_FILL_SOURCE")
    px, qty = number(fill.get("fill_px")), number(fill.get("fill_sz"))
    valid_fill = px is not None and qty is not None and px > 0 and qty > 0
    notional = px * qty if valid_fill else None
    if not valid_fill:
        reasons.append("INVALID_FILL_PRICE_OR_QUANTITY")
    symbol, side = str(fill.get("inst_id", "")), str(fill.get("side", "")).lower()
    if side not in {"buy", "sell"}:
        reasons.append("INVALID_FILL_SIDE")
    fee = number(fill.get("fee"))
    ccy = str(fill.get("fee_ccy") or "").upper()
    base, _, quote = symbol.partition("-")
    fee_cost = None
    if quote != "USDT":
        reasons.append("UNSUPPORTED_QUOTE_CURRENCY")
    elif fee is not None and ccy == "USDT":
        fee_cost = -fee
    elif fee is not None and ccy == base and valid_fill:
        fee_cost = -fee * px
    if fee_cost is None:
        reasons.append("FEE_CONVERSION_UNAVAILABLE")

    req = obj((order or {}).get("req_json"))
    meta = obj(req.get("_v5_order_meta"))
    submit_quote = obj(req.get("_meta"))
    mid = number(submit_quote.get("mid_px_at_submit"))
    bid, ask = number(submit_quote.get("bid")), number(submit_quote.get("ask"))
    quote_ms = number(submit_quote.get("ts_ms"))
    submit_ms = number((order or {}).get("created_ts"))
    fill_ms = number(fill.get("ts_ms"))
    if not order:
        reasons.append("ORDER_LINK_MISSING")
    elif (order.get("inst_id") != symbol or str(order.get("side")).lower() != side
          or (fill.get("cl_ord_id") and order.get("cl_ord_id")
              and str(fill["cl_ord_id"]) != str(order["cl_ord_id"]))
          or (fill.get("ord_id") and order.get("ord_id")
              and str(fill["ord_id"]) != str(order["ord_id"]))):
        reasons.append("ORDER_IDENTITY_MISMATCH")
    quote_ok = all(v is not None and v > 0 for v in (bid, ask, mid))
    if quote_ok:
        quote_ok = bid <= mid <= ask and bid < ask
    if not quote_ok:
        reasons.append("SUBMIT_QUOTE_UNAVAILABLE_OR_INVALID")
    timing_ok = all(v is not None and v > 0 for v in (quote_ms, submit_ms, fill_ms))
    if not timing_ok:
        reasons.append("QUOTE_OR_ORDER_TIMESTAMP_MISSING")
    elif not 0 <= submit_ms - quote_ms <= MAX_QUOTE_AGE_MS:
        reasons.append("SUBMIT_QUOTE_STALE_OR_FUTURE")
    if timing_ok and not 0 <= fill_ms - submit_ms <= MAX_FILL_DELAY_MS:
        reasons.append("FILL_OUTSIDE_IMMEDIATE_EXECUTION_WINDOW")
    if order and str(order.get("ord_type", "")).lower() not in {"market", "ioc", "fok"}:
        reasons.append("NON_IMMEDIATE_ORDER_TYPE")
    expected, expected_source = _expected(meta)
    eligible = not reasons
    signed_slip = None
    fee_bps = fee_cost / notional * 10000 if fee_cost is not None and notional else None
    if eligible:
        signed_slip = (px / mid - 1) * 10000 * (1 if side == "buy" else -1)
    cost_bps = fee_bps + signed_slip if signed_slip is not None and fee_bps is not None else None
    origin = ("cost_probe" if str(meta.get("execution_purpose", "")).lower() == "cost_probe"
              or meta.get("cost_probe_id") or obj(meta.get("cost_probe")) else "strategy")
    def val(value):
        return float(value) if value is not None else None
    return {
        "symbol": symbol, "side": side, "origin": origin,
        "trade_id": str(fill.get("trade_id") or ""),
        "order_id": str((order or {}).get("cl_ord_id") or (order or {}).get("ord_id") or ""),
        "fill_ts_ms": int(fill_ms) if fill_ms is not None else None,
        "notional_usdt": val(notional), "fill_price": val(px),
        "fee_cost_usdt": val(fee_cost), "fee_bps": val(fee_bps),
        "submit_bid": val(bid), "submit_ask": val(ask), "submit_mid": val(mid),
        "quote_ts_ms": int(quote_ms) if quote_ms is not None else None,
        "quote_age_at_submit_ms": val(submit_ms - quote_ms) if timing_ok else None,
        "fill_delay_ms": val(fill_ms - submit_ms) if timing_ok else None,
        "qualified_cost_observation": eligible,
        "signed_slippage_bps": val(signed_slip),
        "signed_slippage_usdt": val((px - mid) * qty * (1 if side == "buy" else -1)) if eligible else None,
        "observed_one_way_cost_bps": val(cost_bps),
        "expected_one_way_cost_bps": val(expected), "expected_cost_source": expected_source,
        "cost_error_bps": val(cost_bps - expected) if cost_bps is not None and expected is not None else None,
        "missing_reasons": reasons,
    }


def _connect_ro(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"cost evidence input missing: {path}")
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def build_live_cost_evidence(
    fills_path: Path, orders_path: Path, *, now: datetime, window_days: int = 30,
) -> dict:
    if now.tzinfo is None or not 1 <= window_days <= 90:
        raise ValueError("timezone-aware time and 1..90 day evidence window required")
    now = now.astimezone(timezone.utc)
    start = now - timedelta(days=window_days)
    with closing(_connect_ro(fills_path)) as fills_db, closing(_connect_ro(orders_path)) as orders_db:
        fills = fills_db.execute(
            "SELECT inst_id,trade_id,ord_id,cl_ord_id,side,ts_ms,fill_px,fill_sz,fee,fee_ccy,source "
            "FROM fills WHERE ts_ms>=? AND ts_ms<=? ORDER BY ts_ms,inst_id,trade_id LIMIT 100001",
            (int(start.timestamp() * 1000), int(now.timestamp() * 1000)),
        ).fetchall()
        if len(fills) > 100000:
            raise ValueError("cost evidence exceeds bounded 100000-fill window")
        rows, cache, seen = [], {}, set()
        for raw in fills:
            fill = dict(raw)
            identity = (fill["inst_id"], fill["trade_id"])
            if identity in seen:
                raise ValueError("duplicate exchange fill identity")
            seen.add(identity)
            key = (fill["inst_id"], fill["cl_ord_id"], fill["ord_id"])
            if key not in cache:
                matches = orders_db.execute(
                    "SELECT inst_id,cl_ord_id,ord_id,side,created_ts,ord_type,req_json FROM orders "
                    "WHERE inst_id=? AND ((?<>'' AND cl_ord_id=?) OR (?<>'' AND ord_id=?))",
                    (key[0], key[1] or "", key[1] or "", key[2] or "", key[2] or ""),
                ).fetchall()
                cache[key] = dict(matches[0]) if len(matches) == 1 else None
            rows.append(fill_evidence(fill, cache[key]))
    qualified = [row for row in rows if row["qualified_cost_observation"]]
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["symbol"], row["side"], row["origin"])].append(row)
    groups = []
    for (symbol, side, origin), values in sorted(grouped.items()):
        eligible = [row for row in values if row["qualified_cost_observation"]]
        weight = sum(row["notional_usdt"] for row in eligible)
        def weighted(field):
            return sum(row[field] * row["notional_usdt"] for row in eligible) / weight if weight else None
        groups.append({
            "symbol": symbol, "side": side, "origin": origin,
            "fill_count": len(values), "qualified_fill_count": len(eligible),
            "qualified_order_count": len({row["order_id"] for row in eligible}),
            "qualified_notional_usdt": weight,
            "mean_fee_bps": weighted("fee_bps"),
            "mean_signed_slippage_bps": weighted("signed_slippage_bps"),
            "mean_one_way_cost_bps": weighted("observed_one_way_cost_bps"),
        })
    return {
        "schema_version": SCHEMA, "status": "OBSERVED" if rows else "NO_FILLS",
        "generated_at_utc": now.isoformat(), "window_start_utc": start.isoformat(),
        "window_days": window_days, "source": "order_linked_exchange_fills",
        "live_order_effect": "none", "updates_live_cost_model": False,
        "calibration_status": "OBSERVATIONS_NOT_CALIBRATED_MODEL",
        "fill_count": len(rows), "qualified_fill_count": len(qualified),
        "qualified_order_count": len({(r["symbol"], r["order_id"]) for r in qualified}),
        "fee_known_fill_count": sum(r["fee_cost_usdt"] is not None for r in rows),
        "quote_coverage_fraction": len(qualified) / len(rows) if rows else None,
        "missing_reason_counts": dict(Counter(reason for r in rows for reason in r["missing_reasons"])),
        "groups": groups, "recent_fills": rows[-30:][::-1],
        "quote_max_age_at_submit_ms": MAX_QUOTE_AGE_MS,
        "maximum_fill_delay_ms": MAX_FILL_DELAY_MS,
        "interpretation": "one_way_fees_plus_signed_submit_mid_shortfall_not_roundtrip_pnl; "
        "partial_fills_are_not_independent_trades; unavailable_quotes_are_not_zero_cost; "
        "market_movement_and_execution_impact_are_not_separated",
    }
