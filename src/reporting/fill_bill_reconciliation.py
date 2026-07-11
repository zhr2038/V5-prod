from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


FILL_BILL_RECONCILIATION_SCHEMA_VERSION = "v5.fill_bill_cost_reconciliation.v1"
FILL_BILL_RECONCILIATION_FIELDS = (
    "schema_version",
    "generated_at",
    "runtime_scope",
    "symbol",
    "order_leg",
    "side",
    "order_id",
    "cl_ord_id",
    "trade_ids",
    "fill_count",
    "first_fill_ts",
    "last_fill_ts",
    "liquidity_role",
    "fill_notional_usdt",
    "fee_currency",
    "fill_fee_raw",
    "fill_fee_usdt",
    "bill_ids",
    "bill_fee_usdt",
    "selected_fee_usdt",
    "rebate_usdt",
    "fee_diff_usdt",
    "fee_missing_count",
    "bill_delay_seconds",
    "bill_match_status",
    "cost_evidence_status",
    "cost_source",
    "fee_complete",
)


def reconcile_runtime_cost_databases(
    reports_dir: str | Path,
    *,
    max_fills_per_database: int = 20_000,
) -> list[dict[str, Any]]:
    reports = Path(reports_dir).resolve()
    fill_paths = sorted(
        {path.resolve() for path in reports.rglob("*fills*.sqlite") if path.is_file()},
        key=lambda path: str(path),
    )
    rows: list[dict[str, Any]] = []
    for fill_path in fill_paths:
        bill_path = _companion_bills_path(fill_path)
        rows.extend(
            build_fill_bill_cost_reconciliation(
                fill_path,
                bill_path,
                runtime_scope=_runtime_scope(reports, fill_path),
                max_fills=max_fills_per_database,
            )
        )
    rows.sort(
        key=lambda row: (
            str(row.get("last_fill_ts") or ""),
            str(row.get("runtime_scope") or ""),
            str(row.get("order_id") or row.get("cl_ord_id") or ""),
        ),
        reverse=True,
    )
    return rows


def build_fill_bill_cost_reconciliation(
    fill_store_path: str | Path,
    bills_store_path: str | Path,
    *,
    runtime_scope: str = "default",
    max_fills: int = 20_000,
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    fill_path = Path(fill_store_path).resolve()
    bill_path = Path(bills_store_path).resolve()
    fills = _read_recent_rows(fill_path, "fills", max_rows=max_fills)
    if not fills:
        return []
    min_ts = min(_as_int(row.get("ts_ms")) or 0 for row in fills)
    max_ts = max(_as_int(row.get("ts_ms")) or 0 for row in fills)
    bills = _read_recent_rows(
        bill_path,
        "bills",
        max_rows=max(max_fills * 4, 1_000),
        min_ts_ms=max(0, min_ts - 15 * 60 * 1_000),
        max_ts_ms=max_ts + 15 * 60 * 1_000,
    )
    stamp = (generated_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    generated = stamp.isoformat().replace("+00:00", "Z")
    return [
        _reconcile_fill_group(
            group,
            bills,
            generated_at=generated,
            runtime_scope=runtime_scope,
        )
        for group in _group_fills(fills)
    ]


def _read_recent_rows(
    path: Path,
    table: str,
    *,
    max_rows: int,
    min_ts_ms: int | None = None,
    max_ts_ms: int | None = None,
) -> list[dict[str, Any]]:
    if not path.is_file() or max_rows <= 0:
        return []
    try:
        uri = f"{path.as_uri()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                return []
            if min_ts_ms is not None and max_ts_ms is not None:
                result = connection.execute(
                    f"SELECT * FROM {table} WHERE ts_ms>=? AND ts_ms<=? "
                    "ORDER BY ts_ms DESC LIMIT ?",
                    (int(min_ts_ms), int(max_ts_ms), int(max_rows)),
                ).fetchall()
            else:
                result = connection.execute(
                    f"SELECT * FROM {table} ORDER BY ts_ms DESC LIMIT ?",
                    (int(max_rows),),
                ).fetchall()
    except (OSError, sqlite3.Error):
        return []
    return [dict(row) for row in reversed(result)]


def _group_fills(rows: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        symbol = str(item.get("inst_id") or "").upper()
        identity = str(
            item.get("ord_id")
            or item.get("cl_ord_id")
            or item.get("trade_id")
            or item.get("ts_ms")
            or ""
        )
        grouped.setdefault((symbol, identity), []).append(item)
    return list(grouped.values())


def _reconcile_fill_group(
    fills: Sequence[Mapping[str, Any]],
    bills: Sequence[Mapping[str, Any]],
    *,
    generated_at: str,
    runtime_scope: str,
) -> dict[str, Any]:
    symbol = str(fills[0].get("inst_id") or "").upper()
    side = str(fills[0].get("side") or "").lower()
    order_id = _first_text(row.get("ord_id") for row in fills)
    cl_ord_id = _first_text(row.get("cl_ord_id") for row in fills)
    trade_ids = sorted({str(row.get("trade_id") or "").strip() for row in fills} - {""})
    first_ts = min(_as_int(row.get("ts_ms")) or 0 for row in fills)
    last_ts = max(_as_int(row.get("ts_ms")) or 0 for row in fills)
    avg_price, quantity, notional = _fill_price_quantity_notional(fills)
    fee_by_currency: dict[str, float] = {}
    fee_missing_count = 0
    for fill in fills:
        fee = _as_float(fill.get("fee"))
        fee_ccy = str(fill.get("fee_ccy") or "").upper()
        if fee is None or not fee_ccy:
            fee_missing_count += 1
            continue
        fee_by_currency[fee_ccy] = fee_by_currency.get(fee_ccy, 0.0) + fee
    fill_fee_usdt = _fee_map_cost_usdt(fee_by_currency, symbol=symbol, price=avg_price)
    if fee_by_currency and fill_fee_usdt is None:
        fee_missing_count += 1

    matching_bills, bill_fee_usdt = _match_bills(
        fills=fills,
        bills=bills,
        symbol=symbol,
        side=side,
        price=avg_price,
        quantity=quantity,
        notional=notional,
        expected_fill_fee_usdt=fill_fee_usdt,
    )
    fee_diff = (
        abs(fill_fee_usdt - bill_fee_usdt)
        if fill_fee_usdt is not None and bill_fee_usdt is not None
        else None
    )
    status, evidence, source = _cost_status(
        fill_fee_usdt=fill_fee_usdt,
        bill_fee_usdt=bill_fee_usdt,
        fee_diff_usdt=fee_diff,
        fee_missing_count=fee_missing_count,
    )
    selected_fee = _selected_fee(
        status=status,
        fill_fee_usdt=fill_fee_usdt,
        bill_fee_usdt=bill_fee_usdt,
    )
    bill_timestamps = [_as_int(row.get("ts_ms")) for row in matching_bills]
    observed_bill_timestamps = [value for value in bill_timestamps if value is not None]
    bill_delay = (
        max(observed_bill_timestamps) / 1_000.0 - last_ts / 1_000.0
        if observed_bill_timestamps and last_ts > 0
        else None
    )
    roles = {_liquidity_role(row.get("exec_type")) for row in fills}
    roles.discard("unknown")
    liquidity_role = (
        next(iter(roles)) if len(roles) == 1 else ("mixed" if roles else "unknown")
    )
    fee_currency = ";".join(sorted(fee_by_currency))
    fill_fee_raw = json.dumps(fee_by_currency, sort_keys=True, separators=(",", ":"))
    bill_ids = sorted(
        {str(row.get("bill_id") or "").strip() for row in matching_bills} - {""}
    )
    return {
        "schema_version": FILL_BILL_RECONCILIATION_SCHEMA_VERSION,
        "generated_at": generated_at,
        "runtime_scope": runtime_scope,
        "symbol": symbol,
        "order_leg": _order_leg(side),
        "side": side,
        "order_id": order_id,
        "cl_ord_id": cl_ord_id,
        "trade_ids": ";".join(trade_ids),
        "fill_count": len(fills),
        "first_fill_ts": _iso_from_ms(first_ts),
        "last_fill_ts": _iso_from_ms(last_ts),
        "liquidity_role": liquidity_role,
        "fill_notional_usdt": notional,
        "fee_currency": fee_currency,
        "fill_fee_raw": fill_fee_raw if fee_by_currency else None,
        "fill_fee_usdt": fill_fee_usdt,
        "bill_ids": ";".join(bill_ids),
        "bill_fee_usdt": bill_fee_usdt,
        "selected_fee_usdt": selected_fee,
        "rebate_usdt": max(-(selected_fee or 0.0), 0.0)
        if selected_fee is not None
        else None,
        "fee_diff_usdt": fee_diff,
        "fee_missing_count": fee_missing_count,
        "bill_delay_seconds": bill_delay,
        "bill_match_status": status,
        "cost_evidence_status": evidence,
        "cost_source": source,
        "fee_complete": status == "PASS",
    }


def _match_bills(
    *,
    fills: Sequence[Mapping[str, Any]],
    bills: Sequence[Mapping[str, Any]],
    symbol: str,
    side: str,
    price: float | None,
    quantity: float | None,
    notional: float | None,
    expected_fill_fee_usdt: float | None,
) -> tuple[list[dict[str, Any]], float | None]:
    fill_ids = _identifiers(fills)
    direct = [dict(row) for row in bills if _identifiers([row]).intersection(fill_ids)]
    direct_fees = [
        value
        for value in (
            _direct_bill_fee_cost_usdt(row, symbol=symbol, price=price)
            for row in direct
        )
        if value is not None
    ]
    if direct_fees:
        return direct, sum(direct_fees)

    candidates = direct or [
        dict(row) for row in bills if _within_seconds(row, fills[-1], seconds=15 * 60)
    ]
    inferred: list[tuple[float, dict[str, Any], float]] = []
    for bill in candidates:
        value = _ledger_fee_cost_usdt(
            bill,
            symbol=symbol,
            side=side,
            price=price,
            quantity=quantity,
            notional=notional,
        )
        if value is None:
            continue
        if expected_fill_fee_usdt is not None:
            tolerance = max(0.000001, abs(expected_fill_fee_usdt) * 0.02)
            if abs(value - expected_fill_fee_usdt) > tolerance:
                continue
        distance = _time_distance_seconds(bill, fills[-1])
        if distance is not None:
            inferred.append((distance, bill, value))
    if not inferred:
        return [], None
    inferred.sort(key=lambda item: item[0])
    _, bill, value = inferred[0]
    return [bill], value


def _direct_bill_fee_cost_usdt(
    row: Mapping[str, Any],
    *,
    symbol: str,
    price: float | None,
) -> float | None:
    payload = _payload(row)
    fee_present = any(key in payload for key in ("fee", "feeUsdt", "fee_usdt"))
    if not fee_present:
        return None
    fee_usdt = _as_float(payload.get("feeUsdt") or payload.get("fee_usdt"))
    if fee_usdt is not None:
        return -fee_usdt
    fee = _as_float(payload.get("fee"))
    currency = str(
        payload.get("feeCcy") or payload.get("fee_ccy") or row.get("ccy") or ""
    )
    return _fee_cost_usdt(fee, currency, symbol=symbol, price=price)


def _ledger_fee_cost_usdt(
    row: Mapping[str, Any],
    *,
    symbol: str,
    side: str,
    price: float | None,
    quantity: float | None,
    notional: float | None,
) -> float | None:
    if price is None or quantity is None or notional is None or notional <= 0:
        return None
    payload = _payload(row)
    amount = _as_float(
        row.get("bal_chg") or payload.get("balChg") or payload.get("amount")
    )
    currency = str(row.get("ccy") or payload.get("ccy") or "").upper()
    if amount is None or not currency:
        return None
    base, _, quote = symbol.partition("-")
    if currency == base:
        if side == "buy" and 0 < amount <= quantity:
            return max((quantity - amount) * price, 0.0)
        if side == "sell" and amount < 0 and abs(amount) >= quantity:
            return max((abs(amount) - quantity) * price, 0.0)
    if currency in {quote, "USDT", "USDC", "USD"}:
        if side == "sell" and 0 < amount <= notional:
            return max(notional - amount, 0.0)
        if side == "buy" and amount < 0 and abs(amount) >= notional:
            return max(abs(amount) - notional, 0.0)
    return None


def _cost_status(
    *,
    fill_fee_usdt: float | None,
    bill_fee_usdt: float | None,
    fee_diff_usdt: float | None,
    fee_missing_count: int,
) -> tuple[str, str, str]:
    if fill_fee_usdt is None and bill_fee_usdt is None:
        return "FEE_MISSING", "PARTIAL", "fee_missing"
    if bill_fee_usdt is None:
        return "BILL_PENDING", "PARTIAL", "actual_fill_partial"
    if fill_fee_usdt is None or fee_missing_count > 0:
        return "FILL_FEE_MISSING", "PARTIAL", "actual_bill_partial"
    tolerance = max(0.000001, abs(fill_fee_usdt) * 0.02)
    if fee_diff_usdt is not None and fee_diff_usdt <= tolerance:
        return "PASS", "ACTUAL", "actual_fills_bills"
    return "FEE_MISMATCH", "MIXED", "mixed_actual_fee_mismatch"


def _selected_fee(
    *,
    status: str,
    fill_fee_usdt: float | None,
    bill_fee_usdt: float | None,
) -> float | None:
    if status == "PASS":
        return bill_fee_usdt
    observed = [value for value in (fill_fee_usdt, bill_fee_usdt) if value is not None]
    return max(observed) if observed else None


def _fill_price_quantity_notional(
    fills: Sequence[Mapping[str, Any]],
) -> tuple[float | None, float | None, float | None]:
    quantity = 0.0
    weighted = 0.0
    for fill in fills:
        price = _as_float(fill.get("fill_px"))
        size = _as_float(fill.get("fill_sz"))
        if price is None or size is None or size <= 0:
            continue
        quantity += size
        weighted += price * size
    if quantity <= 0:
        return None, None, None
    price = weighted / quantity
    return price, quantity, abs(price * quantity)


def _fee_map_cost_usdt(
    fees: Mapping[str, float],
    *,
    symbol: str,
    price: float | None,
) -> float | None:
    if not fees:
        return None
    values = [
        _fee_cost_usdt(value, currency, symbol=symbol, price=price)
        for currency, value in fees.items()
    ]
    if any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None)


def _fee_cost_usdt(
    fee: float | None,
    currency: str,
    *,
    symbol: str,
    price: float | None,
) -> float | None:
    if fee is None:
        return None
    ccy = str(currency or "").upper()
    base = symbol.split("-", 1)[0]
    if ccy in {"USDT", "USDC", "USD"}:
        return -fee
    if ccy == base and price is not None:
        return -fee * price
    return None


def _identifiers(rows: Iterable[Mapping[str, Any]]) -> set[str]:
    values: set[str] = set()
    for row in rows:
        payload = _payload(row)
        for key in (
            "ord_id",
            "ordId",
            "cl_ord_id",
            "clOrdId",
            "trade_id",
            "tradeId",
        ):
            value = row.get(key) if key in row else payload.get(key)
            text = str(value or "").strip()
            if text:
                values.add(text)
    return values


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_json")
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _liquidity_role(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"m", "maker", "post_only"}:
        return "maker"
    if normalized in {"t", "taker", "market"}:
        return "taker"
    return "unknown"


def _order_leg(side: str) -> str:
    if side == "buy":
        return "entry"
    if side == "sell":
        return "exit"
    return "unknown"


def _within_seconds(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    seconds: int,
) -> bool:
    distance = _time_distance_seconds(left, right)
    return distance is not None and distance <= seconds


def _time_distance_seconds(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> float | None:
    left_ts = _as_int(left.get("ts_ms"))
    right_ts = _as_int(right.get("ts_ms"))
    if left_ts is None or right_ts is None:
        return None
    return abs(left_ts - right_ts) / 1_000.0


def _companion_bills_path(fill_path: Path) -> Path:
    if fill_path.name == "fills.sqlite":
        return fill_path.with_name("bills.sqlite")
    return fill_path.with_name(fill_path.name.replace("fills", "bills", 1))


def _runtime_scope(reports: Path, fill_path: Path) -> str:
    try:
        relative = fill_path.relative_to(reports).as_posix()
    except ValueError:
        relative = fill_path.name
    return relative.removesuffix(".sqlite")


def _first_text(values: Iterable[Any]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _as_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() in {"", "null", "None"}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() in {"", "null", "None"}:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _iso_from_ms(value: int) -> str | None:
    if value <= 0:
        return None
    return (
        datetime.fromtimestamp(value / 1_000.0, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
