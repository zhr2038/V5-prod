from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback for local checks.
    fcntl = None  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.execution.account_store import AccountStore  # noqa: E402
from src.execution.fill_store import derive_position_store_path  # noqa: E402
from src.execution.position_store import PositionStore  # noqa: E402
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds  # noqa: E402
from src.reporting.cost_probe_plan import CostProbeEngine  # noqa: E402


AUTHORIZATION_SCOPE = "v5_cost_probe_live_once"
AUTHORIZATION_MAX_TTL_SEC = 300
AUTHORIZATION_CLOCK_SKEW_SEC = 30
AUTHORIZATION_HMAC_SECRET_ENV = "V5_COST_PROBE_AUTH_HMAC_SECRET"
AUTHORIZATION_OPERATOR_ALLOWLIST_ENV = "V5_COST_PROBE_AUTH_OPERATORS"
EXECUTION_MAINTENANCE_LOCK_PATH_ENV = "V5_EXECUTION_MAINTENANCE_LOCK"
GLOBAL_PROBE_LOCK_PATH_ENV = "V5_COST_PROBE_LIVE_LOCK_PATH"
GLOBAL_PROBE_LOCK_PATH = "/tmp/v5_execution_maintenance.lock"
AUTHORIZATION_SIGNATURE_FIELDS = (
    "scope",
    "authorization_id",
    "nonce",
    "code_sha",
    "config_sha256",
    "signed_by",
    "approved_live_order_execution",
    "symbol",
    "max_notional_usdt",
    "issued_at",
    "expires_at",
    "acknowledged_risks",
)
EMERGENCY_FLATTEN_MAX_RETRIES = 3
EMERGENCY_FLATTEN_MAX_SLIPPAGE_BPS = Decimal("100")
REQUIRED_ACKS = {
    "one_time_live_cost_probe",
    "immediate_flat_exit",
    "max_open_seconds_60",
    "kill_switch_on_error",
    "sell_only_on_error",
}


def build_live_probe_preflight(
    cfg: Any,
    *,
    reports_dir: str | Path,
    auth_path: str | Path,
    okx: Any,
    project_root: str | Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    engine = CostProbeEngine(
        cfg,
        reports_dir=reports_dir,
        generated_at=generated_at,
        project_root=project_root,
    )
    payload = engine.build()
    p3 = dict(payload["p3_preflight"])
    auth = _read_authorization(auth_path)
    symbol = str(
        p3.get("manual_probe_symbol") or auth.get("symbol") or _single_configured_symbol(cfg) or ""
    ).strip()
    instrument = _instrument_preflight(okx, symbol, max_notional_usdt=float(p3.get("max_notional_usdt") or 0.0))
    instrument = _with_baseline_base_balance(okx, symbol=symbol, instrument=instrument)
    p3 = _apply_exchange_min_notional_preflight(
        payload=payload,
        p3=p3,
        instrument=instrument,
        symbol=symbol,
    )
    auth_context = _authorization_context(
        cfg,
        p3=p3,
        project_root=project_root,
    )
    auth_blockers = _authorization_blockers(
        auth,
        p3,
        generated_at=engine.generated_at,
        required_context=auth_context,
    )
    blockers = [
        *list(p3.get("blockers") or []),
        *auth_blockers,
        *list(instrument.get("blockers") or []),
    ]
    blockers = sorted(set(blockers))
    return {
        "generated_at": engine.generated_at.isoformat().replace("+00:00", "Z"),
        "state": "READY_FOR_LIVE_EXECUTION" if not blockers else "NOT_READY",
        "approved_live_order_execution": bool(auth.get("approved_live_order_execution")) and not blockers,
        "manual_probe_symbol": symbol,
        "live_order_effect": "none_preflight_only_no_order",
        "blockers": blockers,
        "p3_preflight": p3,
        "authorization": _redacted_authorization(auth),
        "authorization_canonical_payload": _canonical_authorization_payload(auth) if auth else "",
        "authorization_signature_sha256": _authorization_signature_sha256(auth),
        "required_authorization": auth_context,
        "instrument_preflight": instrument,
        "runtime_paths": _runtime_paths(payload),
    }


def run_live_probe_once(
    cfg: Any,
    *,
    reports_dir: str | Path,
    auth_path: str | Path,
    okx: Any,
    project_root: str | Path = PROJECT_ROOT,
    execute_live_order: bool = False,
    operator_confirmed: bool = False,
) -> dict[str, Any]:
    preflight = build_live_probe_preflight(
        cfg,
        reports_dir=reports_dir,
        auth_path=auth_path,
        okx=okx,
        project_root=project_root,
    )
    if preflight["state"] != "READY_FOR_LIVE_EXECUTION":
        return preflight
    if not (execute_live_order and operator_confirmed):
        return {
            **preflight,
            "state": "READY_FOR_OPERATOR_CONFIRMATION",
            "approved_live_order_execution": False,
            "live_order_effect": "none_operator_confirmation_missing_no_order",
        }
    try:
        with _global_probe_execution_lock():
            return _execute_live_probe(
                cfg,
                preflight=preflight,
                okx=okx,
                reports_dir=Path(reports_dir),
                auth_path=auth_path,
                project_root=project_root,
            )
    except RuntimeError as exc:
        blockers = sorted(set([*list(preflight.get("blockers") or []), str(exc)]))
        return {
            **preflight,
            "state": "NOT_READY",
            "approved_live_order_execution": False,
            "live_order_effect": "none_global_probe_lock_unavailable_no_order",
            "blockers": blockers,
        }


def _execute_live_probe(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    reports_dir: Path,
    auth_path: str | Path,
    project_root: str | Path,
) -> dict[str, Any]:
    locked_preflight = build_live_probe_preflight(
        cfg,
        reports_dir=reports_dir,
        auth_path=auth_path,
        okx=okx,
        project_root=project_root,
    )
    if locked_preflight["state"] != "READY_FOR_LIVE_EXECUTION":
        return {
            **locked_preflight,
            "state": "NOT_READY",
            "approved_live_order_execution": False,
            "live_order_effect": "none_runtime_revalidation_failed_no_order",
            "authorization_consumed": False,
            "lock_revalidated": True,
        }
    preflight = locked_preflight
    symbol = str(preflight["manual_probe_symbol"])
    inst_id = symbol.replace("/", "-").upper()
    instrument = dict(preflight["instrument_preflight"])
    instrument = _authorized_order_plan(
        instrument,
        auth=preflight.get("authorization") or {},
        required_context=preflight.get("required_authorization") or {},
    )
    if instrument.get("blockers"):
        blockers = sorted(set([*list(preflight.get("blockers") or []), *list(instrument.get("blockers") or [])]))
        return {
            **preflight,
            "state": "NOT_READY",
            "approved_live_order_execution": False,
            "live_order_effect": "none_authorized_order_plan_invalid_no_order",
            "blockers": blockers,
            "instrument_preflight": instrument,
        }
    plan = instrument["order_plan"]
    max_open_seconds = int(preflight["p3_preflight"].get("max_open_seconds") or 60)
    reports_dir.mkdir(parents=True, exist_ok=True)
    order_events_path = reports_dir / "cost_probe_order_events.jsonl"
    roundtrip_events_path = reports_dir / "cost_probe_roundtrip_events.jsonl"
    try:
        consumed_auth = _consume_authorization_file(
            auth_path,
            preflight=preflight,
            cfg=cfg,
            project_root=project_root,
        )
    except Exception as exc:
        blockers = sorted(set([*list(preflight.get("blockers") or []), str(exc)]))
        return {
            **preflight,
            "state": "NOT_READY",
            "approved_live_order_execution": False,
            "live_order_effect": "none_authorization_consume_failed_no_order",
            "blockers": blockers,
            "authorization_consumed": False,
        }
    clid_prefix = _client_order_prefix(consumed_auth)
    entry_payload = {
        "instId": inst_id,
        "tdMode": "cash",
        "side": "buy",
        "ordType": "ioc",
        "px": plan["entry_px"],
        "sz": plan["base_qty"],
        "clOrdId": f"{clid_prefix}E",
    }
    entry_state: dict[str, Any] = {}
    exit_state: dict[str, Any] = {}
    entry_ord_id = ""
    exit_ord_id = ""
    entry_cl_ord_id = entry_payload["clOrdId"]
    entry_order_attempted = False
    try:
        entry_order_attempted = True
        entry = okx.place_order(entry_payload, exp_time_ms=1500)
        entry_data = _accepted_okx_item(entry, action="entry_order")
        entry_ord_id = str(entry_data.get("ordId") or "")
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="entry",
                status="submitted",
                cl_ord_id=entry_payload["clOrdId"],
                ord_id=entry_ord_id,
                payload=entry_payload,
                authorization=consumed_auth,
                instrument=instrument,
            ),
        )
        entry_state = _poll_order(okx, inst_id=inst_id, cl_ord_id=entry_payload["clOrdId"], max_seconds=max_open_seconds)
        entry_state = _with_order_fills(
            okx,
            inst_id=inst_id,
            row=entry_state,
            ord_id=str(entry_state.get("ordId") or entry_ord_id),
            cl_ord_id=entry_payload["clOrdId"],
        )
        filled_qty_dec = _filled_qty(entry_state)
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="entry",
                status=str(entry_state.get("state") or "unknown"),
                cl_ord_id=entry_payload["clOrdId"],
                ord_id=str(entry_state.get("ordId") or entry_ord_id),
                payload=entry_state,
                authorization=consumed_auth,
                instrument=instrument,
            ),
        )
        if filled_qty_dec <= Decimal("0"):
            return _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="no_entry_fill",
                entry_order_id=entry_ord_id,
                exit_order_id="",
                entry_state=entry_state,
                exit_state={},
                instrument=instrument,
                flat_verification=_verify_roundtrip_flat(
                    cfg,
                    preflight=preflight,
                    okx=okx,
                    inst_id=inst_id,
                    symbol=symbol,
                    instrument=instrument,
                    entry_state=entry_state,
                    exit_state={},
                ),
                completed=False,
                authorization=consumed_auth,
            )
        available_before_exit = _query_base_balance(okx, symbol)
        exit_qty, unsellable_dust = _normal_exit_qty(
            entry_qty=filled_qty_dec,
            available_base_balance=available_before_exit,
            instrument=instrument,
        )
        entry_state["_available_base_before_exit"] = (
            _decimal_text(available_before_exit) if available_before_exit is not None else "unverified"
        )
        entry_state["_baseline_base_balance"] = _decimal_text(_baseline_base_balance(instrument))
        entry_state["_planned_exit_qty"] = _decimal_text(exit_qty)
        entry_state["_unsellable_dust_qty"] = _decimal_text(unsellable_dust)
        if exit_qty <= Decimal("0"):
            _write_kill_switch(cfg, reason="cost_probe_no_sellable_exit_qty")
            emergency = _safe_emergency_flatten_cost_probe(
                okx,
                symbol=symbol,
                inst_id=inst_id,
                instrument=instrument,
                order_events_path=order_events_path,
                fallback_qty=filled_qty_dec,
                reason="no_sellable_exit_qty",
                clid_prefix=clid_prefix,
            )
            flat = _verify_roundtrip_flat(
                cfg,
                preflight=preflight,
                okx=okx,
                inst_id=inst_id,
                symbol=symbol,
                instrument=instrument,
                entry_state=entry_state,
                exit_state={},
                emergency=emergency,
            )
            return _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="no_sellable_exit_qty",
                entry_order_id=entry_ord_id,
                exit_order_id="",
                entry_state=entry_state,
                exit_state={},
                instrument=instrument,
                flat_verification=flat,
                emergency=emergency,
                completed=False,
                authorization=consumed_auth,
            )
        exit_payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "ioc",
            "px": plan["exit_px"],
            "sz": _decimal_text(exit_qty),
            "clOrdId": f"{clid_prefix}X",
        }
        exit_resp = okx.place_order(exit_payload, exp_time_ms=1500)
        exit_data = _accepted_okx_item(exit_resp, action="exit_order")
        exit_ord_id = str(exit_data.get("ordId") or "")
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="exit",
                status="submitted",
                cl_ord_id=exit_payload["clOrdId"],
                ord_id=exit_ord_id,
                payload=exit_payload,
                authorization=consumed_auth,
                instrument=instrument,
            ),
        )
        exit_state = _poll_order(okx, inst_id=inst_id, cl_ord_id=exit_payload["clOrdId"], max_seconds=max_open_seconds)
        exit_state = _with_order_fills(
            okx,
            inst_id=inst_id,
            row=exit_state,
            ord_id=str(exit_state.get("ordId") or exit_ord_id),
            cl_ord_id=exit_payload["clOrdId"],
        )
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="exit",
                status=str(exit_state.get("state") or "unknown"),
                cl_ord_id=exit_payload["clOrdId"],
                ord_id=str(exit_state.get("ordId") or exit_ord_id),
                payload=exit_state,
                authorization=consumed_auth,
                instrument=instrument,
            ),
        )
        flat = _verify_roundtrip_flat(
            cfg,
            preflight=preflight,
            okx=okx,
            inst_id=inst_id,
            symbol=symbol,
            instrument=instrument,
            entry_state=entry_state,
            exit_state=exit_state,
        )
        if not flat["flat_verified"]:
            _write_kill_switch(cfg, reason="cost_probe_incomplete_exit")
            emergency = _safe_emergency_flatten_cost_probe(
                okx,
                symbol=symbol,
                inst_id=inst_id,
                instrument=instrument,
                order_events_path=order_events_path,
                fallback_qty=_filled_qty(entry_state) - _filled_qty(exit_state),
                reason="incomplete_exit",
                clid_prefix=clid_prefix,
            )
            flat = _verify_roundtrip_flat(
                cfg,
                preflight=preflight,
                okx=okx,
                inst_id=inst_id,
                symbol=symbol,
                instrument=instrument,
                entry_state=entry_state,
                exit_state=exit_state,
                emergency=emergency,
            )
            return _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="incomplete_exit",
                entry_order_id=entry_ord_id,
                exit_order_id=exit_ord_id,
                entry_state=entry_state,
                exit_state=exit_state,
                instrument=instrument,
                flat_verification=flat,
                emergency=emergency,
                completed=False,
                authorization=consumed_auth,
            )
        return _finish_roundtrip(
            roundtrip_events_path,
            symbol=symbol,
            status="closed",
            entry_order_id=entry_ord_id,
            exit_order_id=exit_ord_id,
            entry_state=entry_state,
            exit_state=exit_state,
            instrument=instrument,
            flat_verification=flat,
            completed=True,
            authorization=consumed_auth,
        )
    except Exception as exc:
        emergency: dict[str, Any] = {}
        kill_switch_written = False
        if entry_order_attempted:
            _write_kill_switch(cfg, reason=f"cost_probe_live_once_error:{exc}")
            kill_switch_written = True
            if entry_cl_ord_id and not entry_state:
                entry_state = _recover_order_state(
                    okx,
                    inst_id=inst_id,
                    cl_ord_id=entry_cl_ord_id,
                    ord_id=entry_ord_id,
                )
            cancel_result = _cancel_probe_order(
                okx,
                inst_id=inst_id,
                cl_ord_id=entry_cl_ord_id,
                ord_id=entry_ord_id,
            )
            fallback_qty = max(
                _filled_qty(entry_state),
                _probe_balance_delta(okx, symbol=symbol, instrument=instrument),
                Decimal("0"),
            )
            emergency = _safe_emergency_flatten_cost_probe(
                okx,
                symbol=symbol,
                inst_id=inst_id,
                instrument=instrument,
                order_events_path=order_events_path,
                fallback_qty=fallback_qty,
                reason="exception_after_entry_attempt",
                clid_prefix=clid_prefix,
            )
            emergency["entry_cancel"] = cancel_result
            flat = _verify_roundtrip_flat(
                cfg,
                preflight=preflight,
                okx=okx,
                inst_id=inst_id,
                symbol=symbol,
                instrument=instrument,
                entry_state=entry_state,
                exit_state=exit_state,
                emergency=emergency,
            )
            _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="exception_after_entry_attempt",
                entry_order_id=entry_ord_id,
                exit_order_id=exit_ord_id,
                entry_state=entry_state,
                exit_state=exit_state,
                instrument=instrument,
                flat_verification=flat,
                emergency=emergency,
                completed=False,
                authorization=consumed_auth,
            )
        if not kill_switch_written:
            _write_kill_switch(cfg, reason=f"cost_probe_live_once_error:{exc}")
        return {
            "state": "ABORTED_KILL_SWITCH_ENABLED",
            "live_order_effect": "kill_switch_enabled_after_cost_probe_error",
            "error": str(exc),
            "emergency_flatten": emergency,
        }


def _instrument_preflight(okx: Any, symbol: str, *, max_notional_usdt: float) -> dict[str, Any]:
    inst_id = symbol.replace("/", "-").upper()
    blockers: list[str] = []
    spec = _public_okx_item(okx, "/api/v5/public/instruments", {"instType": "SPOT", "instId": inst_id})
    ticker = _public_okx_item(okx, "/api/v5/market/ticker", {"instId": inst_id})
    min_sz = Decimal(str(spec.get("minSz") or "0"))
    lot_sz = Decimal(str(spec.get("lotSz") or "0"))
    tick_sz = Decimal(str(spec.get("tickSz") or "0"))
    ask = Decimal(str(ticker.get("askPx") or ticker.get("last") or "0"))
    bid = Decimal(str(ticker.get("bidPx") or ticker.get("last") or "0"))
    instrument_state = str(spec.get("state") or "").strip().lower()
    quote_balance = _query_quote_balance(okx, symbol)
    max_notional = Decimal(str(max_notional_usdt))
    fee_reserve = max(max_notional * Decimal("0.01"), Decimal("0.01")) if max_notional > 0 else Decimal("0")
    quote_required = max_notional + fee_reserve
    if instrument_state != "live":
        blockers.append("instrument_state_missing" if not instrument_state else "instrument_state_not_live")
    if quote_balance is None:
        blockers.append("quote_balance_unverified")
    elif max_notional > 0 and quote_balance < quote_required:
        blockers.append("quote_balance_insufficient_for_authorized_notional")
    if min_sz <= 0:
        blockers.append("exchange_min_sz_missing")
    if lot_sz <= 0:
        blockers.append("exchange_lot_sz_missing")
    if tick_sz <= 0:
        blockers.append("exchange_tick_sz_missing")
    if ask <= 0 or bid <= 0:
        blockers.append("ticker_bid_ask_missing")
    order_plan: dict[str, str] = {}
    if not blockers:
        entry_px = _round_to_step(ask * Decimal("1.001"), tick_sz, ROUND_UP)
        exit_px = _round_to_step(bid * Decimal("0.999"), tick_sz, ROUND_DOWN)
        qty = _round_to_step(max_notional / entry_px, lot_sz, ROUND_DOWN)
        if qty < min_sz:
            blockers.append("max_notional_below_exchange_min_size")
        if qty * entry_px > max_notional:
            blockers.append("planned_order_exceeds_max_notional")
        order_plan = {
            "base_qty": _decimal_text(qty),
            "entry_px": _decimal_text(entry_px),
            "exit_px": _decimal_text(exit_px),
            "estimated_entry_notional_usdt": _decimal_text(qty * entry_px),
        }
    return {
        "inst_id": inst_id,
        "blockers": sorted(set(blockers)),
        "min_sz": _decimal_text(min_sz),
        "lot_sz": _decimal_text(lot_sz),
        "tick_sz": _decimal_text(tick_sz),
        "ask_px": _decimal_text(ask),
        "bid_px": _decimal_text(bid),
        "instrument_state": instrument_state or "missing",
        "quote_balance": _decimal_text(quote_balance) if quote_balance is not None else "unverified",
        "quote_required": _decimal_text(quote_required),
        "quote_fee_reserve": _decimal_text(fee_reserve),
        "order_plan": order_plan,
        "instrument_preflight_passed": not blockers,
        "normalized_qty": order_plan.get("base_qty", ""),
    }


def _apply_exchange_min_notional_preflight(
    *,
    payload: dict[str, Any],
    p3: dict[str, Any],
    instrument: dict[str, Any],
    symbol: str,
) -> dict[str, Any]:
    if instrument.get("blockers") or not symbol:
        return p3
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    rows = payload.get("plan_rows") if isinstance(payload.get("plan_rows"), list) else []
    configured_symbols = [str(row.get("symbol") or "") for row in rows]
    symbol_rows = [row for row in rows if str(row.get("symbol") or "") == symbol]
    if len(configured_symbols) != 1 or not symbol_rows:
        return p3
    blocked_reasons = {
        reason
        for row in symbol_rows
        for reason in str(row.get("blocked_reasons") or "").split(";")
        if reason
    }
    if blocked_reasons != {"exchange_min_notional_check_pending"}:
        return p3
    if summary.get("runtime_blockers") or summary.get("symbol_runtime_blockers"):
        return p3
    blockers = [
        blocker
        for blocker in list(p3.get("blockers") or [])
        if blocker
        not in {
            "dry_run_plan_not_ready",
            "single_symbol_plan_required",
            "exchange_min_notional_check_pending",
        }
    ]
    out = dict(p3)
    out["blockers"] = sorted(set(blockers))
    out["state"] = "READY_FOR_MANUAL_AUTHORIZATION" if not blockers else "NOT_READY"
    out["ready_to_request_manual_live_probe"] = not blockers
    out["manual_probe_symbol"] = symbol
    out["planned_symbols"] = [symbol]
    out["instrument_preflight_passed"] = True
    out["exchange_min_notional_verified"] = True
    out["exchange_min_notional_source"] = "okx_public_instruments_and_ticker"
    out["min_sz"] = instrument.get("min_sz", "")
    out["lot_sz"] = instrument.get("lot_sz", "")
    out["tick_sz"] = instrument.get("tick_sz", "")
    out["normalized_qty"] = instrument.get("normalized_qty", "")
    out["dry_run_plan_state"] = "DRY_RUN_PLAN_READY_AFTER_EXCHANGE_PREFLIGHT"
    return out


def _with_baseline_base_balance(okx: Any, *, symbol: str, instrument: dict[str, Any]) -> dict[str, Any]:
    out = dict(instrument)
    blockers = list(out.get("blockers") or [])
    balance = _query_base_balance(okx, symbol) if symbol else None
    if balance is None:
        blockers.append("baseline_base_balance_unverified")
        out["baseline_base_balance"] = "unverified"
    else:
        out["baseline_base_balance"] = _decimal_text(balance)
    out["blockers"] = sorted(set(blockers))
    out["instrument_preflight_passed"] = not out["blockers"]
    return out


def _authorized_order_plan(
    instrument: dict[str, Any],
    *,
    auth: dict[str, Any],
    required_context: dict[str, Any],
) -> dict[str, Any]:
    out = dict(instrument)
    blockers = list(out.get("blockers") or [])
    plan = dict(out.get("order_plan") if isinstance(out.get("order_plan"), dict) else {})
    auth_notional = _decimal_or_none(auth.get("max_notional_usdt"))
    preflight_notional = _decimal_or_none(required_context.get("max_notional_usdt"))
    entry_px = _decimal(plan.get("entry_px"))
    lot_sz = _decimal(out.get("lot_sz"))
    min_sz = _decimal(out.get("min_sz"))
    if auth_notional is None or auth_notional <= 0:
        blockers.append("manual_authorization_notional_missing")
    if preflight_notional is None or preflight_notional <= 0:
        blockers.append("preflight_max_notional_missing")
    if entry_px <= 0 or lot_sz <= 0 or min_sz <= 0:
        blockers.append("authorized_order_exchange_plan_invalid")
    if blockers:
        out["blockers"] = sorted(set(blockers))
        return out

    max_notional = min(auth_notional or Decimal("0"), preflight_notional or Decimal("0"))
    qty = _round_to_step(max_notional / entry_px, lot_sz, ROUND_DOWN)
    actual_notional = qty * entry_px
    if qty < min_sz:
        blockers.append("authorized_notional_below_exchange_min_size")
    if actual_notional > (auth_notional or Decimal("0")):
        blockers.append("authorized_order_exceeds_manual_authorization")
    if blockers:
        out["blockers"] = sorted(set(blockers))
        out["authorized_order_plan"] = {
            "authorized_max_notional_usdt": _decimal_text(max_notional),
            "base_qty": _decimal_text(qty),
            "estimated_entry_notional_usdt": _decimal_text(actual_notional),
        }
        return out

    plan.update(
        {
            "base_qty": _decimal_text(qty),
            "authorized_max_notional_usdt": _decimal_text(max_notional),
            "manual_authorization_max_notional_usdt": _decimal_text(auth_notional),
            "preflight_max_notional_usdt": _decimal_text(preflight_notional),
            "estimated_entry_notional_usdt": _decimal_text(actual_notional),
        }
    )
    out["order_plan"] = plan
    out["authorized_order_plan"] = dict(plan)
    out["normalized_qty"] = plan["base_qty"]
    out["blockers"] = []
    return out


def _normal_exit_qty(
    *,
    entry_qty: Decimal,
    available_base_balance: Decimal | None,
    instrument: dict[str, Any],
) -> tuple[Decimal, Decimal]:
    lot_sz = _decimal(instrument.get("lot_sz"))
    baseline = _baseline_base_balance(instrument)
    if available_base_balance is None:
        sellable_delta = entry_qty
    else:
        sellable_delta = max(available_base_balance - baseline, Decimal("0"))
    target = min(entry_qty, sellable_delta)
    exit_qty = _round_to_step(target, lot_sz, ROUND_DOWN) if lot_sz > 0 else target
    dust = max(entry_qty - exit_qty, Decimal("0"))
    return exit_qty, dust


def _baseline_base_balance(instrument: dict[str, Any]) -> Decimal:
    value = instrument.get("baseline_base_balance")
    return Decimal("0") if str(value) == "unverified" else _decimal(value)


def _probe_balance_delta(okx: Any, *, symbol: str, instrument: dict[str, Any]) -> Decimal:
    balance = _query_base_balance(okx, symbol)
    if balance is None:
        return Decimal("0")
    return max(balance - _baseline_base_balance(instrument), Decimal("0"))


def _authorization_blockers(
    auth: dict[str, Any],
    p3: dict[str, Any],
    *,
    generated_at: datetime,
    required_context: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not auth:
        return ["manual_authorization_file_missing_or_invalid"]
    if str(auth.get("scope") or "") != AUTHORIZATION_SCOPE:
        blockers.append("manual_authorization_scope_invalid")
    if auth.get("approved_live_order_execution") is not True:
        blockers.append("manual_authorization_not_approved")
    if str(auth.get("symbol") or "") != str(p3.get("manual_probe_symbol") or ""):
        blockers.append("manual_authorization_symbol_mismatch")
    auth_notional = _decimal_or_none(auth.get("max_notional_usdt"))
    if auth_notional is None:
        blockers.append("manual_authorization_notional_missing")
    elif auth_notional > _decimal(required_context.get("max_notional_usdt")):
        blockers.append("manual_authorization_notional_exceeds_preflight")
    issued = _parse_dt(auth.get("issued_at"))
    expires = _parse_dt(auth.get("expires_at"))
    now = generated_at.astimezone(UTC)
    skew = timedelta(seconds=AUTHORIZATION_CLOCK_SKEW_SEC)
    if issued is None:
        blockers.append("manual_authorization_issued_at_missing_or_invalid")
    if expires is None or expires <= now:
        blockers.append("manual_authorization_expired_or_invalid")
    if issued is not None:
        if now + skew < issued:
            blockers.append("manual_authorization_issued_at_in_future")
        if now - issued > timedelta(seconds=AUTHORIZATION_MAX_TTL_SEC):
            blockers.append("manual_authorization_issued_at_too_old")
        if expires is not None and expires - issued > timedelta(seconds=AUTHORIZATION_MAX_TTL_SEC):
            blockers.append("manual_authorization_ttl_exceeds_5_minutes")
    if not str(auth.get("authorization_id") or "").strip():
        blockers.append("manual_authorization_id_missing")
    if not str(auth.get("nonce") or "").strip():
        blockers.append("manual_authorization_nonce_missing")
    if not str(auth.get("signed_by") or "").strip():
        blockers.append("manual_authorization_signed_by_missing")
    if not str(auth.get("signature") or "").strip():
        blockers.append("manual_authorization_signature_missing")
    else:
        blockers.extend(_authorization_signature_blockers(auth))
    if str(auth.get("code_sha") or "") != str(required_context.get("code_sha") or ""):
        blockers.append("manual_authorization_code_sha_mismatch")
    if str(auth.get("config_sha256") or "") != str(required_context.get("config_sha256") or ""):
        blockers.append("manual_authorization_config_sha_mismatch")
    if str(auth.get("consumed_at") or "").strip():
        blockers.append("manual_authorization_already_consumed")
    acks = {str(item) for item in (auth.get("acknowledged_risks") or [])}
    missing_acks = sorted(REQUIRED_ACKS - acks)
    if missing_acks:
        blockers.append("manual_authorization_acknowledgements_missing")
    return blockers


def _authorization_context(
    cfg: Any,
    *,
    p3: dict[str, Any],
    project_root: str | Path,
) -> dict[str, Any]:
    return {
        "scope": AUTHORIZATION_SCOPE,
        "symbol": str(p3.get("manual_probe_symbol") or ""),
        "max_notional_usdt": _decimal_text(p3.get("max_notional_usdt") or 0),
        "code_sha": _current_code_sha(Path(project_root)),
        "config_sha256": _cost_probe_config_sha(cfg),
        "authorization_max_ttl_sec": AUTHORIZATION_MAX_TTL_SEC,
        "authorization_clock_skew_sec": AUTHORIZATION_CLOCK_SKEW_SEC,
        "signature_algorithm": "hmac-sha256",
        "operator_allowlist_env": AUTHORIZATION_OPERATOR_ALLOWLIST_ENV,
        "required_pending_file_suffix": ".pending.json",
    }


def _authorization_signature_blockers(auth: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    signed_by = str(auth.get("signed_by") or "").strip()
    allowed = {
        item.strip()
        for item in str(os.environ.get(AUTHORIZATION_OPERATOR_ALLOWLIST_ENV) or "").split(",")
        if item.strip()
    }
    if not allowed:
        blockers.append("manual_authorization_operator_allowlist_missing")
    elif signed_by not in allowed:
        blockers.append("manual_authorization_signed_by_not_allowed")
    secret = os.environ.get(AUTHORIZATION_HMAC_SECRET_ENV)
    if not secret:
        blockers.append("manual_authorization_signature_secret_missing")
    actual = str(auth.get("signature") or "").strip()
    if not actual or not secret:
        return blockers
    expected = _authorization_hmac_signature(auth, secret)
    expected_hex = expected.split(":", 1)[1]
    if not (
        hmac.compare_digest(actual, expected)
        or hmac.compare_digest(actual, expected_hex)
    ):
        blockers.append("manual_authorization_signature_invalid")
    return blockers


def _canonical_authorization_payload(auth: dict[str, Any]) -> str:
    payload: dict[str, Any] = {}
    for field in AUTHORIZATION_SIGNATURE_FIELDS:
        value = auth.get(field)
        if field == "acknowledged_risks":
            payload[field] = sorted(str(item) for item in (value or []))
        elif field == "max_notional_usdt":
            payload[field] = _decimal_text(value)
        else:
            payload[field] = str(value or "")
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _authorization_hmac_signature(auth: dict[str, Any], secret: str) -> str:
    digest = hmac.new(
        secret.encode("utf-8"),
        _canonical_authorization_payload(auth).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"hmac-sha256:{digest}"


def _authorization_signature_sha256(auth: dict[str, Any]) -> str:
    signature = str((auth or {}).get("signature") or "").strip()
    return hashlib.sha256(signature.encode("utf-8")).hexdigest() if signature else ""


def _consume_authorization_file(
    path: str | Path,
    *,
    preflight: dict[str, Any],
    cfg: Any,
    project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    source = Path(path)
    if not source.name.endswith(".pending.json"):
        raise RuntimeError("manual_authorization_pending_file_required")
    lock_path = Path(str(source) + ".lock")
    fd: int | None = None
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        if not source.is_file():
            raise RuntimeError("manual_authorization_pending_file_missing")
        fresh = _read_authorization(source)
        if not fresh:
            raise RuntimeError("manual_authorization_file_missing_or_invalid")
        expected = preflight.get("authorization") or {}
        if str(fresh.get("authorization_id") or "") != str(expected.get("authorization_id") or ""):
            raise RuntimeError("manual_authorization_changed_after_preflight")
        fresh_canonical = _canonical_authorization_payload(fresh)
        expected_canonical = str(preflight.get("authorization_canonical_payload") or "")
        if not expected_canonical or fresh_canonical != expected_canonical:
            raise RuntimeError("manual_authorization_changed_after_preflight")
        if _authorization_signature_sha256(fresh) != str(preflight.get("authorization_signature_sha256") or ""):
            raise RuntimeError("manual_authorization_signature_changed_after_preflight")
        current_context = _authorization_context(
            cfg,
            p3=dict(preflight.get("p3_preflight") or {}),
            project_root=project_root,
        )
        blockers = _authorization_blockers(
            fresh,
            dict(preflight.get("p3_preflight") or {}),
            generated_at=datetime.now(UTC),
            required_context=current_context,
        )
        if blockers:
            raise RuntimeError(f"manual_authorization_revalidation_failed:{';'.join(sorted(set(blockers)))}")
        consumed = source.with_name(source.name[: -len(".pending.json")] + ".consumed.json")
        if consumed.exists():
            raise RuntimeError("manual_authorization_consumed_file_exists")
        source.replace(consumed)
        fresh["consumed_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        consumed.write_text(json.dumps(fresh, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return fresh
    finally:
        if fd is not None:
            os.close(fd)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def _read_authorization(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _redacted_authorization(auth: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in auth.items()
        if key not in {"signature", "secret", "token", "api_key"}
    }


class _GlobalProbeExecutionLock:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.fd: int | None = None
        self._owns_exclusive_file = False

    def __enter__(self) -> "_GlobalProbeExecutionLock":
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if fcntl is None:
                self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self._owns_exclusive_file = True
            else:
                self.fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR)
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise RuntimeError("cost_probe_global_execution_lock_held") from exc
                os.ftruncate(self.fd, 0)
            os.write(
                self.fd,
                f"{os.getpid()} {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}\n".encode("utf-8"),
            )
        except FileExistsError as exc:
            raise RuntimeError("cost_probe_global_execution_lock_held") from exc
        except OSError as exc:
            raise RuntimeError(f"cost_probe_global_execution_lock_unavailable:{exc}") from exc
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self.fd is not None:
            if fcntl is not None:
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            os.close(self.fd)
            self.fd = None
        if self._owns_exclusive_file:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass


def _global_probe_execution_lock() -> _GlobalProbeExecutionLock:
    return _GlobalProbeExecutionLock(
        os.environ.get(EXECUTION_MAINTENANCE_LOCK_PATH_ENV)
        or os.environ.get(GLOBAL_PROBE_LOCK_PATH_ENV)
        or GLOBAL_PROBE_LOCK_PATH
    )


def _client_order_prefix(auth: dict[str, Any]) -> str:
    nonce = f"{auth.get('authorization_id') or ''}:{auth.get('nonce') or ''}"
    digest = hashlib.sha256(nonce.encode("utf-8")).hexdigest()[:10]
    stamp = datetime.now(UTC).strftime("%m%d%H%M")
    return f"cp{digest}{stamp}"


def _runtime_paths(payload: dict[str, Any]) -> dict[str, str]:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return {
        "order_store_path": str(summary.get("order_store_path") or ""),
        "position_store_path": str(summary.get("position_store_path") or ""),
        "fill_store_path": str(summary.get("fill_store_path") or ""),
        "reconcile_status_path": str(summary.get("reconcile_status_path") or ""),
    }


def _single_configured_symbol(cfg: Any) -> str:
    execution = getattr(cfg, "execution", cfg)
    symbols = []
    for raw in getattr(execution, "cost_probe_symbols", None) or []:
        symbol = str(raw or "").strip().upper().replace("-", "/")
        if symbol and "/" not in symbol and symbol.endswith("USDT"):
            symbol = f"{symbol[:-4]}/USDT"
        if symbol:
            symbols.append(symbol)
    return symbols[0] if len(symbols) == 1 else ""


def _current_code_sha(project_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(project_root),
            text=True,
            capture_output=True,
            timeout=5,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            sha = proc.stdout.strip()
            dirty = subprocess.run(
                ["git", "diff", "--quiet"],
                cwd=str(project_root),
                text=True,
                capture_output=True,
                timeout=5,
            )
            return f"{sha}+dirty" if dirty.returncode else sha
    except Exception:
        pass
    data = Path(__file__).read_bytes()
    return "file-sha256:" + hashlib.sha256(data).hexdigest()


def _cost_probe_config_sha(cfg: Any) -> str:
    execution = getattr(cfg, "execution", cfg)
    keys = (
        "cost_bootstrap_enabled",
        "cost_probe_enabled",
        "cost_probe_dry_run",
        "cost_probe_live_enabled",
        "cost_probe_symbols",
        "cost_probe_max_notional_usdt",
        "cost_probe_use_exchange_min_notional",
        "cost_probe_max_orders_per_day",
        "cost_probe_max_roundtrips_per_symbol_per_day",
        "cost_probe_cooldown_minutes",
        "cost_probe_max_daily_loss_usdt",
        "cost_probe_order_style",
        "cost_probe_exit_policy",
        "cost_probe_max_open_seconds",
        "cost_probe_require_reconcile_clean",
        "cost_probe_require_no_existing_position",
        "cost_probe_respect_kill_switch",
        "cost_probe_disable_if_order_store_dirty",
        "cost_probe_disable_if_position_state_dirty",
        "order_store_path",
        "kill_switch_path",
        "reconcile_status_path",
    )
    payload = {key: getattr(execution, key, None) for key in keys}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _public_okx_item(okx: Any, path: str, params: dict[str, Any]) -> dict[str, Any]:
    response = okx.request("GET", path, params=params)
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    if isinstance(rows, list) and rows:
        row = rows[0]
        return row if isinstance(row, dict) else {}
    return {}


def _first_okx_item(response: Any) -> dict[str, Any]:
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows[0]
    return {}


def _accepted_okx_item(response: Any, *, action: str) -> dict[str, Any]:
    data = getattr(response, "data", response)
    top_code = str(data.get("code") or "") if isinstance(data, dict) else ""
    top_msg = str(data.get("msg") or data.get("error") or "") if isinstance(data, dict) else ""
    row = _first_okx_item(response)
    if not row:
        raise RuntimeError(f"{action}_rejected:code={top_code or 'missing'}:sCode=missing:no_order_response")
    s_code = str(row.get("sCode") or "0")
    if top_code != "0" or s_code != "0":
        s_msg = str(row.get("sMsg") or top_msg or "unknown")
        raise RuntimeError(f"{action}_rejected:code={top_code or 'missing'}:sCode={s_code or 'missing'}:{s_msg}")
    return row


def _poll_order(okx: Any, *, inst_id: str, cl_ord_id: str, max_seconds: int) -> dict[str, Any]:
    # The production client performs one signed read per poll; tests inject a fake.
    for _ in range(max(int(max_seconds), 1)):
        response = okx.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
        row = _first_okx_item(response)
        state = str(row.get("state") or "").lower()
        if state in {"filled", "canceled", "partially_filled", "partially-filled"}:
            return row
        time.sleep(1.0)
    return {"clOrdId": cl_ord_id, "state": "timeout"}


def _recover_order_state(okx: Any, *, inst_id: str, cl_ord_id: str, ord_id: str) -> dict[str, Any]:
    try:
        row = _poll_order(okx, inst_id=inst_id, cl_ord_id=cl_ord_id, max_seconds=1)
        return _with_order_fills(okx, inst_id=inst_id, row=row, ord_id=ord_id or str(row.get("ordId") or ""), cl_ord_id=cl_ord_id)
    except Exception as exc:
        return {"clOrdId": cl_ord_id, "ordId": ord_id, "state": "unknown_after_recovery_error", "error": str(exc)}


def _cancel_probe_order(okx: Any, *, inst_id: str, cl_ord_id: str, ord_id: str) -> dict[str, Any]:
    payload = {"instId": inst_id}
    if ord_id:
        payload["ordId"] = ord_id
    if cl_ord_id:
        payload["clOrdId"] = cl_ord_id
    if not (ord_id or cl_ord_id):
        return {"attempted": False, "reason": "order_id_unavailable"}
    try:
        if hasattr(okx, "cancel_order"):
            response = okx.cancel_order(inst_id=inst_id, ord_id=ord_id or None, cl_ord_id=cl_ord_id or None)
        else:
            response = okx.request("POST", "/api/v5/trade/cancel-order", json_body=payload)
        return {"attempted": True, "payload": payload, "response": getattr(response, "data", response)}
    except Exception as exc:
        return {"attempted": True, "payload": payload, "error": str(exc)}


def _with_order_fills(
    okx: Any,
    *,
    inst_id: str,
    row: dict[str, Any],
    ord_id: str,
    cl_ord_id: str,
) -> dict[str, Any]:
    out = dict(row)
    fills = _fetch_order_fills(okx, inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
    if fills:
        out["_fills"] = fills
    return out


def _fetch_order_fills(okx: Any, *, inst_id: str, ord_id: str, cl_ord_id: str) -> list[dict[str, Any]]:
    try:
        if hasattr(okx, "get_order_fills"):
            response = okx.get_order_fills(inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
        else:
            response = okx.request(
                "GET",
                "/api/v5/trade/fills",
                params={"instId": inst_id, "ordId": ord_id} if ord_id else {"instId": inst_id},
            )
    except Exception:
        return []
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _finish_roundtrip(
    path: Path,
    *,
    symbol: str,
    status: str,
    entry_order_id: str,
    exit_order_id: str,
    entry_state: dict[str, Any],
    exit_state: dict[str, Any],
    instrument: dict[str, Any],
    flat_verification: dict[str, Any],
    completed: bool,
    authorization: dict[str, Any],
    emergency: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cost = _roundtrip_cost_fields(entry_state, exit_state, symbol)
    flat_verified = bool(flat_verification.get("flat_verified"))
    exchange_flat_verified = bool(flat_verification.get("exchange_flat_verified"))
    local_flat_verified = bool(flat_verification.get("local_flat_verified"))
    reconcile_ok = bool(flat_verification.get("reconcile_ok"))
    cost_evidence_complete = cost["cost_evidence_complete"] == "true"
    eligible_for_cost_model = (
        bool(completed)
        and flat_verified
        and exchange_flat_verified
        and local_flat_verified
        and reconcile_ok
        and cost_evidence_complete
    )
    row = {
        "event_type": f"roundtrip:{status}",
        "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "symbol": symbol,
        "roundtrip_status": status,
        "completed": bool(completed),
        "execution_completed": bool(completed),
        "cost_evidence_complete": cost_evidence_complete,
        "eligible_for_cost_model": eligible_for_cost_model,
        "eligible_for_alpha_pnl": False,
        "eligible_for_live_cost_coverage": False,
        "sample_origin": "cost_probe",
        "source": "bootstrap_cost_probe",
        "roundtrip_id": f"{entry_order_id}:{exit_order_id}",
        "entry_order_id": entry_order_id,
        "exit_order_id": exit_order_id,
        "no_order_submitted": False,
        "live_order_effect": "live_cost_probe_roundtrip" if completed else "live_cost_probe_roundtrip_incomplete",
        "authorization_id": str(authorization.get("authorization_id") or ""),
        "nonce": str(authorization.get("nonce") or ""),
        "entry_state": entry_state,
        "exit_state": exit_state,
        "entry_filled_qty": cost["entry_filled_qty"],
        "exit_filled_qty": cost["exit_filled_qty"],
        "entry_avg_px": cost["entry_avg_px"],
        "exit_avg_px": cost["exit_avg_px"],
        "entry_fee": cost["entry_fee"],
        "entry_fee_ccy": cost["entry_fee_ccy"],
        "entry_fee_usdt": cost["entry_fee_usdt"],
        "exit_fee": cost["exit_fee"],
        "exit_fee_ccy": cost["exit_fee_ccy"],
        "exit_fee_usdt": cost["exit_fee_usdt"],
        "fee_conversion_warnings": cost["fee_conversion_warnings"],
        "gross_pnl_usdt": cost["gross_pnl_usdt"],
        "net_pnl_usdt": cost["net_pnl_usdt"],
        "roundtrip_cost_bps": cost["roundtrip_cost_bps"],
        "arrival_bid_px": instrument.get("bid_px", ""),
        "arrival_ask_px": instrument.get("ask_px", ""),
        "arrival_mid_px": _arrival_mid_px(instrument),
        "flat_verification": flat_verification,
        "flat_verified": flat_verified,
        "local_flat_verified": local_flat_verified,
        "exchange_flat_verified": exchange_flat_verified,
        "reconcile_ok": reconcile_ok,
        "open_order_count": flat_verification.get("open_order_count", -1),
        "emergency_flatten": emergency or {},
    }
    _append_jsonl(path, row)
    return {"state": "COMPLETED" if completed else "INCOMPLETE", **row}


def _event(
    *,
    symbol: str,
    leg: str,
    status: str,
    cl_ord_id: str,
    ord_id: str,
    payload: dict[str, Any],
    authorization: dict[str, Any] | None = None,
    instrument: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ts = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    cost = _order_cost_fields(payload)
    return {
        "event_type": f"order:{leg}:{status}",
        "event_ts": ts,
        "event_id": f"{cl_ord_id}|order:{leg}:{status}|{ts}",
        "order_key": cl_ord_id or ord_id,
        "symbol": symbol,
        "leg": leg,
        "order_status": status,
        "client_order_id": cl_ord_id,
        "exchange_order_id": ord_id,
        "no_order_submitted": False,
        "live_order_effect": "live_cost_probe_order",
        "authorization_id": str((authorization or {}).get("authorization_id") or ""),
        "filled_qty": cost["filled_qty"],
        "avg_px": cost["avg_px"],
        "fee": cost["fee"],
        "fee_ccy": cost["fee_ccy"],
        "fee_usdt": cost["fee_usdt"],
        "arrival_bid_px": (instrument or {}).get("bid_px", ""),
        "arrival_ask_px": (instrument or {}).get("ask_px", ""),
        "arrival_mid_px": _arrival_mid_px(instrument or {}),
        "raw": payload,
    }


def _verify_roundtrip_flat(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    inst_id: str,
    symbol: str,
    instrument: dict[str, Any],
    entry_state: dict[str, Any],
    exit_state: dict[str, Any],
    emergency: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lot_sz = _decimal(instrument.get("lot_sz"))
    tolerance = max(lot_sz, Decimal("0.00000001"))
    entry_qty = _filled_qty(entry_state)
    exit_qty = _filled_qty(exit_state)
    emergency_qty = _decimal((emergency or {}).get("filled_qty"))
    total_exit_qty = exit_qty + max(emergency_qty, Decimal("0"))
    baseline_balance = _baseline_base_balance(instrument)
    exchange_balance = _query_base_balance(okx, symbol)
    open_order_count = _open_order_count(okx, inst_id=inst_id)
    local_qty = _local_position_qty(cfg, preflight=preflight, symbol=symbol)
    reconcile_refresh = _refresh_reconcile_status(cfg, preflight=preflight, okx=okx, symbol=symbol)
    reconcile = _read_reconcile_status(preflight)
    exit_fully_filled = total_exit_qty + tolerance >= entry_qty
    exchange_delta = None if exchange_balance is None else exchange_balance - baseline_balance
    exchange_flat = exchange_delta is not None and abs(exchange_delta) <= tolerance
    local_flat = local_qty is not None and local_qty <= tolerance
    open_orders_clear = open_order_count == 0
    raw_reconcile_ok = bool(reconcile.get("ok"))
    reconcile_probe_dust_accepted = (
        not raw_reconcile_ok
        and bool(exchange_flat)
        and _reconcile_probe_dust_accepted(
            reconcile,
            symbol=symbol,
            base_tolerance=tolerance,
            quote_tolerance=Decimal(str(getattr(getattr(cfg, "execution", cfg), "reconcile_abs_usdt_tol", 1.0) or 1.0)),
        )
    )
    reconcile_ok = raw_reconcile_ok or reconcile_probe_dust_accepted
    return {
        "flat_verified": bool(exit_fully_filled and exchange_flat and local_flat and open_orders_clear and reconcile_ok),
        "exit_fully_filled": bool(exit_fully_filled),
        "entry_filled_qty": _decimal_text(entry_qty),
        "exit_filled_qty": _decimal_text(exit_qty),
        "emergency_exit_filled_qty": _decimal_text(emergency_qty),
        "lot_sz_tolerance": _decimal_text(tolerance),
        "open_order_count": open_order_count,
        "open_orders_clear": open_orders_clear,
        "baseline_base_balance": _decimal_text(baseline_balance),
        "exchange_base_balance": _decimal_text(exchange_balance) if exchange_balance is not None else "unverified",
        "exchange_base_delta_from_baseline": _decimal_text(exchange_delta) if exchange_delta is not None else "unverified",
        "exchange_flat_verified": bool(exchange_flat),
        "local_position_qty": _decimal_text(local_qty) if local_qty is not None else "unverified",
        "local_flat_verified": bool(local_flat),
        "reconcile_ok": reconcile_ok,
        "raw_reconcile_ok": raw_reconcile_ok,
        "reconcile_probe_dust_accepted": bool(reconcile_probe_dust_accepted),
        "reconcile_refreshed": reconcile_refresh.get("refreshed", False),
        "reconcile_refresh": reconcile_refresh,
        "reconcile_status": reconcile,
    }


def _reconcile_probe_dust_accepted(
    reconcile: dict[str, Any],
    *,
    symbol: str,
    base_tolerance: Decimal,
    quote_tolerance: Decimal,
) -> bool:
    reason = str(reconcile.get("reason") or "").strip().lower()
    if reason not in {"probe_dust_only", "below_lot_size_residual"}:
        return False
    rendered = json.dumps(reconcile, ensure_ascii=False).lower()
    hard_fail_fragments = ("network_error", "parse_error", "stale", "missing", "unreadable", "invalid")
    if any(fragment in rendered for fragment in hard_fail_fragments):
        return False
    return _reconcile_all_deltas_within_tolerance(
        reconcile,
        symbol=symbol,
        base_tolerance=base_tolerance,
        quote_tolerance=quote_tolerance,
    )


def _reconcile_all_deltas_within_tolerance(
    reconcile: dict[str, Any],
    *,
    symbol: str,
    base_tolerance: Decimal,
    quote_tolerance: Decimal,
) -> bool:
    base_ccy = symbol.split("/")[0].upper()
    quote_ccy = symbol.split("/")[1].upper() if "/" in symbol else "USDT"
    diffs = reconcile.get("diffs")
    if not isinstance(diffs, list):
        return False
    saw_base = False
    for item in diffs:
        if not isinstance(item, dict):
            return False
        ccy = str(item.get("ccy") or "").upper()
        delta = abs(_decimal(item.get("delta")))
        enforced = item.get("enforced")
        ignored_as_dust = bool(item.get("ignored_as_dust"))
        if ccy == base_ccy:
            saw_base = True
            if delta > base_tolerance:
                return False
        elif ccy == quote_ccy:
            if delta > quote_tolerance:
                return False
        elif enforced is True and delta > Decimal("0") and not ignored_as_dust:
            return False
    return saw_base


def _emergency_flatten_cost_probe(
    okx: Any,
    *,
    symbol: str,
    inst_id: str,
    instrument: dict[str, Any],
    order_events_path: Path,
    fallback_qty: Decimal,
    reason: str,
    clid_prefix: str | None = None,
) -> dict[str, Any]:
    lot_sz = _decimal(instrument.get("lot_sz"))
    baseline = _baseline_base_balance(instrument)
    balance = _query_base_balance(okx, symbol)
    target_qty = max(balance - baseline, Decimal("0")) if balance is not None else max(fallback_qty, Decimal("0"))
    sell_qty = _round_to_step(target_qty, lot_sz, ROUND_DOWN) if lot_sz > 0 else target_qty
    result: dict[str, Any] = {
        "attempted": False,
        "reason": reason,
        "target_qty": _decimal_text(target_qty),
        "sell_qty": _decimal_text(sell_qty),
        "baseline_base_balance": _decimal_text(baseline),
        "balance_before": _decimal_text(balance) if balance is not None else "unverified",
        "max_retries": EMERGENCY_FLATTEN_MAX_RETRIES,
        "max_slippage_bps": _decimal_text(EMERGENCY_FLATTEN_MAX_SLIPPAGE_BPS),
        "attempts": [],
    }
    if sell_qty <= 0:
        result["status"] = "no_sellable_qty"
        return result
    prefix = clid_prefix or ("cp" + datetime.now(UTC).strftime("%Y%m%d%H%M%S"))
    filled_total = Decimal("0")
    last_state: dict[str, Any] = {}
    for attempt in range(1, EMERGENCY_FLATTEN_MAX_RETRIES + 1):
        balance = _query_base_balance(okx, symbol)
        target_qty = max(balance - baseline, Decimal("0")) if balance is not None else max(sell_qty - filled_total, Decimal("0"))
        sell_qty = _round_to_step(target_qty, lot_sz, ROUND_DOWN) if lot_sz > 0 else target_qty
        attempt_row: dict[str, Any] = {
            "attempt": attempt,
            "balance_before": _decimal_text(balance) if balance is not None else "unverified",
            "target_qty": _decimal_text(target_qty),
            "sell_qty": _decimal_text(sell_qty),
        }
        if sell_qty <= 0:
            attempt_row["status"] = "flat_or_no_sellable_qty"
            result["attempts"].append(attempt_row)
            break
        px, px_meta = _fresh_emergency_exit_px(okx, inst_id=inst_id, instrument=instrument)
        attempt_row.update(px_meta)
        if px is None:
            attempt_row["status"] = "price_guard_blocked"
            result["attempts"].append(attempt_row)
            break
        payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "ioc",
            "px": _decimal_text(px),
            "sz": _decimal_text(sell_qty),
            "clOrdId": f"{prefix}F{attempt}",
        }
        response = okx.place_order(payload, exp_time_ms=1500)
        data = _accepted_okx_item(response, action="emergency_exit_order")
        ord_id = str(data.get("ordId") or "")
        result.update({"attempted": True, "order_id": ord_id, "client_order_id": payload["clOrdId"]})
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="emergency_exit",
                status="submitted",
                cl_ord_id=payload["clOrdId"],
                ord_id=ord_id,
                payload=payload,
                instrument=instrument,
            ),
        )
        state = _poll_order(okx, inst_id=inst_id, cl_ord_id=payload["clOrdId"], max_seconds=5)
        last_state = state
        filled = _filled_qty(state)
        filled_total += max(filled, Decimal("0"))
        balance_after = _query_base_balance(okx, symbol)
        flat_after = balance_after is not None and abs(balance_after - baseline) <= max(lot_sz, Decimal("0.00000001"))
        attempt_row.update(
            {
                "status": str(state.get("state") or "unknown"),
                "filled_qty": _decimal_text(filled),
                "balance_after": _decimal_text(balance_after) if balance_after is not None else "unverified",
                "flat_after": bool(flat_after),
            }
        )
        result["attempts"].append(attempt_row)
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="emergency_exit",
                status=str(state.get("state") or "unknown"),
                cl_ord_id=payload["clOrdId"],
                ord_id=str(state.get("ordId") or ord_id),
                payload=state,
                instrument=instrument,
            ),
        )
        if flat_after:
            break
    balance_after = _query_base_balance(okx, symbol)
    result.update(
        {
            "status": str(last_state.get("state") or (result["attempts"][-1].get("status") if result["attempts"] else "unknown")),
            "filled_qty": _decimal_text(filled_total),
            "balance_after": _decimal_text(balance_after) if balance_after is not None else "unverified",
            "flat_after": bool(balance_after is not None and abs(balance_after - baseline) <= max(lot_sz, Decimal("0.00000001"))),
        }
    )
    return result


def _safe_emergency_flatten_cost_probe(
    okx: Any,
    *,
    symbol: str,
    inst_id: str,
    instrument: dict[str, Any],
    order_events_path: Path,
    fallback_qty: Decimal,
    reason: str,
    clid_prefix: str | None = None,
) -> dict[str, Any]:
    try:
        return _emergency_flatten_cost_probe(
            okx,
            symbol=symbol,
            inst_id=inst_id,
            instrument=instrument,
            order_events_path=order_events_path,
            fallback_qty=fallback_qty,
            reason=reason,
            clid_prefix=clid_prefix,
        )
    except Exception as exc:
        return {
            "attempted": True,
            "reason": reason,
            "status": "emergency_flatten_error",
            "error": str(exc),
            "target_qty": _decimal_text(fallback_qty),
            "sell_qty": "unverified",
            "attempts": [],
        }


def _fresh_emergency_exit_px(
    okx: Any,
    *,
    inst_id: str,
    instrument: dict[str, Any],
) -> tuple[Decimal | None, dict[str, Any]]:
    tick_sz = _decimal(instrument.get("tick_sz"))
    arrival_bid = _decimal(instrument.get("bid_px"))
    ticker = _public_okx_item(okx, "/api/v5/market/ticker", {"instId": inst_id})
    bid = _decimal(ticker.get("bidPx") or ticker.get("last"))
    floor = arrival_bid * (Decimal("1") - (EMERGENCY_FLATTEN_MAX_SLIPPAGE_BPS / Decimal("10000")))
    meta = {
        "fresh_bid_px": _decimal_text(bid),
        "arrival_bid_px": _decimal_text(arrival_bid),
        "slippage_floor_px": _decimal_text(floor),
    }
    if bid <= 0:
        return None, {**meta, "price_blocker": "emergency_exit_bid_missing"}
    if arrival_bid > 0 and bid < floor:
        return None, {**meta, "price_blocker": "emergency_exit_bid_below_slippage_floor"}
    raw_px = bid * Decimal("0.999")
    px = _round_to_step(raw_px, tick_sz, ROUND_DOWN) if tick_sz > 0 else raw_px
    if px <= 0:
        return None, {**meta, "price_blocker": "emergency_exit_px_invalid"}
    return px, {**meta, "order_px": _decimal_text(px)}


def _roundtrip_cost_fields(entry_state: dict[str, Any], exit_state: dict[str, Any], symbol: str) -> dict[str, str]:
    entry_qty = _filled_qty(entry_state)
    exit_qty = _filled_qty(exit_state)
    entry_px = _avg_px(entry_state)
    exit_px = _avg_px(exit_state)
    entry_fee = _fee(entry_state)
    exit_fee = _fee(exit_state)
    entry_fee_usdt, entry_warnings = _fee_usdt(entry_state, price=entry_px, symbol=symbol)
    exit_fee_usdt, exit_warnings = _fee_usdt(exit_state, price=exit_px, symbol=symbol)
    entry_notional = entry_qty * entry_px
    exit_notional = exit_qty * exit_px
    gross_pnl = exit_notional - entry_notional
    entry_base_fee_usdt = _base_fee_usdt(entry_state, price=entry_px, symbol=symbol)
    base_fee_reflected_in_exit_qty = _base_fee_reflected_in_exit_quantity(
        entry_state,
        entry_qty=entry_qty,
        exit_qty=exit_qty,
        symbol=symbol,
    )
    entry_fee_for_net = (
        entry_fee_usdt - entry_base_fee_usdt
        if base_fee_reflected_in_exit_qty
        else entry_fee_usdt
    )
    net_pnl = gross_pnl + entry_fee_for_net + exit_fee_usdt
    cost_bps = Decimal("0")
    if entry_notional > 0:
        cost_bps = (Decimal("0") - net_pnl) / entry_notional * Decimal("10000")
    fee_warnings = [*entry_warnings, *exit_warnings]
    entry_has_fills = bool(_fill_rows(entry_state))
    exit_has_fills = bool(_fill_rows(exit_state))
    cost_evidence_complete = (
        entry_has_fills
        and exit_has_fills
        and entry_qty > 0
        and exit_qty > 0
        and entry_px > 0
        and exit_px > 0
        and not fee_warnings
    )
    return {
        "entry_filled_qty": _decimal_text(entry_qty),
        "exit_filled_qty": _decimal_text(exit_qty),
        "entry_avg_px": _decimal_text(entry_px),
        "exit_avg_px": _decimal_text(exit_px),
        "entry_fee": _decimal_text(entry_fee),
        "entry_fee_ccy": _fee_ccy(entry_state),
        "entry_fee_usdt": _decimal_text(entry_fee_usdt),
        "exit_fee": _decimal_text(exit_fee),
        "exit_fee_ccy": _fee_ccy(exit_state),
        "exit_fee_usdt": _decimal_text(exit_fee_usdt),
        "entry_fee_usdt_applied_to_net": _decimal_text(entry_fee_for_net),
        "entry_base_fee_reflected_in_exit_qty": str(base_fee_reflected_in_exit_qty).lower(),
        "entry_base_fee_ledger_adjustment_usdt": _decimal_text(
            Decimal("0") - entry_base_fee_usdt if base_fee_reflected_in_exit_qty else Decimal("0")
        ),
        "entry_has_fill_rows": str(entry_has_fills).lower(),
        "exit_has_fill_rows": str(exit_has_fills).lower(),
        "fee_conversion_warnings": ";".join(fee_warnings),
        "cost_evidence_complete": str(cost_evidence_complete).lower(),
        "gross_pnl_usdt": _decimal_text(gross_pnl),
        "net_pnl_usdt": _decimal_text(net_pnl),
        "roundtrip_cost_bps": _decimal_text(cost_bps),
    }


def _order_cost_fields(row: dict[str, Any]) -> dict[str, str]:
    symbol = str(row.get("instId") or "").replace("-", "/")
    fee_usdt, warnings = _fee_usdt(row, price=_avg_px(row), symbol=symbol)
    return {
        "filled_qty": _decimal_text(_filled_qty(row)),
        "avg_px": _decimal_text(_avg_px(row)),
        "fee": _decimal_text(_fee(row)),
        "fee_ccy": _fee_ccy(row),
        "fee_usdt": _decimal_text(fee_usdt),
        "fee_conversion_warnings": ";".join(warnings),
    }


def _filled_qty(row: dict[str, Any]) -> Decimal:
    fills = _fill_rows(row)
    if fills:
        return sum((_decimal(fill.get("fillSz") or fill.get("sz")) for fill in fills), Decimal("0"))
    return _decimal(row.get("accFillSz") or row.get("fillSz") or row.get("sz"))


def _avg_px(row: dict[str, Any]) -> Decimal:
    fills = _fill_rows(row)
    if fills:
        total_qty = Decimal("0")
        total_notional = Decimal("0")
        for fill in fills:
            qty = _decimal(fill.get("fillSz") or fill.get("sz"))
            px = _decimal(fill.get("fillPx") or fill.get("avgPx") or fill.get("px"))
            total_qty += qty
            total_notional += qty * px
        if total_qty > 0:
            return total_notional / total_qty
    return _decimal(row.get("avgPx") or row.get("fillPx") or row.get("px"))


def _fee(row: dict[str, Any]) -> Decimal:
    fills = _fill_rows(row)
    if fills:
        return sum((_decimal(fill.get("fee")) for fill in fills), Decimal("0"))
    return _decimal(row.get("fee"))


def _fee_ccy(row: dict[str, Any]) -> str:
    fills = _fill_rows(row)
    if fills:
        ccys = {str(fill.get("feeCcy") or "").upper() for fill in fills if str(fill.get("feeCcy") or "").strip()}
        return next(iter(ccys)) if len(ccys) == 1 else ("mixed" if ccys else "")
    return str(row.get("feeCcy") or "")


def _fee_usdt(row: dict[str, Any], *, price: Decimal, symbol: str) -> tuple[Decimal, list[str]]:
    parts = symbol.upper().replace("-", "/").split("/")
    base = parts[0] if parts else ""
    quote = parts[1] if len(parts) > 1 else "USDT"
    total = Decimal("0")
    warnings: list[str] = []
    for item in _fill_rows(row) or [row]:
        fee = _decimal(item.get("fee"))
        ccy = str(item.get("feeCcy") or row.get("feeCcy") or "").upper()
        if fee == 0:
            continue
        if ccy in {quote, "USDT"}:
            total += fee
        elif ccy == base and price > 0:
            fill_px = _decimal(item.get("fillPx") or item.get("avgPx") or item.get("px")) or price
            total += fee * fill_px
        else:
            explicit = _decimal_or_none(
                item.get("fee_usdt")
                or item.get("feeUSDT")
                or item.get("feeUsd")
                or item.get("fee_usd")
            )
            if explicit is not None:
                total += explicit
            else:
                warnings.append(f"fee_ccy_conversion_unavailable:{ccy or 'unknown'}")
    return total, warnings


def _base_fee_usdt(row: dict[str, Any], *, price: Decimal, symbol: str) -> Decimal:
    base = symbol.upper().replace("-", "/").split("/")[0]
    total = Decimal("0")
    for item in _fill_rows(row) or [row]:
        ccy = str(item.get("feeCcy") or row.get("feeCcy") or "").upper()
        if ccy != base:
            continue
        fill_px = _decimal(item.get("fillPx") or item.get("avgPx") or item.get("px")) or price
        if fill_px > 0:
            total += _decimal(item.get("fee")) * fill_px
    return total


def _base_fee_reflected_in_exit_quantity(
    row: dict[str, Any],
    *,
    entry_qty: Decimal,
    exit_qty: Decimal,
    symbol: str,
) -> bool:
    base = symbol.upper().replace("-", "/").split("/")[0]
    base_fee_qty = Decimal("0")
    for item in _fill_rows(row) or [row]:
        ccy = str(item.get("feeCcy") or row.get("feeCcy") or "").upper()
        if ccy == base:
            base_fee_qty += abs(_decimal(item.get("fee")))
    if base_fee_qty <= 0:
        return False
    exit_gap = max(entry_qty - exit_qty, Decimal("0"))
    return exit_gap >= base_fee_qty


def _fill_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    rows = row.get("_fills")
    return [dict(item) for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []


def _query_base_balance(okx: Any, symbol: str) -> Decimal | None:
    base_ccy = symbol.split("/")[0].upper()
    return _query_available_balance(okx, base_ccy)


def _query_quote_balance(okx: Any, symbol: str) -> Decimal | None:
    parts = symbol.upper().replace("-", "/").split("/")
    quote_ccy = parts[1] if len(parts) > 1 else "USDT"
    return _query_available_balance(okx, quote_ccy)


def _query_available_balance(okx: Any, ccy: str) -> Decimal | None:
    ccy = str(ccy or "").upper()
    if not ccy:
        return None
    try:
        response = okx.get_balance(ccy=ccy) if hasattr(okx, "get_balance") else okx.request(
            "GET",
            "/api/v5/account/balance",
            params={"ccy": ccy},
        )
    except Exception:
        return None
    data = getattr(response, "data", response)
    if isinstance(data, dict) and str(data.get("code") or "0") != "0":
        return None
    rows = data.get("data") if isinstance(data, dict) else None
    details = ((rows[0] if isinstance(rows, list) and rows else {}) or {}).get("details")
    if not isinstance(details, list):
        return None
    for item in details:
        if isinstance(item, dict) and str(item.get("ccy") or "").upper() == ccy:
            return _decimal(item.get("availBal") or item.get("cashBal") or item.get("eq") or item.get("bal"))
    return Decimal("0")


def _open_order_count(okx: Any, *, inst_id: str) -> int:
    try:
        if hasattr(okx, "get_open_orders"):
            response = okx.get_open_orders(inst_id=inst_id)
        else:
            response = okx.request("GET", "/api/v5/trade/orders-pending", params={"instId": inst_id})
    except Exception:
        return -1
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    return len(rows) if isinstance(rows, list) else -1


def _local_position_qty(cfg: Any, *, preflight: dict[str, Any], symbol: str) -> Decimal | None:
    path_text = str((preflight.get("runtime_paths") or {}).get("position_store_path") or "")
    path = Path(path_text) if path_text else derive_position_store_path(
        Path(getattr(getattr(cfg, "execution", cfg), "order_store_path", "reports/orders.sqlite"))
    )
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return None
    try:
        with sqlite3.connect(str(path), timeout=2.0) as con:
            cur = con.cursor()
            cur.execute("SELECT qty FROM positions WHERE symbol = ?", (symbol,))
            rows = cur.fetchall()
    except Exception:
        return None
    total = Decimal("0")
    for (qty,) in rows:
        total += _decimal(qty)
    return total


def _read_reconcile_status(preflight: dict[str, Any]) -> dict[str, Any]:
    path_text = str((preflight.get("runtime_paths") or {}).get("reconcile_status_path") or "")
    if not path_text:
        return {"ok": False, "reason": "reconcile_status_path_missing"}
    path = Path(path_text)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"ok": False, "reason": f"reconcile_status_unreadable:{exc}"}
    return payload if isinstance(payload, dict) else {"ok": False, "reason": "reconcile_status_invalid"}


def _refresh_reconcile_status(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    symbol: str,
) -> dict[str, Any]:
    runtime_paths = preflight.get("runtime_paths") or {}
    position_path = Path(str(runtime_paths.get("position_store_path") or ""))
    reconcile_path = Path(str(runtime_paths.get("reconcile_status_path") or ""))
    if not str(position_path) or not str(reconcile_path):
        return {"refreshed": False, "reason": "runtime_paths_missing"}
    try:
        execution = getattr(cfg, "execution", cfg)
        thresholds = ReconcileThresholds(
            abs_usdt_tol=float(getattr(execution, "reconcile_abs_usdt_tol", 50.0) or 50.0),
            abs_base_tol=float(getattr(execution, "reconcile_abs_base_tol", 1e-8) or 1e-8),
            dust_usdt_ignore=float(getattr(execution, "reconcile_dust_usdt_ignore", 1.0) or 1.0),
        )
        status = ReconcileEngine(
            okx=okx,
            position_store=PositionStore(path=str(position_path)),
            account_store=AccountStore(path=str(position_path)),
            thresholds=thresholds,
        ).reconcile(
            out_path=str(reconcile_path),
            universe_bases=[symbol.split("/")[0].upper()],
            ccy_mode=str(getattr(execution, "reconcile_ccy_mode", "universe_only") or "universe_only"),
        )
        status = _annotate_cost_probe_reconcile_residual(
            status,
            symbol=symbol,
            base_tolerance=max(
                _decimal((preflight.get("instrument_preflight") or {}).get("lot_sz")),
                Decimal("0.00000001"),
            ),
            quote_tolerance=Decimal(str(getattr(execution, "reconcile_abs_usdt_tol", 50.0) or 50.0)),
            out_path=reconcile_path,
        )
        return {
            "refreshed": True,
            "ok": bool(status.get("ok")),
            "reason": str(status.get("reason") or ""),
            "path": str(reconcile_path),
        }
    except Exception as exc:
        failure = {
            "schema_version": 1,
            "generated_ts_ms": int(datetime.now(UTC).timestamp() * 1000),
            "ok": False,
            "reason": f"cost_probe_post_exit_reconcile_failed:{type(exc).__name__}",
            "error": str(exc),
        }
        try:
            reconcile_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = reconcile_path.with_suffix(reconcile_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            tmp_path.replace(reconcile_path)
        except Exception:
            pass
        return {"refreshed": False, "reason": failure["reason"], "error": str(exc), "path": str(reconcile_path)}


def _annotate_cost_probe_reconcile_residual(
    status: dict[str, Any],
    *,
    symbol: str,
    base_tolerance: Decimal,
    quote_tolerance: Decimal,
    out_path: Path,
) -> dict[str, Any]:
    if bool(status.get("ok")) or str(status.get("reason") or "") != "base_mismatch":
        return status
    if status.get("error"):
        return status
    if not _reconcile_all_deltas_within_tolerance(
        status,
        symbol=symbol,
        base_tolerance=base_tolerance,
        quote_tolerance=quote_tolerance,
    ):
        return status

    annotated = dict(status)
    annotated["reason"] = "below_lot_size_residual"
    annotated["cost_probe_residual_annotation"] = {
        "reason": "below_lot_size_residual",
        "base_tolerance": _decimal_text(base_tolerance),
        "quote_tolerance": _decimal_text(quote_tolerance),
        "symbol": symbol,
    }
    try:
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(annotated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(out_path)
    except Exception:
        pass
    return annotated


def _arrival_mid_px(instrument: dict[str, Any]) -> str:
    bid = _decimal(instrument.get("bid_px"))
    ask = _decimal(instrument.get("ask_px"))
    if bid <= 0 or ask <= 0:
        return ""
    return _decimal_text((bid + ask) / Decimal("2"))


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _round_to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=rounding) * step


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_text(value: Any) -> str:
    dec = _decimal(value)
    return format(dec.normalize(), "f")


def _write_kill_switch(cfg: Any, *, reason: str) -> None:
    raw_path = getattr(getattr(cfg, "execution", cfg), "kill_switch_path", "reports/kill_switch.json")
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "reason": reason,
                "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "sell_only_on_error": True,
                "manual_clear_required": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one guarded V5 live cost probe.")
    parser.add_argument("--config", default="configs/live_prod.yaml")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--auth-file", required=True)
    parser.add_argument("--execute-live-order", action="store_true")
    parser.add_argument("--i-understand-this-submits-live-okx-orders", action="store_true")
    args = parser.parse_args(argv)

    from src.execution.okx_private_client import OKXPrivateClient

    cfg = load_config(args.config)
    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        result = run_live_probe_once(
            cfg,
            reports_dir=args.reports_dir,
            auth_path=args.auth_file,
            okx=client,
            execute_live_order=args.execute_live_order,
            operator_confirmed=args.i_understand_this_submits_live_okx_orders,
        )
    finally:
        client.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("state") in {"READY_FOR_OPERATOR_CONFIRMATION", "COMPLETED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
