from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from src.core.models import Order


DEFAULT_STATE_PATH = "reports/order_state_machine.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_state(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"version": 1, "symbols": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "symbols": {}}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _order_priority(o: Order) -> int:
    side = str(getattr(o, "side", "")).lower()
    intent = str(getattr(o, "intent", "")).upper()

    # Higher number = higher priority
    if side == "sell" and intent == "CLOSE_LONG":
        return 500
    if side == "sell" and intent == "REBALANCE":
        return 400
    if side == "buy" and intent == "OPEN_LONG":
        return 300
    if side == "buy" and intent == "REBALANCE":
        return 200
    return 100


def _state_for_symbol(state: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    syms = state.setdefault("symbols", {})
    return syms.setdefault(
        symbol,
        {
            "state": "FLAT",
            "cooldown_until_ms": 0,
            "exit_pending_until_ms": 0,
            "last_run_id": None,
            "last_reason": None,
        },
    )


def arbitrate_orders(
    *,
    orders: List[Order],
    positions: List[Any],
    run_id: str,
    cooldown_minutes: int = 10,
    state_path: str = DEFAULT_STATE_PATH,
) -> Tuple[List[Order], List[Dict[str, Any]]]:
    """Arbitrate conflicting orders by symbol with deterministic priority + state machine.

    State machine per symbol: FLAT / LONG / EXIT_PENDING / COOLDOWN
    - CLOSE_LONG dominates all lower-priority actions in same run
    - During EXIT_PENDING/COOLDOWN, block OPEN_LONG/REBALANCE buys
    """
    now_ms = _now_ms()
    state = _load_state(state_path)
    decisions: List[Dict[str, Any]] = []

    held = {str(getattr(p, "symbol", "")) for p in (positions or []) if float(getattr(p, "qty", 0.0) or 0.0) > 1e-12}

    # Expire state timers
    for sym, st in (state.get("symbols") or {}).items():
        if st.get("state") == "EXIT_PENDING":
            ep = int(st.get("exit_pending_until_ms") or 0)
            if ep > 0 and ep <= now_ms:
                if sym in held:
                    # Close was selected earlier but position still exists.
                    # Do not enter post-exit cooldown on an unfilled/failed close.
                    st["state"] = "LONG"
                    st["exit_pending_until_ms"] = 0
                    st["cooldown_until_ms"] = 0
                    st["last_reason"] = "exit_pending_expired_still_held"
                else:
                    st["state"] = "COOLDOWN"
        if st.get("state") == "COOLDOWN" and int(st.get("cooldown_until_ms") or 0) <= now_ms:
            st["state"] = "FLAT"

    # Bootstrap state from current positions after expiry handling so a stale EXIT_PENDING
    # cannot immediately downgrade a still-held symbol into COOLDOWN.
    for sym in held:
        st = _state_for_symbol(state, sym)
        if st.get("state") in {"FLAT", "COOLDOWN"}:
            st["state"] = "LONG"

    grouped: Dict[str, List[Order]] = {}
    for o in orders or []:
        grouped.setdefault(str(getattr(o, "symbol", "")), []).append(o)

    selected: List[Order] = []

    for sym, os in grouped.items():
        if not sym:
            continue
        st = _state_for_symbol(state, sym)
        machine_state = str(st.get("state") or "FLAT")
        cooldown_until = int(st.get("cooldown_until_ms") or 0)

        # Rule 1: if CLOSE_LONG exists, it dominates all buys for same symbol in this run
        close_sells = [o for o in os if str(o.side).lower() == "sell" and str(o.intent).upper() == "CLOSE_LONG"]
        if close_sells:
            winner = sorted(close_sells, key=lambda x: float(getattr(x, "notional_usdt", 0.0) or 0.0), reverse=True)[0]
            selected.append(winner)

            for o in os:
                if o is winner:
                    continue
                decisions.append(
                    {
                        "symbol": sym,
                        "action": "blocked",
                        "code": "ARB_SUPERSEDED_BY_CLOSE",
                        "reason": "CLOSE_LONG has highest priority for this symbol in same run",
                        "blocked": {"side": o.side, "intent": o.intent, "notional_usdt": float(o.notional_usdt or 0.0)},
                        "winner": {"side": winner.side, "intent": winner.intent, "notional_usdt": float(winner.notional_usdt or 0.0)},
                    }
                )

            st["state"] = "EXIT_PENDING"
            st["exit_pending_until_ms"] = now_ms + 2 * 60 * 1000
            st["cooldown_until_ms"] = now_ms + max(0, int(cooldown_minutes)) * 60 * 1000
            st["last_run_id"] = run_id
            st["last_reason"] = "close_dominates"
            continue

        # Rule 2: if in EXIT_PENDING/COOLDOWN, block buys
        blocked_buys = []
        kept = []
        for o in os:
            is_buy = str(o.side).lower() == "buy" and str(o.intent).upper() in {"OPEN_LONG", "REBALANCE"}
            if is_buy and (machine_state == "EXIT_PENDING" or (machine_state == "COOLDOWN" and cooldown_until > now_ms)):
                blocked_buys.append(o)
            else:
                kept.append(o)

        for o in blocked_buys:
            code = "ARB_BLOCKED_BY_EXIT_PENDING" if machine_state == "EXIT_PENDING" else "ARB_BLOCKED_BY_COOLDOWN"
            decisions.append(
                {
                    "symbol": sym,
                    "action": "blocked",
                    "code": code,
                    "reason": f"state={machine_state} cooldown_until_ms={cooldown_until}",
                    "blocked": {"side": o.side, "intent": o.intent, "notional_usdt": float(o.notional_usdt or 0.0)},
                }
            )

        if not kept:
            continue

        # Rule 3: pick one winner by priority then notional
        winner = sorted(kept, key=lambda x: (_order_priority(x), float(getattr(x, "notional_usdt", 0.0) or 0.0)), reverse=True)[0]
        selected.append(winner)

        for o in kept:
            if o is winner:
                continue
            code = "ARB_SUPERSEDED_BY_PRIORITY"
            # explicit code for open-vs-rebalance conflict
            if str(winner.side).lower() == "buy" and str(winner.intent).upper() == "OPEN_LONG" and str(o.side).lower() == "buy" and str(o.intent).upper() == "REBALANCE":
                code = "ARB_SUPERSEDED_BY_OPEN"
            decisions.append(
                {
                    "symbol": sym,
                    "action": "blocked",
                    "code": code,
                    "reason": "higher-priority order selected for same symbol",
                    "blocked": {"side": o.side, "intent": o.intent, "notional_usdt": float(o.notional_usdt or 0.0)},
                    "winner": {"side": winner.side, "intent": winner.intent, "notional_usdt": float(winner.notional_usdt or 0.0)},
                }
            )

        # State transition by winner
        w_side = str(winner.side).lower()
        w_intent = str(winner.intent).upper()
        if w_side == "buy" and w_intent in {"OPEN_LONG", "REBALANCE"}:
            st["state"] = "LONG"
            st["cooldown_until_ms"] = 0
            st["last_reason"] = "buy_selected"
        elif w_side == "sell" and w_intent == "REBALANCE":
            st["state"] = "LONG"
            st["last_reason"] = "reduce_selected"
        else:
            st["last_reason"] = "selected"
        st["last_run_id"] = run_id

    _save_state(state_path, state)
    return selected, decisions
