from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from src.execution.account_store import AccountStore
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.position_store import Position, PositionStore
from src.reporting.spread_snapshot_store import SpreadSnapshotStore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write_json(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        p = Path(path)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _base_ccy(symbol: str) -> str:
    return str(symbol).split("/")[0].upper()


def _mid(symbol: str, ts_ms: int) -> Optional[float]:
    try:
        ss = SpreadSnapshotStore()
        snap = ss.get_latest_before(symbol=symbol, ts_ms=ts_ms)
        if snap is not None and snap.mid and float(snap.mid) > 0:
            return float(snap.mid)
    except Exception:
        pass
    return None


@dataclass
class PatchResult:
    applied: bool
    reason: str
    est_total_drift_usdt: float
    updated_cash: float
    updated_positions: int


def estimate_total_drift_usdt(
    *,
    exchange_ccy_cash: Dict[str, float],
    local_ccy_qty: Dict[str, float],
    ts_ms: int,
) -> float:
    total = 0.0
    for ccy in set(exchange_ccy_cash.keys()) | set(local_ccy_qty.keys()) | {"USDT"}:
        ex = float(exchange_ccy_cash.get(ccy, 0.0) or 0.0)
        lo = float(local_ccy_qty.get(ccy, 0.0) or 0.0)
        d = ex - lo
        if ccy.upper() == "USDT":
            total += abs(d)
        else:
            mid = _mid(f"{ccy}/USDT", ts_ms)
            if mid is not None:
                total += abs(d) * float(mid)
    return float(total)


def controlled_patch_from_okx_balance(
    *,
    okx: OKXPrivateClient,
    position_store: PositionStore,
    account_store: AccountStore,
    max_total_drift_usdt: float = 50.0,
    state_path: str = "reports/bootstrap_patch_state.json",
    min_interval_sec: int = 300,
) -> PatchResult:
    """Patch local cash + base quantities from OKX /account/balance.

    Safety properties:
    - Only touches: AccountStore.cash_usdt and PositionStore.qty.
    - Preserves avg_px/mark/highest/etc. (does not overwrite trading PnL semantics).
    - Refuses to patch if estimated total drift exceeds max_total_drift_usdt.
    - Rate-limited via state_path.
    """

    now_ms = _now_ms()
    st = _read_json(state_path) or {}
    last_ms = int(st.get("last_patch_ts_ms") or 0)
    if min_interval_sec > 0 and last_ms > 0 and (now_ms - last_ms) < int(min_interval_sec) * 1000:
        return PatchResult(False, "rate_limited", 0.0, float(account_store.get().cash_usdt), 0)

    # Fetch exchange cash balances
    r = okx.get_balance(ccy=None)
    data = (r.data or {}).get("data") or []
    details = (data[0] or {}).get("details") if isinstance(data, list) and data else []

    ex: Dict[str, float] = {}
    if isinstance(details, list):
        for d in details:
            if not isinstance(d, dict):
                continue
            ccy = str(d.get("ccy") or "").upper()
            if not ccy:
                continue
            cb = d.get("cashBal")
            if cb is None:
                continue
            try:
                ex[ccy] = float(cb)
            except Exception:
                ex[ccy] = 0.0

    # Local snapshot by ccy
    local: Dict[str, float] = {"USDT": float(account_store.get().cash_usdt)}
    for p in position_store.list():
        local[_base_ccy(p.symbol)] = float(p.qty)

    est = estimate_total_drift_usdt(exchange_ccy_cash=ex, local_ccy_qty=local, ts_ms=now_ms)
    if float(max_total_drift_usdt) > 0 and est > float(max_total_drift_usdt):
        return PatchResult(False, "drift_too_large", float(est), float(account_store.get().cash_usdt), 0)

    # Apply patch: cash
    acc = account_store.get()
    new_cash = float(ex.get("USDT", float(acc.cash_usdt)))
    acc.cash_usdt = float(new_cash)
    account_store.set(acc)

    # Apply patch: positions qty (create if missing). Do NOT delete local positions.
    updated = 0
    for ccy, qty in ex.items():
        if ccy == "USDT":
            continue
        if qty <= 0:
            continue
        sym = f"{ccy}/USDT"
        p = position_store.get(sym)
        if p is None:
            # create minimal position with qty; preserve pricing as 0 (unknown)
            pos = Position(
                symbol=sym,
                qty=float(qty),
                avg_px=0.0,
                entry_ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                highest_px=0.0,
                last_update_ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                last_mark_px=0.0,
                unrealized_pnl_pct=0.0,
                tags_json=json.dumps({"bootstrap_patch": True, "source": "okx_balance"}, ensure_ascii=False),
            )
            position_store.upsert_position(pos)
            updated += 1
        else:
            # update qty only
            p.qty = float(qty)
            position_store.upsert_position(p)
            updated += 1

    _atomic_write_json(
        state_path,
        {
            "schema_version": 1,
            "last_patch_ts_ms": int(now_ms),
            "est_total_drift_usdt": float(est),
            "max_total_drift_usdt": float(max_total_drift_usdt),
        },
    )

    return PatchResult(True, "patched", float(est), float(new_cash), int(updated))
