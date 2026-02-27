from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class BorrowItem:
    """BorrowItem类"""
    ccy: str
    eq: float
    liab: float
    cross_liab: float
    borrow_froz: float


@dataclass
class BorrowCheckResult:
    """BorrowCheckResult类"""
    ok: bool
    items: List[BorrowItem]
    reason: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


def _to_f(x: Any) -> float:
    try:
        return float(x or 0)
    except Exception:
        return 0.0


def check_okx_borrows(
    balance_resp: Dict[str, Any],
    *,
    liab_eps: float = 0.01,  # 提高到 0.01，只检测真正有意义的借贷
    neg_eq_eps: float = 1e-6,
) -> BorrowCheckResult:
    """Parse OKX /api/v5/account/balance response.

    Flags borrows when:
    - liab > liab_eps
    - crossLiab > liab_eps
    - borrowFroz > liab_eps
    - eq < -neg_eq_eps

    Returns ok=True when no borrow-like fields detected.
    """

    obj = balance_resp if isinstance(balance_resp, dict) else {}
    rows = obj.get("data") if isinstance(obj, dict) else None
    r0 = (rows[0] if isinstance(rows, list) and rows else {}) or {}
    details = r0.get("details")
    if not isinstance(details, list):
        return BorrowCheckResult(ok=True, items=[], reason="no_details", raw=obj)

    items: List[BorrowItem] = []
    for d in details:
        if not isinstance(d, dict):
            continue
        ccy = str(d.get("ccy") or "").strip()
        if not ccy:
            continue
        eq = _to_f(d.get("eq"))
        liab = _to_f(d.get("liab"))
        cross_liab = _to_f(d.get("crossLiab"))
        borrow_froz = _to_f(d.get("borrowFroz"))

        has_borrow = (
            liab > float(liab_eps)
            or cross_liab > float(liab_eps)
            or borrow_froz > float(liab_eps)
            or eq < -float(neg_eq_eps)
        )
        if has_borrow:
            items.append(
                BorrowItem(
                    ccy=ccy,
                    eq=float(eq),
                    liab=float(liab),
                    cross_liab=float(cross_liab),
                    borrow_froz=float(borrow_froz),
                )
            )

    if items:
        return BorrowCheckResult(ok=False, items=items, reason="borrow_detected", raw=obj)

    return BorrowCheckResult(ok=True, items=[], reason="ok", raw=obj)
