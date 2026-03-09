from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RouterDecision:
    """RouterDecision类"""
    order_type: str  # market|limit|post_only
    reason: str
    params: Dict[str, str]


class OKXOrderRouter:
    """Execution quality router (decision logic only).

    V5 phase-1 is dry-run, but we implement routing decisions & unit tests.

    Notes (OKX spot):
    - tdMode should be cash
    - For market orders, tgtCcy decides whether sz is base or quote
    """

    def __init__(self, max_spread_pct: float = 0.0015):
        self.max_spread_pct = float(max_spread_pct)

    def decide(self, best_bid: float, best_ask: float, force_market: bool = False) -> RouterDecision:
        """Decide"""
        bid = float(best_bid)
        ask = float(best_ask)
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
        spread = (ask - bid) / mid if mid else 1.0

        if force_market:
            return RouterDecision(order_type="market", reason="force_market", params={"tdMode": "cash", "tgtCcy": "quote_ccy"})

        if spread <= self.max_spread_pct:
            # prefer limit IOC in tight spread
            return RouterDecision(order_type="limit", reason=f"spread_ok({spread:.4%})", params={"tdMode": "cash"})

        # too wide => avoid; fallback to market with tgtCcy quote (USDT notional)
        return RouterDecision(order_type="market", reason=f"spread_wide({spread:.4%})", params={"tdMode": "cash", "tgtCcy": "quote_ccy"})
