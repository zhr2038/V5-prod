from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.core.clock import TradingClock, SystemClock

from src.core.models import Order
from src.execution.position_store import Position
from src.risk.atr_trailing import update_atr_trailing, ATRTrailingState
from src.core.models import MarketSeries


@dataclass
class ExitConfig:
    atr_mult: float = 2.2
    atr_n: int = 14
    time_stop_days: int = 20
    enable_regime_exit: bool = True


class ExitPolicy:
    """Evaluate exits for existing long positions.

    - ATR trailing stop: stop = highest - atr_mult*ATR
    - Time stop: if held > N days AND PnL <= 0 => close
    - Regime exit: in Risk-Off, optionally force delever/close (scaffold closes all)
    """

    def __init__(self, cfg: ExitConfig, clock: Optional[TradingClock] = None):
        self.cfg = cfg
        self.clock = clock or SystemClock()

    @staticmethod
    def _parse_ts(ts: str) -> Optional[datetime]:
        try:
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            return datetime.fromisoformat(ts)
        except Exception:
            return None

    def evaluate(
        self,
        positions: List[Position],
        market_data: Dict[str, MarketSeries],
        regime_state: str,
    ) -> List[Order]:
        orders: List[Order] = []

        # Regime exit (scaffold): if Risk-Off and enabled => close all
        if self.cfg.enable_regime_exit and str(regime_state) == "Risk-Off":
            for p in positions:
                last = float(market_data.get(p.symbol).close[-1]) if market_data.get(p.symbol) else float(p.avg_px)
                orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * float(last),
                        signal_price=float(last),
                        meta={"reason": "regime_exit"},
                    )
                )
            return orders

        now = self.clock.now().astimezone(timezone.utc)

        for p in positions:
            s = market_data.get(p.symbol)
            if not s or not s.close:
                continue

            last = float(s.close[-1])
            # ATR trailing
            st = ATRTrailingState(highest_price=float(p.highest_px), stop_price=0.0)
            st2 = update_atr_trailing(s, st, atr_mult=float(self.cfg.atr_mult), n=int(self.cfg.atr_n))
            if last <= float(st2.stop_price):
                orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * last,
                        signal_price=last,
                        meta={"reason": "atr_trailing", "stop": float(st2.stop_price), "highest": float(st2.highest_price)},
                    )
                )
                continue

            # Time stop: held > N days and not profitable
            ent = self._parse_ts(p.entry_ts)
            if ent is not None:
                days = (now.date() - ent.astimezone(timezone.utc).date()).days
                if days >= int(self.cfg.time_stop_days):
                    # Use store-provided mark pnl when available, fallback to computed.
                    pnl = float(getattr(p, 'unrealized_pnl_pct', 0.0) or 0.0)
                    if pnl == 0.0 and p.avg_px:
                        pnl = (last - float(p.avg_px)) / float(p.avg_px)
                    if pnl <= 0:
                        orders.append(
                            Order(
                                symbol=p.symbol,
                                side="sell",
                                intent="CLOSE_LONG",
                                notional_usdt=float(p.qty) * last,
                                signal_price=last,
                                meta={"reason": "time_stop", "days": days, "pnl": pnl},
                            )
                        )

        return orders
