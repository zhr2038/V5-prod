from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.core.clock import TradingClock, SystemClock

from src.core.models import Order
from src.execution.position_store import Position
from src.risk.atr_trailing import update_atr_trailing, ATRTrailingState, atr
from src.core.models import MarketSeries


@dataclass
class ExitConfig:
    """退出策略配置"""
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
        """初始化退出策略

        Args:
            cfg: 退出策略配置
            clock: 交易时钟
        """
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

    @staticmethod
    def _normalize_market_series(series: MarketSeries) -> MarketSeries:
        points = []
        for idx, values in enumerate(
            zip(
                series.ts or [],
                series.open or [],
                series.high or [],
                series.low or [],
                series.close or [],
                series.volume or [],
            )
        ):
            ts_value, open_px, high_px, low_px, close_px, volume = values
            try:
                ts_ms = int(ts_value)
            except Exception:
                continue
            if abs(ts_ms) < 10_000_000_000:
                ts_ms *= 1000
            points.append((ts_ms, idx, open_px, high_px, low_px, close_px, volume))

        if not points:
            return MarketSeries(symbol=series.symbol, timeframe=series.timeframe, ts=[], open=[], high=[], low=[], close=[], volume=[])

        points.sort(key=lambda item: (item[0], item[1]))
        deduped = []
        for point in points:
            if deduped and deduped[-1][0] == point[0]:
                deduped[-1] = point
            else:
                deduped.append(point)

        return MarketSeries(
            symbol=series.symbol,
            timeframe=series.timeframe,
            ts=[int(item[0]) for item in deduped],
            open=[item[2] for item in deduped],
            high=[item[3] for item in deduped],
            low=[item[4] for item in deduped],
            close=[float(item[5]) for item in deduped],
            volume=[item[6] for item in deduped],
        )

    def evaluate(
        self,
        positions: List[Position],
        market_data: Dict[str, MarketSeries],
        regime_state: str,
    ) -> List[Order]:
        """评估退出条件

        Args:
            positions: 持仓列表
            market_data: 市场数据
            regime_state: 市场状态

        Returns:
            退出订单列表
        """
        orders: List[Order] = []

        # Regime exit (scaffold): if Risk-Off and enabled => close all
        if self.cfg.enable_regime_exit and str(regime_state) == "Risk-Off":
            for p in positions:
                series = market_data.get(p.symbol)
                normalized_series = self._normalize_market_series(series) if series is not None else None
                last = float(normalized_series.close[-1]) if normalized_series and normalized_series.close else float(p.avg_px)
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
            s = self._normalize_market_series(s)
            if not s.close:
                continue

            last = float(s.close[-1])
            # ATR trailing
            st = ATRTrailingState(highest_price=float(p.highest_px), stop_price=0.0)
            st2 = update_atr_trailing(s, st, atr_mult=float(self.cfg.atr_mult), n=int(self.cfg.atr_n))
            a = atr(s, n=int(self.cfg.atr_n))
            if last <= float(st2.stop_price):
                orders.append(
                    Order(
                        symbol=p.symbol,
                        side="sell",
                        intent="CLOSE_LONG",
                        notional_usdt=float(p.qty) * last,
                        signal_price=last,
                        meta={
                            "reason": "atr_trailing",
                            "last": float(last),
                            "stop": float(st2.stop_price),
                            "highest": float(st2.highest_price),
                            "atr": float(a),
                            "atr_mult": float(self.cfg.atr_mult),
                            "atr_n": int(self.cfg.atr_n),
                        },
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
