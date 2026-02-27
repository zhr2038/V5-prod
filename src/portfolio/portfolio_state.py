from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PortfolioState:
    """PortfolioState类"""
    cash_usdt: float
    equity_usdt: float
    peak_equity_usdt: float

    @property
    def drawdown_pct(self) -> float:
        """Drawdown pct"""
        p = float(self.peak_equity_usdt or 0.0)
        e = float(self.equity_usdt or 0.0)
        if p <= 0:
            return 0.0
        return (p - e) / p

    def update_equity(self, equity_usdt: float) -> None:
        """Update equity"""
        e = float(equity_usdt)
        self.equity_usdt = e
        if e > float(self.peak_equity_usdt or 0.0):
            self.peak_equity_usdt = e
