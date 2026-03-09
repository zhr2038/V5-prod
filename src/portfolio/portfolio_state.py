from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PortfolioState:
    """PortfolioState类 - 支持资金规模感知的回撤计算"""
    cash_usdt: float
    equity_usdt: float
    peak_equity_usdt: float
    # 新增：记录资金规模变化历史
    scale_basis_usdt: float = 0.0  # 当前资金规模基准

    @property
    def drawdown_pct(self) -> float:
        """Drawdown pct - 基于当前资金规模计算"""
        p = float(self.peak_equity_usdt or 0.0)
        e = float(self.equity_usdt or 0.0)
        s = float(self.scale_basis_usdt or 0.0)
        
        if p <= 0:
            return 0.0
            
        # 如果设置了资金规模基准，确保回撤不会超出合理范围
        # 例如：20U规模，峰值25U，当前18U → 回撤 (25-18)/25 = 28%
        # 但相对于20U规模，最大回撤只能是 (20-0)/20 = 100%
        if s > 0:
            # 如果峰值超过规模的2倍，可能是历史数据，限制峰值
            if p > s * 2:
                p = max(s, e)
        
        return (p - e) / p if p > 0 else 0.0

    def update_equity(self, equity_usdt: float) -> None:
        """Update equity"""
        e = float(equity_usdt)
        self.equity_usdt = e
        if e > float(self.peak_equity_usdt or 0.0):
            self.peak_equity_usdt = e
    
    def update_scale_basis(self, new_basis: float) -> None:
        """更新资金规模基准 - 当加仓/减仓时调用"""
        old_basis = float(self.scale_basis_usdt or 0.0)
        new_basis = float(new_basis or 0.0)
        
        if old_basis > 0 and new_basis > 0:
            # 按比例调整峰值
            # 例如：20U→100U，峰值25U→125U (按比例放大)
            scale_ratio = new_basis / old_basis
            self.peak_equity_usdt = float(self.peak_equity_usdt or old_basis) * scale_ratio
        
        self.scale_basis_usdt = new_basis
