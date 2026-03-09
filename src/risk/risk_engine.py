from __future__ import annotations

import logging
from dataclasses import dataclass

from configs.schema import RiskConfig
from src.core.models import PositionState, RiskDecision

log = logging.getLogger(__name__)


class RiskEngine:
    """风险引擎
    
    基于回撤计算风险调整系数，控制仓位暴露
    """
    
    def __init__(self, cfg: RiskConfig):
        """初始化风险引擎
        
        Args:
            cfg: 风险配置
        """
        self.cfg = cfg
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate risk configuration on initialization."""
        trigger = float(getattr(self.cfg, 'drawdown_trigger', 0.0) or 0.0)
        delever = float(getattr(self.cfg, 'drawdown_delever', 1.0) or 1.0)
        
        if not (0 < trigger < 1):
            log.warning("Invalid drawdown_trigger: %s, should be between 0 and 1", trigger)
        if not (0 < delever <= 1):
            log.warning("Invalid drawdown_delever: %s, should be between 0 and 1", delever)

    def apply(self, position_state: PositionState) -> RiskDecision:
        """Backwards-compatible API using PositionState."""
        try:
            peak = float(position_state.equity_peak_usdt or 0.0)
            eq = float(position_state.equity_usdt or 0.0)
        except (TypeError, ValueError) as e:
            log.error("Invalid position state values: %s", e)
            return RiskDecision(delever_mult=1.0, reason="invalid_state")
            
        if peak <= 0:
            return RiskDecision(delever_mult=1.0, reason="no_peak")
        
        if eq < 0:
            log.warning("Negative equity detected: %s", eq)
            eq = 0.0

        dd = (peak - eq) / peak
        
        # Clamp dd to reasonable range
        dd = max(0.0, min(dd, 1.0))
        
        try:
            trigger = float(self.cfg.drawdown_trigger)
            if dd > trigger:
                delever = float(self.cfg.drawdown_delever)
                return RiskDecision(delever_mult=delever, reason=f"drawdown={dd:.2%}>{trigger:.2%}")
        except (TypeError, ValueError) as e:
            log.error("Invalid risk config values: %s", e)
            
        return RiskDecision(delever_mult=1.0, reason="ok")

    def exposure_multiplier(self, drawdown_pct: float) -> float:
        """计算风险调整后的暴露乘数
        
        Args:
            drawdown_pct: 回撤百分比 (0-1)
            
        Returns:
            暴露乘数 (0-1)，回撤超过阈值时降低
        """
        try:
            dd = float(drawdown_pct)
            # Clamp to reasonable range
            dd = max(0.0, min(dd, 1.0))
            trigger = float(self.cfg.drawdown_trigger)
            if dd > trigger:
                return float(self.cfg.drawdown_delever)
        except (TypeError, ValueError) as e:
            log.error("Invalid drawdown_pct: %s", e)
        return 1.0
