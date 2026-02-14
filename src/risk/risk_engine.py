from __future__ import annotations

from dataclasses import dataclass

from configs.schema import RiskConfig
from src.core.models import PositionState, RiskDecision


class RiskEngine:
    def __init__(self, cfg: RiskConfig):
        self.cfg = cfg

    def apply(self, position_state: PositionState) -> RiskDecision:
        """Backwards-compatible API using PositionState."""
        peak = float(position_state.equity_peak_usdt or 0.0)
        eq = float(position_state.equity_usdt or 0.0)
        if peak <= 0:
            return RiskDecision(delever_mult=1.0, reason="no_peak")

        dd = (peak - eq) / peak
        if dd > float(self.cfg.drawdown_trigger):
            return RiskDecision(delever_mult=float(self.cfg.drawdown_delever), reason=f"drawdown={dd:.2%}>")
        return RiskDecision(delever_mult=1.0, reason="ok")

    def exposure_multiplier(self, drawdown_pct: float) -> float:
        dd = float(drawdown_pct)
        if dd > float(self.cfg.drawdown_trigger):
            return float(self.cfg.drawdown_delever)
        return 1.0
