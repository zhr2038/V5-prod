"""
Position Builder - 分批建仓系统
实现 dollar-cost averaging (DCA) 策略，降低择时风险
"""

from dataclasses import dataclass
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

@dataclass
class PositionStage:
    """建仓阶段"""
    stage: int  # 0, 1, 2
    target_pct: float  # 目标仓位比例
    filled: bool
    filled_notional: float
    filled_price: float
    filled_time: Optional[datetime]

class PositionBuilder:
    """
    分批建仓管理器
    
    策略：
    - 第一批：30%，信号触发立即买入
    - 第二批：30%，价格下跌2%时抄底买入
    - 第三批：40%，趋势确认后加仓
    """
    
    def __init__(
        self,
        stages: List[float] = None,
        price_drop_threshold: float = 0.02,  # 2%下跌买入第二批
        trend_confirmation_bars: int = 2,     # 2根K线确认趋势
        max_stage_interval_hours: int = 48,   # 最大建仓周期48小时
        state_path: str = "reports/position_builder_state.json",
    ):
        self.stages = stages or [0.3, 0.3, 0.4]
        self.price_drop_threshold = price_drop_threshold
        self.trend_confirmation_bars = trend_confirmation_bars
        self.max_stage_interval_hours = max_stage_interval_hours
        
        # 存储每个币种的建仓状态
        self.position_states: Dict[str, PositionStage] = {}
        self.state_file = self._resolve_state_path(state_path)
        self._load_state()

    @staticmethod
    def _resolve_state_path(state_path: str | Path) -> Path:
        path = Path(state_path)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path
    
    def _load_state(self):
        """加载建仓状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    for sym, state in data.items():
                        self.position_states[sym] = PositionStage(
                            stage=state['stage'],
                            target_pct=state['target_pct'],
                            filled=state['filled'],
                            filled_notional=state['filled_notional'],
                            filled_price=state['filled_price'],
                            filled_time=datetime.fromisoformat(state['filled_time']) if state['filled_time'] else None
                        )
            except Exception:
                pass
    
    def _save_state(self):
        """保存建仓状态"""
        data = {}
        for sym, stage in self.position_states.items():
            data[sym] = {
                'stage': stage.stage,
                'target_pct': stage.target_pct,
                'filled': stage.filled,
                'filled_notional': stage.filled_notional,
                'filled_price': stage.filled_price,
                'filled_time': stage.filled_time.isoformat() if stage.filled_time else None
            }
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_build_notional(
        self,
        symbol: str,
        target_notional: float,
        current_price: float,
        price_history: List[float],
        current_stage: int = None
    ) -> float:
        """
        计算本次应该建仓的金额
        
        Args:
            symbol: 交易对
            target_notional: 目标总金额
            current_price: 当前价格
            price_history: 近期价格历史
            current_stage: 当前阶段（如果已知）
        
        Returns:
            本次应该买入的金额
        """
        if current_stage is None:
            current_stage = self._get_current_stage(symbol)
        
        # 检查是否已完成所有建仓
        if current_stage >= len(self.stages):
            return 0.0
        
        # 检查建仓周期是否超时
        if current_stage > 0:
            last_fill_time = self._get_last_fill_time(symbol)
            if last_fill_time:
                hours_since = (datetime.now() - last_fill_time).total_seconds() / 3600
                if hours_since > self.max_stage_interval_hours:
                    # 超时，强制完成建仓
                    self._complete_position(symbol, current_stage, target_notional, current_price)
                    return 0.0
        
        stage_notional = target_notional * self.stages[current_stage]
        
        # 第一批：立即买入
        if current_stage == 0:
            self._record_fill(symbol, 0, stage_notional, current_price)
            return stage_notional
        
        # 第二批：价格下跌时买入
        if current_stage == 1:
            avg_entry = self._get_avg_entry_price(symbol)
            if avg_entry > 0 and current_price <= avg_entry * (1 - self.price_drop_threshold):
                # 价格已下跌阈值，买入
                self._record_fill(symbol, 1, stage_notional, current_price)
                return stage_notional
            else:
                # 等待更好的价格
                return 0.0
        
        # 第三批：趋势确认后买入
        if current_stage == 2:
            if self._trend_confirmed(price_history):
                self._record_fill(symbol, 2, stage_notional, current_price)
                return stage_notional
            else:
                # 等待趋势确认
                return 0.0
        
        return 0.0
    
    def _get_current_stage(self, symbol: str) -> int:
        """获取当前建仓阶段"""
        if symbol not in self.position_states:
            return 0
        return self.position_states[symbol].stage + 1 if self.position_states[symbol].filled else self.position_states[symbol].stage
    
    def _get_avg_entry_price(self, symbol: str) -> float:
        """获取平均入场价格"""
        # 简化：返回第一批的入场价格
        if symbol in self.position_states:
            return self.position_states[symbol].filled_price
        return 0.0
    
    def _get_last_fill_time(self, symbol: str) -> Optional[datetime]:
        """获取上次建仓时间"""
        if symbol in self.position_states:
            return self.position_states[symbol].filled_time
        return None
    
    def _record_fill(self, symbol: str, stage: int, notional: float, price: float):
        """记录建仓"""
        self.position_states[symbol] = PositionStage(
            stage=stage,
            target_pct=self.stages[stage],
            filled=True,
            filled_notional=notional,
            filled_price=price,
            filled_time=datetime.now()
        )
        self._save_state()
    
    def _complete_position(self, symbol: str, current_stage: int, target_notional: float, current_price: float):
        """强制完成建仓（超时处理）"""
        # 计算剩余应建仓金额
        remaining_pct = sum(self.stages[current_stage:])
        if remaining_pct > 0:
            self._record_fill(symbol, len(self.stages) - 1, target_notional * remaining_pct, current_price)
    
    def _trend_confirmed(self, price_history: List[float]) -> bool:
        """
        确认趋势
        简单逻辑：连续N根K线上涨
        """
        if len(price_history) < self.trend_confirmation_bars + 1:
            return False
        
        recent_prices = price_history[-(self.trend_confirmation_bars + 1):]
        # 检查是否连续上涨
        for i in range(1, len(recent_prices)):
            if recent_prices[i] <= recent_prices[i-1]:
                return False
        return True
    
    def reset_position(self, symbol: str):
        """
        重置币种的建仓状态
        在清仓后调用
        """
        if symbol in self.position_states:
            del self.position_states[symbol]
            self._save_state()
    
    def get_position_summary(self, symbol: str) -> dict:
        """获取建仓状态摘要"""
        if symbol not in self.position_states:
            return {
                'stage': 0,
                'progress': 0.0,
                'avg_entry_price': 0.0,
                'status': 'not_started'
            }
        
        state = self.position_states[symbol]
        total_filled = sum(
            self.stages[i] for i in range(state.stage + 1)
        )
        
        return {
            'stage': state.stage,
            'progress': total_filled,
            'avg_entry_price': state.filled_price,
            'status': 'building' if state.stage < len(self.stages) - 1 else 'completed'
        }

# 集成到Pipeline的用法示例
"""
# 在 pipeline.py 中使用

from src.execution.position_builder import PositionBuilder

class V5Pipeline:
    def __init__(self, cfg, clock=None):
        # ... 其他初始化
        self.position_builder = PositionBuilder(
            stages=[0.3, 0.3, 0.4],
            price_drop_threshold=0.02,
            trend_confirmation_bars=2
        )
    
    def run(self, ...):
        # ... 生成target_weights后
        
        for sym in target_symbols:
            target_notional = target_weights[sym] * equity
            
            # 使用PositionBuilder分批建仓
            build_notional = self.position_builder.get_build_notional(
                symbol=sym,
                target_notional=target_notional,
                current_price=prices[sym],
                price_history=market_data_1h[sym].close  # 历史收盘价
            )
            
            if build_notional > 0:
                # 生成买入订单
                orders.append(Order(
                    symbol=sym,
                    side='buy',
                    notional_usdt=build_notional,
                    ...
                ))
"""
