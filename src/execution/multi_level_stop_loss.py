"""
Multi-Level Stop Loss - 多级动态止损系统
实现追踪止损、保本止损、分级止损
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from enum import Enum
import json
from pathlib import Path
from datetime import datetime

class StopLevel(Enum):
    """止损级别"""
    TIGHT = "tight"      # 3% - 快速止损
    NORMAL = "normal"    # 5% - 正常止损
    LOOSE = "loose"      # 8% - 宽松止损（给趋势留空间）

@dataclass
class StopLossConfig:
    """止损配置"""
    tight_pct: float = 0.03
    normal_pct: float = 0.05
    loose_pct: float = 0.08
    
    # 盈利保护阈值
    profit_threshold_1: float = 0.05   # 5%盈利
    profit_threshold_2: float = 0.10   # 10%盈利
    profit_threshold_3: float = 0.15   # 15%盈利
    
    # 追踪止损保护比例
    trailing_protection_1: float = 0.50  # 保护50%利润
    trailing_protection_2: float = 0.70  # 保护70%利润
    trailing_protection_3: float = 0.80  # 保护80%利润
    
    # 保本后缓冲区
    breakeven_buffer: float = 0.01  # 保本后1%缓冲区

@dataclass
class PositionStopState:
    """持仓止损状态"""
    symbol: str
    entry_price: float
    entry_time: datetime
    highest_price: float
    current_stop_price: float
    current_stop_type: str
    
    # 状态跟踪
    is_breakeven: bool = False
    is_trailing: bool = False
    profit_high_watermark: float = 0.0

class MultiLevelStopLoss:
    """
    多级动态止损管理器
    
    策略：
    1. 未盈利：根据市场状态选择3%/5%/8%止损
    2. 盈利5%+：保本（止损移到成本价+1%）
    3. 盈利10%+：保本+5%
    4. 盈利15%+：追踪止损（保护80%利润）
    """
    
    def __init__(self, config: StopLossConfig = None):
        self.config = config or StopLossConfig()
        self.positions: Dict[str, PositionStopState] = {}
        self.state_file = Path("reports/stop_loss_state.json")
        self._load_state()
    
    def _load_state(self):
        """加载止损状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    for sym, state in data.items():
                        self.positions[sym] = PositionStopState(
                            symbol=state['symbol'],
                            entry_price=state['entry_price'],
                            entry_time=datetime.fromisoformat(state['entry_time']),
                            highest_price=state['highest_price'],
                            current_stop_price=state['current_stop_price'],
                            current_stop_type=state['current_stop_type'],
                            is_breakeven=state.get('is_breakeven', False),
                            is_trailing=state.get('is_trailing', False),
                            profit_high_watermark=state.get('profit_high_watermark', 0.0)
                        )
            except Exception as e:
                print(f"Error loading stop loss state: {e}")
    
    def _save_state(self):
        """保存止损状态"""
        data = {}
        for sym, state in self.positions.items():
            data[sym] = {
                'symbol': state.symbol,
                'entry_price': state.entry_price,
                'entry_time': state.entry_time.isoformat(),
                'highest_price': state.highest_price,
                'current_stop_price': state.current_stop_price,
                'current_stop_type': state.current_stop_type,
                'is_breakeven': state.is_breakeven,
                'is_trailing': state.is_trailing,
                'profit_high_watermark': state.profit_high_watermark
            }
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def initialize_position(
        self,
        symbol: str,
        entry_price: float,
        market_state: str = "Sideways"
    ) -> float:
        """
        初始化持仓止损
        
        Args:
            symbol: 交易对
            entry_price: 入场价格
            market_state: 市场状态 (Risk-Off/Sideways/Trending)
        
        Returns:
            初始止损价格
        """
        # 根据市场状态选择止损级别
        if market_state == "Risk-Off":
            stop_pct = self.config.tight_pct  # 3%
            stop_type = "initial_tight"
        elif market_state == "Trending":
            stop_pct = self.config.loose_pct  # 8%
            stop_type = "initial_loose"
        else:  # Sideways
            stop_pct = self.config.normal_pct  # 5%
            stop_type = "initial_normal"
        
        stop_price = entry_price * (1 - stop_pct)
        
        self.positions[symbol] = PositionStopState(
            symbol=symbol,
            entry_price=entry_price,
            entry_time=datetime.now(),
            highest_price=entry_price,
            current_stop_price=stop_price,
            current_stop_type=stop_type,
            is_breakeven=False,
            is_trailing=False,
            profit_high_watermark=0.0
        )
        
        self._save_state()
        return stop_price
    
    def update_stop_price(
        self,
        symbol: str,
        current_price: float,
        market_state: str = None
    ) -> Tuple[float, str, bool]:
        """
        更新止损价格
        
        Args:
            symbol: 交易对
            current_price: 当前价格
            market_state: 当前市场状态（可选）
        
        Returns:
            (新的止损价格, 止损类型, 是否触发止损)
        """
        if symbol not in self.positions:
            raise ValueError(f"Position {symbol} not initialized")
        
        state = self.positions[symbol]
        entry_price = state.entry_price
        
        # 更新最高价（用于追踪止损）
        if current_price > state.highest_price:
            state.highest_price = current_price
        
        # 计算当前盈利
        profit_pct = (current_price - entry_price) / entry_price
        
        # 计算从最高点回撤
        drawdown_from_high = (state.highest_price - current_price) / state.highest_price if state.highest_price > 0 else 0
        
        new_stop_price = state.current_stop_price
        new_stop_type = state.current_stop_type
        triggered = False
        
        # 盈利15%+：追踪止损（保护80%利润）
        if profit_pct >= self.config.profit_threshold_3:
            state.is_trailing = True
            # 止损价 = 最高价 - (最高价 - 成本价) * 20%
            profit_range = state.highest_price - entry_price
            trailing_stop = state.highest_price - profit_range * (1 - self.config.trailing_protection_3)
            
            if trailing_stop > new_stop_price:
                new_stop_price = trailing_stop
                new_stop_type = "trailing_15pct_80protect"
        
        # 盈利10%+：保本+5%
        elif profit_pct >= self.config.profit_threshold_2:
            state.is_breakeven = True
            breakeven_plus = entry_price * (1 + 0.05)
            
            if breakeven_plus > new_stop_price:
                new_stop_price = breakeven_plus
                new_stop_type = "breakeven_plus_5pct"
        
        # 盈利5%+：保本
        elif profit_pct >= self.config.profit_threshold_1:
            state.is_breakeven = True
            breakeven = entry_price * (1 + self.config.breakeven_buffer)
            
            if breakeven > new_stop_price:
                new_stop_price = breakeven
                new_stop_type = "breakeven"
        
        # 未盈利：根据市场状态调整（可选）
        elif market_state:
            # 如果市场状态变化，可以调整止损
            if market_state == "Risk-Off":
                # 收紧止损
                tight_stop = entry_price * (1 - self.config.tight_pct)
                if tight_stop > new_stop_price:
                    new_stop_price = tight_stop
                    new_stop_type = "adjusted_tight_riskoff"
        
        # 检查是否触发止损
        if current_price <= new_stop_price:
            triggered = True
        
        # 更新状态
        state.current_stop_price = new_stop_price
        state.current_stop_type = new_stop_type
        if profit_pct > state.profit_high_watermark:
            state.profit_high_watermark = profit_pct
        
        self._save_state()
        return new_stop_price, new_stop_type, triggered
    
    def should_exit(
        self,
        symbol: str,
        current_price: float,
        reason: str = None
    ) -> Tuple[bool, str, float]:
        """
        判断是否应该出场
        
        Returns:
            (是否出场, 出场原因, 出场价格)
        """
        if symbol not in self.positions:
            return False, "", 0.0
        
        state = self.positions[symbol]
        stop_price, stop_type, triggered = self.update_stop_price(symbol, current_price)
        
        if triggered:
            return True, f"stop_loss_{stop_type}", stop_price
        
        # 其他出场条件（如时间止损）
        # hours_held = (datetime.now() - state.entry_time).total_seconds() / 3600
        # if hours_held > 48 and current_price < state.entry_price:
        #     return True, "time_stop", current_price
        
        return False, "", 0.0
    
    def remove_position(self, symbol: str):
        """
        移除持仓止损状态
        在清仓后调用
        """
        if symbol in self.positions:
            del self.positions[symbol]
            self._save_state()
    
    def get_stop_summary(self, symbol: str, current_price: float) -> dict:
        """获取止损状态摘要"""
        if symbol not in self.positions:
            return {
                'status': 'not_initialized',
                'stop_price': 0.0,
                'stop_type': '',
                'profit_pct': 0.0
            }
        
        state = self.positions[symbol]
        profit_pct = (current_price - state.entry_price) / state.entry_price
        
        return {
            'status': 'active',
            'entry_price': state.entry_price,
            'highest_price': state.highest_price,
            'current_price': current_price,
            'stop_price': state.current_stop_price,
            'stop_type': state.current_stop_type,
            'profit_pct': profit_pct,
            'max_profit_pct': state.profit_high_watermark,
            'is_breakeven': state.is_breakeven,
            'is_trailing': state.is_trailing
        }
    
    def evaluate_stop(self, symbol: str, current_price: float) -> Tuple[bool, float, str, float]:
        """
        评估止损（供pipeline调用）
        
        Returns:
            (是否触发, 止损价格, 止损类型, 盈利百分比)
        """
        if symbol not in self.positions:
            # 未注册，不触发
            return False, 0.0, "not_initialized", 0.0
        
        state = self.positions[symbol]
        
        # 更新最高价
        if current_price > state.highest_price:
            state.highest_price = current_price
        
        entry_price = state.entry_price
        profit_pct = (current_price - entry_price) / entry_price
        
        # 调用update_stop_price计算最新止损
        new_stop_price, new_stop_type, triggered = self.update_stop_price(symbol, current_price)
        
        return triggered, new_stop_price, new_stop_type, profit_pct
    
    def register_position(self, symbol: str, entry_price: float, market_state: str = "Sideways"):
        """注册持仓（兼容pipeline调用）"""
        if symbol not in self.positions:
            self.initialize_position(symbol, entry_price, market_state)

    def batch_update_stops(
        self,
        prices: Dict[str, float],
        market_state: str = None
    ) -> Dict[str, Tuple[bool, str, float]]:
        """
        批量更新所有持仓的止损
        
        Returns:
            {symbol: (是否触发, 原因, 止损价格)}
        """
        results = {}
        for symbol, price in prices.items():
            if symbol in self.positions:
                should_exit, reason, exit_price = self.should_exit(symbol, price)
                results[symbol] = (should_exit, reason, exit_price)
        return results

# 集成到ExitPolicy的用法示例
"""
# 在 exit_policy.py 中使用

from src.execution.multi_level_stop_loss import MultiLevelStopLoss, StopLossConfig

class ExitPolicy:
    def __init__(self, ...):
        self.stop_loss_manager = MultiLevelStopLoss(
            config=StopLossConfig(
                tight_pct=0.03,
                normal_pct=0.05,
                loose_pct=0.08
            )
        )
    
    def on_position_opened(self, symbol, entry_price, market_state):
        # 初始化止损
        stop_price = self.stop_loss_manager.initialize_position(
            symbol, entry_price, market_state
        )
        return stop_price
    
    def check_exit_conditions(self, positions, current_prices, market_state):
        exit_orders = []
        
        for pos in positions:
            symbol = pos.symbol
            current_price = current_prices.get(symbol)
            
            if current_price:
                should_exit, reason, exit_price = self.stop_loss_manager.should_exit(
                    symbol, current_price
                )
                
                if should_exit:
                    exit_orders.append(Order(
                        symbol=symbol,
                        side='sell',
                        intent='STOP_LOSS',
                        reason=reason,
                        ...
                    ))
                    # 移除止损状态
                    self.stop_loss_manager.remove_position(symbol)
        
        return exit_orders
"""
