#!/usr/bin/env python3
"""
Profit Taking Manager - 程序化利润管理

自动根据盈利水平调整止损和减仓策略
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
from pathlib import Path
import json
from datetime import datetime


@dataclass
class ProfitLevel:
    """利润级别配置"""
    profit_pct: float      # 盈利百分比触发
    action: str            # 动作: 'breakeven', 'partial_sell', 'trailing'
    stop_pct: float        # 止损位置 (相对于成本价)
    sell_pct: float = 0.0  # 减仓比例 (partial_sell时使用)
    trail_buffer: float = 0.0  # 追踪止损缓冲 (trailing时使用)


@dataclass
class PositionProfitState:
    """持仓利润状态"""
    symbol: str
    entry_price: float
    entry_time: datetime
    highest_price: float
    profit_high: float = 0.0  # 最高盈利百分比
    current_stop: float = 0.0
    current_action: str = "hold"
    partial_sold: bool = False
    partial_sell_time: Optional[datetime] = None


class ProfitTakingManager:
    """
    程序化利润管理器
    
    策略:
    1. +10%利润: 止损移到成本价 (保本)
    2. +20%利润: 卖出30%锁定利润，止损移到+10%
    3. +30%利润: 追踪止损，保护70%利润
    4. +50%利润: 卖出50%，剩余追踪止损
    
    同时结合排名: 跌出前3名也触发卖出
    """
    
    def __init__(self):
        # 利润阶梯配置
        self.profit_levels = [
            ProfitLevel(profit_pct=0.10, action='breakeven', stop_pct=0.0),      # 保本
            ProfitLevel(profit_pct=0.20, action='partial_sell', stop_pct=0.10, 
                       sell_pct=0.30, trail_buffer=0.05),  # 卖30%，止损+10%
            ProfitLevel(profit_pct=0.30, action='trailing', stop_pct=0.20,
                       trail_buffer=0.08),  # 追踪止损，保护70%利润
            ProfitLevel(profit_pct=0.50, action='partial_sell', stop_pct=0.35,
                       sell_pct=0.50, trail_buffer=0.10),  # 再卖50%
        ]
        
        self.positions: Dict[str, PositionProfitState] = {}
        self.state_file = Path("reports/profit_taking_state.json")
        self._load_state()
    
    def _load_state(self):
        """加载状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    for sym, state in data.items():
                        self.positions[sym] = PositionProfitState(
                            symbol=state['symbol'],
                            entry_price=state['entry_price'],
                            entry_time=datetime.fromisoformat(state['entry_time']),
                            highest_price=state['highest_price'],
                            profit_high=state.get('profit_high', 0.0),
                            current_stop=state.get('current_stop', 0.0),
                            current_action=state.get('current_action', 'hold'),
                            partial_sold=state.get('partial_sold', False),
                            partial_sell_time=datetime.fromisoformat(state['partial_sell_time']) if state.get('partial_sell_time') else None
                        )
            except Exception as e:
                print(f"[ProfitTaking] 加载状态失败: {e}")
    
    def _save_state(self):
        """保存状态"""
        try:
            data = {}
            for sym, state in self.positions.items():
                data[sym] = {
                    'symbol': state.symbol,
                    'entry_price': state.entry_price,
                    'entry_time': state.entry_time.isoformat(),
                    'highest_price': state.highest_price,
                    'profit_high': state.profit_high,
                    'current_stop': state.current_stop,
                    'current_action': state.current_action,
                    'partial_sold': state.partial_sold,
                    'partial_sell_time': state.partial_sell_time.isoformat() if state.partial_sell_time else None
                }
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[ProfitTaking] 保存状态失败: {e}")
    
    def register_position(self, symbol: str, entry_price: float, current_price: float = None):
        """注册新持仓，如果已存在则更新入场价（避免使用旧价格）"""
        if symbol in self.positions:
            # 已有持仓，检查是否需要更新（价格变化超过1%视为新交易）
            old_entry = self.positions[symbol].entry_price
            price_diff_pct = abs(entry_price - old_entry) / old_entry
            
            if price_diff_pct > 0.01:  # 价格变化超过1%，视为新交易
                print(f"[ProfitTaking] {symbol} 更新入场价: {old_entry:.4f} -> {entry_price:.4f} (变化 {price_diff_pct:.2%})")
                self.positions[symbol].entry_price = entry_price
                self.positions[symbol].entry_time = datetime.now()
                self.positions[symbol].highest_price = current_price or entry_price
                self.positions[symbol].profit_high = 0.0
                self.positions[symbol].current_stop = entry_price * 0.95  # 重新计算止损
                self.positions[symbol].current_action = 'hold'
                self.positions[symbol].partial_sold = False
                self.positions[symbol].partial_sell_time = None
                self._save_state()
        else:
            # 新持仓
            self.positions[symbol] = PositionProfitState(
                symbol=symbol,
                entry_price=entry_price,
                entry_time=datetime.now(),
                highest_price=current_price or entry_price,
                current_stop=entry_price * 0.95  # 初始止损-5%
            )
            self._save_state()
            print(f"[ProfitTaking] {symbol} 注册 @ {entry_price:.4f}, 初始止损 {self.positions[symbol].current_stop:.4f}")
    
    def evaluate(self, symbol: str, current_price: float) -> Tuple[str, float, str]:
        """
        评估持仓，返回建议动作
        
        Returns:
            (action, target_price, reason)
            action: 'hold', 'sell_all', 'sell_partial', 'move_stop'
        """
        if symbol not in self.positions:
            return 'hold', 0, 'not_registered'
        
        state = self.positions[symbol]
        entry = state.entry_price
        
        # 计算当前盈利
        profit_pct = (current_price - entry) / entry
        
        # 更新最高盈利
        if profit_pct > state.profit_high:
            state.profit_high = profit_pct
            state.highest_price = current_price
        
        # 检查是否触发止损
        if current_price <= state.current_stop:
            return 'sell_all', current_price, f'stop_loss_hit_{state.current_action}'
        
        # 检查利润阶梯
        for level in self.profit_levels:
            if profit_pct >= level.profit_pct and state.profit_high >= level.profit_pct:
                # 已触发此级别
                if level.action == 'breakeven' and state.current_action == 'hold':
                    # 保本：止损移到成本价+1%
                    new_stop = entry * 1.01
                    if new_stop > state.current_stop:
                        state.current_stop = new_stop
                        state.current_action = 'breakeven'
                        self._save_state()
                        return 'move_stop', new_stop, f'breakeven_at_{level.profit_pct:.0%}'
                
                elif level.action == 'partial_sell' and not state.partial_sold:
                    # 部分减仓
                    state.partial_sold = True
                    state.partial_sell_time = datetime.now()
                    # 更新止损
                    new_stop = entry * (1 + level.stop_pct)
                    if new_stop > state.current_stop:
                        state.current_stop = new_stop
                        state.current_action = f'partial_{level.profit_pct:.0%}'
                    self._save_state()
                    return 'sell_partial', level.sell_pct, f'profit_{level.profit_pct:.0%}_take_{level.sell_pct:.0%}'
                
                elif level.action == 'trailing':
                    # 追踪止损：最高利润 - buffer
                    trail_stop = state.highest_price * (1 - level.trail_buffer)
                    min_stop = entry * (1 + level.stop_pct)  # 至少保本+stop_pct
                    new_stop = max(trail_stop, min_stop, state.current_stop)
                    
                    if new_stop > state.current_stop:
                        state.current_stop = new_stop
                        state.current_action = f'trailing_{level.profit_pct:.0%}'
                        self._save_state()
                        return 'move_stop', new_stop, f'trail_at_{level.profit_pct:.0%}'
        
        return 'hold', 0, f'profit_{profit_pct:.1%}_holding'
    
    def should_exit_by_rank(self, symbol: str, current_rank: int, max_rank: int = 3) -> Tuple[bool, str]:
        """
        根据排名决定是否退出
        
        Returns:
            (should_exit, reason)
        """
        if symbol not in self.positions:
            return False, 'not_in_positions'
        
        state = self.positions[symbol]
        
        # 如果已经在高利润状态，排名退出可以延后
        if state.profit_high > 0.20:
            # 盈利20%以上，允许排名跌出前5才卖
            if current_rank > 5:
                return True, f'rank_{current_rank}_exceeds_5_with_profit'
            return False, f'rank_{current_rank}_but_high_profit'
        
        # 正常情况：跌出前3就卖
        if current_rank > max_rank:
            return True, f'rank_{current_rank}_exceeds_{max_rank}'
        
        return False, f'rank_{current_rank}_ok'
    
    def get_position_summary(self, symbol: str, current_price: float) -> dict:
        """获取持仓摘要"""
        if symbol not in self.positions:
            return None
        
        state = self.positions[symbol]
        entry = state.entry_price
        profit_pct = (current_price - entry) / entry
        
        return {
            'symbol': symbol,
            'entry': entry,
            'current': current_price,
            'profit_pct': profit_pct,
            'profit_high': state.profit_high,
            'stop_price': state.current_stop,
            'stop_distance': (current_price - state.current_stop) / current_price,
            'action': state.current_action,
            'days_held': (datetime.now() - state.entry_time).days
        }
    
    def clear_position(self, symbol: str):
        """清除持仓（卖出后调用）"""
        if symbol in self.positions:
            del self.positions[symbol]
            self._save_state()


# 使用示例
if __name__ == '__main__':
    pm = ProfitTakingManager()
    
    # 模拟DOT持仓
    pm.register_position('DOT/USDT', 1.30, 1.63)
    
    # 评估
    action, value, reason = pm.evaluate('DOT/USDT', 1.63)
    print(f"DOT @ 1.63: {action}, {value}, {reason}")
    
    # 模拟价格下跌
    action, value, reason = pm.evaluate('DOT/USDT', 1.31)
    print(f"DOT @ 1.31: {action}, {value}, {reason}")
    
    # 排名检查
    should_exit, reason = pm.should_exit_by_rank('DOT/USDT', 5)
    print(f"DOT rank 5: exit={should_exit}, {reason}")
