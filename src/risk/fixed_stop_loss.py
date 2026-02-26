#!/usr/bin/env python3
"""
固定比例止损 - 针对新买入仓位的硬性止损

解决：买入后如果市场反向，在固定亏损比例时立即出局
"""

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Dict, Optional
from datetime import datetime


@dataclass
class FixedStopLossConfig:
    """固定比例止损配置"""
    enabled: bool = True
    # 基础止损比例（买入后立即生效）
    base_stop_pct: float = 0.05  # 5%止损
    
    # 可以按币种设置不同止损
    symbol_stops: Dict[str, float] = None
    
    def __post_init__(self):
        if self.symbol_stops is None:
            self.symbol_stops = {
                'BTC/USDT': 0.04,  # BTC波动小，4%止损
                'ETH/USDT': 0.045, # ETH 4.5%
            }
    
    def get_stop_pct(self, symbol: str) -> float:
        """获取指定币种的止损比例"""
        return self.symbol_stops.get(symbol, self.base_stop_pct)


class FixedStopLossManager:
    """
    固定比例止损管理器
    
    与多级动态止损配合使用：
    - FixedStopLoss: 买入后立即生效的硬性止损（如-5%）
    - MultiLevelStopLoss: 盈利后的动态止损（保本/追踪）
    """
    
    def __init__(self, config: FixedStopLossConfig = None):
        self.config = config or FixedStopLossConfig()
        self.entry_prices: Dict[str, float] = {}  # symbol -> entry_price
        self.entry_times: Dict[str, datetime] = {}  # symbol -> entry_time
        self.state_file = Path("reports/fixed_stop_loss_state.json")
        self._load_state()
    
    def _load_state(self):
        """加载状态"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    for sym, state in data.items():
                        self.entry_prices[sym] = state['entry_price']
                        self.entry_times[sym] = datetime.fromisoformat(state['entry_time'])
            except Exception as e:
                print(f"[FixedStopLoss] 加载状态失败: {e}")
    
    def _save_state(self):
        """保存状态"""
        try:
            data = {}
            for sym in self.entry_prices:
                data[sym] = {
                    'entry_price': self.entry_prices[sym],
                    'entry_time': self.entry_times[sym].isoformat()
                }
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"[FixedStopLoss] 保存状态失败: {e}")
    
    def register_position(self, symbol: str, entry_price: float):
        """注册新持仓（买入时调用）"""
        if not self.config.enabled:
            return
        
        self.entry_prices[symbol] = entry_price
        self.entry_times[symbol] = datetime.now()
        self._save_state()
        stop_pct = self.config.get_stop_pct(symbol)
        stop_price = entry_price * (1 - stop_pct)
        print(f"[FixedStopLoss] {symbol} 买入 @ {entry_price:.4f}, 止损位 @ {stop_price:.4f} ({stop_pct*100:.1f}%)")
    
    def should_stop_loss(self, symbol: str, current_price: float) -> tuple:
        """
        检查是否应该止损
        
        Returns:
            (should_stop: bool, stop_price: float, loss_pct: float)
        """
        if not self.config.enabled:
            return False, 0, 0
        
        if symbol not in self.entry_prices:
            return False, 0, 0
        
        entry_price = self.entry_prices[symbol]
        stop_pct = self.config.get_stop_pct(symbol)
        stop_price = entry_price * (1 - stop_pct)
        
        loss_pct = (current_price - entry_price) / entry_price
        
        if current_price <= stop_price:
            return True, stop_price, loss_pct
        
        return False, stop_price, loss_pct
    
    def clear_position(self, symbol: str):
        """清除持仓（卖出后调用）"""
        if symbol in self.entry_prices:
            del self.entry_prices[symbol]
            del self.entry_times[symbol]
            self._save_state()
    
    def get_position_info(self, symbol: str) -> dict:
        """获取持仓止损信息"""
        if symbol not in self.entry_prices:
            return None
        
        entry = self.entry_prices[symbol]
        stop_pct = self.config.get_stop_pct(symbol)
        return {
            'symbol': symbol,
            'entry_price': entry,
            'stop_price': entry * (1 - stop_pct),
            'stop_pct': stop_pct,
            'entry_time': self.entry_times.get(symbol)
        }


# 简化函数接口
def create_fixed_stop_loss(base_pct: float = 0.05) -> FixedStopLossManager:
    """创建固定比例止损管理器"""
    config = FixedStopLossConfig(
        enabled=True,
        base_stop_pct=base_pct
    )
    return FixedStopLossManager(config)
