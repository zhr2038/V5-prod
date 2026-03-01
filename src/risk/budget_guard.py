"""
预算限制模块 - 确保总权益不超过设定上限
"""
import logging
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)


class BudgetGuard:
    """
    预算守卫 - 硬限制总资金暴露
    """
    
    def __init__(self, equity_cap_usdt: float = 20.0):
        self.equity_cap = Decimal(str(equity_cap_usdt))
        self.warning_threshold = self.equity_cap * Decimal('0.9')  # 90% 警告
        
    def check_equity(self, current_equity: float) -> dict:
        """
        检查当前权益是否超过限制
        
        Returns:
            {
                'allowed': bool,      # 是否允许继续交易
                'exceeded': bool,     # 是否已超限
                'current': float,     # 当前权益
                'cap': float,         # 上限
                'utilization': float, # 使用率
                'action': str         # 建议操作
            }
        """
        equity = Decimal(str(current_equity))
        utilization = equity / self.equity_cap
        
        result = {
            'allowed': True,
            'exceeded': False,
            'current': float(equity),
            'cap': float(self.equity_cap),
            'utilization': float(utilization),
            'action': 'normal'
        }
        
        if equity > self.equity_cap:
            result['allowed'] = False
            result['exceeded'] = True
            result['action'] = 'stop_all_trading'
            logger.error(f"🚨 预算超限! {equity:.2f} > {self.equity_cap:.2f} USDT. 立即停止交易!")
            
        elif equity > self.warning_threshold:
            result['action'] = 'warning'
            logger.warning(f"⚠️ 预算接近上限: {equity:.2f} / {self.equity_cap:.2f} USDT ({float(utilization)*100:.0f}%)")
            
        else:
            logger.info(f"✅ 预算正常: {equity:.2f} / {self.equity_cap:.2f} USDT ({float(utilization)*100:.0f}%)")
            
        return result
    
    def calculate_max_buy(self, current_equity: float, current_position_value: float) -> float:
        """
        计算最大可买入金额
        
        Returns:
            最大可买入金额 (USDT)
        """
        equity = Decimal(str(current_equity))
        position = Decimal(str(current_position_value))
        
        # 剩余预算
        remaining = self.equity_cap - equity
        
        # 如果已超限，返回0
        if remaining <= 0:
            logger.error(f"预算已用完: {equity:.2f} / {self.equity_cap:.2f} USDT")
            return 0.0
        
        # 预留10%缓冲
        max_buy = remaining * Decimal('0.9')
        
        logger.info(f"预算检查: 已用 {equity:.2f} / {self.equity_cap:.2f}, 可买入 {float(max_buy):.2f} USDT")
        return float(max_buy)
