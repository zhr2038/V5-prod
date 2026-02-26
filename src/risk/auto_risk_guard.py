#!/usr/bin/env python3
"""
Auto Risk Guard - 自动风险档位管理器

根据市场状态和账户表现自动切换风险档位
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class RiskLevel:
    """风险档位配置"""
    name: str                                    # 档位名称
    pos_mult_sideways: float                     # 震荡仓位倍数
    pos_mult_trending: float                     # 趋势仓位倍数
    deadband_sideways: float                     # 震荡调仓死区
    min_trade_notional_base: float               # 最小下单额
    drawdown_trigger: float                      # 回撤触发线
    drawdown_delever: float                      # 回撤降仓比例
    score_threshold_pct: float                   # 信号阈值（百分比）
    max_positions: int                           # 最大持仓数
    cooldown_hours: int                          # 同币冷却时间
    description: str                             # 描述


class AutoRiskGuard:
    """
    自动风险档位管理器
    
    档位:
    - ATTACK (进攻): 市场趋势明确，账户表现良好
    - NEUTRAL (中性): 正常震荡，标准参数
    - DEFENSE (防守): 回撤扩大或噪声增加，降低风险
    - PROTECT (保护): 大幅回撤或连续亏损，接近空仓
    """
    
    # 档位定义
    LEVELS = {
        'ATTACK': RiskLevel(
            name='ATTACK',
            pos_mult_sideways=0.85,
            pos_mult_trending=1.3,
            deadband_sideways=0.03,
            min_trade_notional_base=2.0,
            drawdown_trigger=0.15,
            drawdown_delever=0.80,
            score_threshold_pct=0.0,    # 接受所有信号
            max_positions=5,
            cooldown_hours=2,
            description='趋势明确，积极进攻'
        ),
        'NEUTRAL': RiskLevel(
            name='NEUTRAL',
            pos_mult_sideways=0.70,
            pos_mult_trending=1.2,
            deadband_sideways=0.035,
            min_trade_notional_base=2.5,
            drawdown_trigger=0.12,
            drawdown_delever=0.60,
            score_threshold_pct=0.10,   # 前10%信号
            max_positions=4,
            cooldown_hours=3,
            description='正常震荡，标准操作'
        ),
        'DEFENSE': RiskLevel(
            name='DEFENSE',
            pos_mult_sideways=0.50,
            pos_mult_trending=0.90,
            deadband_sideways=0.04,
            min_trade_notional_base=3.0,
            drawdown_trigger=0.08,
            drawdown_delever=0.50,
            score_threshold_pct=0.20,   # 前20%信号（更高门槛）
            max_positions=3,
            cooldown_hours=4,
            description='回撤扩大，降低风险'
        ),
        'PROTECT': RiskLevel(
            name='PROTECT',
            pos_mult_sideways=0.20,
            pos_mult_trending=0.50,
            deadband_sideways=0.05,
            min_trade_notional_base=5.0,
            drawdown_trigger=0.05,
            drawdown_delever=0.30,
            score_threshold_pct=0.30,   # 前30%信号（只做强信号）
            max_positions=2,
            cooldown_hours=6,
            description='大幅回撤，优先保本金'
        ),
    }
    
    def __init__(self, state_path: str = None):
        if state_path is None:
            # 使用绝对路径，避免工作目录问题
            base_dir = Path(__file__).parent.parent.parent
            state_path = base_dir / "reports" / "auto_risk_guard.json"
        self.state_path = Path(state_path)
        self.current_level = 'NEUTRAL'
        self.history: List[Dict] = []
        self.metrics = {
            'consecutive_loss_rounds': 0,
            'consecutive_noise_rounds': 0,
            'last_dd_pct': 0.0,
            'last_conversion_rate': 0.0,
        }
        self._load_state()
    
    def _load_state(self):
        """加载状态"""
        if self.state_path.exists():
            try:
                with open(self.state_path, 'r') as f:
                    data = json.load(f)
                    self.current_level = data.get('current_level', 'NEUTRAL')
                    self.history = data.get('history', [])
                    self.metrics = data.get('metrics', self.metrics)
            except Exception as e:
                print(f"[AutoRiskGuard] 加载状态失败: {e}")
    
    def _save_state(self):
        """保存状态"""
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_path, 'w') as f:
                json.dump({
                    'current_level': self.current_level,
                    'current_config': asdict(self.LEVELS[self.current_level]),
                    'metrics': self.metrics,
                    'history': self.history[-50:],  # 保留最近50条
                    'last_update': datetime.now().isoformat(),
                }, f, indent=2, default=str)
        except Exception as e:
            print(f"[AutoRiskGuard] 保存状态失败: {e}")
    
    def evaluate(self, 
                 dd_pct: float,                    # 当前回撤
                 conversion_rate: float,           # 成交转化率
                 dust_reject_rate: float,          # dust拒单率
                 recent_pnl_trend: str,            # 最近盈亏趋势 'up'|'down'|'flat'
                 consecutive_losses: int = 0,      # 连续亏损轮数
                 ) -> Tuple[str, RiskLevel, str]:
        """
        评估并返回建议档位
        
        Returns:
            (level_name, level_config, reason)
        """
        old_level = self.current_level
        new_level = old_level
        reasons = []
        
        # 更新指标
        self.metrics['last_dd_pct'] = dd_pct
        self.metrics['last_conversion_rate'] = conversion_rate
        
        # 降级条件（优先级高）
        if dd_pct >= 0.12:
            new_level = 'PROTECT'
            reasons.append(f"大幅回撤{dd_pct:.1%}，进入保护模式")
        elif dd_pct >= 0.08:
            new_level = 'DEFENSE'
            reasons.append(f"回撤扩大{dd_pct:.1%}，降低风险")
        elif dust_reject_rate > 0.5 and conversion_rate < 0.3:
            new_level = 'DEFENSE'
            reasons.append(f"噪声交易过高（拒单{dust_reject_rate:.0%}），降低频率")
        elif consecutive_losses >= 3:
            new_level = 'DEFENSE'
            reasons.append(f"连续{consecutive_losses}轮亏损，防守为主")
        
        # 升级条件（需要更严格的确认）
        if new_level == old_level or self._is_lower_level(new_level, old_level):
            if old_level in ['DEFENSE', 'PROTECT']:
                if dd_pct < 0.05 and conversion_rate > 0.5 and recent_pnl_trend == 'up':
                    if consecutive_losses == 0:
                        new_level = 'NEUTRAL'
                        reasons.append("回撤控制，成交改善，恢复中性")
        
        # 执行切换
        if new_level != old_level:
            self.current_level = new_level
            self.history.append({
                'ts': datetime.now().isoformat(),
                'from': old_level,
                'to': new_level,
                'reason': '; '.join(reasons),
                'metrics': dict(self.metrics),
            })
            self._save_state()
            print(f"[AutoRiskGuard] {old_level} -> {new_level}: {'; '.join(reasons)}")
        
        return new_level, self.LEVELS[new_level], '; '.join(reasons) if reasons else '维持当前档位'
    
    def _is_lower_level(self, level1: str, level2: str) -> bool:
        """判断level1是否比level2更保守（风险更低）"""
        order = ['ATTACK', 'NEUTRAL', 'DEFENSE', 'PROTECT']
        return order.index(level1) > order.index(level2)
    
    def get_current_config(self) -> Dict:
        """获取当前档位配置"""
        return asdict(self.LEVELS[self.current_level])
    
    def force_level(self, level: str, reason: str = "manual"):
        """强制切换到指定档位"""
        if level not in self.LEVELS:
            raise ValueError(f"Unknown level: {level}")
        old = self.current_level
        self.current_level = level
        self.history.append({
            'ts': datetime.now().isoformat(),
            'from': old,
            'to': level,
            'reason': f"[FORCE] {reason}",
            'metrics': dict(self.metrics),
        })
        self._save_state()
        print(f"[AutoRiskGuard] [FORCE] {old} -> {level}: {reason}")


# 全局实例
_guard_instance: Optional[AutoRiskGuard] = None

def get_auto_risk_guard() -> AutoRiskGuard:
    """获取全局风险守卫实例"""
    global _guard_instance
    if _guard_instance is None:
        _guard_instance = AutoRiskGuard()
    return _guard_instance


if __name__ == '__main__':
    # 测试
    guard = AutoRiskGuard()
    level, config, reason = guard.evaluate(
        dd_pct=0.15,
        conversion_rate=0.25,
        dust_reject_rate=0.6,
        recent_pnl_trend='down',
        consecutive_losses=2
    )
    print(f"建议档位: {level}")
    print(f"配置: {config}")
    print(f"原因: {reason}")
