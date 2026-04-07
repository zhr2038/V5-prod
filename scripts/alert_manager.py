#!/usr/bin/env python3
"""
V5 Telegram 分级告警系统

功能：
- 按严重程度分级告警
- 避免重复告警
- 支持告警静音时段
"""

import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 告警级别
ALERT_LEVELS = {
    'CRITICAL': {'emoji': '🚨', 'cooldown_minutes': 30, 'always_send': True},
    'HIGH': {'emoji': '⚠️', 'cooldown_minutes': 60, 'always_send': False},
    'MEDIUM': {'emoji': '🔔', 'cooldown_minutes': 120, 'always_send': False},
    'INFO': {'emoji': 'ℹ️', 'cooldown_minutes': 240, 'always_send': False}
}

# 静音时段 (24小时制)
QUIET_HOURS = (23, 8)  # 23:00 - 08:00 静音（除非CRITICAL）


class AlertManager:
    """告警管理器"""
    
    def __init__(self, workspace: Path = PROJECT_ROOT):
        self.workspace = Path(workspace).resolve()
        self.reports_dir = self.workspace / 'reports'
        self.alert_state_file = self.reports_dir / 'alert_state.json'
        self.state = self.load_state()
    
    def load_state(self):
        """加载告警状态"""
        if self.alert_state_file.exists():
            try:
                with open(self.alert_state_file) as f:
                    return json.load(f)
            except:
                pass
        return {'last_alerts': {}}
    
    def save_state(self):
        """保存告警状态"""
        self.alert_state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.alert_state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def is_quiet_hours(self):
        """检查是否在静音时段"""
        hour = datetime.now().hour
        start, end = QUIET_HOURS
        if start < end:
            return start <= hour < end
        else:
            return hour >= start or hour < end
    
    def should_send(self, level, alert_type):
        """检查是否应该发送告警"""
        config = ALERT_LEVELS.get(level, ALERT_LEVELS['INFO'])
        
        # CRITICAL级别无视静音时段
        if level == 'CRITICAL':
            return True
        
        # 静音时段不发送非紧急告警
        if self.is_quiet_hours():
            return False
        
        # 检查冷却期
        now = datetime.now()
        key = f"{level}:{alert_type}"
        last_sent = self.state['last_alerts'].get(key)
        
        if last_sent:
            last_time = datetime.fromisoformat(last_sent)
            cooldown = timedelta(minutes=config['cooldown_minutes'])
            if now - last_time < cooldown:
                return False
        
        return True
    
    def record_sent(self, level, alert_type):
        """记录已发送告警"""
        key = f"{level}:{alert_type}"
        self.state['last_alerts'][key] = datetime.now().isoformat()
        self.save_state()
    
    def format_message(self, level, title, message, details=None):
        """格式化告警消息"""
        config = ALERT_LEVELS.get(level, ALERT_LEVELS['INFO'])
        emoji = config['emoji']
        
        text = f"{emoji} *{title}*\n\n"
        text += f"{message}\n\n"
        
        if details:
            text += f"```\n{json.dumps(details, indent=2, ensure_ascii=False)}\n```"
        
        text += f"\n_时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
        
        return text
    
    async def send_alert(self, level, title, message, details=None, alert_type='general'):
        """发送告警"""
        if not self.should_send(level, alert_type):
            print(f"⏸️  告警被抑制（{level}）: {title}")
            return False
        
        # 这里调用实际的Telegram发送逻辑
        # 由于实际发送需要Telegram bot配置，这里只记录
        formatted = self.format_message(level, title, message, details)
        
        print(f"📤 发送{level}告警:")
        print(formatted)
        print("-" * 60)
        
        self.record_sent(level, alert_type)
        return True
    
    # 预定义告警类型
    async def alert_kill_switch(self, reason):
        """Kill Switch触发"""
        return await self.send_alert(
            'CRITICAL',
            'V5 Kill Switch 已触发',
            f'交易已紧急停止\n原因: {reason}',
            alert_type='kill_switch'
        )
    
    async def alert_drawdown(self, drawdown_pct, level):
        """回撤告警"""
        return await self.send_alert(
            'HIGH' if drawdown_pct > 0.15 else 'MEDIUM',
            f'回撤告警 - 当前{drawdown_pct:.1%}',
            f'风险档位: {level}\n建议检查策略暴露',
            {'drawdown': f'{drawdown_pct:.2%}', 'level': level},
            alert_type='drawdown'
        )
    
    async def alert_no_trades(self, hours):
        """长时间无交易"""
        return await self.send_alert(
            'MEDIUM',
            f'长时间无交易 ({hours}小时)',
            '系统可能有异常，请检查定时任务状态',
            alert_type='no_trades'
        )
    
    async def alert_api_error(self, service, error):
        """API错误"""
        return await self.send_alert(
            'HIGH',
            f'{service} API 错误',
            str(error),
            alert_type='api_error'
        )


# 便捷的同步包装函数
def send_alert_sync(level, title, message, details=None, alert_type='general'):
    """同步发送告警"""
    manager = AlertManager()
    asyncio.run(manager.send_alert(level, title, message, details, alert_type))


if __name__ == '__main__':
    # 测试
    async def test():
        manager = AlertManager()
        await manager.alert_drawdown(0.18, 'PROTECT')
        await manager.alert_no_trades(6)
    
    asyncio.run(test())
