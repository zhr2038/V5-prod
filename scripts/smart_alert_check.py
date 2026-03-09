#!/usr/bin/env python3
"""
智能告警发送脚本
- 运行告警检测
- 如有告警，通过Telegram发送
- 无告警则静默退出
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

from pathlib import Path
from src.monitoring.smart_alert import SmartAlertEngine


def send_telegram_alert(alert: dict):
    """发送Telegram告警"""
    try:
        import requests
        
        # 从环境变量读取配置
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            print("[Alert] Telegram config not found, skipping send")
            return False
        
        level_emoji = {
            'critical': '🚨',
            'high': '⚠️',
            'medium': '📊',
            'low': 'ℹ️'
        }
        
        emoji = level_emoji.get(alert['level'], '🔔')
        
        message = f"""
{emoji} <b>V5智能告警</b>

<b>{alert['title']}</b>

{alert['message']}

💡 <b>建议:</b> {alert['suggestion']}

⏰ {alert.get('time', '')}
        """.strip()
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print(f"[Alert] Sent: {alert['title']}")
            return True
        else:
            print(f"[Alert] Failed to send: {response.text}")
            return False
            
    except Exception as e:
        print(f"[Alert] Send error: {e}")
        return False


def main():
    """主函数"""
    print("[SmartAlert] Starting alert check...")
    
    # 运行检测
    engine = SmartAlertEngine()
    alerts = engine.run_all_checks()
    
    if not alerts:
        print("[SmartAlert] No alerts, exiting silently")
        return 0
    
    print(f"[SmartAlert] Found {len(alerts)} alert(s)")
    
    # 发送告警
    sent_count = 0
    for alert in alerts:
        alert['time'] = __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if send_telegram_alert(alert):
            sent_count += 1
    
    print(f"[SmartAlert] Sent {sent_count}/{len(alerts)} alert(s)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
