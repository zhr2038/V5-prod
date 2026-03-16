#!/usr/bin/env python3
"""
V5 交易监控报警系统
检查交易状态，异常时通过 Telegram 通知
"""

import json
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import sys
import re

# 配置
V5_DIR = Path("/home/admin/clawd/v5-prod")
REPORTS_DIR = V5_DIR / "reports"
LOGS_DIR = V5_DIR / "logs"
DB_PATH = V5_DIR / "data" / "v5_live.db"

# 报警阈值
ALERT_THRESHOLDS = {
    "no_trade_hours": 6,  # 6小时无交易报警
    "no_trade_critical": 12,  # 12小时无交易严重报警
    "borrow_detected": True,  # 检测到借贷立即报警
    "preflight_abort": True,  # preflight失败立即报警
    "consecutive_errors": 3,  # 连续3次错误报警
}

TELEGRAM_CHAT_ID = "5065024131"


def run_command(cmd, timeout=30):
    """运行命令并返回输出（兼容Python 3.6）"""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate(timeout=timeout)
        return stdout.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"[ERROR] 命令执行失败: {e}")
        return ""


def get_last_trade_time():
    """获取最后一笔交易时间"""
    try:
        # 从 fills 表查询
        if DB_PATH.exists():
            conn = sqlite3.connect(str(DB_PATH))
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(ts) FROM fills")
            result = cursor.fetchone()
            conn.close()
            if result and result[0]:
                return datetime.fromtimestamp(result[0] / 1000)
    except Exception as e:
        print(f"[ERROR] 查询数据库失败: {e}")
    
    # 回退：从日志解析
    try:
        output = run_command([
            "journalctl", "--user", "-u", "v5-prod.user.service", 
            "--since", "12 hours ago", "--no-pager", "-n", "1000"
        ])
        
        for line in reversed(output.split('\n')):
            if 'FILLS_SYNC new_fills=' in line and 'new_fills=0' not in line:
                # 解析时间
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        current_year = datetime.now().year
                        date_str = f"{parts[0]} {parts[1]} {parts[2]} {current_year}"
                        return datetime.strptime(date_str, "%b %d %H:%M:%S %Y")
                    except:
                        continue
    except Exception as e:
        print(f"[ERROR] 查询日志失败: {e}")
    
    return None


def get_recent_errors():
    """获取最近的错误信息"""
    errors = []
    try:
        output = run_command([
            "journalctl", "--user", "-u", "v5-prod.user.service",
            "--since", "2 hours ago", "--no-pager", "-n", "500"
        ])
        
        # 检查各类错误
        if "borrow_detected" in output:
            count = output.count("borrow_detected")
            if count > 0:
                errors.append(f"检测到借贷阻塞 (近2小时出现{count}次)")
        
        if "ABORT" in output:
            count = output.count("ABORT")
            if count > 0:
                errors.append(f"交易被中止 (近2小时出现{count}次)")
        
        if "RuntimeError" in output:
            count = output.count("RuntimeError")
            if count > 0:
                errors.append(f"运行时错误 (近2小时出现{count}次)")
            
    except Exception as e:
        print(f"[ERROR] 检查错误日志失败: {e}")
    
    return errors


def get_recent_trades_count():
    """获取最近6小时成交次数"""
    try:
        output = run_command([
            "journalctl", "--user", "-u", "v5-prod.user.service",
            "--since", "6 hours ago", "--no-pager"
        ])
        
        # 统计成交次数
        trades = output.count("FILLS_SYNC new_fills=")
        total_fills = 0
        for line in output.split('\n'):
            if 'FILLS_SYNC new_fills=' in line:
                try:
                    match = re.search(r'new_fills=(\d+)', line)
                    if match:
                        total_fills += int(match.group(1))
                except:
                    continue
        return trades, total_fills
    except Exception as e:
        print(f"[ERROR] 统计交易次数失败: {e}")
        return 0, 0


def send_telegram_alert(message, priority="normal"):
    """发送Telegram报警"""
    try:
        emoji = "🚨" if priority == "critical" else "⚠️" if priority == "warning" else "ℹ️"
        full_message = f"{emoji} V5监控报警\n\n{message}\n\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        # 读取bot token
        env_file = V5_DIR / ".env"
        bot_token = None
        chat_id = TELEGRAM_CHAT_ID
        
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    if line.startswith("TELEGRAM_BOT_TOKEN="):
                        bot_token = line.strip().split("=", 1)[1]
                    if line.startswith("TELEGRAM_CHAT_ID="):
                        chat_id = line.strip().split("=", 1)[1]
        
        if bot_token:
            # 使用curl发送
            import urllib.parse
            encoded_msg = urllib.parse.quote(full_message)
            cmd = [
                "curl", "-s", "-X", "POST",
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                "-d", f"chat_id={chat_id}",
                "-d", f"text={encoded_msg}"
            ]
            result = subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result == 0:
                print(f"[INFO] 报警已发送")
                return True
        
        # 回退：写入文件
        alert_file = V5_DIR / "reports" / "monitor_alert.txt"
        with open(alert_file, 'w') as f:
            f.write(full_message)
        print(f"[INFO] 报警已记录到文件")
        return True
        
    except Exception as e:
        print(f"[ERROR] 发送报警失败: {e}")
        return False


def check_and_alert():
    """主检查函数"""
    alerts = []
    priority = "normal"
    
    # 1. 检查交易活跃度
    last_trade = get_last_trade_time()
    trade_runs, total_fills = get_recent_trades_count()
    
    if last_trade:
        hours_since_trade = (datetime.now() - last_trade).total_seconds() / 3600
        if hours_since_trade >= ALERT_THRESHOLDS["no_trade_critical"]:
            alerts.append(f"🔴 严重：已 {hours_since_trade:.1f} 小时无交易")
            priority = "critical"
        elif hours_since_trade >= ALERT_THRESHOLDS["no_trade_hours"]:
            alerts.append(f"🟡 警告：已 {hours_since_trade:.1f} 小时无交易")
            priority = "warning"
    else:
        alerts.append("🟡 无法获取最近交易时间")
    
    # 2. 检查交易执行次数
    if trade_runs == 0:
        alerts.append("🟡 最近6小时无交易轮次")
    elif total_fills == 0 and trade_runs > 0:
        alerts.append(f"ℹ️ 最近6小时有{trade_runs}轮运行但无成交")
    
    # 3. 检查错误
    errors = get_recent_errors()
    if errors:
        alerts.extend(errors)
        if any("借贷" in e or "中止" in e for e in errors):
            priority = "critical"
    
    # 4. 检查当前状态
    try:
        regime_file = REPORTS_DIR / "regime.json"
        if regime_file.exists():
            regime = json.loads(regime_file.read_text())
            state = regime.get("state", "unknown")
            if state == "Risk-Off":
                alerts.append(f"ℹ️ 当前市场状态: {state} (谨慎交易)")
    except Exception as e:
        pass
    
    # 发送报警
    if alerts:
        message = "\n".join(alerts)
        message += f"\n\n📊 统计: 近6小时{trade_runs}轮运行, {total_fills}笔成交"
        send_telegram_alert(message, priority)
        return True
    
    print(f"[OK] {datetime.now().strftime('%H:%M')} 检查通过，无异常")
    return False


if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] V5监控检查开始...")
    
    # 参数解析
    if "--silent" in sys.argv:
        has_alert = check_and_alert()
        sys.exit(0 if not has_alert else 1)
    else:
        check_and_alert()
