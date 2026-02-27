#!/usr/bin/env python3
"""
V5 系统健康检查脚本

检查项：
- 定时任务最近执行时间
- 数据库连接状态
- OKX API延迟
- 磁盘空间
"""

import json
import sqlite3
import os
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import requests

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
WORKSPACE = Path('/home/admin/clawd/v5-trading-bot')

class HealthChecker:
    def __init__(self):
        self.checks = []
        self.status = 'healthy'  # healthy, warning, critical
    
    def check_timer_health(self):
        """检查定时任务最近执行时间"""
        timers = [
            ('v5-live-20u', 70),      # 每小时执行，允许70分钟延迟
            ('v5-reconcile', 10),      # 每5分钟执行，允许10分钟延迟
            ('v5-trade-auditor', 70),  # 每小时执行
        ]
        
        issues = []
        for timer_name, max_delay_min in timers:
            try:
                # 获取timer状态
                result = subprocess.run(
                    ['systemctl', '--user', 'show', f'{timer_name}.user.timer', '--property=LastTriggerUSec'],
                    capture_output=True, text=True, timeout=5
                )
                
                last_trigger = None
                for line in result.stdout.split('\n'):
                    if line.startswith('LastTriggerUSec='):
                        val = line.split('=', 1)[1].strip()
                        if val and val != 'n/a':
                            # 解析时间戳
                            try:
                                last_trigger = datetime.fromtimestamp(int(val) / 1_000_000)
                            except:
                                pass
                
                if last_trigger:
                    delay = (datetime.now() - last_trigger).total_seconds() / 60
                    if delay > max_delay_min:
                        issues.append({
                            'timer': timer_name,
                            'last_run': last_trigger.strftime('%Y-%m-%d %H:%M'),
                            'delay_min': round(delay, 1),
                            'status': 'stalled' if delay > max_delay_min * 2 else 'delayed'
                        })
            except Exception as e:
                issues.append({'timer': timer_name, 'error': str(e)})
        
        return {
            'name': '定时任务',
            'status': 'critical' if any(i.get('status') == 'stalled' for i in issues) else ('warning' if issues else 'healthy'),
            'details': issues if issues else '所有定时任务正常运行'
        }
    
    def check_database_health(self):
        """检查数据库状态"""
        checks = []
        
        # 检查orders.sqlite
        orders_db = REPORTS_DIR / 'orders.sqlite'
        if orders_db.exists():
            try:
                conn = sqlite3.connect(str(orders_db))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM orders")
                count = cursor.fetchone()[0]
                conn.close()
                
                # 检查文件大小
                size_mb = orders_db.stat().st_size / (1024 * 1024)
                
                checks.append({
                    'db': 'orders.sqlite',
                    'records': count,
                    'size_mb': round(size_mb, 2),
                    'status': 'healthy'
                })
            except Exception as e:
                checks.append({
                    'db': 'orders.sqlite',
                    'error': str(e),
                    'status': 'critical'
                })
        
        return {
            'name': '数据库',
            'status': 'critical' if any(c.get('status') == 'critical' for c in checks) else 'healthy',
            'details': checks
        }
    
    def check_okx_api(self):
        """检查OKX API延迟"""
        try:
            import hmac
            import hashlib
            import base64
            from dotenv import load_dotenv
            
            load_dotenv(str(WORKSPACE / '.env'))
            key = os.getenv('EXCHANGE_API_KEY')
            sec = os.getenv('EXCHANGE_API_SECRET')
            pp = os.getenv('EXCHANGE_PASSPHRASE')
            
            if not (key and sec and pp):
                return {
                    'name': 'OKX API',
                    'status': 'warning',
                    'details': 'API密钥未配置'
                }
            
            start = time.time()
            ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
            path = '/api/v5/account/balance'
            msg = ts + 'GET' + path
            sig = base64.b64encode(hmac.new(sec.encode(), msg.encode(), hashlib.sha256).digest()).decode()
            
            headers = {
                'OK-ACCESS-KEY': key,
                'OK-ACCESS-SIGN': sig,
                'OK-ACCESS-TIMESTAMP': ts,
                'OK-ACCESS-PASSPHRASE': pp,
            }
            
            resp = requests.get('https://www.okx.com' + path, headers=headers, timeout=10)
            latency = (time.time() - start) * 1000  # ms
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == '0':
                    return {
                        'name': 'OKX API',
                        'status': 'healthy',
                        'details': {
                            'latency_ms': round(latency, 1),
                            'status_code': resp.status_code
                        }
                    }
            
            return {
                'name': 'OKX API',
                'status': 'critical',
                'details': {
                    'latency_ms': round(latency, 1),
                    'status_code': resp.status_code,
                    'error': data.get('msg', 'Unknown')
                }
            }
        except Exception as e:
            return {
                'name': 'OKX API',
                'status': 'critical',
                'details': f'请求失败: {str(e)}'
            }
    
    def check_disk_space(self):
        """检查磁盘空间"""
        try:
            result = subprocess.run(['df', '-h', str(WORKSPACE)], capture_output=True, text=True)
            lines = result.stdout.strip().split('\n')
            if len(lines) >= 2:
                parts = lines[1].split()
                if len(parts) >= 6:
                    size = parts[1]
                    used = parts[2]
                    available = parts[3]
                    use_percent = int(parts[4].replace('%', ''))
                    
                    status = 'healthy'
                    if use_percent > 90:
                        status = 'critical'
                    elif use_percent > 80:
                        status = 'warning'
                    
                    # 检查reports目录大小
                    reports_size = subprocess.run(
                        ['du', '-sh', str(REPORTS_DIR)],
                        capture_output=True, text=True
                    ).stdout.split()[0]
                    
                    return {
                        'name': '磁盘空间',
                        'status': status,
                        'details': {
                            'total': size,
                            'used': used,
                            'available': available,
                            'use_percent': use_percent,
                            'reports_size': reports_size
                        }
                    }
        except Exception as e:
            return {
                'name': '磁盘空间',
                'status': 'warning',
                'details': f'检查失败: {str(e)}'
            }
    
    def run_all_checks(self):
        """运行所有检查"""
        self.checks = [
            self.check_timer_health(),
            self.check_database_health(),
            self.check_okx_api(),
            self.check_disk_space()
        ]
        
        # 确定整体状态
        if any(c['status'] == 'critical' for c in self.checks):
            self.status = 'critical'
        elif any(c['status'] == 'warning' for c in self.checks):
            self.status = 'warning'
        else:
            self.status = 'healthy'
        
        return {
            'timestamp': datetime.now().isoformat(),
            'overall_status': self.status,
            'checks': self.checks
        }
    
    def print_report(self):
        """打印报告"""
        result = self.run_all_checks()
        
        print("=" * 60)
        print("🩺 V5 系统健康检查报告")
        print("=" * 60)
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"整体状态: {result['overall_status'].upper()}")
        print()
        
        for check in result['checks']:
            status_emoji = {'healthy': '✅', 'warning': '⚠️', 'critical': '❌'}
            print(f"{status_emoji.get(check['status'], '❓')} {check['name']}: {check['status'].upper()}")
            
            if isinstance(check['details'], list):
                for item in check['details']:
                    if isinstance(item, dict):
                        print(f"   - {item}")
                    else:
                        print(f"   - {item}")
            elif isinstance(check['details'], dict):
                for k, v in check['details'].items():
                    print(f"   {k}: {v}")
            else:
                print(f"   {check['details']}")
            print()
        
        print("=" * 60)
        return result


def main():
    checker = HealthChecker()
    result = checker.print_report()
    
    # 保存报告
    health_file = REPORTS_DIR / 'health_status.json'
    with open(health_file, 'w') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    # 如果有严重问题，返回非0退出码
    if result['overall_status'] == 'critical':
        exit(1)
    exit(0)


if __name__ == '__main__':
    main()
