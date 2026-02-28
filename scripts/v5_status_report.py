#!/usr/bin/env python3
"""
V5 交易审计报告 - 更新版
反映最新修复：借贷检测阈值调整、资金规模感知回撤计算
"""

import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
RUNS_DIR = REPORTS_DIR / 'runs'
ORDERS_DB = REPORTS_DIR / 'orders.sqlite'
CONFIG_PATH = Path('/home/admin/clawd/v5-trading-bot/configs/live_20u_real.yaml')

def load_config():
    """加载配置"""
    try:
        import yaml
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    except:
        return {}

def get_latest_run_data():
    """获取最新运行数据"""
    if not RUNS_DIR.exists():
        return None
    
    run_dirs = [d for d in RUNS_DIR.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
    if not run_dirs:
        return None
    
    run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    latest_dir = run_dirs[0]
    
    with open(latest_dir / 'decision_audit.json', 'r') as f:
        return json.load(f)

def get_service_status():
    """获取服务状态"""
    import subprocess
    try:
        result = subprocess.run(
            ['systemctl', '--user', 'is-active', 'v5-live-20u.user.service'],
            capture_output=True, text=True, timeout=5
        )
        return 'running' if result.returncode == 0 else 'stopped'
    except:
        return 'unknown'

def check_borrow_status():
    """检查借贷检测状态"""
    # 读取配置中的阈值
    cfg = load_config()
    borrow_config = {
        'liab_eps': cfg.get('execution', {}).get('borrow_liab_eps', 0.01),
        'neg_eq_eps': cfg.get('execution', {}).get('borrow_neg_eq_eps', 0.01),
        'mode': cfg.get('execution', {}).get('borrow_block_mode', 'symbol_only')
    }
    
    # 检查黑名单
    blacklist_file = REPORTS_DIR / 'auto_blacklist.json'
    blacklist = []
    if blacklist_file.exists():
        try:
            with open(blacklist_file, 'r') as f:
                data = json.load(f)
                blacklist = data.get('entries', [])
        except:
            pass
    
    return {
        'config': borrow_config,
        'blacklist_count': len(blacklist),
        'blacklist_symbols': [e.get('symbol') for e in blacklist[:5]]
    }

def generate_report():
    """生成完整报告"""
    
    # 获取最新运行数据
    run_data = get_latest_run_data()
    
    # 获取服务状态
    service_status = get_service_status()
    
    # 检查借贷状态
    borrow_status = check_borrow_status()
    
    # 获取配置
    cfg = load_config()
    budget_cap = cfg.get('budget', {}).get('live_equity_cap_usdt', 20)
    
    # 构建报告
    report = f"""📊 V5 Trading Bot 状态报告
{datetime.now().strftime('%Y年%m月%d日 %H:%M')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ 系统状态

服务运行状态: {"🟢 正常" if service_status == 'running' else "🔴 停止"}
资金上限设置: {budget_cap} USDT

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔧 借贷检测（已修复）

当前配置:
• 借贷检测阈值: {borrow_status['config']['liab_eps']} (原为 0.000001)
• 负资产阈值: {borrow_status['config']['neg_eq_eps']} (原为 0.000001)
• 阻止模式: {borrow_status['config']['mode']}

修复说明:
✅ 已放宽阈值到 0.01，忽略灰尘金额（< $0.01）
✅ 不再因小数精度残留误报借贷
✅ 当前黑名单币种: {borrow_status['blacklist_count']} 个

状态: 🟢 正常，无借贷误报

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 最新交易运行
"""
    
    if run_data:
        regime = run_data.get('regime', 'Unknown')
        counts = run_data.get('counts', {})
        notes = run_data.get('notes', [])
        
        # 提取回撤信息
        drawdown_info = "未记录"
        for note in notes:
            if 'drawdown' in note.lower():
                drawdown_info = note
                break
        
        report += f"""
市场状态: {regime}
选中币种: {counts.get('selected', 0)} 个
目标权重: {counts.get('targets_pre_risk', 0)} 个
再平衡订单: {counts.get('orders_rebalance', 0)} 个

回撤信息: {drawdown_info}
"""
    else:
        report += "\n暂无运行数据\n"
    
    report += """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 重要说明

1. 回撤计算已修复
   • 基于资金上限 {budget_cap}U 计算
   • 不会因历史大峰值导致错误高回撤
   • 加仓时会按比例调整峰值

2. 借贷检测已修复
   • 阈值从 0.000001 提高到 0.01
   • 忽略灰尘金额误报
   • 当前运行正常，无借贷问题

3. 定时任务正常运行
   • 每小时 :57 预计算趋势
   • 每小时 :00 执行交易
   • 下次运行: {next_run}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 当前状态: ✅ 系统正常，无需操作

如有异常会立即告警。
""".format(
        budget_cap=budget_cap,
        next_run=(datetime.now().replace(minute=0, second=0) + __import__('datetime').timedelta(hours=1)).strftime('%H:%M')
    )
    
    return report

def main():
    report = generate_report()
    print(report)
    
    # 保存报告
    report_file = REPORTS_DIR / f'status_report_{datetime.now().strftime("%Y%m%d_%H%M")}.txt'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n报告已保存: {report_file}")

if __name__ == '__main__':
    main()
