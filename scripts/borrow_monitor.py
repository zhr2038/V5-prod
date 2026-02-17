#!/usr/bin/env python3
"""
借币监控脚本
定期检查账户是否有借贷，防止意外借币
"""

from __future__ import annotations

import os
import time
import json
from datetime import datetime
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient


def safe_float(value, default=0.0):
    """安全转换为浮点数"""
    if value is None or value == '':
        return default
    try:
        return float(value)
    except:
        return default


class BorrowMonitor:
    """借币监控器"""
    
    def __init__(self, config_path: str = "configs/live_small.yaml"):
        self.cfg = load_config(config_path, env_path=".env")
        self.okx = OKXPrivateClient(exchange=self.cfg.exchange)
        self.state_file = "reports/borrow_monitor_state.json"
        
    def check_borrows(self) -> dict:
        """检查所有币种的借贷情况"""
        resp = self.okx.get_balance()
        account = resp.data['data'][0]
        
        borrows = []
        total_liability_usdt = 0
        
        for d in account['details']:
            ccy = d.get('ccy', '')
            eq = float(d.get('eq', 0))
            liab = float(d.get('liab', 0))
            cross_liab = float(d.get('crossLiab', 0))
            borrow_froz = float(d.get('borrowFroz', 0))
            
            # 检查是否有借贷
            has_borrow = (
                eq < -0.001 or  # 负权益
                liab > 0.001 or  # 有负债
                cross_liab > 0.001 or  # 交叉负债
                borrow_froz > 0.001  # 冻结借款
            )
            
            if has_borrow:
                # 估算 USDT 价值（简化）
                # 实际中应该获取每个币种的价格
                usdt_value = abs(eq) * 0.064 if ccy == 'MERL' else abs(eq)
                
                borrows.append({
                    'ccy': ccy,
                    'eq': eq,
                    'liab': liab,
                    'cross_liab': cross_liab,
                    'borrow_froz': borrow_froz,
                    'usdt_value': usdt_value,
                    'timestamp': int(time.time())
                })
                total_liability_usdt += usdt_value
        
        return {
            'timestamp': int(time.time()),
            'total_liability_usdt': total_liability_usdt,
            'borrows': borrows,
            'account_total_eq': safe_float(account.get('totalEq', 0)),
            'account_avail_eq': safe_float(account.get('availEq', 0))
        }
    
    def load_state(self) -> dict:
        """加载上次检查状态"""
        try:
            with open(self.state_file, 'r') as f:
                return json.load(f)
        except:
            return {'last_check': 0, 'last_borrows': []}
    
    def save_state(self, state: dict):
        """保存当前状态"""
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def check_for_new_borrows(self) -> tuple[bool, list]:
        """检查是否有新的借贷出现"""
        current = self.check_borrows()
        previous = self.load_state()
        
        new_borrows = []
        
        # 检查新的借贷币种
        current_ccys = {b['ccy'] for b in current['borrows']}
        previous_ccys = {b['ccy'] for b in previous.get('last_borrows', [])}
        
        new_ccys = current_ccys - previous_ccys
        if new_ccys:
            for b in current['borrows']:
                if b['ccy'] in new_ccys:
                    new_borrows.append(b)
        
        # 检查借贷金额显著增加
        for curr in current['borrows']:
            for prev in previous.get('last_borrows', []):
                if curr['ccy'] == prev['ccy']:
                    # 如果负债增加超过10%
                    if curr['liab'] > prev['liab'] * 1.1:
                        new_borrows.append(curr)
                    break
        
        # 保存当前状态
        self.save_state({
            'last_check': current['timestamp'],
            'last_borrows': current['borrows']
        })
        
        return len(new_borrows) > 0, new_borrows
    
    def alert_format(self, borrows: list) -> str:
        """格式化警报消息"""
        if not borrows:
            return "✅ No borrows detected"
        
        lines = ["🚨 **BORROW ALERT** 🚨"]
        lines.append(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        for b in borrows:
            lines.append(f"**{b['ccy']}**:")
            lines.append(f"  Equity: {b['eq']:.6f}")
            if b['liab'] > 0:
                lines.append(f"  Liability: {b['liab']:.6f}")
            if b['cross_liab'] > 0:
                lines.append(f"  Cross Liability: {b['cross_liab']:.6f}")
            if b['borrow_froz'] > 0:
                lines.append(f"  Borrow Frozen: {b['borrow_froz']:.6f}")
            lines.append(f"  Est. USDT Value: {b['usdt_value']:.4f}")
            lines.append("")
        
        lines.append("⚠️ **ACTION REQUIRED**:")
        lines.append("1. Check OKX website immediately")
        lines.append("2. Repay any unexpected borrows")
        lines.append("3. Review recent trades")
        
        return "\n".join(lines)


def main():
    """主函数：检查借币并输出结果"""
    print("🔍 Borrow Monitor")
    print("=" * 50)
    
    # 安全检查
    if os.getenv("V5_LIVE_ARM") != "YES":
        print("❌ Set V5_LIVE_ARM=YES to proceed")
        return
    
    monitor = BorrowMonitor()
    
    # 检查当前借贷状态
    current = monitor.check_borrows()
    
    print(f"Total Equity: {current['account_total_eq']:.4f} USDT")
    print(f"Available Equity: {current['account_avail_eq']:.4f} USDT")
    print(f"Total Liability: {current['total_liability_usdt']:.4f} USDT")
    print("")
    
    if current['borrows']:
        print("⚠️ **ACTIVE BORROWS DETECTED**:")
        for b in current['borrows']:
            print(f"  {b['ccy']}: eq={b['eq']:.6f}, liab={b['liab']:.6f}")
        
        # 检查是否有新的借贷
        has_new, new_borrows = monitor.check_for_new_borrows()
        if has_new:
            print("\n🚨 **NEW BORROWS SINCE LAST CHECK**:")
            for b in new_borrows:
                print(f"  {b['ccy']}: eq={b['eq']:.6f}")
            
            # 输出警报格式
            print("\n" + "=" * 50)
            print(monitor.alert_format(new_borrows))
    else:
        print("✅ No active borrows detected")
        monitor.save_state({
            'last_check': current['timestamp'],
            'last_borrows': []
        })
    
    print("\n" + "=" * 50)
    print("Next check: Add to cron or heartbeat")
    print("Example: */30 * * * * cd /path && python3 scripts/borrow_monitor.py")


if __name__ == "__main__":
    main()