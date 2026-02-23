#!/usr/bin/env python3
"""
增强版借币监控和预防
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

def enhanced_borrow_monitor():
    print("🛡️ 增强版借币监控和预防")
    print("=" * 60)
    
    # 1. 检查当前借币状态
    try:
        from configs.loader import load_config
        from src.execution.okx_private_client import OKXPrivateClient
        
        cfg = load_config('configs/live_20u_real.yaml', env_path='.env')
        okx = OKXPrivateClient(exchange=cfg.exchange)
        
        resp = okx.get_balance()
        if not resp.data or 'data' not in resp.data:
            print("❌ 无法获取账户数据")
            return
        
        account = resp.data['data'][0]
        
        # 分析借币情况
        borrows = []
        total_liability_usdt = 0
        
        for detail in account.get('details', []):
            ccy = detail.get('ccy', '')
            liab = float(detail.get('liab', 0))
            
            if liab > 0:
                # 估算 USDT 价值
                # 这里需要市场价格，暂时使用简单估算
                if ccy == 'PEPE':
                    usdt_value = liab * 0.0000045
                elif ccy == 'MERL':
                    usdt_value = liab * 0.065
                elif ccy == 'SPACE':
                    usdt_value = liab * 0.011
                else:
                    usdt_value = liab * 1.0  # 默认 1:1
                
                borrows.append({
                    'currency': ccy,
                    'liability': liab,
                    'usdt_value': usdt_value
                })
                total_liability_usdt += usdt_value
        
        print(f"借币检测结果:")
        print(f"  总负债: {total_liability_usdt:.6f} USDT")
        
        if borrows:
            for borrow in borrows:
                print(f"  {borrow['currency']}: {borrow['liability']} ≈ {borrow['usdt_value']:.6f} USDT")
        else:
            print("  ✅ 无借币")
        
        # 2. 保存状态
        state = {
            'timestamp': datetime.now().isoformat(),
            'total_liability_usdt': total_liability_usdt,
            'borrows': borrows,
            'total_equity': account.get('totalEq', '0')
        }
        
        state_file = Path('reports/borrow_monitor_state.json')
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        print(f"\n📊 状态已保存: {state_file}")
        
        # 3. 预防措施
        print(f"\n🛡️ 预防措施:")
        
        # 检查黑名单
        blacklist_file = Path('configs/blacklist.json')
        if blacklist_file.exists():
            with open(blacklist_file, 'r') as f:
                blacklist = json.load(f)
            
            blacklisted = blacklist.get('symbols', [])
            print(f"  黑名单币种: {len(blacklisted)} 个")
            
            # 检查是否有借币的币种不在黑名单
            for borrow in borrows:
                symbol = f"{borrow['currency']}/USDT"
                if symbol not in blacklisted:
                    print(f"  ⚠️ {symbol} 有借币但不在黑名单，建议添加")
        
        # 4. 风险评估
        print(f"\n📈 风险评估:")
        
        if total_liability_usdt > 10:
            print(f"  🚨 高风险：总负债 > 10 USDT")
            print(f"     建议：立即处理借币问题")
        elif total_liability_usdt > 1:
            print(f"  ⚠️ 中风险：总负债 > 1 USDT")
            print(f"     建议：监控并计划处理")
        else:
            print(f"  ✅ 低风险：总负债 < 1 USDT")
        
        # 5. 自动化建议
        print(f"\n🤖 自动化建议:")
        print("  1. 将本脚本加入 crontab（每30分钟运行）")
        print("  2. 设置告警阈值（如负债 > 5 USDT 时通知）")
        print("  3. 自动将借币币种加入黑名单")
        
    except Exception as e:
        print(f"监控失败: {e}")
    
    print("\n" + "=" * 60)
    print("✅ 增强版借币监控完成")
    print("=" * 60)

if __name__ == "__main__":
    enhanced_borrow_monitor()