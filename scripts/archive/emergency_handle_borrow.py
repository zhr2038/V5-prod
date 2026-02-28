#!/usr/bin/env python3
"""
紧急处理借币问题
"""

import json
from datetime import datetime
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient

def emergency_handle_borrow():
    print("🚨 紧急处理：PEPE 借币问题")
    print("=" * 60)
    
    # 1. 检查当前借币状态
    cfg = load_config('configs/live_20u_real.yaml', env_path='.env')
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    resp = okx.get_balance()
    if not resp.data or 'data' not in resp.data:
        print("❌ 无法获取账户数据")
        return
    
    account = resp.data['data'][0]
    total_eq = account.get('totalEq', '0')
    print(f"账户总权益: {total_eq} USDT")
    
    # 检查 PEPE 借币
    pepe_borrow = 0
    for detail in account.get('details', []):
        ccy = detail.get('ccy', '')
        if ccy == 'PEPE':
            eq = float(detail.get('eq', 0))
            liab = float(detail.get('liab', 0))
            cash_bal = float(detail.get('cashBal', 0))
            
            print(f"PEPE 状态:")
            print(f"  总余额: {eq}")
            print(f"  现金余额: {cash_bal}")
            print(f"  负债: {liab}")
            
            if liab > 0:
                pepe_borrow = liab
                print(f"  🚨 检测到借币: {liab} PEPE")
    
    # 2. 分析问题
    if pepe_borrow > 0:
        print(f"\n📊 借币分析:")
        print(f"  借币数量: {pepe_borrow} PEPE")
        
        # 估算 USDT 价值（假设价格 0.0000045）
        estimated_value = pepe_borrow * 0.0000045
        print(f"  估算价值: {estimated_value:.6f} USDT")
        
        # 检查是否有 PEPE 持仓可以卖出
        print(f"\n🔍 检查解决方案:")
        
        # 检查持仓数据库
        import sqlite3
        conn = sqlite3.connect('reports/positions.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT qty FROM positions WHERE symbol = 'PEPE/USDT'")
        pepe_position = cursor.fetchone()
        conn.close()
        
        if pepe_position:
            pepe_qty = float(pepe_position[0])
            print(f"  当前持仓: {pepe_qty} PEPE")
            
            if pepe_qty > pepe_borrow:
                print(f"  ✅ 可以卖出部分持仓还清借币")
                print(f"    需要卖出: {pepe_borrow} PEPE")
                print(f"    剩余持仓: {pepe_qty - pepe_borrow} PEPE")
            else:
                print(f"  ⚠️ 持仓不足，需要充值: {pepe_borrow - pepe_qty} PEPE")
        else:
            print(f"  ❌ 无 PEPE 持仓，需要充值: {pepe_borrow} PEPE")
    
    # 3. 建议操作步骤
    print(f"\n🎯 建议操作步骤:")
    print("1. 登录 OKX 网页/APP")
    print("2. 检查 PEPE 借币详情（可能有利息）")
    print("3. 选择方案:")
    print("   A. 充值少量 PEPE 还清借币（推荐）")
    print("   B. 卖出部分 PEPE 持仓还清借币")
    print("4. 还清借币后，重新运行借币监控")
    
    # 4. 临时解决方案：创建手动还款脚本
    print(f"\n🛠️ 临时解决方案（如果无法充值）:")
    print("   可以尝试手动卖出少量其他币种，购买 PEPE 还款")
    
    # 5. 记录状态
    state = {
        'timestamp': datetime.now().isoformat(),
        'pepe_borrow': pepe_borrow,
        'total_equity': total_eq,
        'recommendation': '充值 PEPE 还清借币'
    }
    
    with open('reports/borrow_emergency_state.json', 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"\n📝 状态已保存: reports/borrow_emergency_state.json")
    print("\n" + "=" * 60)
    print("⚠️ 重要：借币可能产生利息，请尽快处理！")
    print("=" * 60)

if __name__ == "__main__":
    emergency_handle_borrow()