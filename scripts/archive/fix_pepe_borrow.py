#!/usr/bin/env python3
"""
紧急修复：PEPE 借币问题
问题：手续费以 PEPE 币种扣除导致自动借币
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import json

def fix_pepe_borrow():
    print("🚨 紧急修复：PEPE 借币问题")
    print("=" * 60)
    
    # 1. 检查当前持仓
    positions_db = Path("reports/positions.sqlite")
    conn = sqlite3.connect(str(positions_db))
    cursor = conn.cursor()
    
    cursor.execute("SELECT symbol, qty FROM positions WHERE symbol = 'PEPE/USDT'")
    pepe_position = cursor.fetchone()
    
    if pepe_position:
        symbol, qty = pepe_position
        print(f"当前 PEPE 持仓: {qty}")
        
        # 2. 检查订单数据库中的 PEPE 交易
        orders_db = Path("reports/orders.sqlite")
        if orders_db.exists():
            conn_orders = sqlite3.connect(str(orders_db))
            cursor_orders = conn_orders.cursor()
            
            cursor_orders.execute("""
                SELECT inst_id, side, fee, acc_fill_sz, avg_px, created_ts
                FROM orders 
                WHERE inst_id = 'PEPE-USDT' 
                AND state = 'FILLED'
                ORDER BY created_ts DESC
                LIMIT 1
            """)
            
            pepe_order = cursor_orders.fetchone()
            if pepe_order:
                inst_id, side, fee, fill_sz, avg_px, created_ts = pepe_order
                print(f"PEPE 订单详情:")
                print(f"  方向: {side}")
                print(f"  成交数量: {fill_sz}")
                print(f"  手续费: {fee} PEPE")
                print(f"  均价: {avg_px}")
                
                # 分析问题
                fee_pepe = abs(float(fee)) if fee and str(fee).startswith('-') else 0
                print(f"  手续费（PEPE）: {fee_pepe}")
                
                # 实际持仓应该是：买入数量 - 手续费
                actual_qty = float(fill_sz) - fee_pepe
                print(f"  实际应有持仓: {actual_qty} (买入 {fill_sz} - 手续费 {fee_pepe})")
                
                # 3. 修复持仓数据
                if abs(qty - actual_qty) > 1:
                    print(f"  需要修复: {qty} -> {actual_qty}")
                    
                    # 更新持仓
                    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                    cursor.execute("""
                        UPDATE positions 
                        SET qty = ?, last_update_ts = ?
                        WHERE symbol = 'PEPE/USDT'
                    """, (actual_qty, now_iso))
                    
                    conn.commit()
                    print(f"  ✅ 持仓已更新: {actual_qty}")
                
                conn_orders.close()
    
    conn.close()
    
    # 4. 建议解决方案
    print(f"\n📋 解决方案建议:")
    print("1. ✅ 已修复持仓数据（反映实际余额）")
    print("2. 🔧 修改配置：避免交易手续费高的低价值币种")
    print("3. ⚠️ 手动处理：可能需要充值少量 PEPE 还清借币")
    print("4. 🛡️ 更新风控：检测并阻止可能导致借币的交易")
    
    # 5. 更新黑名单（暂时排除 PEPE）
    blacklist_file = Path("configs/blacklist.json")
    if blacklist_file.exists():
        with open(blacklist_file, 'r') as f:
            blacklist = json.load(f)
        
        if 'PEPE/USDT' not in blacklist.get('symbols', []):
            blacklist['symbols'].append('PEPE/USDT')
            
            with open(blacklist_file, 'w') as f:
                json.dump(blacklist, f, indent=2)
            
            print(f"\n✅ 已将 PEPE/USDT 加入黑名单（避免进一步问题）")
    
    print("\n" + "=" * 60)
    print("🚨 重要：需要检查 OKX 账户并处理 PEPE 借币！")
    print("=" * 60)

if __name__ == "__main__":
    fix_pepe_borrow()