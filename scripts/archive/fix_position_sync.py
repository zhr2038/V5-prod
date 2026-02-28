#!/usr/bin/env python3
"""
修复持仓同步bug
问题：订单成交但持仓数据库未更新
"""

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

def fix_position_sync():
    print("🔧 修复持仓同步bug")
    print("=" * 60)
    
    # 1. 检查订单数据库
    orders_db = Path("reports/orders.sqlite")
    positions_db = Path("reports/positions.sqlite")
    
    if not orders_db.exists():
        print("❌ 订单数据库不存在")
        return
    
    # 连接到订单数据库
    conn_orders = sqlite3.connect(str(orders_db))
    cursor_orders = conn_orders.cursor()
    
    # 获取最新成交的订单
    cursor_orders.execute("""
        SELECT inst_id, side, intent, acc_fill_sz, avg_px, created_ts 
        FROM orders 
        WHERE state = 'FILLED' 
        AND created_ts > (SELECT MAX(created_ts) FROM orders) - 3600000  -- 最近1小时
        ORDER BY created_ts DESC
    """)
    
    filled_orders = cursor_orders.fetchall()
    
    print(f"找到 {len(filled_orders)} 个已成交订单:")
    for order in filled_orders:
        inst_id, side, intent, fill_sz, avg_px, created_ts = order
        symbol = inst_id.replace('-', '/')
        print(f"  {symbol}: {side} {fill_sz} @ {avg_px}")
    
    # 2. 检查持仓数据库
    conn_positions = sqlite3.connect(str(positions_db))
    cursor_positions = conn_positions.cursor()
    
    # 获取当前持仓
    cursor_positions.execute("SELECT symbol, qty FROM positions WHERE qty > 0")
    current_positions = {row[0]: float(row[1]) for row in cursor_positions.fetchall()}
    
    print(f"\n当前持仓 ({len(current_positions)} 个):")
    for symbol, qty in current_positions.items():
        print(f"  {symbol}: {qty}")
    
    # 3. 修复缺失的持仓
    missing_positions = []
    for order in filled_orders:
        inst_id, side, intent, fill_sz, avg_px, created_ts = order
        symbol = inst_id.replace('-', '/')
        
        if side == 'buy' and intent == 'OPEN_LONG':
            # 检查是否在持仓中
            if symbol not in current_positions or float(current_positions.get(symbol, 0)) < float(fill_sz or 0):
                missing_positions.append({
                    'symbol': symbol,
                    'qty': float(fill_sz or 0),
                    'avg_px': float(avg_px or 0),
                    'side': side,
                    'intent': intent
                })
    
    print(f"\n发现 {len(missing_positions)} 个缺失持仓:")
    for pos in missing_positions:
        print(f"  {pos['symbol']}: {pos['qty']} @ {pos['avg_px']}")
    
    # 4. 修复持仓
    if missing_positions:
        print("\n开始修复持仓...")
        now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        for pos in missing_positions:
            symbol = pos['symbol']
            qty = pos['qty']
            avg_px = pos['avg_px']
            
            # 检查是否已存在
            cursor_positions.execute("SELECT qty FROM positions WHERE symbol = ?", (symbol,))
            existing = cursor_positions.fetchone()
            
            if existing:
                # 更新现有持仓
                existing_qty = float(existing[0])
                new_qty = existing_qty + qty
                
                # 计算新的平均价格
                if avg_px > 0:
                    new_avg_px = (existing_qty * 0 + qty * avg_px) / new_qty
                else:
                    new_avg_px = 0
                
                cursor_positions.execute("""
                    UPDATE positions 
                    SET qty = ?, avg_px = ?, entry_ts = ?, last_update_ts = ?
                    WHERE symbol = ?
                """, (new_qty, new_avg_px, now_iso, now_iso, symbol))
                
                print(f"  ✅ 更新持仓: {symbol} {existing_qty} -> {new_qty}")
            else:
                # 插入新持仓
                cursor_positions.execute("""
                    INSERT INTO positions 
                    (symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, qty, avg_px, now_iso, 
                    avg_px if avg_px > 0 else 0,  # highest_px
                    now_iso,  # last_update_ts
                    avg_px if avg_px > 0 else 0,  # last_mark_px
                    0.0,  # unrealized_pnl_pct
                    json.dumps({"source": "fix_position_sync", "fixed_at": now_iso})
                ))
                
                print(f"  ✅ 新增持仓: {symbol} {qty} @ {avg_px}")
        
        conn_positions.commit()
        print("✅ 持仓修复完成")
    
    # 5. 验证修复
    print("\n验证修复结果:")
    cursor_positions.execute("SELECT symbol, qty, avg_px FROM positions WHERE qty > 0 ORDER BY qty DESC")
    fixed_positions = cursor_positions.fetchall()
    
    print(f"修复后持仓 ({len(fixed_positions)} 个):")
    for symbol, qty, avg_px in fixed_positions:
        print(f"  {symbol}: {qty} @ {avg_px}")
    
    # 6. 检查特定币种
    check_symbols = ['SPACE/USDT', 'PEPE/USDT', 'PI/USDT', 'MERL/USDT']
    print(f"\n检查关键币种:")
    for symbol in check_symbols:
        cursor_positions.execute("SELECT qty, avg_px FROM positions WHERE symbol = ?", (symbol,))
        row = cursor_positions.fetchone()
        if row:
            print(f"  ✅ {symbol}: {row[0]} @ {row[1]}")
        else:
            print(f"  ❌ {symbol}: 无持仓")
    
    # 关闭连接
    conn_orders.close()
    conn_positions.close()
    
    print("\n" + "=" * 60)
    print("📋 修复完成总结:")
    print(f"1. 检查订单: {len(filled_orders)} 个已成交")
    print(f"2. 发现缺失: {len(missing_positions)} 个持仓")
    print(f"3. 修复后持仓: {len(fixed_positions)} 个")
    print("=" * 60)

if __name__ == "__main__":
    fix_position_sync()