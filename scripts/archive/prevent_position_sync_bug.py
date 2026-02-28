#!/usr/bin/env python3
"""
预防性修复：确保每次运行后持仓同步
在自动化运行脚本中调用
"""

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

def sync_positions_from_orders(run_id: str):
    """从订单数据库同步持仓"""
    
    orders_db = Path("reports/orders.sqlite")
    positions_db = Path("reports/positions.sqlite")
    
    if not orders_db.exists() or not positions_db.exists():
        return False
    
    conn_orders = sqlite3.connect(str(orders_db))
    cursor_orders = conn_orders.cursor()
    
    # 获取该运行的成交订单
    cursor_orders.execute("""
        SELECT inst_id, side, intent, acc_fill_sz, avg_px
        FROM orders 
        WHERE run_id = ? AND state = 'FILLED'
    """, (run_id,))
    
    filled_orders = cursor_orders.fetchall()
    
    if not filled_orders:
        conn_orders.close()
        return True  # 无成交，不需要同步
    
    conn_positions = sqlite3.connect(str(positions_db))
    cursor_positions = conn_positions.cursor()
    
    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    updated_count = 0
    
    for order in filled_orders:
        inst_id, side, intent, fill_sz, avg_px = order
        
        if side != 'buy' or intent != 'OPEN_LONG':
            continue  # 只处理买入开仓
        
        symbol = inst_id.replace('-', '/')
        qty = float(fill_sz or 0)
        price = float(avg_px or 0)
        
        if qty <= 0:
            continue
        
        # 检查是否已存在
        cursor_positions.execute("SELECT qty, avg_px FROM positions WHERE symbol = ?", (symbol,))
        existing = cursor_positions.fetchone()
        
        if existing:
            # 更新现有持仓
            existing_qty = float(existing[0])
            existing_avg = float(existing[1])
            
            if existing_avg <= 0:
                new_avg = price
            else:
                # 加权平均
                new_avg = (existing_qty * existing_avg + qty * price) / (existing_qty + qty)
            
            new_qty = existing_qty + qty
            
            cursor_positions.execute("""
                UPDATE positions 
                SET qty = ?, avg_px = ?, last_update_ts = ?
                WHERE symbol = ?
            """, (new_qty, new_avg, now_iso, symbol))
        else:
            # 插入新持仓
            cursor_positions.execute("""
                INSERT INTO positions 
                (symbol, qty, avg_px, entry_ts, highest_px, last_update_ts, last_mark_px, unrealized_pnl_pct, tags_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, qty, price, now_iso, 
                price if price > 0 else 0,
                now_iso,
                price if price > 0 else 0,
                0.0,
                json.dumps({"source": "auto_sync", "run_id": run_id})
            ))
        
        updated_count += 1
    
    conn_positions.commit()
    conn_positions.close()
    conn_orders.close()
    
    return updated_count > 0

def update_trade_stats(run_id: str):
    """更新交易统计"""
    
    orders_db = Path("reports/orders.sqlite")
    if not orders_db.exists():
        return 0
    
    conn = sqlite3.connect(str(orders_db))
    cursor = conn.cursor()
    
    # 统计成交订单
    cursor.execute("SELECT COUNT(*) FROM orders WHERE run_id = ? AND state = 'FILLED'", (run_id,))
    num_trades = cursor.fetchone()[0]
    
    conn.close()
    
    # 更新 summary.json
    summary_file = Path(f"reports/runs/{run_id}/summary.json")
    if summary_file.exists():
        with open(summary_file, 'r') as f:
            summary = json.load(f)
        
        summary['num_trades'] = num_trades
        
        # 如果有交易，计算手续费
        if num_trades > 0:
            conn = sqlite3.connect(str(orders_db))
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(fee) FROM orders WHERE run_id = ? AND state = 'FILLED'", (run_id,))
            total_fee = cursor.fetchone()[0] or 0
            conn.close()
            
            summary['fees_usdt_total'] = float(total_fee)
            summary['cost_usdt_total'] = float(total_fee)
        
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
    
    return num_trades

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        run_id = sys.argv[1]
    else:
        # 使用最新运行
        runs_dir = Path("reports/runs")
        run_dirs = sorted(runs_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True)
        if run_dirs:
            run_id = run_dirs[0].name
        else:
            print("❌ 无运行目录")
            sys.exit(1)
    
    print(f"🔧 运行预防性修复: {run_id}")
    
    # 同步持仓
    if sync_positions_from_orders(run_id):
        print("✅ 持仓同步完成")
    else:
        print("ℹ️  无持仓需要同步")
    
    # 更新统计
    num_trades = update_trade_stats(run_id)
    print(f"✅ 交易统计更新: {num_trades} 笔交易")
    
    print("🎯 预防性修复完成")