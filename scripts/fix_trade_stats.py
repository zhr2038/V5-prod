#!/usr/bin/env python3
"""
修复交易统计bug
问题：num_trades=0 但实际有成交
"""

import json
import csv
from pathlib import Path
import sqlite3

def fix_trade_stats():
    print("🔧 修复交易统计bug")
    print("=" * 60)
    
    # 1. 找到最新运行
    runs_dir = Path("reports/runs")
    run_dirs = sorted(runs_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not run_dirs:
        print("❌ 无运行目录")
        return
    
    latest_run = run_dirs[0]
    print(f"最新运行: {latest_run.name}")
    
    # 2. 检查订单数据库中的成交
    orders_db = Path("reports/orders.sqlite")
    if not orders_db.exists():
        print("❌ 订单数据库不存在")
        return
    
    conn = sqlite3.connect(str(orders_db))
    cursor = conn.cursor()
    
    # 获取该运行的成交订单
    cursor.execute("""
        SELECT inst_id, side, intent, acc_fill_sz, avg_px, fee, created_ts
        FROM orders 
        WHERE run_id = ? AND state = 'FILLED'
    """, (latest_run.name,))
    
    filled_orders = cursor.fetchall()
    conn.close()
    
    print(f"找到 {len(filled_orders)} 个成交订单:")
    
    trades = []
    for order in filled_orders:
        inst_id, side, intent, fill_sz, avg_px, fee, created_ts = order
        symbol = inst_id.replace('-', '/')
        
        # 计算交易金额
        notional = float(fill_sz or 0) * float(avg_px or 0) if avg_px else 0
        
        trade = {
            'ts': created_ts,
            'run_id': latest_run.name,
            'symbol': symbol,
            'intent': intent,
            'side': side,
            'qty': fill_sz,
            'price': avg_px,
            'notional_usdt': notional,
            'fee_usdt': fee if fee else 0,
            'slippage_usdt': 0,  # 需要从其他数据源获取
            'realized_pnl_usdt': 0,  # 新开仓无已实现盈亏
            'realized_pnl_pct': 0
        }
        trades.append(trade)
        
        print(f"  {symbol}: {side} {fill_sz} @ {avg_px} = {notional:.2f} USDT")
    
    # 3. 修复 trades.csv
    trades_file = latest_run / "trades.csv"
    
    if trades:
        print(f"\n修复 {trades_file}")
        
        # 写入 CSV
        fieldnames = ['ts', 'run_id', 'symbol', 'intent', 'side', 'qty', 'price', 
                     'notional_usdt', 'fee_usdt', 'slippage_usdt', 
                     'realized_pnl_usdt', 'realized_pnl_pct']
        
        with open(trades_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for trade in trades:
                writer.writerow(trade)
        
        print(f"✅ 写入 {len(trades)} 笔交易到 trades.csv")
    
    # 4. 修复 summary.json
    summary_file = latest_run / "summary.json"
    if summary_file.exists():
        with open(summary_file, 'r') as f:
            summary = json.load(f)
        
        # 更新交易统计
        old_num_trades = summary.get('num_trades', 0)
        summary['num_trades'] = len(trades)
        
        # 如果有交易，更新其他相关字段
        if trades:
            # 计算总手续费
            total_fees = sum(float(trade['fee_usdt']) for trade in trades)
            summary['fees_usdt_total'] = total_fees
            
            # 计算总成本（手续费 + 滑点）
            total_slippage = sum(float(trade.get('slippage_usdt', 0)) for trade in trades)
            summary['slippage_usdt_total'] = total_slippage
            summary['cost_usdt_total'] = total_fees + total_slippage
            
            # 如果有成本，计算成本比率
            equity_start = summary.get('equity_start', 0)
            if equity_start and equity_start > 0:
                summary['cost_ratio'] = (total_fees + total_slippage) / equity_start
        
        # 保存更新
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n✅ 更新 summary.json:")
        print(f"  num_trades: {old_num_trades} -> {summary['num_trades']}")
        if 'fees_usdt_total' in summary:
            print(f"  fees_usdt_total: {summary['fees_usdt_total']}")
        if 'cost_usdt_total' in summary:
            print(f"  cost_usdt_total: {summary['cost_usdt_total']}")
    
    # 5. 修复所有历史运行的统计
    print(f"\n修复所有历史运行的交易统计...")
    fixed_count = 0
    
    for run_dir in run_dirs[:10]:  # 修复最近10个运行
        run_id = run_dir.name
        summary_file = run_dir / "summary.json"
        
        if not summary_file.exists():
            continue
        
        # 检查该运行的成交订单
        conn = sqlite3.connect(str(orders_db))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders WHERE run_id = ? AND state = 'FILLED'", (run_id,))
        num_filled = cursor.fetchone()[0]
        conn.close()
        
        if num_filled > 0:
            with open(summary_file, 'r') as f:
                summary = json.load(f)
            
            old_trades = summary.get('num_trades', 0)
            if old_trades != num_filled:
                summary['num_trades'] = num_filled
                
                with open(summary_file, 'w') as f:
                    json.dump(summary, f, indent=2)
                
                print(f"  ✅ {run_id}: {old_trades} -> {num_filled}")
                fixed_count += 1
    
    print(f"\n✅ 共修复 {fixed_count} 个历史运行的交易统计")
    
    print("\n" + "=" * 60)
    print("📋 修复完成总结:")
    print(f"1. 最新运行: {latest_run.name}")
    print(f"2. 成交订单: {len(filled_orders)} 个")
    print(f"3. 修复 trades.csv: {len(trades)} 笔交易")
    print(f"4. 修复历史统计: {fixed_count} 个运行")
    print("=" * 60)

if __name__ == "__main__":
    fix_trade_stats()