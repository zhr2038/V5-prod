#!/usr/bin/env python3
"""
修复手续费计算问题
问题：手续费以币种计价（负值），需要转换为 USDT
"""

import sqlite3
import json
from pathlib import Path

def fix_fee_calculation():
    print("🔧 修复手续费计算问题")
    print("=" * 60)
    
    orders_db = Path("reports/orders.sqlite")
    if not orders_db.exists():
        print("❌ 订单数据库不存在")
        return
    
    conn = sqlite3.connect(str(orders_db))
    cursor = conn.cursor()
    
    # 1. 分析所有成交订单的手续费
    cursor.execute("""
        SELECT run_id, inst_id, side, fee, acc_fill_sz, avg_px, notional_usdt, created_ts
        FROM orders 
        WHERE state = 'FILLED'
        ORDER BY created_ts DESC
    """)
    
    filled_orders = cursor.fetchall()
    print(f"找到 {len(filled_orders)} 个成交订单")
    
    # 2. 修复每个运行的手续费计算
    run_fees = {}  # {run_id: total_fee_usdt}
    
    for order in filled_orders:
        run_id, inst_id, side, fee, fill_sz, avg_px, notional, created_ts = order
        symbol = inst_id.replace('-', '/')
        
        # 解析手续费
        fee_str = str(fee)
        fee_usdt = 0
        
        if fee_str and fee_str != 'None':
            try:
                fee_value = float(fee_str)
                
                if fee_value < 0:
                    # 负值表示手续费（以币种计价）
                    fee_coin = abs(fee_value)
                    
                    # 转换为 USDT
                    if avg_px and float(avg_px) > 0:
                        fee_usdt = fee_coin * float(avg_px)
                    elif notional and float(notional) > 0:
                        # 使用交易金额估算（假设手续费率 0.1%）
                        fee_usdt = float(notional) * 0.001
                    else:
                        # 默认估算
                        fee_usdt = fee_coin * 0.01  # 假设价格 0.01 USDT
                else:
                    # 正值或零
                    fee_usdt = float(fee_value)
                    
            except (ValueError, TypeError):
                fee_usdt = 0
        
        # 累加到运行统计
        if run_id not in run_fees:
            run_fees[run_id] = 0
        run_fees[run_id] += fee_usdt
        
        print(f"  {run_id} {symbol}: {fee_str} -> {fee_usdt:.6f} USDT")
    
    # 3. 更新每个运行的 summary.json
    print(f"\n更新运行统计:")
    updated_count = 0
    
    for run_id, total_fee in run_fees.items():
        summary_file = Path(f"reports/runs/{run_id}/summary.json")
        
        if not summary_file.exists():
            continue
        
        with open(summary_file, 'r') as f:
            summary = json.load(f)
        
        # 更新手续费
        old_fee = summary.get('fees_usdt_total', 0)
        summary['fees_usdt_total'] = total_fee
        summary['cost_usdt_total'] = total_fee  # 假设无滑点成本
        
        # 计算成本比率
        equity_start = summary.get('equity_start', 0)
        if equity_start and equity_start > 0:
            summary['cost_ratio'] = total_fee / equity_start
        
        # 保存更新
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"  ✅ {run_id}: {old_fee} -> {total_fee:.6f} USDT")
        updated_count += 1
    
    # 4. 修复 trades.csv 中的手续费
    print(f"\n修复 trades.csv 手续费:")
    
    for run_id in run_fees.keys():
        trades_file = Path(f"reports/runs/{run_id}/trades.csv")
        
        if trades_file.exists():
            # 重新从订单数据库生成 trades.csv
            cursor.execute("""
                SELECT inst_id, side, intent, acc_fill_sz, avg_px, fee, created_ts
                FROM orders 
                WHERE run_id = ? AND state = 'FILLED'
                ORDER BY created_ts
            """, (run_id,))
            
            run_orders = cursor.fetchall()
            
            if run_orders:
                import csv
                
                trades = []
                for order in run_orders:
                    inst_id, side, intent, fill_sz, avg_px, fee, created_ts = order
                    symbol = inst_id.replace('-', '/')
                    
                    # 计算交易金额
                    notional = float(fill_sz or 0) * float(avg_px or 0) if avg_px else 0
                    
                    # 计算手续费（USDT）
                    fee_usdt = 0
                    if fee and str(fee) != 'None':
                        try:
                            fee_value = float(fee)
                            if fee_value < 0 and avg_px:
                                fee_usdt = abs(fee_value) * float(avg_px)
                        except:
                            pass
                    
                    trade = {
                        'ts': created_ts,
                        'run_id': run_id,
                        'symbol': symbol,
                        'intent': intent,
                        'side': side,
                        'qty': fill_sz,
                        'price': avg_px,
                        'notional_usdt': notional,
                        'fee_usdt': fee_usdt,
                        'slippage_usdt': 0,
                        'realized_pnl_usdt': 0,
                        'realized_pnl_pct': 0
                    }
                    trades.append(trade)
                
                # 写入 CSV
                fieldnames = ['ts', 'run_id', 'symbol', 'intent', 'side', 'qty', 'price', 
                             'notional_usdt', 'fee_usdt', 'slippage_usdt', 
                             'realized_pnl_usdt', 'realized_pnl_pct']
                
                with open(trades_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for trade in trades:
                        writer.writerow(trade)
                
                print(f"  ✅ {run_id}: 更新 {len(trades)} 笔交易")
    
    conn.close()
    
    print(f"\n" + "=" * 60)
    print("📋 修复完成总结:")
    print(f"1. 分析订单: {len(filled_orders)} 个成交")
    print(f"2. 更新统计: {updated_count} 个运行")
    print(f"3. 修复手续费: 从币种计价转换为 USDT")
    print("=" * 60)

if __name__ == "__main__":
    fix_fee_calculation()