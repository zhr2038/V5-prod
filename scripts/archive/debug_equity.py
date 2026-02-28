#!/usr/bin/env python3
"""
调试 equity 计算问题
"""

import sqlite3
import requests
import json
from datetime import datetime

def debug_equity_calculation():
    print("🔍 调试 equity 计算")
    print("=" * 60)
    
    # 1. 从 positions.sqlite 获取数据
    db_path = "reports/positions.sqlite"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取 account_state
    cursor.execute("SELECT cash_usdt, equity_peak_usdt FROM account_state WHERE k='default'")
    cash_usdt, equity_peak = cursor.fetchone()
    
    print(f"account_state:")
    print(f"  cash_usdt: {cash_usdt}")
    print(f"  equity_peak_usdt: {equity_peak}")
    print(f"  差异: {equity_peak - cash_usdt:.6f} USDT")
    
    # 获取持仓
    cursor.execute("SELECT symbol, qty, avg_px FROM positions WHERE qty > 0")
    positions = cursor.fetchall()
    
    print(f"\n持仓 ({len(positions)} 个):")
    
    # 获取当前价格
    total_position_value = 0
    position_details = []
    
    for symbol, qty, avg_px in positions:
        # 获取当前价格
        inst_id = symbol.replace('/', '-')
        url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
        
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == '0' and data.get('data'):
                    current_price = float(data['data'][0]['last'])
                    value = qty * current_price
                    total_position_value += value
                    
                    position_details.append({
                        'symbol': symbol,
                        'qty': qty,
                        'avg_px': avg_px,
                        'current_price': current_price,
                        'value': value
                    })
                    
                    if value > 0.001:
                        print(f"  {symbol}:")
                        print(f"    数量: {qty:.10f}")
                        print(f"    均价: {avg_px:.6f}")
                        print(f"    现价: {current_price:.6f}")
                        print(f"    价值: {value:.6f} USDT")
        except Exception as e:
            print(f"  {symbol}: 价格获取失败 - {e}")
    
    conn.close()
    
    # 2. 计算 equity
    calculated_equity = cash_usdt + total_position_value
    
    print(f"\n📊 计算结果:")
    print(f"  现金: {cash_usdt:.6f} USDT")
    print(f"  持仓总价值: {total_position_value:.6f} USDT")
    print(f"  计算 equity: {calculated_equity:.6f} USDT")
    print(f"  account_state equity_peak: {equity_peak:.6f} USDT")
    print(f"  差异: {equity_peak - calculated_equity:.6f} USDT")
    
    # 3. 检查是否有持仓数量错误
    print(f"\n🔎 检查持仓数量:")
    
    # 常见币种的合理持仓范围
    reasonable_ranges = {
        'BTC/USDT': (0.0001, 0.01),  # 0.0001-0.01 BTC
        'ETH/USDT': (0.001, 0.1),    # 0.001-0.1 ETH
        'SOL/USDT': (0.01, 1.0),     # 0.01-1 SOL
        'BNB/USDT': (0.01, 1.0),     # 0.01-1 BNB
    }
    
    for pos in position_details:
        symbol = pos['symbol']
        qty = pos['qty']
        
        if symbol in reasonable_ranges:
            min_qty, max_qty = reasonable_ranges[symbol]
            if qty < min_qty:
                print(f"  ⚠️  {symbol}: 数量过小 ({qty:.10f} < {min_qty})")
            elif qty > max_qty:
                print(f"  ⚠️  {symbol}: 数量过大 ({qty:.10f} > {max_qty})")
    
    # 4. 检查最近的 equity.jsonl
    print(f"\n📁 检查最近的 equity.jsonl:")
    
    import os
    import glob
    
    run_dirs = sorted(glob.glob("reports/runs/*"), key=os.path.getmtime, reverse=True)
    
    if run_dirs:
        latest_dir = run_dirs[0]
        equity_file = os.path.join(latest_dir, "equity.jsonl")
        
        if os.path.exists(equity_file):
            with open(equity_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    print(f"  最新运行: {os.path.basename(latest_dir)}")
                    for i, line in enumerate(lines[:3]):
                        data = json.loads(line.strip())
                        print(f"  行 {i+1}: {data}")
    
    # 5. 修复建议
    print(f"\n🔧 修复建议:")
    
    if abs(equity_peak - calculated_equity) > 1.0:
        print(f"  1. equity_peak_usdt 错误: {equity_peak:.6f}")
        print(f"     应该重置为: {calculated_equity:.6f}")
        print(f"     执行: sqlite3 reports/positions.sqlite \"UPDATE account_state SET equity_peak_usdt = {calculated_equity} WHERE k='default'\"")
    
    # 检查是否有异常持仓
    suspicious_positions = [p for p in position_details if p['value'] > 10.0]
    if suspicious_positions:
        print(f"  2. 发现异常持仓:")
        for pos in suspicious_positions:
            print(f"     {pos['symbol']}: {pos['qty']} * {pos['current_price']} = {pos['value']:.2f} USDT")
    
    print(f"\n" + "=" * 60)
    
    return {
        'cash_usdt': cash_usdt,
        'equity_peak': equity_peak,
        'calculated_equity': calculated_equity,
        'difference': equity_peak - calculated_equity,
        'positions': position_details
    }


def fix_equity_peak():
    """修复 equity_peak_usdt"""
    print("\n🛠️ 修复 equity_peak_usdt")
    print("=" * 60)
    
    result = debug_equity_calculation()
    
    diff = result['difference']
    if abs(diff) > 1.0:
        print(f"\n❌ equity_peak_usdt 错误: 差异 {diff:.6f} USDT")
        
        # 计算正确的 equity
        cash = result['cash_usdt']
        positions_value = sum(p['value'] for p in result['positions'])
        correct_equity = cash + positions_value
        
        print(f"✅ 正确的 equity: {correct_equity:.6f}")
        
        # 更新数据库
        db_path = "reports/positions.sqlite"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE account_state SET equity_peak_usdt = ? WHERE k='default'", (correct_equity,))
        conn.commit()
        
        # 验证
        cursor.execute("SELECT equity_peak_usdt FROM account_state WHERE k='default'")
        new_value = cursor.fetchone()[0]
        
        print(f"📝 更新完成:")
        print(f"  旧值: {result['equity_peak']:.6f}")
        print(f"  新值: {new_value:.6f}")
        
        conn.close()
        
        return True
    else:
        print(f"✅ equity_peak_usdt 正常 (差异: {diff:.6f} USDT)")
        return False


def main():
    print("🚀 Equity 计算调试工具")
    print("=" * 60)
    
    # 调试
    result = debug_equity_calculation()
    
    # 询问是否修复
    print("\n是否修复 equity_peak_usdt？(y/n): ", end="")
    response = input().strip().lower()
    
    if response == 'y':
        fix_equity_peak()
    
    print("\n📋 后续步骤:")
    print("1. 运行 V5 测试: export V5_CONFIG=configs/live_20u_test.yaml && export V5_LIVE_ARM=YES && python3 main.py")
    print("2. 检查结果: python3 scripts/debug_equity.py")
    print("3. 验证修复: 查看最新的 summary.json")
    print("=" * 60)


if __name__ == "__main__":
    main()