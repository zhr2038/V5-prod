#!/usr/bin/env python3
"""
立即修复 SOL 负债
"""

from __future__ import annotations

import os
import time
import requests
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


def main() -> None:
    print("🔧 IMMEDIATE SOL LIABILITY FIX")
    print("=" * 50)
    
    # 安全检查
    if os.getenv("V5_LIVE_ARM") != "YES":
        print("❌ Set V5_LIVE_ARM=YES to proceed")
        return
    
    # 加载配置
    cfg = load_config("configs/live_small.yaml", env_path=".env")
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 检查 SOL 负债
    resp = okx.get_balance()
    sol_eq = 0
    sol_liab = 0
    
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'SOL':
            sol_eq = float(d.get('eq', 0))
            sol_liab = float(d.get('liab', 0))
            break
    
    print(f"SOL eq: {sol_eq:.6f}")
    print(f"SOL liab: {sol_liab:.6f}")
    
    if sol_eq >= -0.0001:
        print("✅ No SOL liability found")
        return
    
    amount_needed = abs(sol_eq)
    print(f"\nNeed to buy: {amount_needed:.6f} SOL")
    
    # 获取 SOL 价格
    url = "https://www.okx.com/api/v5/market/ticker?instId=SOL-USDT"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0' and data.get('data'):
                price = float(data['data'][0]['last'])
                print(f"SOL price: {price:.4f}")
            else:
                price = 85.4  # 默认价格
        else:
            price = 85.4
    except:
        price = 85.4
    
    # 计算所需 USDT
    usdt_needed = amount_needed * price * 1.02  # 加2%缓冲
    print(f"USDT needed: {usdt_needed:.4f}")
    
    # 检查 USDT 余额
    usdt_balance = 0
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'USDT':
            usdt_balance = float(d.get('availBal', 0))
            break
    
    print(f"Available USDT: {usdt_balance:.4f}")
    
    if usdt_balance < usdt_needed:
        print(f"❌ Insufficient USDT. Need {usdt_needed:.4f}, have {usdt_balance:.4f}")
        return
    
    # 准备执行
    run_id = f"sol_repair_{int(time.time())}"
    os_store = OrderStore("reports/orders.sqlite")
    ps = PositionStore("reports/positions.sqlite")
    
    live = LiveExecutionEngine(
        cfg.execution, 
        okx=okx, 
        order_store=os_store, 
        position_store=ps, 
        run_id=run_id,
        exp_time_ms=getattr(cfg.execution, "okx_exp_time_ms", None)
    )
    
    # 创建购买订单
    print(f"\n📥 Placing buy order for {usdt_needed:.4f} USDT of SOL...")
    
    buy_order = Order(
        symbol="SOL/USDT",
        side="buy",
        intent="REPAY_LIABILITY",
        notional_usdt=usdt_needed,
        signal_price=price,
        meta={
            "purpose": "repay_sol_liability",
            "required_sol": amount_needed,
            "original_eq": sol_eq,
            "original_liab": sol_liab
        }
    )
    
    try:
        result = live.place(buy_order)
        print(f"Order ID: {result.cl_ord_id}")
        print(f"Initial state: {result.state}")
        
        # 等待并轮询
        print("Waiting for order execution...")
        time.sleep(3)
        
        live.poll_open(limit=10)
        
        # 检查结果
        if result.state == "FILLED":
            print("✅ SOL purchased successfully!")
            
            # 检查新余额
            time.sleep(2)
            resp = okx.get_balance()
            new_sol_eq = 0
            for d in resp.data['data'][0]['details']:
                if d.get('ccy') == 'SOL':
                    new_sol_eq = float(d.get('eq', 0))
                    break
            
            print(f"\nNew SOL eq: {new_sol_eq:.6f}")
            
            if new_sol_eq >= -0.0001:
                print("🎉 SOL liability fully repaid!")
            else:
                print(f"⚠️  Partial repayment. Remaining: {abs(new_sol_eq):.6f} SOL")
                
        else:
            print(f"❌ Order not filled. State: {result.state}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("Next: Run borrow_monitor.py to verify")


if __name__ == "__main__":
    main()