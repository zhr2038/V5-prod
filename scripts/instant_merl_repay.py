#!/usr/bin/env python3
"""
立即抹平 MERL 负债
购买 MERL 归还借贷
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


def get_merl_price() -> float:
    """获取 MERL 当前价格"""
    url = "https://www.okx.com/api/v5/market/ticker?instId=MERL-USDT"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0' and data.get('data'):
                return float(data['data'][0]['last'])
    except:
        pass
    return 0.06425  # 默认价格


def get_merl_liability(okx: OKXPrivateClient) -> tuple[float, float]:
    """获取 MERL 负债信息"""
    resp = okx.get_balance()
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'MERL':
            eq = float(d.get('eq', 0))
            liab = float(d.get('liab', 0))
            return eq, liab
    return 0.0, 0.0


def main() -> None:
    print("🚨 IMMEDIATE MERL LIABILITY REPAYMENT 🚨")
    print("=" * 50)
    
    # 安全检查
    if os.getenv("V5_LIVE_ARM") != "YES":
        print("❌ Set V5_LIVE_ARM=YES to proceed")
        return
    
    # 加载配置
    cfg = load_config("configs/live_small.yaml", env_path=".env")
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 检查负债
    merl_eq, merl_liab = get_merl_liability(okx)
    print(f"MERL eq: {merl_eq:.6f}")
    print(f"MERL liab: {merl_liab:.6f}")
    
    if merl_eq >= -0.001:
        print("✅ No MERL liability found")
        return
    
    amount_needed = abs(merl_eq)
    print(f"\nNeed to buy: {amount_needed:.6f} MERL")
    
    # 获取价格
    price = get_merl_price()
    print(f"MERL price: {price:.6f}")
    
    usdt_needed = amount_needed * price * 1.03  # 加3%缓冲
    print(f"USDT needed: {usdt_needed:.4f}")
    
    # 检查 USDT 余额
    usdt_balance = 0
    resp = okx.get_balance()
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'USDT':
            usdt_balance = float(d.get('availBal', 0))
            break
    
    print(f"Available USDT: {usdt_balance:.4f}")
    
    if usdt_balance < usdt_needed:
        print(f"❌ Insufficient USDT. Need {usdt_needed:.4f}, have {usdt_balance:.4f}")
        # 尝试用全部可用 USDT 购买
        usdt_needed = min(usdt_balance * 0.95, usdt_needed)  # 留5%缓冲
        amount_needed = usdt_needed / price / 1.03
        print(f"Will buy {amount_needed:.6f} MERL with {usdt_needed:.4f} USDT")
    
    # 准备执行
    run_id = f"merl_repay_{int(time.time())}"
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
    print(f"\n📥 Placing buy order for {usdt_needed:.4f} USDT of MERL...")
    
    buy_order = Order(
        symbol="MERL/USDT",
        side="buy",
        intent="REPAY_LOAN",
        notional_usdt=usdt_needed,
        signal_price=price,
        meta={
            "purpose": "immediate_liability_repayment",
            "required_merl": amount_needed,
            "liability_eq": merl_eq,
            "liability_liab": merl_liab,
            "timestamp": int(time.time())
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
            print("✅ MERL purchased successfully!")
            
            # 检查新余额
            time.sleep(2)
            new_eq, new_liab = get_merl_liability(okx)
            print(f"\nNew MERL eq: {new_eq:.6f}")
            print(f"New MERL liab: {new_liab:.6f}")
            
            if new_eq >= -0.001:
                print("🎉 MERL liability fully repaid!")
            else:
                print(f"⚠️  Partial repayment. Remaining: {abs(new_eq):.6f} MERL")
                
        else:
            print(f"❌ Order not filled. State: {result.state}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("NEXT STEPS:")
    print("1. Check OKX website for actual MERL status")
    print("2. If liability remains, may need manual repayment")
    print("3. Run: python3 scripts/reconcile_with_retry.py")
    print("=" * 50)


if __name__ == "__main__":
    main()