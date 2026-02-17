#!/usr/bin/env python3
"""
修复所有负债
自动购买并归还所有负余额的币种
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


def get_price(symbol: str) -> float:
    """获取币种价格"""
    inst_id = symbol.replace("/", "-")
    url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0' and data.get('data'):
                return float(data['data'][0]['last'])
    except:
        pass
    
    # 默认价格（基于常见币种）
    defaults = {
        "MERL/USDT": 0.064,
        "SOL/USDT": 150.0,
        "BTC/USDT": 60000.0,
        "ETH/USDT": 3000.0,
        "BNB/USDT": 500.0,
    }
    return defaults.get(symbol, 1.0)


def check_liabilities(okx: OKXPrivateClient) -> list:
    """检查所有负债"""
    resp = okx.get_balance()
    liabilities = []
    
    for d in resp.data['data'][0]['details']:
        ccy = d.get('ccy', '')
        eq = float(d.get('eq', 0))
        liab = float(d.get('liab', 0))
        
        # 检查是否有负债
        if eq < -0.0001 or liab > 0.0001:
            symbol = f"{ccy}/USDT"
            amount_needed = abs(eq) if eq < 0 else liab
            
            liabilities.append({
                'ccy': ccy,
                'symbol': symbol,
                'eq': eq,
                'liab': liab,
                'amount_needed': amount_needed,
                'details': d
            })
    
    return liabilities


def repay_liability(
    cfg,
    okx: OKXPrivateClient,
    liability: dict,
    live_engine: LiveExecutionEngine
) -> bool:
    """归还单个负债"""
    ccy = liability['ccy']
    symbol = liability['symbol']
    amount = liability['amount_needed']
    
    print(f"\n🔧 Repairing {ccy} liability: {liability['eq']:.6f}")
    
    # 获取价格
    price = get_price(symbol)
    if price <= 0:
        print(f"❌ Cannot get price for {symbol}")
        return False
    
    # 计算所需 USDT
    usdt_needed = amount * price * 1.03  # 加3%缓冲
    
    print(f"   Price: {price:.6f}")
    print(f"   Need: {amount:.6f} {ccy}")
    print(f"   Cost: {usdt_needed:.4f} USDT")
    
    # 检查 USDT 余额
    usdt_balance = 0
    resp = okx.get_balance()
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'USDT':
            usdt_balance = float(d.get('availBal', 0))
            break
    
    print(f"   Available USDT: {usdt_balance:.4f}")
    
    if usdt_balance < usdt_needed:
        print(f"❌ Insufficient USDT for {ccy}. Need {usdt_needed:.4f}, have {usdt_balance:.4f}")
        return False
    
    # 创建购买订单
    buy_order = Order(
        symbol=symbol,
        side="buy",
        intent="REPAY_LIABILITY",
        notional_usdt=usdt_needed,
        signal_price=price,
        meta={
            "purpose": "repay_liability",
            "ccy": ccy,
            "required_amount": amount,
            "original_eq": liability['eq'],
            "original_liab": liability['liab']
        }
    )
    
    print(f"   Placing buy order for {usdt_needed:.4f} USDT...")
    
    try:
        result = live_engine.place(buy_order)
        print(f"   Order ID: {result.cl_ord_id}, State: {result.state}")
        
        # 等待并轮询
        time.sleep(3)
        live_engine.poll_open(limit=10)
        
        if result.state == "FILLED":
            print(f"✅ {ccy} purchased successfully")
            return True
        else:
            print(f"❌ {ccy} order not filled: {result.state}")
            return False
            
    except Exception as e:
        print(f"❌ Error buying {ccy}: {e}")
        return False


def main() -> None:
    print("🔧 FIX ALL LIABILITIES")
    print("=" * 50)
    
    # 安全检查
    if os.getenv("V5_LIVE_ARM") != "YES":
        print("❌ Set V5_LIVE_ARM=YES to proceed")
        return
    
    # 加载配置
    cfg = load_config("configs/live_small.yaml", env_path=".env")
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 检查负债
    liabilities = check_liabilities(okx)
    
    if not liabilities:
        print("✅ No liabilities found")
        return
    
    print(f"Found {len(liabilities)} liabilities:")
    for l in liabilities:
        print(f"  {l['ccy']}: eq={l['eq']:.6f}, liab={l['liab']:.6f}, need={l['amount_needed']:.6f}")
    
    # 询问确认
    total_usdt = sum(l['amount_needed'] * get_price(l['symbol']) * 1.03 for l in liabilities)
    print(f"\nTotal estimated cost: {total_usdt:.4f} USDT")
    print("\nType 'YES' to proceed with repairs:")
    
    try:
        confirmation = input().strip().upper()
    except:
        confirmation = "NO"
    
    if confirmation != "YES":
        print("Cancelled.")
        return
    
    # 初始化执行引擎
    run_id = f"liability_repair_{int(time.time())}"
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
    
    # 修复每个负债
    success_count = 0
    for liability in liabilities:
        if repay_liability(cfg, okx, liability, live):
            success_count += 1
        time.sleep(2)  # 间隔避免 API 限制
    
    # 最终检查
    print("\n" + "=" * 50)
    print("FINAL CHECK")
    print("=" * 50)
    
    final_liabilities = check_liabilities(okx)
    if not final_liabilities:
        print("✅ All liabilities repaired!")
    else:
        print(f"⚠️  {len(final_liabilities)} liabilities remain:")
        for l in final_liabilities:
            print(f"  {l['ccy']}: eq={l['eq']:.6f}")
    
    print(f"\nRepaired {success_count}/{len(liabilities)} liabilities")
    print("\nNext: Run reconcile to verify account state")


if __name__ == "__main__":
    main()