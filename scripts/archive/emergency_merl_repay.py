#!/usr/bin/env python3
"""
紧急 MERL 还款脚本
尝试购买 MERL 并归还借贷
"""

from __future__ import annotations

import os
import time
import json
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


def get_merl_price(okx: OKXPrivateClient) -> float:
    """获取 MERL 当前价格"""
    try:
        # 使用公共 API 获取价格
        import requests
        url = "https://www.okx.com/api/v5/market/ticker?instId=MERL-USDT"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0':
                ticker = data['data'][0]
                return float(ticker['last'])
    except Exception as e:
        print(f"Error getting MERL price: {e}")
    
    # 备用方法：使用最近成交价
    try:
        from src.data.okx_ccxt_provider import OKXCCXTProvider
        cfg = load_config("configs/live_small.yaml", env_path=".env")
        provider = OKXCCXTProvider(cfg.data)
        ticker = provider.fetch_ticker("MERL/USDT")
        return float(ticker.get('last', 0))
    except:
        return 0.064  # 默认价格（基于最近成交）


def check_usdt_balance(okx: OKXPrivateClient) -> float:
    """检查 USDT 余额"""
    resp = okx.get_balance()
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'USDT':
            return float(d.get('availBal', 0))
    return 0.0


def buy_merl_for_repayment(
    cfg,
    okx: OKXPrivateClient,
    amount_merl: float,
    max_usdt: float = None
) -> bool:
    """购买 MERL 用于还款"""
    print(f"\n[BUY MERL] Attempting to buy {amount_merl:.6f} MERL for repayment")
    
    # 获取当前价格
    price = get_merl_price(okx)
    if price <= 0:
        print("❌ Cannot get MERL price")
        return False
    
    print(f"   MERL current price: {price:.6f}")
    
    # 计算所需 USDT
    needed_usdt = amount_merl * price * 1.02  # 加2%缓冲（价格波动+手续费）
    
    # 检查 USDT 余额
    usdt_balance = check_usdt_balance(okx)
    print(f"   Available USDT: {usdt_balance:.4f}")
    print(f"   Needed USDT: {needed_usdt:.4f}")
    
    if max_usdt:
        needed_usdt = min(needed_usdt, max_usdt)
        amount_merl = needed_usdt / price / 1.02
    
    if usdt_balance < needed_usdt:
        print(f"❌ Insufficient USDT. Need {needed_usdt:.4f}, have {usdt_balance:.4f}")
        return False
    
    # 初始化执行引擎
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
    buy_order = Order(
        symbol="MERL/USDT",
        side="buy",
        intent="REPAY_LOAN",
        notional_usdt=needed_usdt,
        signal_price=price,
        meta={
            "purpose": "emergency_merl_repayment",
            "required_merl": amount_merl,
            "estimated_qty": amount_merl,
            "timestamp": int(time.time())
        }
    )
    
    print(f"   Placing buy order for {needed_usdt:.4f} USDT...")
    
    try:
        result = live.place(buy_order)
        print(f"   Order submitted: {result.cl_ord_id}, state: {result.state}")
        
        # 等待并轮询状态
        time.sleep(3)
        live.poll_open(limit=10)
        
        # 检查最终状态
        if result.state == "FILLED":
            print("✅ MERL purchased successfully")
            
            # 获取实际成交数量
            from sqlite3 import connect
            conn = connect("reports/orders.sqlite")
            cursor = conn.cursor()
            cursor.execute(
                "SELECT acc_fill_sz, avg_px FROM orders WHERE cl_ord_id = ?",
                (result.cl_ord_id,)
            )
            row = cursor.fetchone()
            conn.close()
            
            if row and row[0]:
                actual_qty = float(row[0])
                actual_price = float(row[1]) if row[1] else price
                print(f"   Actual: {actual_qty:.6f} MERL @ {actual_price:.6f}")
                print(f"   Cost: {actual_qty * actual_price:.6f} USDT")
            
            return True
        else:
            print(f"❌ Order not filled. State: {result.state}")
            return False
            
    except Exception as e:
        print(f"❌ Error placing order: {e}")
        return False


def attempt_repay_via_api(okx: OKXPrivateClient, amount: float) -> bool:
    """尝试通过 API 还款"""
    print(f"\n[API REPAY] Attempting to repay {amount:.6f} MERL via OKX API")
    
    # OKX 还款 API 端点（需要确认）
    # 通常需要调用 /api/v5/account/quick-margin-borrow-repay
    # 但我们的客户端可能不支持
    
    print("⚠️  Repay API not implemented in current client")
    print("   Manual repayment required on OKX website")
    
    return False


def main() -> None:
    """主函数：紧急 MERL 还款"""
    print("=" * 60)
    print("EMERGENCY MERL REPAYMENT")
    print("=" * 60)
    
    # 安全检查
    arm_env = "V5_LIVE_ARM"
    arm_val = "YES"
    if os.getenv(arm_env) != arm_val:
        print(f"❌ SAFETY CHECK FAILED: Set {arm_env}={arm_val} to proceed")
        return
    
    # 加载配置
    cfg = load_config("configs/live_small.yaml", env_path=".env")
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 检查负债
    resp = okx.get_balance()
    merl_liab = 0
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'MERL':
            merl_liab = abs(float(d.get('eq', 0)))
            break
    
    if merl_liab < 0.1:
        print("✅ No significant MERL liability found")
        return
    
    print(f"MERL liability: {merl_liab:.6f}")
    
    # 询问用户确认
    print(f"\nThis will attempt to buy {merl_liab:.2f} MERL to repay liability.")
    print(f"Estimated cost: ~{merl_liab * 0.064:.2f} USDT")
    print("\nType 'YES' to proceed, anything else to cancel:")
    
    try:
        confirmation = input().strip().upper()
    except:
        confirmation = "NO"
    
    if confirmation != "YES":
        print("Cancelled.")
        return
    
    # 步骤1: 购买 MERL
    print("\n" + "=" * 60)
    print("STEP 1: BUYING MERL")
    print("=" * 60)
    
    success = buy_merl_for_repayment(cfg, okx, merl_liab, max_usdt=50.0)
    
    if not success:
        print("\n❌ Failed to buy MERL. Manual action required.")
        return
    
    # 步骤2: 尝试还款
    print("\n" + "=" * 60)
    print("STEP 2: ATTEMPTING REPAYMENT")
    print("=" * 60)
    
    # 等待购买结算
    print("Waiting 5 seconds for purchase to settle...")
    time.sleep(5)
    
    # 检查新余额
    print("\nChecking new MERL balance...")
    resp = okx.get_balance()
    new_merl_eq = 0
    for d in resp.data['data'][0]['details']:
        if d.get('ccy') == 'MERL':
            new_merl_eq = float(d.get('eq', 0))
            break
    
    print(f"New MERL eq: {new_merl_eq:.6f}")
    
    if new_merl_eq > 0:
        print("✅ MERL purchased successfully")
        print("⚠️  Now you need to MANUALLY repay on OKX website:")
        print("   1. Go to OKX website/app")
        print("   2. Navigate to Borrow/Lending")
        print("   3. Find MERL loan")
        print("   4. Click 'Repay'")
    elif new_merl_eq < -0.1:
        print("❌ MERL still negative. Liability persists.")
        print("   Possible reasons:")
        print("   - Purchase didn't cover full liability")
        print("   - OKX system error")
        print("   - Need to use specific repay API")
    else:
        print("✅ MERL liability appears resolved!")
    
    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("1. Check OKX website for actual MERL status")
    print("2. If liability remains, contact OKX support")
    print("3. Run reconcile to verify: python3 scripts/reconcile_with_retry.py")
    print("=" * 60)


if __name__ == "__main__":
    main()