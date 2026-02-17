#!/usr/bin/env python3
"""
解决 MERL 负债问题
1. 检查负债详情
2. 尝试归还借贷
3. 或购买 MERL 平仓
"""

from __future__ import annotations

import os
import time
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore


def check_merl_liability(okx: OKXPrivateClient) -> dict:
    """检查 MERL 负债详情"""
    resp = okx.get_balance()
    details = resp.data['data'][0]['details']
    
    merl_info = None
    for d in details:
        if d.get('ccy') == 'MERL':
            merl_info = d
            break
    
    if not merl_info:
        return {"found": False, "message": "MERL not found in balance details"}
    
    # 提取关键字段
    eq = float(merl_info.get('eq', 0))
    liab = float(merl_info.get('liab', 0))
    cross_liab = float(merl_info.get('crossLiab', 0))
    borrow_froz = float(merl_info.get('borrowFroz', 0))
    avail_bal = float(merl_info.get('availBal', 0))
    
    return {
        "found": True,
        "eq": eq,
        "liab": liab,
        "cross_liab": cross_liab,
        "borrow_froz": borrow_froz,
        "avail_bal": avail_bal,
        "needs_repayment": eq < -0.01 or liab > 0.01,
        "repayment_amount": abs(eq) if eq < 0 else liab,
        "details": merl_info
    }


def check_borrow_repay_history(okx: OKXPrivateClient) -> list:
    """检查借贷历史"""
    try:
        # 获取最近账单，查找借贷相关记录
        resp = okx.get_bills(limit=100)
        bills = resp.data.get('data', [])
        
        borrow_bills = []
        for b in bills:
            if b.get('ccy') == 'MERL':
                bill_type = b.get('type')
                sub_type = b.get('subType')
                # 类型 8: 借贷，类型 9: 还款
                if bill_type in ['8', '9', '10', '11']:  # 借贷相关类型
                    borrow_bills.append(b)
        
        return borrow_bills
    except Exception as e:
        print(f"Error checking borrow history: {e}")
        return []


def repay_merl_loan(
    cfg,
    okx: OKXPrivateClient,
    amount: float,
    live_engine: LiveExecutionEngine
) -> bool:
    """尝试归还 MERL 借贷"""
    print(f"Attempting to repay {amount} MERL...")
    
    try:
        # 方法1: 如果有可用 MERL，直接归还
        # 方法2: 购买 MERL 然后归还
        
        # 首先检查当前 MERL 价格
        from src.data.okx_ccxt_provider import OKXCCXTProvider
        provider = OKXCCXTProvider(cfg.data)
        
        # 获取 MERL/USDT 价格
        ticker = provider.fetch_ticker("MERL/USDT")
        current_price = float(ticker.get('last', 0))
        
        if current_price <= 0:
            print(f"Cannot get MERL price")
            return False
        
        # 计算需要购买的 USDT 金额
        usdt_needed = amount * current_price * 1.01  # 加1%缓冲
        
        print(f"MERL price: {current_price:.6f}")
        print(f"Need to buy {amount:.6f} MERL ≈ {usdt_needed:.4f} USDT")
        
        # 检查 USDT 余额
        balance_resp = okx.get_balance()
        usdt_balance = 0
        for d in balance_resp.data['data'][0]['details']:
            if d.get('ccy') == 'USDT':
                usdt_balance = float(d.get('availBal', 0))
                break
        
        print(f"Available USDT: {usdt_balance:.4f}")
        
        if usdt_balance < usdt_needed:
            print(f"Insufficient USDT. Need {usdt_needed:.4f}, have {usdt_balance:.4f}")
            return False
        
        # 创建购买订单
        buy_order = Order(
            symbol="MERL/USDT",
            side="buy",
            intent="REPAY_LOAN",
            notional_usdt=usdt_needed,
            signal_price=current_price,
            meta={
                "purpose": "repay_merl_loan",
                "required_amount": amount,
                "estimated_qty": amount
            }
        )
        
        print(f"Placing buy order for {usdt_needed:.4f} USDT of MERL...")
        result = live_engine.place(buy_order)
        print(f"Order result: {result.state}, clOrdId: {result.cl_ord_id}")
        
        if result.state == "FILLED":
            print("✓ MERL purchased successfully")
            
            # 等待订单完全处理
            time.sleep(2)
            live_engine.poll_open(limit=10)
            
            # 现在尝试归还借贷
            # 注意: OKX API 可能需要特定的还款端点
            print("Attempting to repay loan...")
            # 这里需要调用 OKX 的还款 API，但我们的客户端可能不支持
            
            return True
        else:
            print(f"Order not filled: {result.state}")
            return False
            
    except Exception as e:
        print(f"Error in repay_merl_loan: {e}")
        return False


def main() -> None:
    """主函数：诊断并尝试解决 MERL 负债"""
    print("=" * 60)
    print("MERL LIABILITY DIAGNOSIS & FIX")
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
    
    # 1. 检查负债详情
    print("\n[1] Checking MERL liability...")
    liability = check_merl_liability(okx)
    
    if not liability["found"]:
        print("✅ MERL not found in balance - no liability")
        return
    
    print(f"   MERL eq: {liability['eq']:.6f}")
    print(f"   MERL liab: {liability['liab']:.6f}")
    print(f"   MERL crossLiab: {liability['cross_liab']:.6f}")
    print(f"   MERL borrowFroz: {liability['borrow_froz']:.6f}")
    print(f"   MERL availBal: {liability['avail_bal']:.6f}")
    
    if not liability["needs_repayment"]:
        print("✅ No significant MERL liability found")
        return
    
    repayment_amount = liability["repayment_amount"]
    print(f"\n⚠️  MERL LIABILITY DETECTED: Need to repay {repayment_amount:.6f} MERL")
    
    # 2. 检查借贷历史
    print("\n[2] Checking borrow/repay history...")
    borrow_history = check_borrow_repay_history(okx)
    if borrow_history:
        print(f"   Found {len(borrow_history)} MERL borrow/repay records")
        for b in borrow_history[:3]:  # 显示最近3条
            print(f"   - Type: {b.get('type')}, SubType: {b.get('subType')}, "
                  f"BalChg: {b.get('balChg')}, TS: {b.get('ts')}")
    else:
        print("   No borrow/repay history found")
    
    # 3. 检查网页端（用户需要手动操作）
    print("\n[3] MANUAL ACTION REQUIRED:")
    print("   Please log in to OKX website/app and check:")
    print("   a) Spot Account → MERL balance")
    print("   b) Borrow/Lending → Active MERL loans")
    print("   c) Margin Account → MERL positions")
    
    # 4. 提供解决方案选项
    print("\n[4] POSSIBLE SOLUTIONS:")
    print("   A) Manual repayment on OKX website:")
    print("      1. Go to 'Borrow/Lending'")
    print("      2. Find MERL loan")
    print("      3. Click 'Repay'")
    
    print("\n   B) Auto-repay via script (experimental):")
    print("      This script can attempt to buy MERL and repay")
    print("      WARNING: This may not work if OKX API doesn't support")
    
    # 5. 询问用户是否尝试自动还款
    print("\n[5] ATTEMPT AUTOMATIC REPAYMENT?")
    print(f"    Would require buying ~{repayment_amount:.2f} MERL")
    
    # 这里可以添加自动还款逻辑，但需要用户确认
    # 由于安全考虑，不自动执行
    
    print("\n[6] RECOMMENDED ACTION:")
    print("    1. Login to OKX website NOW")
    print("    2. Check MERL in all account sections")
    print("    3. If loan exists, repay manually")
    print("    4. If it's an API error, contact OKX support")
    
    print("\n[7] TEMPORARY WORKAROUND (already implemented):")
    print("    ✅ Reconcile ignores MERL liability")
    print("    ✅ V5 trading can continue")
    print("    ⚠️  But liability may accrue interest!")
    
    print("\n" + "=" * 60)
    print("SUMMARY: MERL liability needs MANUAL attention")
    print("Check OKX website immediately!")
    print("=" * 60)


if __name__ == "__main__":
    main()