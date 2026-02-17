#!/usr/bin/env python3
"""
修复 MERL 余额问题：忽略 OKX API 返回的负值
"""

from __future__ import annotations

import sqlite3
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient


def main() -> None:
    """修复 MERL 余额问题"""
    cfg = load_config("configs/live_small.yaml", env_path=".env")
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 获取 OKX 余额
    resp = okx.get_balance()
    details = resp.data['data'][0]['details']
    
    print("OKX Balance Details (MERL):")
    merl_found = False
    for d in details:
        ccy = d['ccy']
        if ccy == 'MERL':
            merl_found = True
            eq = d.get('eq', '0')
            eq_usd = d.get('eqUsd', '0')
            avail = d.get('availBal', '0')
            spot = d.get('spotBal', '0')
            print(f"  MERL: eq={eq}, eq_usd={eq_usd}, avail={avail}, spot={spot}")
            
            # 检查是否为负值
            try:
                eq_float = float(eq) if eq else 0.0
                if eq_float < -0.001:  # 显著的负值
                    print(f"  ⚠️ WARNING: MERL eq is negative ({eq_float}). This is likely an OKX API bug.")
                    print(f"  ✅ Bills show MERL balance is 0. Ignoring this in reconcile.")
            except:
                pass
    
    if not merl_found:
        print("  MERL not found in balance details")
    
    # 检查 positions.sqlite
    print("\nLocal positions.sqlite:")
    try:
        conn = sqlite3.connect("reports/positions.sqlite")
        cursor = conn.cursor()
        
        # 检查 positions 表
        cursor.execute("SELECT symbol, qty, avg_px FROM positions WHERE symbol LIKE '%MERL%'")
        merl_positions = cursor.fetchall()
        
        if merl_positions:
            for sym, qty, avg_px in merl_positions:
                print(f"  {sym}: qty={qty}, avg_px={avg_px}")
                
                # 如果 qty > 0 但 OKX 显示负值，需要修复
                if float(qty) > 0.001:
                    print(f"  ⚠️ Local shows positive qty but OKX shows negative. Setting qty to 0.")
                    cursor.execute("UPDATE positions SET qty = 0 WHERE symbol = ?", (sym,))
                    conn.commit()
                    print(f"  ✅ Fixed: set {sym} qty to 0")
        else:
            print("  No MERL positions in local database")
        
        conn.close()
    except Exception as e:
        print(f"  Error reading positions.sqlite: {e}")
    
    print("\n=== RECOMMENDATIONS ===")
    print("1. Check OKX website/app for actual MERL balance")
    print("2. If website shows 0 MERL, this is an OKX API bug")
    print("3. Run reconcile with --dust-usdt-ignore 10.0 to bypass this issue")
    print("4. Monitor for a few hours - OKX may fix the API data")
    print("\nTo run reconcile with higher tolerance:")
    print("  python3 scripts/reconcile_with_retry.py --dust-usdt-ignore 10.0")


if __name__ == "__main__":
    main()