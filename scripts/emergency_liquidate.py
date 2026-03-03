#!/usr/bin/env python3
"""
Emergency Liquidation Script - 紧急清仓
Sell all non-USDT positions immediately
"""

import os
import sys
from pathlib import Path
from decimal import Decimal, ROUND_DOWN

# Load env
for line in Path('.env').read_text().splitlines():
    if '=' in line and not line.strip().startswith('#'):
        k, v = line.split('=', 1)
        os.environ[k.strip()] = v.strip().strip('"').strip("'")

sys.path.insert(0, '.')

from src.execution.okx_private_client import OKXPrivateClient
from configs.loader import load_config

def emergency_liquidate():
    cfg = load_config('configs/live_20u_real.yaml', env_path='.env')
    client = OKXPrivateClient(cfg.exchange)
    
    print("=" * 50)
    print("EMERGENCY LIQUIDATION - 紧急清仓")
    print("=" * 50)
    
    # Get current positions
    bal = client.get_balance()
    positions = []
    
    if hasattr(bal, 'data') and bal.data:
        data = bal.data.get('data', [{}])[0]
        details = data.get('details', [])
        for d in details:
            ccy = d.get('ccy', '')
            eq = float(d.get('eq', 0))
            avail = float(d.get('availBal', 0))
            if ccy != 'USDT' and eq > 0.01:
                positions.append({'ccy': ccy, 'eq': eq, 'avail': avail})
    
    if not positions:
        print("No positions to liquidate.")
        client.close()
        return
    
    print(f"\nPositions to liquidate: {len(positions)}")
    
    for pos in positions:
        ccy = pos['ccy']
        avail = pos['avail']
        inst_id = f"{ccy}-USDT"
        
        print(f"\n{ccy}: {avail:.6f} available")
        
        # Place market sell order
        try:
            # Get instrument specs
            specs_resp = client.request("GET", "/api/v5/public/instruments", params={"instType": "SPOT", "instId": inst_id})
            specs = specs_resp.data.get('data', [{}])[0] if specs_resp.data else {}
            lot_sz = float(specs.get('lotSz', 0))
            min_sz = float(specs.get('minSz', 0))
            
            if lot_sz > 0:
                lot_dec = Decimal(str(lot_sz))
                qty_dec = (Decimal(str(avail)) / lot_dec).to_integral_value(rounding=ROUND_DOWN) * lot_dec
                qty = float(qty_dec)
            else:
                qty = avail
            
            if qty < min_sz:
                print(f"  SKIP: qty {qty} < minSz {min_sz}")
                continue
            
            payload = {
                "instId": inst_id,
                "tdMode": "cash",
                "side": "sell",
                "ordType": "market",
                "sz": str(qty),
            }
            
            result = client.place_order(payload)
            
            if hasattr(result, 'data') and result.data:
                code = result.data.get('code', '')
                if code == '0':
                    ord_id = result.data.get('data', [{}])[0].get('ordId')
                    print(f"  ✓ SELL ORDER PLACED: ordId={ord_id}")
                else:
                    msg = result.data.get('msg', 'unknown')
                    print(f"  ✗ FAILED: code={code}, msg={msg}")
            else:
                print(f"  ✗ FAILED: {result}")
                    
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
    
    print(f"\n{'=' * 50}")
    print("Check fills in OKX app or web interface.")
    print("=" * 50)
    
    client.close()

if __name__ == "__main__":
    emergency_liquidate()
