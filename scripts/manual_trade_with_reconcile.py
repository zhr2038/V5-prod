#!/usr/bin/env python3
"""
手动交易后自动运行 reconcile，确保状态同步。
用法：
  export V5_LIVE_ARM=YES
  python3 manual_trade_with_reconcile.py --symbol SOL/USDT --side sell --notional 10.0
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime

from configs.loader import load_config
from src.core.models import Order
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.order_store import OrderStore
from src.execution.position_store import PositionStore
from src.execution.account_store import AccountStore
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/live_small.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--symbol", required=True, help="交易对，如 SOL/USDT")
    ap.add_argument("--side", required=True, choices=["buy", "sell"])
    ap.add_argument("--notional", type=float, default=0.0, help="交易金额（USDT），0表示市价全仓")
    ap.add_argument("--sleep", type=float, default=3.0, help="下单后等待确认的时间（秒）")
    ap.add_argument("--reconcile-retries", type=int, default=3)
    ap.add_argument("--reconcile-delay", type=float, default=2.0)
    args = ap.parse_args()

    # 安全检查
    arm_env = "V5_LIVE_ARM"
    arm_val = "YES"
    if os.getenv(arm_env) != arm_val:
        raise RuntimeError(f"Refuse to place live order: set {arm_env}={arm_val}")

    cfg = load_config(args.config, env_path=args.env)
    
    run_id = datetime.utcnow().strftime("manual_%Y%m%d_%H%M%S")
    
    okx = OKXPrivateClient(exchange=cfg.exchange)
    try:
        ps = PositionStore(path="reports/positions.sqlite")
        os_store = OrderStore(cfg.execution.order_store_path)
        live = LiveExecutionEngine(
            cfg.execution, 
            okx=okx, 
            order_store=os_store, 
            position_store=ps, 
            run_id=run_id,
            exp_time_ms=getattr(cfg.execution, "okx_exp_time_ms", None)
        )

        # 创建订单
        o = Order(
            symbol=args.symbol,
            side=args.side,
            intent="MANUAL",
            notional_usdt=float(args.notional),
            signal_price=0.0,
            meta={
                "regime": "Manual", 
                "ts": int(time.time()),
                "note": "manual trade with auto-reconcile"
            },
        )

        print(f"Placing {args.side} order for {args.symbol} (notional: {args.notional} USDT)...")
        res = live.place(o)
        
        # 等待订单处理
        print(f"Order submitted: clOrdId={res.cl_ord_id}, state={res.state}")
        if args.sleep > 0:
            print(f"Waiting {args.sleep}s for order confirmation...")
            time.sleep(args.sleep)
        
        # 轮询订单状态
        live.poll_open(limit=200)
        print(f"Final state: {res.state}, ordId={res.ord_id}")
        
        # 立即运行 reconcile（带重试）
        print("\n=== Running reconcile after manual trade ===")
        for attempt in range(args.reconcile_retries):
            if attempt > 0:
                print(f"Reconcile retry {attempt}/{args.reconcile_retries-1} after {args.reconcile_delay}s...")
                time.sleep(args.reconcile_delay)
            
            try:
                eng = ReconcileEngine(
                    okx=okx,
                    position_store=PositionStore(path="reports/positions.sqlite"),
                    account_store=AccountStore(path="reports/positions.sqlite"),
                    thresholds=ReconcileThresholds(
                        abs_usdt_tol=2.0,
                        abs_base_tol=1e-4,
                        dust_usdt_ignore=5.0,
                    ),
                )
                obj = eng.reconcile(out_path="reports/reconcile_status.json")
                
                ok = obj.get("ok", False)
                reason = obj.get("reason")
                
                print(f"Reconcile attempt {attempt+1}: ok={ok}, reason={reason}")
                
                if ok:
                    print("✓ Reconcile successful after manual trade")
                    
                    # 检查并自动修复 kill_switch
                    import json
                    from pathlib import Path
                    ks_path = cfg.execution.kill_switch_path
                    if Path(ks_path).exists():
                        with open(ks_path) as f:
                            ks = json.load(f)
                        if ks.get("enabled"):
                            ks["enabled"] = False
                            ks["auto_disabled_ts_ms"] = int(time.time() * 1000)
                            ks["auto_disabled_reason"] = "manual_trade_reconcile_succeeded"
                            with open(ks_path, "w") as f:
                                json.dump(ks, f, indent=2)
                            print(f"✓ Kill switch auto-disabled")
                    
                    return
                else:
                    print(f"Reconcile failed: {reason}")
                    
            except Exception as e:
                print(f"Reconcile attempt {attempt+1} exception: {e}")
        
        print(f"✗ All {args.reconcile_retries} reconcile attempts failed")
        
    finally:
        okx.close()


if __name__ == "__main__":
    main()