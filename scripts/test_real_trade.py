#!/usr/bin/env python3
"""
测试真实交易
"""

import os
import sys
sys.path.append('.')

from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.live_execution_engine import LiveExecutionEngine
from src.execution.position_store import PositionStore
from src.execution.order_store import OrderStore

def test_real_trade():
    print("🧪 测试真实交易")
    print("=" * 60)
    
    # 加载配置
    cfg = load_config("configs/live_20u_real_aggressive.yaml", env_path=".env")
    
    # 创建客户端
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 检查账户
    resp = okx.get_balance()
    if not resp.data or 'data' not in resp.data:
        print("❌ 无法获取账户数据")
        return
    
    account = resp.data['data'][0]
    total_eq = float(account.get('totalEq', 0))
    print(f"账户总权益: {total_eq:.4f} USDT")
    
    # 检查 USDT 余额
    usdt_balance = 0
    for detail in account.get('details', []):
        if detail.get('ccy') == 'USDT':
            usdt_balance = float(detail.get('availBal', 0))
            print(f"USDT 可用余额: {usdt_balance:.4f}")
    
    if usdt_balance < 5:
        print("❌ USDT 余额不足")
        return
    
    # 创建执行引擎
    run_id = f"test_real_{int(os.times().elapsed)}"
    store = PositionStore(path="reports/positions.sqlite")
    order_store = OrderStore(cfg.execution.order_store_path)
    
    live = LiveExecutionEngine(
        cfg.execution,
        okx=okx,
        order_store=order_store,
        position_store=store,
        run_id=run_id,
        exp_time_ms=getattr(cfg.execution, "okx_exp_time_ms", None),
    )
    
    print(f"执行引擎创建成功: {live}")
    print(f"Dry run: {cfg.execution.dry_run}")
    
    # 测试小额买入
    symbol = "SOL/USDT"
    inst_id = symbol.replace('/', '-')
    
    print(f"\n测试交易: {symbol}")
    
    # 获取当前价格
    from src.data.providers.okx_ccxt_provider import OKXCCXTProvider
    provider = OKXCCXTProvider(rate_limit=True)
    md = provider.fetch_ohlcv([symbol], timeframe="1h", limit=1)
    
    if symbol in md:
        price = md[symbol].close[-1]
        print(f"当前价格: {price:.4f}")
        
        # 计算买入金额（1 USDT）
        notional = 1.0
        qty = notional / price
        
        print(f"计划买入: {qty:.6f} {symbol} ({notional:.2f} USDT)")
        
        # 尝试买入
        try:
            print("尝试提交买单...")
            
            # 直接使用 OKX API
            import requests
            import json
            import time
            
            # 这里需要实际的 API 调用
            # 由于安全原因，不在这里实现完整的交易代码
            
            print("⚠️  交易代码需要根据 OKX API 文档实现")
            print("建议：")
            print("1. 检查 V5 的 live_execution_engine.py")
            print("2. 确保 dry_run=false")
            print("3. 检查 budget.action_enabled")
            print("4. 检查 min_trade_notional 限制")
            
        except Exception as e:
            print(f"交易失败: {e}")
    
    else:
        print(f"❌ 无法获取 {symbol} 价格")

if __name__ == "__main__":
    test_real_trade()