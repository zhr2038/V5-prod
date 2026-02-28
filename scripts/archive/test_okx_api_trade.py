#!/usr/bin/env python3
"""
直接测试 OKX API 交易功能
"""

import os
import sys
sys.path.append('.')

from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient
import time

def test_okx_trade():
    print("🧪 直接测试 OKX API 交易")
    print("=" * 60)
    
    # 加载配置
    cfg = load_config("configs/live_20u_real.yaml", env_path=".env")
    
    # 创建客户端
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 1. 检查账户
    print("1. 检查账户...")
    resp = okx.get_balance()
    
    if not resp.data or 'data' not in resp.data:
        print("❌ 无法获取账户数据")
        return
    
    account = resp.data['data'][0]
    total_eq = float(account.get('totalEq', 0))
    print(f"  账户总权益: {total_eq:.4f} USDT")
    
    # 2. 检查持仓
    print("\n2. 检查持仓...")
    positions_resp = okx.get_positions()
    
    if positions_resp.data and 'data' in positions_resp.data:
        positions = positions_resp.data['data']
        print(f"  当前持仓数: {len(positions)}")
        
        for pos in positions:
            inst_id = pos.get('instId', '')
            pos_side = pos.get('posSide', '')
            pos_qty = float(pos.get('pos', 0))
            if pos_qty > 0:
                print(f"  {inst_id}: {pos_qty} ({pos_side})")
    else:
        print("  无持仓或获取失败")
    
    # 3. 测试小额交易（1 USDT 的 SOL）
    print("\n3. 测试小额交易...")
    
    symbol = "SOL-USDT"
    side = "buy"
    td_mode = "cash"  # 现货交易
    ord_type = "market"  # 市价单
    
    # 获取当前价格
    from src.data.providers.okx_ccxt_provider import OKXCCXTProvider
    provider = OKXCCXTProvider(rate_limit=True)
    
    # 转换 symbol 格式
    ccxt_symbol = symbol.replace('-', '/')
    md = provider.fetch_ohlcv([ccxt_symbol], timeframe="1m", limit=1)
    
    if ccxt_symbol in md:
        price = md[ccxt_symbol].close[-1]
        print(f"  {symbol} 当前价格: {price:.4f}")
        
        # 计算数量（0.5 USDT）
        notional = 0.5
        sz = notional / price
        
        print(f"  计划买入: {sz:.6f} {symbol} ({notional:.2f} USDT)")
        
        # 确认
        confirm = input("\n确认提交测试交易？(输入 YES 确认): ")
        if confirm != "YES":
            print("❌ 交易取消")
            return
        
        # 提交订单
        print("  提交订单...")
        try:
            order_resp = okx.place_order(
                inst_id=symbol,
                td_mode=td_mode,
                side=side,
                ord_type=ord_type,
                sz=str(sz),
                cl_ord_id=f"TEST_{int(time.time())}"
            )
            
            print(f"  订单响应: {order_resp}")
            
            if order_resp.data and order_resp.data.get('code') == '0':
                print("  ✅ 订单提交成功！")
                ord_id = order_resp.data.get('data', [{}])[0].get('ordId', '')
                
                # 检查订单状态
                if ord_id:
                    time.sleep(2)
                    order_info = okx.get_order(inst_id=symbol, ord_id=ord_id)
                    print(f"  订单详情: {order_info}")
            else:
                print(f"  ❌ 订单提交失败: {order_resp.data}")
                
        except Exception as e:
            print(f"  ❌ 交易异常: {e}")
    
    else:
        print(f"  ❌ 无法获取 {symbol} 价格")
    
    print("\n" + "=" * 60)
    print("📋 测试完成")

if __name__ == "__main__":
    test_okx_trade()