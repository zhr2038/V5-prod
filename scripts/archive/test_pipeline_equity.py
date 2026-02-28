#!/usr/bin/env python3
"""
测试 pipeline 的 equity 计算
"""

import sys
sys.path.append('.')

from src.core.pipeline import V5Pipeline
from configs.loader import load_config
from src.execution.position_store import Position
from src.core.models import MarketSeries
import numpy as np

def test_equity_calculation():
    print("🧪 测试 pipeline equity 计算")
    print("=" * 60)
    
    # 加载配置
    cfg = load_config("configs/live_20u_test.yaml", env_path=".env")
    
    # 创建 pipeline
    pipeline = V5Pipeline(cfg)
    
    # 模拟持仓（从 positions.sqlite）
    positions = [
        Position(symbol="MERL/USDT", qty=0.000763, avg_px=0.06448),
        Position(symbol="SOL/USDT", qty=7.52761596659e-05, avg_px=0.0),
        Position(symbol="ETH/USDT", qty=1.5962e-06, avg_px=0.0),
        Position(symbol="AAVE/USDT", qty=4.85e-07, avg_px=0.0),
        Position(symbol="BNB/USDT", qty=3.9e-08, avg_px=0.0),
        Position(symbol="BTC/USDT", qty=2.22e-10, avg_px=0.0),
        Position(symbol="DOT/USDT", qty=7.42e-07, avg_px=0.0),
        Position(symbol="DOGE/USDT", qty=9.552e-07, avg_px=0.0),
        Position(symbol="TRX/USDT", qty=2.058e-07, avg_px=0.0),
        Position(symbol="USDG/USDT", qty=9.298e-09, avg_px=0.0),
    ]
    
    # 现金
    cash_usdt = 112.55520148772824
    
    # 模拟市场数据（使用当前价格）
    import requests
    
    market_data_1h = {}
    
    symbols = [p.symbol for p in positions]
    
    for symbol in symbols:
        inst_id = symbol.replace('/', '-')
        url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar=1H&limit=2"
        
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == '0' and data.get('data'):
                    candles = data['data']
                    
                    # 创建 MarketSeries
                    closes = [float(candle[4]) for candle in candles]
                    
                    market_data_1h[symbol] = MarketSeries(
                        open=np.array([float(candle[1]) for candle in candles]),
                        high=np.array([float(candle[2]) for candle in candles]),
                        low=np.array([float(candle[3]) for candle in candles]),
                        close=np.array(closes),
                        volume=np.array([float(candle[5]) for candle in candles]),
                        timestamp=[int(candle[0]) for candle in candles]
                    )
                    
                    print(f"{symbol}: 最新价格 = {closes[-1]:.6f}")
        except Exception as e:
            print(f"{symbol}: 获取失败 - {e}")
    
    if not market_data_1h:
        print("❌ 无法获取市场数据")
        return
    
    # 计算 equity
    equity = pipeline.compute_equity(cash_usdt, positions, market_data_1h)
    
    print(f"\n📊 Pipeline 计算结果:")
    print(f"  现金: {cash_usdt:.6f} USDT")
    print(f"  计算 equity: {equity:.6f} USDT")
    print(f"  差异: {equity - cash_usdt:.6f} USDT")
    
    # 手动计算验证
    print(f"\n🔍 手动计算验证:")
    
    manual_equity = cash_usdt
    for p in positions:
        if p.symbol in market_data_1h:
            price = market_data_1h[p.symbol].close[-1]
            value = p.qty * price
            manual_equity += value
            
            if value > 0.001:
                print(f"  {p.symbol}: {p.qty:.10f} * {price:.6f} = {value:.6f}")
    
    print(f"  手动计算 equity: {manual_equity:.6f} USDT")
    print(f"  Pipeline equity: {equity:.6f} USDT")
    print(f"  差异: {equity - manual_equity:.6f} USDT")
    
    # 检查是否有持仓计算错误
    print(f"\n🔎 检查每个持仓的计算:")
    
    for p in positions:
        if p.symbol in market_data_1h:
            price = market_data_1h[p.symbol].close[-1]
            value = p.qty * price
            
            if value > 1.0:  # 大于1 USDT的持仓
                print(f"  ⚠️  大额持仓: {p.symbol}")
                print(f"     数量: {p.qty}")
                print(f"     价格: {price}")
                print(f"     价值: {value:.2f} USDT")
    
    return equity


def main():
    print("🚀 Pipeline Equity 计算测试")
    print("=" * 60)
    
    equity = test_equity_calculation()
    
    print(f"\n📋 结论:")
    
    if abs(equity - 112.55520148772824) > 10.0:
        print(f"  ❌ equity 计算错误: {equity:.6f}")
        print(f"     应该是 ~112.56 USDT")
        print(f"     差异: {equity - 112.5552:.2f} USDT")
        
        print(f"\n🔧 可能的问题:")
        print(f"  1. market_data_1h 中的价格数据错误")
        print(f"  2. 某个持仓的 qty 错误")
        print(f"  3. Position 对象有额外属性影响计算")
    else:
        print(f"  ✅ equity 计算正常: {equity:.6f} USDT")
    
    print("=" * 60)


if __name__ == "__main__":
    main()