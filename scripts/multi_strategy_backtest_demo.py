#!/usr/bin/env python3
"""
多策略回测对比演示

对比:
1. 单策略 (仅趋势跟踪)
2. 多策略并行 (趋势60% + 均值回归40%)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# 模拟市场数据 - 使用真实波动特征
np.random.seed(42)

def generate_market_data(days=30, symbols=['BTC', 'ETH', 'SOL']):
    """生成模拟市场数据"""
    data = []
    
    base_prices = {'BTC': 65000, 'ETH': 3500, 'SOL': 150}
    
    for symbol in symbols:
        price = base_prices[symbol]
        for i in range(days * 24):  # 小时数据
            # 添加趋势和均值回归成分
            trend = np.sin(i / 50) * 0.001  # 缓慢趋势
            noise = np.random.randn() * 0.002  # 随机噪声
            mean_reversion = - (price - base_prices[symbol]) / base_prices[symbol] * 0.0001
            
            returns = trend + noise + mean_reversion
            price *= (1 + returns)
            
            data.append({
                'timestamp': datetime(2026, 2, 1) + timedelta(hours=i),
                'symbol': symbol,
                'close': price,
                'high': price * (1 + abs(np.random.randn()) * 0.005),
                'low': price * (1 - abs(np.random.randn()) * 0.005),
                'volume': np.random.randint(1000000, 10000000)
            })
    
    return pd.DataFrame(data)


def trend_following_strategy(df):
    """趋势跟踪策略信号"""
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    
    # 双均线交叉
    signal = 0
    if df['ma20'].iloc[-1] > df['ma60'].iloc[-1]:
        signal = 1  # 看涨
    elif df['ma20'].iloc[-1] < df['ma60'].iloc[-1]:
        signal = -1  # 看跌
    
    return signal


def mean_reversion_strategy(df):
    """均值回归策略信号"""
    df['rsi'] = calculate_rsi(df['close'], 14)
    
    signal = 0
    if df['rsi'].iloc[-1] < 30:
        signal = 1  # 超卖，买入
    elif df['rsi'].iloc[-1] > 70:
        signal = -1  # 超买，卖出
    
    return signal


def calculate_rsi(prices, period=14):
    """计算RSI"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def multi_strategy_signal(signals, weights):
    """多策略信号融合"""
    total_signal = sum(s * w for s, w in zip(signals, weights))
    return np.sign(total_signal)


def run_backtest_comparison():
    """运行回测对比"""
    print("="*70)
    print("多策略回测对比")
    print("="*70)
    
    # 生成数据
    data = generate_market_data(days=30)
    symbols = data['symbol'].unique()
    
    # 策略配置
    strategies = {
        '单策略-趋势跟踪': {
            'signals': [trend_following_strategy],
            'weights': [1.0]
        },
        '单策略-均值回归': {
            'signals': [mean_reversion_strategy],
            'weights': [1.0]
        },
        '多策略-60/40': {
            'signals': [trend_following_strategy, mean_reversion_strategy],
            'weights': [0.6, 0.4]
        },
        '多策略-50/50': {
            'signals': [trend_following_strategy, mean_reversion_strategy],
            'weights': [0.5, 0.5]
        }
    }
    
    results = {}
    
    for strategy_name, config in strategies.items():
        portfolio_values = [100.0]  # 初始资金
        trades = []
        
        for symbol in symbols:
            symbol_data = data[data['symbol'] == symbol].copy()
            
            for i in range(60, len(symbol_data)):
                window = symbol_data.iloc[i-60:i]
                
                # 获取各策略信号
                signals = [sig(window) for sig in config['signals']]
                final_signal = multi_strategy_signal(signals, config['weights'])
                
                # 模拟交易
                if final_signal != 0 and np.random.random() < 0.1:  # 10%概率执行
                    price = symbol_data['close'].iloc[i]
                    trade_return = final_signal * (symbol_data['close'].iloc[i+1] - price) / price if i+1 < len(symbol_data) else 0
                    
                    # 扣除手续费 (6bps)
                    trade_return -= 0.0006
                    
                    portfolio_values.append(portfolio_values[-1] * (1 + trade_return))
                    trades.append({
                        'symbol': symbol,
                        'signal': final_signal,
                        'return': trade_return
                    })
        
        # 计算指标
        total_return = (portfolio_values[-1] - 100) / 100
        
        # 计算最大回撤
        peak = 100
        max_drawdown = 0
        for val in portfolio_values:
            if val > peak:
                peak = val
            drawdown = (peak - val) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # 计算胜率
        if trades:
            win_rate = sum(1 for t in trades if t['return'] > 0) / len(trades)
        else:
            win_rate = 0
        
        results[strategy_name] = {
            'total_return': total_return * 100,
            'max_drawdown': max_drawdown * 100,
            'win_rate': win_rate * 100,
            'trade_count': len(trades),
            'final_value': portfolio_values[-1]
        }
    
    # 输出结果
    print(f"\n回测期间: 30天")
    print(f"标的: BTC, ETH, SOL")
    print(f"初始资金: $100")
    print()
    
    print("-"*70)
    print(f"{'策略':<25} {'收益率':<12} {'最大回撤':<12} {'胜率':<10} {'交易数':<10}")
    print("-"*70)
    
    for name, result in results.items():
        print(f"{name:<25} {result['total_return']:>+10.2f}% {result['max_drawdown']:>10.2f}% {result['win_rate']:>8.1f}% {result['trade_count']:>8}")
    
    print("-"*70)
    
    # 分析
    best_return = max(results.items(), key=lambda x: x[1]['total_return'])
    best_sharpe = max(results.items(), key=lambda x: x[1]['total_return'] / (x[1]['max_drawdown'] + 0.01))
    
    print(f"\n📊 结论:")
    print(f"  最佳收益: {best_return[0]} ({best_return[1]['total_return']:+.2f}%)")
    print(f"  最佳风险收益比: {best_sharpe[0]}")
    
    print(f"\n💡 多策略优势:")
    
    single_return = results['单策略-趋势跟踪']['total_return']
    multi_return = results['多策略-60/40']['total_return']
    
    if multi_return > single_return:
        improvement = multi_return - single_return
        print(f"  ✓ 相比单策略，收益提升 {improvement:.2f}%")
    
    single_dd = results['单策略-趋势跟踪']['max_drawdown']
    multi_dd = results['多策略-60/40']['max_drawdown']
    
    if multi_dd < single_dd:
        reduction = single_dd - multi_dd
        print(f"  ✓ 相比单策略，回撤降低 {reduction:.2f}%")
    
    print(f"\n⚠️ 注意:")
    print(f"  - 这是模拟数据演示")
    print(f"  - 实际效果需实盘验证")
    print(f"  - 当前Risk-Off模式无法交易")
    
    print("="*70)
    
    return results


if __name__ == "__main__":
    run_backtest_comparison()
