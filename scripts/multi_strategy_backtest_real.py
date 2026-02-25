#!/usr/bin/env python3
"""
多策略 vs 单策略 回测对比
使用真实历史数据（最近7天）
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

from src.strategy.multi_strategy_system import (
    StrategyOrchestrator,
    TrendFollowingStrategy,
    MeanReversionStrategy,
    Alpha6FactorStrategy
)
from decimal import Decimal

def load_historical_data():
    """加载历史K线数据"""
    cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/cache')
    symbols = ['BTC_USDT', 'ETH_USDT', 'SOL_USDT', 'BNB_USDT']
    
    all_data = {}
    for sym in symbols:
        files = list(cache_dir.glob(f'{sym}_1H_*.csv'))
        if files:
            latest = max(files, key=lambda x: x.stat().st_mtime)
            df = pd.read_csv(latest)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['symbol'] = sym.replace('_', '-')
            all_data[sym.replace('_', '/')] = df
    
    return all_data

def run_backtest(data_dict, strategy_type='multi', initial_capital=20.0):
    """
    运行回测
    
    Args:
        strategy_type: 'trend_only', 'mean_revert_only', 'alpha6_only', 'multi'
        initial_capital: 初始资金
    """
    capital = initial_capital
    positions = {}  # symbol -> {'shares': x, 'entry_price': y}
    trades = []
    equity_curve = []
    
    # 获取所有时间点
    all_timestamps = set()
    for df in data_dict.values():
        all_timestamps.update(df['timestamp'].tolist())
    all_timestamps = sorted(all_timestamps)
    
    # 初始化策略
    if strategy_type == 'multi':
        orchestrator = StrategyOrchestrator(total_capital=Decimal(str(initial_capital)))
        orchestrator.register_strategy(TrendFollowingStrategy(), Decimal('0.35'))
        orchestrator.register_strategy(MeanReversionStrategy(), Decimal('0.25'))
        orchestrator.register_strategy(Alpha6FactorStrategy(), Decimal('0.40'))
    elif strategy_type == 'trend_only':
        orchestrator = StrategyOrchestrator(total_capital=Decimal(str(initial_capital)))
        orchestrator.register_strategy(TrendFollowingStrategy(), Decimal('1.0'))
    elif strategy_type == 'mean_revert_only':
        orchestrator = StrategyOrchestrator(total_capital=Decimal(str(initial_capital)))
        orchestrator.register_strategy(MeanReversionStrategy(), Decimal('1.0'))
    elif strategy_type == 'alpha6_only':
        orchestrator = StrategyOrchestrator(total_capital=Decimal(str(initial_capital)))
        orchestrator.register_strategy(Alpha6FactorStrategy(), Decimal('1.0'))
    
    from src.strategy.multi_strategy_system import MultiStrategyAdapter
    adapter = MultiStrategyAdapter(orchestrator)
    
    # 模拟交易
    fee_rate = 0.0006  # 0.06% 手续费
    
    for i, ts in enumerate(all_timestamps[60:]):  # 跳过前60条（计算指标需要）
        # 构建当前市场数据
        current_data = []
        for symbol, df in data_dict.items():
            mask = df['timestamp'] <= ts
            if mask.sum() < 60:
                continue
            hist = df[mask].tail(100).copy()
            hist['symbol'] = symbol.replace('/', '-')
            current_data.append(hist)
        
        if not current_data:
            continue
        
        market_df = pd.concat(current_data, ignore_index=True)
        
        # 获取当前价格
        current_prices = {}
        for symbol, df in data_dict.items():
            mask = df['timestamp'] <= ts
            if mask.any():
                current_prices[symbol] = df[mask].iloc[-1]['close']
        
        # 获取策略信号
        targets = adapter.run_strategy_cycle(market_df)
        
        # 执行交易
        for target in targets:
            symbol = target['symbol'].replace('-', '/')
            side = target['side']
            signal_score = target['signal_score']
            
            if symbol not in current_prices:
                continue
            
            price = current_prices[symbol]
            
            # 根据信号强度决定仓位大小
            position_size = min(capital * 0.1 * signal_score, capital * 0.3)  # 最大30%资金
            
            if side == 'buy' and position_size > 1.0:  # 最小1USDT
                # 买入
                shares = position_size / price
                cost = position_size * (1 + fee_rate)
                
                if cost <= capital:
                    capital -= cost
                    if symbol in positions:
                        # 加仓
                        old_shares = positions[symbol]['shares']
                        old_cost = positions[symbol]['entry_price'] * old_shares
                        new_shares = old_shares + shares
                        positions[symbol] = {
                            'shares': new_shares,
                            'entry_price': (old_cost + position_size) / new_shares
                        }
                    else:
                        positions[symbol] = {'shares': shares, 'entry_price': price}
                    
                    trades.append({
                        'timestamp': ts,
                        'symbol': symbol,
                        'side': 'buy',
                        'price': price,
                        'shares': shares,
                        'cost': cost
                    })
            
            elif side == 'sell' and symbol in positions:
                # 卖出
                shares = positions[symbol]['shares']
                proceeds = shares * price * (1 - fee_rate)
                capital += proceeds
                
                pnl = proceeds - (shares * positions[symbol]['entry_price'])
                
                trades.append({
                    'timestamp': ts,
                    'symbol': symbol,
                    'side': 'sell',
                    'price': price,
                    'shares': shares,
                    'proceeds': proceeds,
                    'pnl': pnl
                })
                
                del positions[symbol]
        
        # 计算权益
        position_value = sum(
            positions[sym]['shares'] * current_prices.get(sym, 0)
            for sym in positions if sym in current_prices
        )
        total_equity = capital + position_value
        
        equity_curve.append({
            'timestamp': ts,
            'cash': capital,
            'position_value': position_value,
            'total': total_equity
        })
    
    # 清仓
    final_price = {}
    for symbol, df in data_dict.items():
        if len(df) > 0:
            final_price[symbol] = df.iloc[-1]['close']
    
    for symbol in list(positions.keys()):
        if symbol in final_price:
            shares = positions[symbol]['shares']
            proceeds = shares * final_price[symbol] * (1 - fee_rate)
            capital += proceeds
            del positions[symbol]
    
    final_equity = capital
    
    return {
        'initial': initial_capital,
        'final': final_equity,
        'return_pct': (final_equity - initial_capital) / initial_capital * 100,
        'trades': len(trades),
        'equity_curve': equity_curve,
        'trade_details': trades
    }

def main():
    print("="*70)
    print("多策略回测对比")
    print("="*70)
    
    # 加载数据
    print("\n加载历史数据...")
    data = load_historical_data()
    print(f"数据: {len(data)}个币种, {len(list(data.values())[0])}条K线")
    
    # 运行不同策略的回测
    strategies = [
        ('trend_only', '趋势跟踪策略'),
        ('mean_revert_only', '均值回归策略'),
        ('alpha6_only', '6因子Alpha策略'),
        ('multi', '多策略融合(35/25/40)')
    ]
    
    results = {}
    
    for strategy_type, name in strategies:
        print(f"\n{'='*70}")
        print(f"回测: {name}")
        print('='*70)
        
        result = run_backtest(data, strategy_type)
        results[strategy_type] = result
        
        print(f"初始资金: ${result['initial']:.2f}")
        print(f"最终资金: ${result['final']:.2f}")
        print(f"收益率: {result['return_pct']:+.2f}%")
        print(f"交易次数: {result['trades']}")
        
        if result['trades'] > 0:
            trades_df = pd.DataFrame(result['trade_details'])
            if 'pnl' in trades_df.columns:
                wins = trades_df[trades_df['pnl'] > 0]
                win_rate = len(wins) / len(trades_df) * 100
                print(f"胜率: {win_rate:.1f}%")
    
    # 对比总结
    print("\n" + "="*70)
    print("回测结果对比")
    print("="*70)
    print(f"{'策略':<25} {'收益率':>12} {'交易次数':>10} {'胜率':>10}")
    print("-"*70)
    
    for strategy_type, name in strategies:
        r = results[strategy_type]
        trades_df = pd.DataFrame(r['trade_details'])
        win_rate = 0
        if len(trades_df) > 0 and 'pnl' in trades_df.columns:
            wins = trades_df[trades_df['pnl'] > 0]
            win_rate = len(wins) / len(trades_df) * 100
        
        print(f"{name:<25} {r['return_pct']:>+11.2f}% {r['trades']:>10} {win_rate:>9.1f}%")
    
    print("="*70)
    print("\n注意: 回测基于最近7天真实数据，包含手续费")
    print("多策略融合 = 趋势跟踪(35%) + 均值回归(25%) + 6因子Alpha(40%)")

if __name__ == "__main__":
    main()
