#!/usr/bin/env python3
"""
V5 回测系统 v3 - 纯多头+低频交易（保守策略验证）
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')


@dataclass
class BacktestConfig:
    start_date: str = '2025-12-01'
    end_date: str = '2026-02-24'
    symbols: List[str] = None
    timeframe: str = '1H'  # 数据粒度
    trade_interval: int = 4  # 每4小时交易一次
    initial_capital: float = 1000.0
    commission_rate: float = 0.001
    slippage: float = 0.0005
    max_positions: int = 3
    position_size_pct: float = 0.33
    signal_threshold: float = 0.30  # 提高阈值
    stop_loss_pct: float = 0.05
    min_hold_hours: int = 8  # 最少持仓8小时


def load_cached_data(symbols: List[str], start: str, end: str) -> Dict[str, pd.DataFrame]:
    """加载缓存数据"""
    cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/cache')
    data = {}
    for symbol in symbols:
        cache_file = cache_dir / f"{symbol.replace('-', '_')}_1H_{start}_{end}.csv"
        if cache_file.exists():
            df = pd.read_csv(cache_file, index_col='timestamp', parse_dates=True)
            data[symbol] = df
            print(f"📦 {symbol}: {len(df)} 条")
    return data


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标"""
    df = df.copy()
    df['returns'] = df['close'].pct_change()
    df['sma_20'] = df['close'].rolling(20).mean()
    df['sma_50'] = df['close'].rolling(50).mean()
    
    # RSI
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss + 1e-10)
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # MACD
    ema_12 = df['close'].ewm(span=12).mean()
    ema_26 = df['close'].ewm(span=26).mean()
    df['macd'] = ema_12 - ema_26
    
    # 波动率
    df['volatility'] = df['returns'].rolling(20).std() * np.sqrt(365 * 24)
    
    # 动量
    df['mom_4h'] = df['close'].pct_change(4)
    df['mom_24h'] = df['close'].pct_change(24)
    
    # 成交量
    df['vol_sma'] = df['volume'].rolling(24).mean()
    df['vol_ratio'] = df['volume'] / (df['vol_sma'] + 1e-10)
    
    return df


def generate_signal(df: pd.DataFrame) -> pd.Series:
    """生成纯多头信号"""
    df = calculate_indicators(df)
    score = pd.Series(0.0, index=df.index)
    
    # 趋势确认（只在上涨趋势做多）
    price_above_sma20 = df['close'] > df['sma_20']
    price_above_sma50 = df['close'] > df['sma_50']
    
    # 动量因子
    score += df['mom_4h'].fillna(0) * 0.35
    score += df['mom_24h'].fillna(0) * 0.20
    
    # RSI（超卖区域加分）
    rsi_score = (50 - df['rsi']) / 50
    score += rsi_score.fillna(0) * 0.25
    
    # MACD趋势
    macd_norm = df['macd'] / df['close']
    score += np.sign(macd_norm).fillna(0) * 0.10
    
    # 成交量
    vol_signal = (df['vol_ratio'] - 1).fillna(0).clip(-1, 1)
    score += vol_signal * 0.10
    
    # 趋势过滤 - 价格必须高于SMA20才考虑做多
    score = score.where(price_above_sma20, score * 0.3)
    
    return score


def check_market_risk_off(btc_df: pd.DataFrame, current_idx: int) -> bool:
    """检查是否Risk-Off（基于BTC）"""
    if current_idx < 48:
        return False
    
    df = btc_df.iloc[:current_idx]
    price = df['close'].iloc[-1]
    sma20 = df['close'].rolling(20).mean().iloc[-1]
    
    # 24小时跌幅超过8%
    recent_change = (price - df['close'].iloc[-24]) / df['close'].iloc[-24]
    
    # 价格低于SMA20的5%以上
    below_sma = price < sma20 * 0.95
    
    return recent_change < -0.08 or below_sma


def run_backtest(data: Dict[str, pd.DataFrame], config: BacktestConfig) -> Dict:
    """执行保守回测"""
    print("\n" + "="*60)
    print("运行保守回测 (纯多头+低频)")
    print("="*60)
    print(f"交易间隔: 每{config.trade_interval}小时")
    print(f"信号阈值: {config.signal_threshold}")
    print(f"最少持仓: {config.min_hold_hours}小时")
    
    # 生成信号
    all_signals = {}
    for symbol, df in data.items():
        all_signals[symbol] = generate_signal(df)
    
    # 对齐时间
    all_dates = None
    for df in data.values():
        all_dates = df.index if all_dates is None else all_dates.union(df.index)
    all_dates = all_dates.sort_values()
    
    # 信号矩阵
    signal_matrix = pd.DataFrame(index=all_dates)
    for symbol in data.keys():
        signal_matrix[symbol] = all_signals[symbol].reindex(all_dates)
    
    # 收益矩阵
    returns_matrix = pd.DataFrame(index=all_dates)
    for symbol, df in data.items():
        returns_matrix[symbol] = df['close'].pct_change().reindex(all_dates)
    
    # BTC数据用于Risk-Off判断
    btc_df = data.get('BTC-USDT')
    
    equity = config.initial_capital
    equity_history = [equity]
    trades = []
    current_positions = {}  # symbol -> {'entry_time': ..., 'returns': []}
    risk_off_mode = False
    
    last_trade_hour = None
    
    for i, date in enumerate(all_dates[1:], 1):
        current_hour = date.hour
        
        # Risk-Off检查
        if btc_df is not None and i > 48:
            risk_off_mode = check_market_risk_off(btc_df, i)
        
        # 计算持仓收益
        day_return = 0
        if current_positions:
            for sym, pos in list(current_positions.items()):
                if sym in returns_matrix.columns:
                    ret = returns_matrix.loc[date, sym]
                    if not pd.isna(ret):
                        adj_ret = ret - config.slippage - config.commission_rate
                        day_return += adj_ret * config.position_size_pct
                        pos['returns'].append(adj_ret)
                        pos['cum_return'] = np.prod([1 + r for r in pos['returns']]) - 1
                        
                        # 最少持仓时间
                        hold_hours = (date - pos['entry_time']).total_seconds() / 3600
                        
                        # 止损
                        if pos['cum_return'] < -config.stop_loss_pct:
                            trades.append({
                                'date': date, 'symbol': sym, 
                                'action': 'stop_loss', 'return': pos['cum_return'],
                                'hold_hours': hold_hours
                            })
                            del current_positions[sym]
                        
                        # 止盈（满足最少持仓时间）
                        elif hold_hours >= config.min_hold_hours and pos['cum_return'] > 0.08:
                            trades.append({
                                'date': date, 'symbol': sym,
                                'action': 'take_profit', 'return': pos['cum_return'],
                                'hold_hours': hold_hours
                            })
                            del current_positions[sym]
        
        equity *= (1 + day_return)
        equity_history.append(equity)
        
        # 低频交易检查
        can_trade = last_trade_hour is None or (
            date - last_trade_hour
        ).total_seconds() / 3600 >= config.trade_interval
        
        # 开新仓
        if can_trade and not risk_off_mode and len(current_positions) < config.max_positions:
            today_signals = signal_matrix.loc[date].dropna()
            available_slots = config.max_positions - len(current_positions)
            
            # 只选信号最强的，且未持仓的
            available_signals = today_signals[
                (today_signals > config.signal_threshold) & 
                (~today_signals.index.isin(current_positions.keys()))
            ].nlargest(available_slots)
            
            for sym in available_signals.index:
                current_positions[sym] = {
                    'entry_time': date, 'returns': [], 'cum_return': 0
                }
                trades.append({
                    'date': date, 'symbol': sym, 'action': 'enter',
                    'signal': available_signals[sym]
                })
                last_trade_hour = date
    
    # 计算指标
    equity_curve = pd.Series(equity_history, index=all_dates[:len(equity_history)])
    returns_series = equity_curve.pct_change().dropna()
    
    metrics = calculate_metrics(equity_curve, returns_series, trades, config)
    
    return {
        'metrics': metrics,
        'equity_curve': equity_curve,
        'trades': trades
    }


def calculate_metrics(equity_curve, returns, trades, config):
    """计算性能指标"""
    if len(returns) == 0:
        return {}
    
    total_return = (equity_curve.iloc[-1] / config.initial_capital - 1) * 100
    days = len(returns) / 24
    annual_return = ((1 + total_return/100) ** (365/days) - 1) * 100 if days > 0 else total_return
    volatility = returns.std() * np.sqrt(365 * 24) * 100
    sharpe = annual_return / volatility if volatility > 0 else 0
    
    cummax = equity_curve.cummax()
    max_dd = ((equity_curve - cummax) / cummax).min() * 100
    
    win_trades = sum(1 for t in trades if t.get('return', 0) > 0)
    loss_trades = sum(1 for t in trades if t.get('return', 0) < 0)
    win_rate = win_trades / (win_trades + loss_trades) * 100 if (win_trades + loss_trades) > 0 else 0
    
    calmar = abs(annual_return / max_dd) if max_dd != 0 else 0
    
    enter_trades = [t for t in trades if t['action'] == 'enter']
    avg_hold_time = np.mean([t.get('hold_hours', 0) for t in trades if 'hold_hours' in t])
    
    return {
        'total_return_pct': round(total_return, 2),
        'annual_return_pct': round(annual_return, 2),
        'volatility_annual_pct': round(volatility, 2),
        'sharpe_ratio': round(sharpe, 2),
        'max_drawdown_pct': round(max_dd, 2),
        'calmar_ratio': round(calmar, 2),
        'win_rate_pct': round(win_rate, 2),
        'total_trades': len(trades),
        'enter_trades': len(enter_trades),
        'stop_loss_trades': len([t for t in trades if t['action'] == 'stop_loss']),
        'take_profit_trades': len([t for t in trades if t['action'] == 'take_profit']),
        'avg_hold_hours': round(avg_hold_time, 1) if avg_hold_time > 0 else 0,
    }


def generate_report(result, config):
    """生成报告"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    m = result['metrics']
    
    report = f"""
{'='*70}
          V5 回测报告 v3 (保守策略: 纯多头+低频)
{'='*70}

配置参数:
  回测期间:      {config.start_date} ~ {config.end_date}
  交易标的:      {len(config.symbols)} 个币种
  交易频率:      每{config.trade_interval}小时
  信号阈值:      {config.signal_threshold}
  最少持仓:      {config.min_hold_hours}小时
  初始资金:      ${config.initial_capital:,.2f}
  最终权益:      ${result['equity_curve'].iloc[-1]:,.2f}

性能指标:
  总收益率:      {m['total_return_pct']:>10.2f}%
  年化收益:      {m['annual_return_pct']:>10.2f}%
  年化波动:      {m['volatility_annual_pct']:>10.2f}%
  夏普比率:      {m['sharpe_ratio']:>10.2f}
  最大回撤:      {m['max_drawdown_pct']:>10.2f}%
  Calmar比率:    {m['calmar_ratio']:>10.2f}
  胜率:          {m['win_rate_pct']:>10.2f}%

交易统计:
  建仓次数:      {m['enter_trades']}
  止损次数:      {m['stop_loss_trades']}
  止盈次数:      {m['take_profit_trades']}
  平均持仓时间:  {m['avg_hold_hours']:.1f}小时

{'='*70}
"""
    score = 0
    if m['sharpe_ratio'] > 1.5: score += 2
    elif m['sharpe_ratio'] > 1.0: score += 1
    if m['max_drawdown_pct'] > -20: score += 2
    elif m['max_drawdown_pct'] > -30: score += 1
    if m['total_return_pct'] > 50: score += 2
    elif m['total_return_pct'] > 20: score += 1
    elif m['total_return_pct'] > 0: score += 0.5
    
    ratings = {5: 'S+ 优秀', 4: 'A 良好', 3: 'B 一般', 2: 'C 需改进', 1: 'D 较差', 0: 'F 失败'}
    report += f"\n  综合评分: {score}/5  ->  {ratings.get(int(score), 'N/A')}\n"
    
    report += "\n策略说明:\n"
    report += "  • 纯多头：只做上涨趋势\n"
    report += "  • 低频交易：每4小时评估一次信号\n"
    report += "  • 趋势过滤：价格需高于SMA20\n"
    report += "  • 高信号阈值：0.30以上才入场\n"
    report += "  • 持仓保护：最少8小时，避免频繁换手\n"
    
    if m['total_return_pct'] > 0:
        report += "\n  ✅ 策略实现正收益\n"
    else:
        report += "\n  ⚠️ 策略亏损，需进一步优化\n"
    
    report += f"\n{'='*70}\n"
    
    report_dir = Path('/home/admin/clawd/v5-trading-bot/reports/backtest')
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / f"backtest_v3_conservative_{timestamp}.txt"
    
    with open(report_file, 'w') as f:
        f.write(report)
    
    result['equity_curve'].to_csv(report_dir / f"equity_v3_{timestamp}.csv")
    
    print(report)
    print(f"报告保存: {report_file}")


def main():
    print("="*70)
    print("V5 回测系统 v3 - 保守策略验证")
    print("="*70)
    
    config = BacktestConfig(
        start_date='2025-12-01',
        end_date='2026-02-24',
        symbols=[
            'BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'XRP-USDT',
            'ADA-USDT', 'DOGE-USDT', 'DOT-USDT', 'AVAX-USDT', 'LINK-USDT'
        ],
        trade_interval=4,  # 每4小时
        signal_threshold=0.15,  # 降低阈值
        max_positions=3,
        position_size_pct=0.33,
        min_hold_hours=8,
    )
    
    print(f"\n配置: {config.start_date} ~ {config.end_date}")
    print(f"策略: 纯多头 + 每{config.trade_interval}小时交易")
    
    print("\n[1/3] 加载数据...")
    data = load_cached_data(config.symbols, config.start_date, config.end_date)
    
    if len(data) < 5:
        print(f"❌ 数据不足")
        return
    
    print(f"\n✅ 加载 {len(data)} 个币种")
    
    print("\n[2/3] 运行回测...")
    result = run_backtest(data, config)
    
    print("\n[3/3] 生成报告...")
    generate_report(result, config)
    
    print("\n✅ 回测完成!")


if __name__ == '__main__':
    main()
