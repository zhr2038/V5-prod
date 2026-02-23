#!/usr/bin/env python3
"""
V5 回测系统 - 简化同步版本

功能：
- 从OKX获取历史K线数据
- 向量化回测
- 生成完整回测报告
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
import time
import warnings
warnings.filterwarnings('ignore')


@dataclass
class BacktestConfig:
    """回测配置"""
    start_date: str = '2026-02-01'
    end_date: str = '2026-02-24'
    symbols: List[str] = None
    timeframe: str = '1H'
    initial_capital: float = 1000.0
    commission_rate: float = 0.001
    slippage: float = 0.0005
    max_positions: int = 3
    position_size_pct: float = 0.33


class OKXDataProvider:
    """OKX历史数据获取"""
    
    def __init__(self):
        self.base_url = 'https://www.okx.com'
        self.cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.last_request = 0
        self.min_delay = 0.15
    
    def _rate_limit(self):
        """速率限制"""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self.last_request = time.time()
    
    def fetch_klines(self, symbol: str, timeframe: str = '1H', 
                    start: str = None, end: str = None) -> pd.DataFrame:
        """获取K线数据"""
        cache_file = self.cache_dir / f"{symbol.replace('-', '_')}_{timeframe}_{start}_{end}.csv"
        
        if cache_file.exists():
            df = pd.read_csv(cache_file, index_col='timestamp', parse_dates=True)
            print(f"📦 {symbol}: 缓存 {len(df)} 条")
            return df
        
        tf_map = {'1H': '1H', '4H': '4H', '1D': '1D'}
        bar = tf_map.get(timeframe, '1H')
        
        start_ts = int(datetime.strptime(start, '%Y-%m-%d').timestamp() * 1000) if start else None
        end_ts = int(datetime.strptime(end, '%Y-%m-%d').timestamp() * 1000) if end else None
        
        all_data = []
        limit = 100
        
        while True:
            self._rate_limit()
            
            url = f"{self.base_url}/api/v5/market/history-candles"
            params = {'instId': symbol, 'bar': bar, 'limit': limit}
            if start_ts:
                params['before'] = start_ts
            if end_ts:
                params['after'] = end_ts
            
            try:
                resp = requests.get(url, params=params, timeout=30)
                if resp.status_code == 429:
                    print(f"  {symbol}: 限流，等待2s...")
                    time.sleep(2)
                    continue
                
                if resp.status_code != 200:
                    print(f"⚠️  {symbol}: HTTP {resp.status_code}")
                    break
                
                data = resp.json()
                if data.get('code') != '0':
                    print(f"⚠️  {symbol}: {data.get('msg', 'Error')}")
                    break
                
                candles = data.get('data', [])
                if not candles:
                    break
                
                all_data.extend(candles)
                end_ts = int(candles[-1][0]) - 1
                
                if len(candles) < limit:
                    break
                    
            except Exception as e:
                print(f"⚠️  {symbol}: {e}")
                break
        
        if not all_data:
            return pd.DataFrame()
        
        # 转换DataFrame
        df = pd.DataFrame(all_data)
        df = df.iloc[:, :7]
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vol_ccy']
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        df = df.sort_values('timestamp').set_index('timestamp')
        
        df.to_csv(cache_file)
        print(f"✅ {symbol}: {len(df)} 条K线")
        return df
    
    def fetch_multiple(self, symbols: List[str], timeframe: str,
                      start: str, end: str) -> Dict[str, pd.DataFrame]:
        """批量获取"""
        data = {}
        for symbol in symbols:
            result = self.fetch_klines(symbol, timeframe, start, end)
            if not result.empty:
                data[symbol] = result
            else:
                print(f"⚠️  {symbol}: 无数据")
        return data


class VectorizedBacktest:
    """向量化回测引擎"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.equity_curve = None
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        
        # 移动平均线
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
        
        # V5因子
        df['mom_4h'] = df['close'].pct_change(4)
        df['mom_24h'] = df['close'].pct_change(24)
        df['vol_sma'] = df['volume'].rolling(24).mean()
        df['vol_ratio'] = df['volume'] / (df['vol_sma'] + 1e-10)
        
        return df
    
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """生成交易信号"""
        df = self.calculate_indicators(df)
        signal = pd.Series(0.0, index=df.index)
        
        # V5简化因子组合
        signal += df['mom_4h'].fillna(0) * 0.35
        signal += (50 - df['rsi']).fillna(0) / 50 * 0.20
        signal += np.sign(df['macd']).fillna(0) * 0.25
        signal += (1 - df['volatility'].fillna(0.5) / 2).clip(0, 1) * 0.12
        signal += (df['vol_ratio'] - 1).fillna(0).clip(-1, 1) * 0.08
        
        return signal
    
    def run_backtest(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """执行回测"""
        print("\n" + "="*60)
        print("运行回测...")
        print("="*60)
        
        # 生成信号
        all_signals = {}
        for symbol, df in data.items():
            all_signals[symbol] = self.generate_signals(df)
        
        # 对齐时间
        all_dates = None
        for df in data.values():
            all_dates = df.index if all_dates is None else all_dates.union(df.index)
        all_dates = all_dates.sort_values()
        
        # 信号矩阵
        signal_matrix = pd.DataFrame(index=all_dates)
        for symbol, signals in all_signals.items():
            signal_matrix[symbol] = signals.reindex(all_dates)
        
        # 收益矩阵 - 从原始价格计算
        returns_matrix = pd.DataFrame(index=all_dates)
        for symbol, df in data.items():
            # 计算收益率
            returns = df['close'].pct_change()
            returns_matrix[symbol] = returns.reindex(all_dates)
        
        # 回测模拟
        equity = self.config.initial_capital
        equity_history = [equity]
        trades = []
        current_positions = {}
        
        for i, date in enumerate(all_dates[1:], 1):
            # 当日选股
            today_signals = signal_matrix.loc[date].dropna()
            top_signals = today_signals[today_signals > 0.1].nlargest(self.config.max_positions)
            
            # 计算收益
            day_return = 0
            if current_positions:
                for sym in list(current_positions.keys()):
                    if sym in returns_matrix.columns:
                        ret = returns_matrix.loc[date, sym]
                        if not pd.isna(ret):
                            adj_ret = ret - self.config.slippage - self.config.commission_rate
                            day_return += adj_ret * self.config.position_size_pct
                            current_positions[sym]['returns'].append(adj_ret)
                            
                            # 止损检查
                            cum_ret = np.prod([1 + r for r in current_positions[sym]['returns']]) - 1
                            if cum_ret < -0.05:  # 5%止损
                                trades.append({'date': date, 'symbol': sym, 'action': 'stop', 'return': cum_ret})
                                del current_positions[sym]
            
            equity *= (1 + day_return)
            equity_history.append(equity)
            
            # 更新持仓
            for sym in top_signals.index:
                if sym not in current_positions:
                    current_positions[sym] = {'entry': date, 'returns': []}
                    trades.append({'date': date, 'symbol': sym, 'action': 'enter', 'signal': top_signals[sym]})
        
        # 计算指标
        self.equity_curve = pd.Series(equity_history, index=all_dates[:len(equity_history)])
        returns_series = self.equity_curve.pct_change().dropna()
        
        metrics = self._calculate_metrics(returns_series, trades)
        
        return {'metrics': metrics, 'equity_curve': self.equity_curve, 'trades': trades}
    
    def _calculate_metrics(self, returns: pd.Series, trades: List) -> Dict:
        """计算性能指标"""
        if len(returns) == 0:
            return {}
        
        total_return = (self.equity_curve.iloc[-1] / self.config.initial_capital - 1) * 100
        days = len(returns) / 24
        annual_return = ((1 + total_return/100) ** (365/days) - 1) * 100 if days > 0 else total_return
        volatility = returns.std() * np.sqrt(365 * 24) * 100
        sharpe = annual_return / volatility if volatility > 0 else 0
        
        cummax = self.equity_curve.cummax()
        max_dd = ((self.equity_curve - cummax) / cummax).min() * 100
        
        win_trades = sum(1 for t in trades if t.get('return', 0) > 0)
        loss_trades = sum(1 for t in trades if t.get('return', 0) < 0)
        win_rate = win_trades / (win_trades + loss_trades) * 100 if (win_trades + loss_trades) > 0 else 0
        
        return {
            'total_return_pct': round(total_return, 2),
            'annual_return_pct': round(annual_return, 2),
            'volatility_annual_pct': round(volatility, 2),
            'sharpe_ratio': round(sharpe, 2),
            'max_drawdown_pct': round(max_dd, 2),
            'win_rate_pct': round(win_rate, 2),
            'total_trades': len(trades),
            'enter_trades': len([t for t in trades if t['action'] == 'enter']),
        }


class BacktestReport:
    """回测报告"""
    
    def __init__(self, result: Dict, config: BacktestConfig):
        self.result = result
        self.config = config
        self.report_dir = Path('/home/admin/clawd/v5-trading-bot/reports/backtest')
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def generate(self):
        """生成报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        m = self.result['metrics']
        
        report = f"""
{'='*70}
                      V5 回测报告
{'='*70}

配置参数:
  回测期间:   {self.config.start_date} ~ {self.config.end_date}
  交易标的:   {len(self.config.symbols)} 个币种
  时间周期:   {self.config.timeframe}
  初始资金:   ${self.config.initial_capital:,.2f}
  最终权益:   ${self.result['equity_curve'].iloc[-1]:,.2f}
  手续费:     {self.config.commission_rate*100:.2f}%
  滑点:       {self.config.slippage*100:.3f}%

性能指标:
  总收益率:   {m['total_return_pct']:>10.2f}%
  年化收益:   {m['annual_return_pct']:>10.2f}%
  年化波动:   {m['volatility_annual_pct']:>10.2f}%
  夏普比率:   {m['sharpe_ratio']:>10.2f}
  最大回撤:   {m['max_drawdown_pct']:>10.2f}%
  胜率:       {m['win_rate_pct']:>10.2f}%

交易统计:
  总交易:     {m['total_trades']}
  建仓次数:   {m['enter_trades']}

{'='*70}
"""
        # 评级
        score = 0
        if m['sharpe_ratio'] > 1.5: score += 2
        elif m['sharpe_ratio'] > 1.0: score += 1
        if m['max_drawdown_pct'] > -20: score += 2
        elif m['max_drawdown_pct'] > -30: score += 1
        if m['total_return_pct'] > 50: score += 2
        elif m['total_return_pct'] > 20: score += 1
        
        ratings = {5: 'S+ 优秀', 4: 'A 良好', 3: 'B 一般', 2: 'C 需改进', 1: 'D 较差', 0: 'F 失败'}
        report += f"\n  综合评分: {score}/5  ->  {ratings.get(score, 'N/A')}\n"
        
        # 建议
        report += "\n优化建议:\n"
        if m['sharpe_ratio'] < 1.0:
            report += "  • 夏普比率偏低，建议优化信号阈值\n"
        if m['max_drawdown_pct'] < -25:
            report += "  • 回撤过大，建议收紧止损\n"
        if score >= 4:
            report += "  • 策略表现良好，可考虑实盘测试\n"
        
        report += f"\n{'='*70}\n"
        
        report_file = self.report_dir / f"backtest_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        # 保存权益曲线
        self.result['equity_curve'].to_csv(self.report_dir / f"equity_{timestamp}.csv")
        
        print(report)
        print(f"报告保存: {report_file}")
        return report_file


def main():
    """主函数"""
    print("="*70)
    print("V5 回测系统")
    print("="*70)
    
    config = BacktestConfig(
        start_date='2026-02-01',
        end_date='2026-02-24',
        symbols=['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'XRP-USDT'],
        timeframe='1H',
        initial_capital=1000.0,
        max_positions=3,
        position_size_pct=0.33
    )
    
    print(f"\n配置: {config.start_date} ~ {config.end_date}, {len(config.symbols)}个币种")
    
    # 获取数据
    print("\n[1/3] 获取历史数据...")
    provider = OKXDataProvider()
    data = provider.fetch_multiple(config.symbols, config.timeframe, 
                                   config.start_date, config.end_date)
    
    if not data:
        print("❌ 无法获取数据")
        return
    
    print(f"\n✅ 获取 {len(data)} 个币种")
    
    # 运行回测
    print("\n[2/3] 运行回测...")
    engine = VectorizedBacktest(config)
    result = engine.run_backtest(data)
    
    # 生成报告
    print("\n[3/3] 生成报告...")
    report = BacktestReport(result, config)
    report.generate()
    
    print("\n✅ 回测完成!")


if __name__ == '__main__':
    main()
