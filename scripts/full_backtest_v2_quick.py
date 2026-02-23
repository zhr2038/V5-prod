#!/usr/bin/env python3
"""
V5 回测系统 v2 - 使用已有缓存数据快速运行
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')


class MarketRegime(Enum):
    RISK_OFF = "risk_off"
    SIDEWAYS = "sideways"
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"


@dataclass
class BacktestConfig:
    start_date: str = '2025-12-01'
    end_date: str = '2026-02-24'
    symbols: List[str] = None
    timeframe: str = '1H'
    initial_capital: float = 1000.0
    commission_rate: float = 0.001
    slippage: float = 0.0005
    max_positions: int = 5
    position_size_pct: float = 0.2
    enable_risk_off: bool = True
    risk_off_recovery_threshold: float = 0.02
    enable_short: bool = True
    signal_threshold_long: float = 0.15
    signal_threshold_short: float = -0.15
    stop_loss_pct: float = 0.05
    take_profit_pct: float = 0.10


class CachedDataProvider:
    """使用已有缓存数据"""
    
    def __init__(self):
        self.cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/cache')
    
    def load_cached(self, symbols: List[str], start: str, end: str) -> Dict[str, pd.DataFrame]:
        """加载缓存数据"""
        data = {}
        for symbol in symbols:
            cache_file = self.cache_dir / f"{symbol.replace('-', '_')}_1H_{start}_{end}.csv"
            if cache_file.exists():
                df = pd.read_csv(cache_file, index_col='timestamp', parse_dates=True)
                data[symbol] = df
                print(f"📦 {symbol}: {len(df)} 条")
        return data


class MarketRegimeDetector:
    @staticmethod
    def detect(df: pd.DataFrame, lookback: int = 48) -> MarketRegime:
        if len(df) < lookback:
            return MarketRegime.SIDEWAYS
        
        df = df.copy()
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        
        price = df['close'].iloc[-1]
        sma20 = df['sma_20'].iloc[-1]
        sma50 = df['sma_50'].iloc[-1]
        
        if pd.isna(sma20) or pd.isna(sma50):
            return MarketRegime.SIDEWAYS
        
        price_change = (price - df['close'].iloc[-lookback]) / df['close'].iloc[-lookback]
        recent_returns = df['close'].pct_change().iloc[-24:].sum()
        
        if price < sma20 * 0.95 and price < sma50 * 0.97:
            if price_change < -0.10:
                return MarketRegime.TRENDING_DOWN
            elif recent_returns < -0.08:
                return MarketRegime.RISK_OFF
        
        elif price > sma20 * 1.05 and price > sma50 * 1.03:
            if price_change > 0.10:
                return MarketRegime.TRENDING_UP
        
        if recent_returns < -0.08:
            return MarketRegime.RISK_OFF
        
        return MarketRegime.SIDEWAYS


class VectorizedBacktest:
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.equity_curve = None
        self.regime_history = []
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        df['sma_10'] = df['close'].rolling(10).mean()
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / (loss + 1e-10)
        df['rsi'] = 100 - (100 / (1 + rs))
        
        ema_12 = df['close'].ewm(span=12).mean()
        ema_26 = df['close'].ewm(span=26).mean()
        df['macd'] = ema_12 - ema_26
        
        df['volatility'] = df['returns'].rolling(20).std() * np.sqrt(365 * 24)
        df['mom_4h'] = df['close'].pct_change(4)
        df['mom_24h'] = df['close'].pct_change(24)
        df['vol_sma'] = df['volume'].rolling(24).mean()
        df['vol_ratio'] = df['volume'] / (df['vol_sma'] + 1e-10)
        
        return df
    
    def generate_signals(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        df = self.calculate_indicators(df)
        score = pd.Series(0.0, index=df.index)
        
        score += df['mom_4h'].fillna(0) * 0.30
        score += df['mom_24h'].fillna(0) * 0.15
        rsi_score = (50 - df['rsi']) / 50
        score += rsi_score.fillna(0) * 0.20
        macd_norm = df['macd'] / df['close']
        score += np.sign(macd_norm).fillna(0) * 0.15
        vol_score = (1 - df['volatility'].fillna(0.5)).clip(0, 1)
        score += vol_score * 0.10
        vol_signal = (df['vol_ratio'] - 1).fillna(0).clip(-1, 1)
        score += vol_signal * 0.10
        
        long_signal = score.where(score > self.config.signal_threshold_long, 0)
        short_signal = score.where(score < self.config.signal_threshold_short, 0)
        
        return long_signal, short_signal
    
    def run_backtest(self, data: Dict[str, pd.DataFrame]) -> Dict:
        print("\n" + "="*60)
        print("运行回测 v2 (Risk-Off + 多空双向)")
        print("="*60)
        
        all_long_signals = {}
        all_short_signals = {}
        for symbol, df in data.items():
            long_sig, short_sig = self.generate_signals(df)
            all_long_signals[symbol] = long_sig
            all_short_signals[symbol] = short_sig
        
        all_dates = None
        for df in data.values():
            all_dates = df.index if all_dates is None else all_dates.union(df.index)
        all_dates = all_dates.sort_values()
        
        long_matrix = pd.DataFrame(index=all_dates)
        short_matrix = pd.DataFrame(index=all_dates)
        for symbol in data.keys():
            long_matrix[symbol] = all_long_signals[symbol].reindex(all_dates)
            short_matrix[symbol] = all_short_signals[symbol].reindex(all_dates)
        
        returns_matrix = pd.DataFrame(index=all_dates)
        for symbol, df in data.items():
            returns_matrix[symbol] = df['close'].pct_change().reindex(all_dates)
        
        equity = self.config.initial_capital
        equity_history = [equity]
        trades = []
        current_positions = {}
        risk_off_mode = False
        risk_off_triggered_at = None
        
        regime_detector = MarketRegimeDetector()
        
        for i, date in enumerate(all_dates[1:], 1):
            if self.config.enable_risk_off and i > 50:
                btc_df = data.get('BTC-USDT')
                if btc_df is not None:
                    current_regime = regime_detector.detect(btc_df.iloc[:i])
                    self.regime_history.append((date, current_regime))
                    
                    if current_regime == MarketRegime.RISK_OFF and not risk_off_mode:
                        risk_off_mode = True
                        risk_off_triggered_at = equity
                        for sym in list(current_positions.keys()):
                            trades.append({
                                'date': date, 'symbol': sym, 
                                'action': 'close_risk_off',
                                'return': current_positions[sym].get('cum_return', 0)
                            })
                        current_positions = {}
                    
                    elif risk_off_mode:
                        recovery = (equity / risk_off_triggered_at - 1) if risk_off_triggered_at else 0
                        if recovery > self.config.risk_off_recovery_threshold:
                            risk_off_mode = False
            
            day_return = 0
            if current_positions:
                for sym, pos in list(current_positions.items()):
                    if sym in returns_matrix.columns:
                        ret = returns_matrix.loc[date, sym]
                        if not pd.isna(ret):
                            if pos['side'] == 'short':
                                ret = -ret
                            
                            adj_ret = ret - self.config.slippage - self.config.commission_rate
                            day_return += adj_ret * self.config.position_size_pct
                            pos['returns'].append(adj_ret)
                            pos['cum_return'] = np.prod([1 + r for r in pos['returns']]) - 1
                            
                            if pos['cum_return'] < -self.config.stop_loss_pct:
                                trades.append({
                                    'date': date, 'symbol': sym, 
                                    'side': pos['side'], 'action': 'stop_loss',
                                    'return': pos['cum_return']
                                })
                                del current_positions[sym]
                            elif pos['cum_return'] > self.config.take_profit_pct:
                                trades.append({
                                    'date': date, 'symbol': sym,
                                    'side': pos['side'], 'action': 'take_profit',
                                    'return': pos['cum_return']
                                })
                                del current_positions[sym]
            
            equity *= (1 + day_return)
            equity_history.append(equity)
            
            if not risk_off_mode:
                today_long = long_matrix.loc[date].dropna()
                top_long = today_long[today_long > 0].nlargest(self.config.max_positions // 2 + 1)
                
                for sym in top_long.index:
                    if sym not in current_positions:
                        current_positions[sym] = {
                            'side': 'long', 'entry': date, 
                            'returns': [], 'cum_return': 0
                        }
                        trades.append({
                            'date': date, 'symbol': sym, 
                            'side': 'long', 'action': 'enter',
                            'signal': top_long[sym]
                        })
                
                if self.config.enable_short:
                    today_short = short_matrix.loc[date].dropna()
                    top_short = today_short[today_short < 0].nsmallest(self.config.max_positions // 2 + 1)
                    
                    for sym in top_short.index:
                        if sym not in current_positions:
                            current_positions[sym] = {
                                'side': 'short', 'entry': date,
                                'returns': [], 'cum_return': 0
                            }
                            trades.append({
                                'date': date, 'symbol': sym,
                                'side': 'short', 'action': 'enter',
                                'signal': top_short[sym]
                            })
        
        self.equity_curve = pd.Series(equity_history, index=all_dates[:len(equity_history)])
        returns_series = self.equity_curve.pct_change().dropna()
        
        metrics = self._calculate_metrics(returns_series, trades)
        metrics['risk_off_triggers'] = len([r for r in self.regime_history if r[1] == MarketRegime.RISK_OFF])
        
        return {
            'metrics': metrics,
            'equity_curve': self.equity_curve,
            'trades': trades,
            'regime_history': self.regime_history
        }
    
    def _calculate_metrics(self, returns: pd.Series, trades: List) -> Dict:
        if len(returns) == 0:
            return {}
        
        total_return = (self.equity_curve.iloc[-1] / self.config.initial_capital - 1) * 100
        days = len(returns) / 24
        annual_return = ((1 + total_return/100) ** (365/days) - 1) * 100 if days > 0 else total_return
        volatility = returns.std() * np.sqrt(365 * 24) * 100
        sharpe = annual_return / volatility if volatility > 0 else 0
        
        cummax = self.equity_curve.cummax()
        max_dd = ((self.equity_curve - cummax) / cummax).min() * 100
        
        long_trades = [t for t in trades if t.get('side') == 'long']
        short_trades = [t for t in trades if t.get('side') == 'short']
        
        win_trades = sum(1 for t in trades if t.get('return', 0) > 0)
        loss_trades = sum(1 for t in trades if t.get('return', 0) < 0)
        win_rate = win_trades / (win_trades + loss_trades) * 100 if (win_trades + loss_trades) > 0 else 0
        
        calmar = abs(annual_return / max_dd) if max_dd != 0 else 0
        
        return {
            'total_return_pct': round(total_return, 2),
            'annual_return_pct': round(annual_return, 2),
            'volatility_annual_pct': round(volatility, 2),
            'sharpe_ratio': round(sharpe, 2),
            'max_drawdown_pct': round(max_dd, 2),
            'calmar_ratio': round(calmar, 2),
            'win_rate_pct': round(win_rate, 2),
            'total_trades': len(trades),
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'winning_trades': win_trades,
            'losing_trades': loss_trades,
        }


class BacktestReport:
    def __init__(self, result: Dict, config: BacktestConfig):
        self.result = result
        self.config = config
        self.report_dir = Path('/home/admin/clawd/v5-trading-bot/reports/backtest')
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def generate(self):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        m = self.result['metrics']
        
        report = f"""
{'='*70}
              V5 回测报告 v2 (Risk-Off + 多空双向)
{'='*70}

配置参数:
  回测期间:      {self.config.start_date} ~ {self.config.end_date}
  交易标的:      {len(self.config.symbols)} 个币种
  初始资金:      ${self.config.initial_capital:,.2f}
  最终权益:      ${self.result['equity_curve'].iloc[-1]:,.2f}
  手续费:        {self.config.commission_rate*100:.2f}%
  滑点:          {self.config.slippage*100:.3f}%
  Risk-Off:      {'启用' if self.config.enable_risk_off else '禁用'}
  多空双向:      {'启用' if self.config.enable_short else '禁用'}

性能指标:
  总收益率:      {m['total_return_pct']:>10.2f}%
  年化收益:      {m['annual_return_pct']:>10.2f}%
  年化波动:      {m['volatility_annual_pct']:>10.2f}%
  夏普比率:      {m['sharpe_ratio']:>10.2f}
  最大回撤:      {m['max_drawdown_pct']:>10.2f}%
  Calmar比率:    {m['calmar_ratio']:>10.2f}
  胜率:          {m['win_rate_pct']:>10.2f}%

交易统计:
  总交易:        {m['total_trades']}
  多头交易:      {m['long_trades']}
  空头交易:      {m['short_trades']}
  盈利笔数:      {m['winning_trades']}
  亏损笔数:      {m['losing_trades']}
  Risk-Off触发:  {m.get('risk_off_triggers', 0)} 次

{'='*70}
"""
        score = 0
        if m['sharpe_ratio'] > 1.5: score += 2
        elif m['sharpe_ratio'] > 1.0: score += 1
        if m['max_drawdown_pct'] > -20: score += 2
        elif m['max_drawdown_pct'] > -30: score += 1
        if m['total_return_pct'] > 50: score += 2
        elif m['total_return_pct'] > 20: score += 1
        
        ratings = {5: 'S+ 优秀', 4: 'A 良好', 3: 'B 一般', 2: 'C 需改进', 1: 'D 较差', 0: 'F 失败'}
        report += f"\n  综合评分: {score}/5  ->  {ratings.get(score, 'N/A')}\n"
        
        report += "\n优化建议:\n"
        if m['sharpe_ratio'] < 1.0:
            report += "  • 夏普比率偏低，建议优化信号阈值\n"
        if m['max_drawdown_pct'] < -25:
            report += "  • 回撤过大，建议收紧止损\n"
        if m['win_rate_pct'] < 40:
            report += "  • 胜率偏低，建议提高信号质量门槛\n"
        if score >= 4:
            report += "  • 策略表现良好，可考虑实盘测试\n"
        
        report += f"\n{'='*70}\n"
        
        report_file = self.report_dir / f"backtest_v2_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        self.result['equity_curve'].to_csv(self.report_dir / f"equity_v2_{timestamp}.csv")
        
        print(report)
        print(f"报告保存: {report_file}")
        return report_file


def main():
    print("="*70)
    print("V5 回测系统 v2 - 使用缓存数据")
    print("="*70)
    
    config = BacktestConfig(
        start_date='2025-12-01',
        end_date='2026-02-24',
        symbols=[
            'BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'XRP-USDT',
            'ADA-USDT', 'DOGE-USDT', 'DOT-USDT', 'AVAX-USDT', 'LINK-USDT'
        ],
        enable_risk_off=True,
        enable_short=True,
    )
    
    print(f"\n配置: {config.start_date} ~ {config.end_date}")
    
    print("\n[1/3] 加载缓存数据...")
    provider = CachedDataProvider()
    data = provider.load_cached(config.symbols, config.start_date, config.end_date)
    
    if len(data) < 5:
        print(f"❌ 数据不足 ({len(data)}个币种)，需要至少5个")
        return
    
    print(f"\n✅ 加载 {len(data)} 个币种")
    
    print("\n[2/3] 运行回测...")
    engine = VectorizedBacktest(config)
    result = engine.run_backtest(data)
    
    print("\n[3/3] 生成报告...")
    report = BacktestReport(result, config)
    report.generate()
    
    print("\n✅ 回测完成!")


if __name__ == '__main__':
    main()
