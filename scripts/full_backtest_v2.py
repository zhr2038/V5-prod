#!/usr/bin/env python3
"""
V5 回测系统 v2 - 完整版本

新增功能：
- Risk-Off机制（市场状态检测）
- 多空双向交易
- 更长回测期间
- 参数优化
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import time
import warnings
warnings.filterwarnings('ignore')


class MarketRegime(Enum):
    """市场状态"""
    RISK_OFF = "risk_off"      # 风险规避 - 空仓
    SIDEWAYS = "sideways"      # 震荡 - 正常交易
    TRENDING_UP = "trending_up"   # 上涨趋势
    TRENDING_DOWN = "trending_down"  # 下跌趋势


@dataclass
class BacktestConfig:
    """回测配置"""
    start_date: str = '2025-11-01'
    end_date: str = '2026-02-24'
    symbols: List[str] = None
    timeframe: str = '1H'
    initial_capital: float = 1000.0
    commission_rate: float = 0.001
    slippage: float = 0.0005
    max_positions: int = 3
    position_size_pct: float = 0.33
    
    # Risk-Off配置
    enable_risk_off: bool = True
    risk_off_drawdown_threshold: float = 0.05  # 5%回撤触发
    risk_off_recovery_threshold: float = 0.02  # 2%恢复解除
    
    # 多空配置
    enable_short: bool = True
    signal_threshold_long: float = 0.15
    signal_threshold_short: float = -0.15
    
    # 止损配置
    stop_loss_pct: float = 0.05
    take_profit_pct: float = 0.10


class OKXDataProvider:
    """OKX历史数据获取"""
    
    def __init__(self):
        self.base_url = 'https://www.okx.com'
        self.cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.last_request = 0
        self.min_delay = 0.15
    
    def _rate_limit(self):
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
                    time.sleep(2)
                    continue
                
                if resp.status_code != 200:
                    break
                
                data = resp.json()
                if data.get('code') != '0':
                    break
                
                candles = data.get('data', [])
                if not candles:
                    break
                
                all_data.extend(candles)
                end_ts = int(candles[-1][0]) - 1
                
                if len(candles) < limit:
                    break
                    
            except Exception as e:
                break
        
        if not all_data:
            return pd.DataFrame()
        
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


class MarketRegimeDetector:
    """市场状态检测器"""
    
    @staticmethod
    def detect(df: pd.DataFrame, lookback: int = 48) -> MarketRegime:
        """
        检测市场状态
        
        基于：
        - 短期/长期均线位置
        - 波动率
        - 趋势强度
        """
        if len(df) < lookback:
            return MarketRegime.SIDEWAYS
        
        # 计算指标
        df = df.copy()
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        
        # 价格相对均线的位置
        price = df['close'].iloc[-1]
        sma20 = df['sma_20'].iloc[-1]
        sma50 = df['sma_50'].iloc[-1]
        
        if pd.isna(sma20) or pd.isna(sma50):
            return MarketRegime.SIDEWAYS
        
        # 趋势强度 (使用48小时价格变化)
        price_change = (price - df['close'].iloc[-lookback]) / df['close'].iloc[-lookback]
        
        # 波动率
        volatility = df['close'].pct_change().rolling(lookback).std().iloc[-1] * np.sqrt(365 * 24)
        
        # 判断逻辑
        if price < sma20 * 0.95 and price < sma50 * 0.97:  # 显著低于均线
            if price_change < -0.10:  # 大幅下跌
                return MarketRegime.TRENDING_DOWN
            elif volatility > 0.5:  # 高波动下跌
                return MarketRegime.RISK_OFF
        
        elif price > sma20 * 1.05 and price > sma50 * 1.03:  # 显著高于均线
            if price_change > 0.10:  # 大幅上涨
                return MarketRegime.TRENDING_UP
        
        # 检查是否处于Risk-Off状态（连续下跌）
        recent_returns = df['close'].pct_change().iloc[-24:].sum()
        if recent_returns < -0.08:  # 24小时内下跌8%
            return MarketRegime.RISK_OFF
        
        return MarketRegime.SIDEWAYS


class VectorizedBacktest:
    """向量化回测引擎 v2"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.equity_curve = None
        self.regime_history = []
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        
        # 移动平均线
        df['sma_10'] = df['close'].rolling(10).mean()
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
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        
        # 波动率
        df['volatility'] = df['returns'].rolling(20).std() * np.sqrt(365 * 24)
        
        # V5因子
        df['mom_1h'] = df['close'].pct_change(1)
        df['mom_4h'] = df['close'].pct_change(4)
        df['mom_24h'] = df['close'].pct_change(24)
        df['vol_sma'] = df['volume'].rolling(24).mean()
        df['vol_ratio'] = df['volume'] / (df['vol_sma'] + 1e-10)
        
        # 布林带
        df['bb_middle'] = df['close'].rolling(20).mean()
        bb_std = df['close'].rolling(20).std()
        df['bb_upper'] = df['bb_middle'] + 2 * bb_std
        df['bb_lower'] = df['bb_middle'] - 2 * bb_std
        df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'] + 1e-10)
        
        return df
    
    def generate_signals(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        生成多空信号
        
        Returns:
            (long_signal, short_signal)
        """
        df = self.calculate_indicators(df)
        
        # 综合评分信号 (-1 到 1)
        score = pd.Series(0.0, index=df.index)
        
        # 动量因子
        score += df['mom_4h'].fillna(0) * 0.30
        score += df['mom_24h'].fillna(0) * 0.15
        
        # RSI因子 (反转)
        rsi_score = (50 - df['rsi']) / 50
        score += rsi_score.fillna(0) * 0.20
        
        # MACD趋势
        macd_norm = df['macd'] / df['close']
        score += np.sign(macd_norm).fillna(0) * 0.15
        
        # 波动率调整
        vol_score = (1 - df['volatility'].fillna(0.5)).clip(0, 1)
        score += vol_score * 0.10
        
        # 成交量
        vol_signal = (df['vol_ratio'] - 1).fillna(0).clip(-1, 1)
        score += vol_signal * 0.10
        
        # 分离多空信号
        long_signal = score.where(score > self.config.signal_threshold_long, 0)
        short_signal = score.where(score < self.config.signal_threshold_short, 0)
        
        return long_signal, short_signal
    
    def run_backtest(self, data: Dict[str, pd.DataFrame]) -> Dict:
        """执行回测"""
        print("\n" + "="*60)
        print("运行回测 v2 (Risk-Off + 多空双向)")
        print("="*60)
        
        # 生成信号
        all_long_signals = {}
        all_short_signals = {}
        for symbol, df in data.items():
            long_sig, short_sig = self.generate_signals(df)
            all_long_signals[symbol] = long_sig
            all_short_signals[symbol] = short_sig
        
        # 对齐时间
        all_dates = None
        for df in data.values():
            all_dates = df.index if all_dates is None else all_dates.union(df.index)
        all_dates = all_dates.sort_values()
        
        # 构建信号矩阵
        long_matrix = pd.DataFrame(index=all_dates)
        short_matrix = pd.DataFrame(index=all_dates)
        for symbol in data.keys():
            long_matrix[symbol] = all_long_signals[symbol].reindex(all_dates)
            short_matrix[symbol] = all_short_signals[symbol].reindex(all_dates)
        
        # 收益矩阵
        returns_matrix = pd.DataFrame(index=all_dates)
        for symbol, df in data.items():
            returns_matrix[symbol] = df['close'].pct_change().reindex(all_dates)
        
        # 回测模拟
        equity = self.config.initial_capital
        equity_history = [equity]
        peak_equity = equity
        trades = []
        current_positions = {}  # symbol -> {'side': 'long'/'short', 'entry_price': ..., 'returns': []}
        
        risk_off_mode = False
        risk_off_triggered_at = None
        
        regime_detector = MarketRegimeDetector()
        
        for i, date in enumerate(all_dates[1:], 1):
            # 检测市场状态
            if self.config.enable_risk_off and i > 50:
                # 使用BTC作为市场基准
                btc_df = data.get('BTC-USDT')
                if btc_df is not None:
                    current_regime = regime_detector.detect(btc_df.iloc[:i])
                    self.regime_history.append((date, current_regime))
                    
                    # Risk-Off逻辑
                    if current_regime == MarketRegime.RISK_OFF and not risk_off_mode:
                        risk_off_mode = True
                        risk_off_triggered_at = equity
                        # 平掉所有仓位
                        for sym in list(current_positions.keys()):
                            trades.append({
                                'date': date, 'symbol': sym, 
                                'action': 'close_risk_off',
                                'return': current_positions[sym].get('cum_return', 0)
                            })
                        current_positions = {}
                    
                    elif risk_off_mode:
                        # 检查是否恢复
                        recovery = (equity / risk_off_triggered_at - 1) if risk_off_triggered_at else 0
                        if recovery > self.config.risk_off_recovery_threshold:
                            risk_off_mode = False
            
            # 计算当日收益
            day_return = 0
            if current_positions:
                for sym, pos in list(current_positions.items()):
                    if sym in returns_matrix.columns:
                        ret = returns_matrix.loc[date, sym]
                        if not pd.isna(ret):
                            # 根据持仓方向调整收益符号
                            if pos['side'] == 'short':
                                ret = -ret
                            
                            adj_ret = ret - self.config.slippage - self.config.commission_rate
                            day_return += adj_ret * self.config.position_size_pct
                            pos['returns'].append(adj_ret)
                            
                            # 计算累计收益
                            pos['cum_return'] = np.prod([1 + r for r in pos['returns']]) - 1
                            
                            # 止损/止盈检查
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
            
            # 更新峰值
            if equity > peak_equity:
                peak_equity = equity
            
            # 开新仓 (不在Risk-Off模式下)
            if not risk_off_mode:
                # 多头信号
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
                
                # 空头信号 (如果启用)
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
        
        # 计算指标
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
        
        # 交易统计
        long_trades = [t for t in trades if t.get('side') == 'long']
        short_trades = [t for t in trades if t.get('side') == 'short']
        
        win_trades = sum(1 for t in trades if t.get('return', 0) > 0)
        loss_trades = sum(1 for t in trades if t.get('return', 0) < 0)
        win_rate = win_trades / (win_trades + loss_trades) * 100 if (win_trades + loss_trades) > 0 else 0
        
        # Calmar比率
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
    """回测报告 v2"""
    
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
                    V5 回测报告 v2 (Risk-Off + 多空双向)
{'='*70}

配置参数:
  回测期间:      {self.config.start_date} ~ {self.config.end_date}
  交易标的:      {len(self.config.symbols)} 个币种
  时间周期:      {self.config.timeframe}
  初始资金:      ${self.config.initial_capital:,.2f}
  最终权益:      ${self.result['equity_curve'].iloc[-1]:,.2f}
  手续费:        {self.config.commission_rate*100:.2f}%
  滑点:          {self.config.slippage*100:.3f}%
  
  Risk-Off:      {'启用' if self.config.enable_risk_off else '禁用'}
  多空双向:      {'启用' if self.config.enable_short else '禁用'}
  信号阈值(多):  {self.config.signal_threshold_long}
  信号阈值(空):  {self.config.signal_threshold_short}

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
            report += "  • 回撤过大，建议收紧止损或调整Risk-Off阈值\n"
        if m['win_rate_pct'] < 40:
            report += "  • 胜率偏低，建议提高信号质量门槛\n"
        if m.get('risk_off_triggers', 0) > 10:
            report += "  • Risk-Off触发频繁，可能过于敏感\n"
        if score >= 4:
            report += "  • 策略表现良好，可考虑实盘小资金测试\n"
        
        report += f"\n{'='*70}\n"
        
        report_file = self.report_dir / f"backtest_v2_{timestamp}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        
        # 保存权益曲线
        self.result['equity_curve'].to_csv(self.report_dir / f"equity_v2_{timestamp}.csv")
        
        print(report)
        print(f"报告保存: {report_file}")
        return report_file


def main():
    """主函数"""
    print("="*70)
    print("V5 回测系统 v2")
    print("="*70)
    
    config = BacktestConfig(
        start_date='2025-11-01',  # 4个月回测
        end_date='2026-02-24',
        symbols=[
            'BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'XRP-USDT',
            'ADA-USDT', 'DOGE-USDT', 'DOT-USDT', 'AVAX-USDT', 'LINK-USDT'
        ],
        timeframe='1H',
        initial_capital=1000.0,
        max_positions=5,
        position_size_pct=0.2,
        
        # Risk-Off配置
        enable_risk_off=True,
        risk_off_drawdown_threshold=0.05,
        risk_off_recovery_threshold=0.02,
        
        # 多空配置
        enable_short=True,
        signal_threshold_long=0.15,
        signal_threshold_short=-0.15,
        
        # 止损止盈
        stop_loss_pct=0.05,
        take_profit_pct=0.10
    )
    
    print(f"\n配置: {config.start_date} ~ {config.end_date}")
    print(f"币种: {len(config.symbols)}个主流币")
    print(f"功能: Risk-Off + 多空双向")
    
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
