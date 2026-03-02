"""
多策略并行交易系统 - Multi-Strategy Parallel Trading System

设计目标:
1. 同时运行多个策略（趋势跟踪 + 均值回归 + 动量）
2. 动态资金分配（基于策略近期表现）
3. 策略间信号融合
4. 独立风控（每个策略有自己的止损）
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
import pandas as pd
import numpy as np
from decimal import Decimal
import json
from datetime import datetime, timedelta
from pathlib import Path


class StrategyType(Enum):
    """策略类型"""
    TREND_FOLLOWING = "trend"           # 趋势跟踪
    MEAN_REVERSION = "mean_reversion"   # 均值回归
    ALPHA_6FACTOR = "alpha_6factor"     # 6因子Alpha
    MOMENTUM = "momentum"               # 动量策略
    BREAKOUT = "breakout"               # 突破策略


@dataclass
class Signal:
    """交易信号"""
    symbol: str
    side: str           # 'buy' or 'sell'
    score: float        # 信号强度 0-1
    confidence: float   # 置信度 0-1
    strategy: str       # 来源策略
    timestamp: datetime
    metadata: Dict = field(default_factory=dict)


@dataclass
class StrategyPerformance:
    """策略绩效指标"""
    strategy_name: str
    total_pnl: float = 0.0
    win_rate: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    recent_7d_return: float = 0.0
    trade_count: int = 0
    last_updated: datetime = field(default_factory=datetime.now)


class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(self, name: str, strategy_type: StrategyType, config: Dict):
        self.name = name
        self.strategy_type = strategy_type
        self.config = config
        self.performance = StrategyPerformance(strategy_name=name)
        self.signals_history: List[Signal] = []
        
    @abstractmethod
    def generate_signals(self, market_data: pd.DataFrame) -> List[Signal]:
        """生成交易信号 - 子类必须实现"""
        pass
    
    @abstractmethod
    def calculate_position_size(self, signal: Signal, available_capital: Decimal) -> Decimal:
        """计算仓位大小 - 子类必须实现"""
        pass
    
    def update_performance(self, pnl: float, is_win: bool):
        """更新策略绩效"""
        self.performance.total_pnl += pnl
        self.performance.trade_count += 1
        
        # 简化胜率计算
        if self.performance.trade_count > 0:
            wins = self.performance.win_rate * (self.performance.trade_count - 1)
            if is_win:
                wins += 1
            self.performance.win_rate = wins / self.performance.trade_count
        
        self.performance.last_updated = datetime.now()
    
    def get_recent_signals(self, n: int = 10) -> List[Signal]:
        """获取最近n个信号"""
        return self.signals_history[-n:]


class TrendFollowingStrategy(BaseStrategy):
    """
    趋势跟踪策略
    
    核心逻辑:
    - 双均线交叉 (MA20 > MA60 且价格 > MA20 = 多头趋势)
    - ADX > 25 确认趋势强度
    - 突破近期高点加仓
    """
    
    def __init__(self, config: Dict = None):
        default_config = {
            'fast_ma': 20,
            'slow_ma': 60,
            'adx_threshold': 25,
            'position_size_pct': 0.3,  # 单信号仓位比例
            'trailing_stop': 0.05      # 5%追踪止损
        }
        if config:
            default_config.update(config)
        
        super().__init__(
            name="TrendFollowing",
            strategy_type=StrategyType.TREND_FOLLOWING,
            config=default_config
        )
    
    def generate_signals(self, market_data: pd.DataFrame) -> List[Signal]:
        """生成趋势跟踪信号"""
        signals = []
        
        for symbol in market_data['symbol'].unique():
            df = market_data[market_data['symbol'] == symbol].copy()
            if len(df) < 60:
                continue
            
            # 计算指标
            df['ma_fast'] = df['close'].rolling(self.config['fast_ma']).mean()
            df['ma_slow'] = df['close'].rolling(self.config['slow_ma']).mean()
            df['adx'] = self._calculate_adx(df)
            
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            # 趋势判断（增加二次确认，减少1h假突破）
            is_uptrend = (
                latest['ma_fast'] > latest['ma_slow']
                and latest['close'] > latest['ma_fast']
                and prev['ma_fast'] > prev['ma_slow']
                and prev['close'] > prev['ma_fast']
            )
            is_downtrend = (
                latest['ma_fast'] < latest['ma_slow']
                and latest['close'] < latest['ma_fast']
                and prev['ma_fast'] < prev['ma_slow']
                and prev['close'] < prev['ma_fast']
            )
            strong_trend = latest['adx'] > self.config['adx_threshold']
            
            # 生成信号
            if is_uptrend and strong_trend:
                score = min(latest['adx'] / 50, 1.0)  # ADX归一化
                signal = Signal(
                    symbol=symbol,
                    side='buy',
                    score=score,
                    confidence=score * 0.8 + 0.2,
                    strategy=self.name,
                    timestamp=datetime.now(),
                    metadata={
                        'ma_fast': latest['ma_fast'],
                        'ma_slow': latest['ma_slow'],
                        'adx': latest['adx']
                    }
                )
                signals.append(signal)
                self.signals_history.append(signal)
            
            elif is_downtrend and strong_trend:
                score = min(latest['adx'] / 50, 1.0)
                signal = Signal(
                    symbol=symbol,
                    side='sell',
                    score=score,
                    confidence=score * 0.8 + 0.2,
                    strategy=self.name,
                    timestamp=datetime.now(),
                    metadata={
                        'ma_fast': latest['ma_fast'],
                        'ma_slow': latest['ma_slow'],
                        'adx': latest['adx']
                    }
                )
                signals.append(signal)
                self.signals_history.append(signal)
        
        return signals
    
    def calculate_position_size(self, signal: Signal, available_capital: Decimal) -> Decimal:
        """基于信号强度计算仓位"""
        base_size = available_capital * Decimal(self.config['position_size_pct'])
        # 根据置信度调整
        adjusted_size = base_size * Decimal(signal.confidence)
        return adjusted_size
    
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算ADX指标"""
        df = df.copy()
        df['tr1'] = abs(df['high'] - df['low'])
        df['tr2'] = abs(df['high'] - df['close'].shift())
        df['tr3'] = abs(df['low'] - df['close'].shift())
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        df['+dm'] = np.where((df['high'] - df['high'].shift()) > (df['low'].shift() - df['low']),
                             np.maximum(df['high'] - df['high'].shift(), 0), 0)
        df['-dm'] = np.where((df['low'].shift() - df['low']) > (df['high'] - df['high'].shift()),
                             np.maximum(df['low'].shift() - df['low'], 0), 0)
        
        df['+di'] = 100 * df['+dm'].rolling(period).mean() / df['tr'].rolling(period).mean()
        df['-di'] = 100 * df['-dm'].rolling(period).mean() / df['tr'].rolling(period).mean()
        df['dx'] = 100 * abs(df['+di'] - df['-di']) / (df['+di'] + df['-di'])
        df['adx'] = df['dx'].rolling(period).mean()
        
        return df['adx']


class MeanReversionStrategy(BaseStrategy):
    """
    均值回归策略
    
    核心逻辑:
    - RSI超买超卖 (RSI < 30 买入, RSI > 70 卖出)
    - 价格偏离Bollinger Bands
    - 成交量萎缩确认反转
    """
    
    def __init__(self, config: Dict = None):
        default_config = {
            'rsi_period': 14,
            'rsi_oversold': 30,
            'rsi_overbought': 70,
            'bb_period': 20,
            'bb_std': 2,
            'position_size_pct': 0.2,
            'mean_rev_threshold': 0.02  # 偏离均值2%以上才考虑
        }
        if config:
            default_config.update(config)
        
        super().__init__(
            name="MeanReversion",
            strategy_type=StrategyType.MEAN_REVERSION,
            config=default_config
        )
    
    def generate_signals(self, market_data: pd.DataFrame) -> List[Signal]:
        """生成均值回归信号"""
        signals = []
        
        for symbol in market_data['symbol'].unique():
            df = market_data[market_data['symbol'] == symbol].copy()
            if len(df) < 20:
                continue
            
            # 计算指标
            df['rsi'] = self._calculate_rsi(df['close'], self.config['rsi_period'])
            df['bb_upper'], df['bb_middle'], df['bb_lower'] = self._calculate_bollinger(df)
            df['volume_ma'] = df['volume'].rolling(20).mean()
            
            latest = df.iloc[-1]
            
            # 计算偏离度
            deviation = (latest['close'] - latest['bb_middle']) / latest['bb_middle']
            
            # 超买超卖判断
            oversold = latest['rsi'] < self.config['rsi_oversold'] and deviation < -self.config['mean_rev_threshold']
            overbought = latest['rsi'] > self.config['rsi_overbought'] and deviation > self.config['mean_rev_threshold']
            
            # 成交量萎缩
            volume_dry_up = latest['volume'] < latest['volume_ma'] * 0.8
            
            if oversold and volume_dry_up:
                score = (self.config['rsi_oversold'] - latest['rsi']) / self.config['rsi_oversold']
                signal = Signal(
                    symbol=symbol,
                    side='buy',
                    score=min(score, 1.0),
                    confidence=min(score * 0.9 + 0.1, 1.0),
                    strategy=self.name,
                    timestamp=datetime.now(),
                    metadata={
                        'rsi': latest['rsi'],
                        'deviation': deviation,
                        'bb_lower': latest['bb_lower']
                    }
                )
                signals.append(signal)
                self.signals_history.append(signal)
            
            elif overbought and volume_dry_up:
                score = (latest['rsi'] - self.config['rsi_overbought']) / (100 - self.config['rsi_overbought'])
                signal = Signal(
                    symbol=symbol,
                    side='sell',
                    score=min(score, 1.0),
                    confidence=min(score * 0.9 + 0.1, 1.0),
                    strategy=self.name,
                    timestamp=datetime.now(),
                    metadata={
                        'rsi': latest['rsi'],
                        'deviation': deviation,
                        'bb_upper': latest['bb_upper']
                    }
                )
                signals.append(signal)
                self.signals_history.append(signal)
        
        return signals
    
    def calculate_position_size(self, signal: Signal, available_capital: Decimal) -> Decimal:
        """均值回归策略仓位更小，因为胜率相对较低"""
        base_size = available_capital * Decimal(self.config['position_size_pct'])
        adjusted_size = base_size * Decimal(signal.confidence) * Decimal('0.8')  # 额外降仓
        return adjusted_size
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def _calculate_bollinger(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """计算布林带"""
        middle = df['close'].rolling(self.config['bb_period']).mean()
        std = df['close'].rolling(self.config['bb_period']).std()
        upper = middle + self.config['bb_std'] * std
        lower = middle - self.config['bb_std'] * std
        return upper, middle, lower


class Alpha6FactorStrategy(BaseStrategy):
    """
    6因子Alpha策略
    
    核心逻辑:
    - f1_mom_5d: 5日动量 (短期趋势)
    - f2_mom_20d: 20日动量 (中期趋势)
    - f3_vol_adj_ret: 波动率调整收益
    - f4_volume_expansion: 成交量扩张
    - f5_rsi_trend_confirm: RSI趋势确认
    - f6_sentiment: 情绪因子 (AI分析)
    
    信号生成:
    - 计算6因子综合评分 (z-score标准化后加权)
    - 评分 > threshold: 买入
    - 评分 < -threshold: 卖出
    """
    
    def __init__(self, config: Dict = None):
        default_config = {
            'weights': {
                'f1_mom_5d': 0.15,
                'f2_mom_20d': 0.25,
                'f3_vol_adj_ret': 0.15,
                'f4_volume_expansion': 0.15,
                'f5_rsi_trend_confirm': 0.15,
                'f6_sentiment': 0.15
            },
            'position_size_pct': 0.25,
            'score_threshold': 0.3,  # 评分阈值，超过才产生信号
            'use_sentiment': True
        }
        if config:
            default_config.update(config)
        
        super().__init__(
            name="Alpha6Factor",
            strategy_type=StrategyType.ALPHA_6FACTOR,
            config=default_config
        )
        
        self.factor_weights = self.config['weights']
        self.sentiment_cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/sentiment_cache')
    
    def generate_signals(self, market_data: pd.DataFrame) -> List[Signal]:
        """生成6因子Alpha信号

        根治修复：
        - 先计算全截面分数，再做截面中心化(rel_score = score - mean)
        - 避免在“全市场同向偏空”时出现 0 buy / 全 sell 的退化输出
        """
        signals: List[Signal] = []
        per_symbol = []

        for symbol in market_data['symbol'].unique():
            df = market_data[market_data['symbol'] == symbol].copy()
            if len(df) < 60:
                continue

            factors = self._calculate_factors(df, symbol)
            z_factors = self._zscore_factors(factors)
            score = self._calculate_score(z_factors)
            per_symbol.append((symbol, factors, z_factors, float(score)))

        if not per_symbol:
            return signals

        # 截面中心化：解决“绝对分数全负 => 全卖无买”
        cs_mean = float(np.mean([x[3] for x in per_symbol]))
        threshold = float(self.config['score_threshold'])

        buy_count = 0
        for symbol, factors, z_factors, score in per_symbol:
            rel_score = score - cs_mean
            if abs(rel_score) <= threshold:
                continue

            side = 'buy' if rel_score > 0 else 'sell'
            if side == 'buy':
                buy_count += 1
            confidence = min(abs(rel_score), 1.0)

            signal = Signal(
                symbol=symbol,
                side=side,
                score=abs(rel_score),
                confidence=confidence,
                strategy=self.name,
                timestamp=datetime.now(),
                metadata={
                    'raw_factors': factors,
                    'z_factors': z_factors,
                    'final_score': score,
                    'cross_section_mean': cs_mean,
                    'relative_score': rel_score,
                }
            )
            signals.append(signal)
            self.signals_history.append(signal)

        # 兜底：如果仍无buy，强制给最强相对分一个低置信度buy，防止长期全空仓
        if buy_count == 0 and per_symbol:
            top = max(per_symbol, key=lambda x: x[3] - cs_mean)
            rel_top = float(top[3] - cs_mean)
            if rel_top > -threshold:
                signals.append(
                    Signal(
                        symbol=top[0],
                        side='buy',
                        score=max(0.05, abs(rel_top)),
                        confidence=0.35,
                        strategy=self.name,
                        timestamp=datetime.now(),
                        metadata={
                            'fallback_buy': True,
                            'final_score': float(top[3]),
                            'cross_section_mean': cs_mean,
                            'relative_score': rel_top,
                        }
                    )
                )

        return signals
    
    def _calculate_factors(self, df: pd.DataFrame, symbol: str) -> Dict[str, float]:
        """计算6个原始因子"""
        close = df['close'].values
        volume = df['volume'].values if 'volume' in df.columns else np.ones(len(close))
        
        # f1: 5日动量 (5*24=120根1小时K线)
        f1 = (close[-1] - close[-min(120, len(close))]) / close[-min(120, len(close))]
        
        # f2: 20日动量 (20*24=480根1小时K线)
        f2 = (close[-1] - close[-min(480, len(close))]) / close[-min(480, len(close))]
        
        # f3: 波动率调整收益 (20日)
        window = close[-min(481, len(close)):]
        # 对齐分母长度，避免广播错误
        returns = np.diff(window) / window[:-1]
        vol = np.std(returns) if len(returns) > 0 else 1e-12
        f3 = f2 / (vol + 1e-12)
        
        # f4: 成交量扩张 (最近24h vs 前7天平均)
        if len(volume) >= 24 * 8:
            vol_recent = np.mean(volume[-24:])
            vol_prev = np.mean([np.mean(volume[-24*(i+1):-24*i]) for i in range(1, 8)])
            f4 = (vol_recent / (vol_prev + 1e-12)) - 1.0
        else:
            f4 = 0.0
        
        # f5: RSI趋势确认 (RSI-50)/50
        rsi = self._calculate_rsi_single(close)
        f5 = (rsi - 50.0) / 50.0
        
        # f6: 情绪因子（从缓存读取，失败则回退0）
        f6 = self._load_sentiment_factor(symbol) if self.config.get('use_sentiment', True) else 0.0
        
        return {
            'f1_mom_5d': f1,
            'f2_mom_20d': f2,
            'f3_vol_adj_ret': f3,
            'f4_volume_expansion': f4,
            'f5_rsi_trend_confirm': f5,
            'f6_sentiment': f6
        }
    
    def _zscore_factors(self, factors: Dict[str, float]) -> Dict[str, float]:
        """对因子进行z-score标准化 (简化版，实际应该用历史均值/std)"""
        # 这里使用简化的标准化：除以典型值范围
        typical_ranges = {
            'f1_mom_5d': 0.10,  # 10%波动
            'f2_mom_20d': 0.20,  # 20%波动
            'f3_vol_adj_ret': 2.0,
            'f4_volume_expansion': 0.50,
            'f5_rsi_trend_confirm': 1.0,
            'f6_sentiment': 1.0
        }
        
        z_factors = {}
        for name, value in factors.items():
            z_factors[name] = value / typical_ranges.get(name, 1.0)
        
        return z_factors
    
    def _calculate_score(self, z_factors: Dict[str, float]) -> float:
        """计算加权综合评分"""
        score = 0.0
        for name, z_value in z_factors.items():
            weight = self.factor_weights.get(name, 0.0)
            score += weight * z_value
        return score
    
    def _calculate_rsi_single(self, prices: np.ndarray, period: int = 14) -> float:
        """计算RSI"""
        if len(prices) <= period:
            return 50.0
        deltas = np.diff(prices[-(period + 1):])
        gains = np.clip(deltas, 0, None)
        losses = -np.clip(deltas, None, 0)
        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    def _load_sentiment_factor(self, symbol: str) -> float:
        """从本地缓存读取情绪分值（-1~1）；支持多种数据源。"""
        try:
            s = symbol.replace('/', '-').replace('_', '-')
            data = None
            
            # 1. 优先尝试 funding_rate（资金费率，最实时）
            funding_files = sorted(self.sentiment_cache_dir.glob(f"funding_{s}_*.json"))
            if funding_files:
                data = json.loads(funding_files[-1].read_text())
            
            # 2. 尝试 deepseek AI分析
            if data is None:
                deepseek_files = sorted(self.sentiment_cache_dir.glob(f"deepseek_{s}_*.json"))
                if deepseek_files:
                    data = json.loads(deepseek_files[-1].read_text())
            
            # 3. 尝试其他格式
            if data is None:
                other_files = sorted(self.sentiment_cache_dir.glob(f"{s}_*.json"))
                if other_files:
                    data = json.loads(other_files[-1].read_text())
            
            # 4. 若该币种没有缓存，尝试用DeepSeek生成一次
            if data is None:
                try:
                    from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor
                    factor = DeepSeekSentimentFactor(cache_dir=str(self.sentiment_cache_dir))
                    factor.calculate(s)
                    # 重新尝试读取
                    funding_files = sorted(self.sentiment_cache_dir.glob(f"funding_{s}_*.json"))
                    if funding_files:
                        data = json.loads(funding_files[-1].read_text())
                except Exception:
                    pass
            
            if data is None:
                return 0.0
                
            v = float(data.get('f6_sentiment', 0.0))
            return max(-1.0, min(1.0, v))
        except Exception:
            return 0.0
    
    def calculate_position_size(self, signal: Signal, available_capital: Decimal) -> Decimal:
        """根据信号强度计算仓位"""
        base_size = available_capital * Decimal(self.config['position_size_pct'])
        adjusted_size = base_size * Decimal(signal.confidence)
        return adjusted_size


class StrategyOrchestrator:
    """
    策略编排器 - 管理多个策略的并行运行
    
    职责:
    1. 注册/管理多个策略
    2. 收集各策略信号
    3. 信号融合与冲突解决
    4. 动态资金分配
    """
    
    def __init__(self, total_capital: Decimal = Decimal('100')):
        self.strategies: Dict[str, BaseStrategy] = {}
        self.total_capital = total_capital
        self.strategy_allocations: Dict[str, Decimal] = {}  # 资金分配比例
        
        # 默认资金分配
        self.default_allocations = {
            'TrendFollowing': Decimal('0.5'),      # 50% 趋势策略
            'MeanReversion': Decimal('0.3'),       # 30% 均值回归
            'Momentum': Decimal('0.2')             # 20% 动量 (预留)
        }
        
        self.performance_history: List[Dict] = []
    
    def register_strategy(self, strategy: BaseStrategy, allocation: Optional[Decimal] = None):
        """注册策略"""
        self.strategies[strategy.name] = strategy
        
        if allocation is None:
            allocation = self.default_allocations.get(strategy.name, Decimal('0.1'))
        
        self.strategy_allocations[strategy.name] = allocation
        print(f"[Orchestrator] 注册策略: {strategy.name}, 资金分配: {allocation}")
    
    def generate_combined_signals(self, market_data: pd.DataFrame) -> List[Signal]:
        """生成融合后的交易信号"""
        all_signals = []
        
        # 策略信号审计记录
        strategy_signal_audit = []
        
        import sys
        # 收集各策略信号
        for name, strategy in self.strategies.items():
            signals = strategy.generate_signals(market_data)
            all_signals.extend(signals)
            
            # 详细记录每个策略的信号
            buy_count = len([s for s in signals if s.side == 'buy'])
            sell_count = len([s for s in signals if s.side == 'sell'])
            print(f"[Orchestrator] {name}: 总信号={len(signals)}, 买={buy_count}, 卖={sell_count}", flush=True)
            sys.stdout.flush()
            
            # 记录策略信号到审计
            audit_entry = {
                'strategy': name,
                'type': strategy.strategy_type.value,
                'allocation': float(self.strategy_allocations.get(name, 0)),
                'total_signals': len(signals),
                'buy_signals': buy_count,
                'sell_signals': sell_count,
                'signals': []
            }
            
            # 记录前5个信号详情
            for s in signals[:5]:
                print(f"[Orchestrator]   -> {s.symbol}: {s.side}, score={s.score:.4f}, conf={s.confidence:.2f}", flush=True)
                sys.stdout.flush()
                audit_entry['signals'].append({
                    'symbol': s.symbol,
                    'side': s.side,
                    'score': float(s.score),
                    'confidence': float(s.confidence),
                    'metadata': s.metadata
                })
            
            strategy_signal_audit.append(audit_entry)
        
        # 信号融合（按币种聚合）
        combined = self._fuse_signals(all_signals)
        
        # 记录融合结果
        print(f"[Orchestrator] 信号融合: 输入={len(all_signals)}, 输出={len(combined)}", flush=True)
        sys.stdout.flush()
        for s in combined[:5]:
            print(f"[Orchestrator]   FUSED -> {s.symbol}: {s.side}, score={s.score:.4f}, strategy={s.strategy}", flush=True)
            sys.stdout.flush()
        
        # 保存策略信号审计到文件（包括融合结果）
        try:
            audit_file = Path(f"reports/runs/{datetime.now().strftime('%Y%m%d_%H')}/strategy_signals.json")
            audit_file.parent.mkdir(parents=True, exist_ok=True)
            
            # 构建融合信号审计
            fused_audit = []
            for s in combined:
                fused_audit.append({
                    'symbol': s.symbol,
                    'direction': s.side,
                    'score': float(s.score),
                    'confidence': float(s.confidence),
                    'strategy': s.strategy,
                    'rank': 0  # Will be calculated later
                })
            
            with open(audit_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'strategies': strategy_signal_audit,
                    'fused': {s['symbol']: s for s in fused_audit}  # Add fused signals
                }, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            print(f"[Orchestrator] 审计记录失败: {e}")
        
        return combined
    
    def _fuse_signals(self, signals: List[Signal]) -> List[Signal]:
        """信号融合 - 解决冲突、加权汇总"""
        if not signals:
            return []
        
        # 按币种分组
        symbol_signals: Dict[str, List[Signal]] = {}
        for s in signals:
            if s.symbol not in symbol_signals:
                symbol_signals[s.symbol] = []
            symbol_signals[s.symbol].append(s)
        
        fused_signals = []
        
        for symbol, sigs in symbol_signals.items():
            if len(sigs) == 1:
                fused_signals.append(sigs[0])
                continue
            
            # 多策略信号冲突解决
            buy_signals = [s for s in sigs if s.side == 'buy']
            sell_signals = [s for s in sigs if s.side == 'sell']
            
            # 策略：同向信号加权，反向信号抵消
            if buy_signals and not sell_signals:
                # 纯买入信号，加权平均
                avg_score = np.mean([s.score for s in buy_signals])
                avg_confidence = np.mean([s.confidence for s in buy_signals])
                
                fused = Signal(
                    symbol=symbol,
                    side='buy',
                    score=avg_score,
                    confidence=avg_confidence,
                    strategy="FUSED",
                    timestamp=datetime.now(),
                    metadata={
                        'source_strategies': [s.strategy for s in buy_signals],
                        'original_scores': [s.score for s in buy_signals]
                    }
                )
                fused_signals.append(fused)
            
            elif sell_signals and not buy_signals:
                # 纯卖出信号
                avg_score = np.mean([s.score for s in sell_signals])
                avg_confidence = np.mean([s.confidence for s in sell_signals])
                
                fused = Signal(
                    symbol=symbol,
                    side='sell',
                    score=avg_score,
                    confidence=avg_confidence,
                    strategy="FUSED",
                    timestamp=datetime.now(),
                    metadata={
                        'source_strategies': [s.strategy for s in sell_signals],
                        'original_scores': [s.score for s in sell_signals]
                    }
                )
                fused_signals.append(fused)
            
            else:
                # 多空冲突 - 选择置信度更高的一方
                buy_conf = max([s.confidence for s in buy_signals])
                sell_conf = max([s.confidence for s in sell_signals])
                
                # 提高冲突过滤阈值，降低噪声交易
                if buy_conf > sell_conf * 1.35:  # 买入置信度显著更高
                    best_buy = max(buy_signals, key=lambda s: s.confidence)
                    if best_buy.confidence >= 0.60:
                        fused_signals.append(best_buy)
                elif sell_conf > buy_conf * 1.35:
                    best_sell = max(sell_signals, key=lambda s: s.confidence)
                    if best_sell.confidence >= 0.60:
                        fused_signals.append(best_sell)
                else:
                    # 置信度接近，放弃该币种
                    print(f"[Orchestrator] {symbol} 多空冲突且置信度接近，放弃交易")
        
        return fused_signals
    
    def get_strategy_capital(self, strategy_name: str) -> Decimal:
        """获取策略分配的资金"""
        allocation = self.strategy_allocations.get(strategy_name, Decimal('0.1'))
        return self.total_capital * allocation
    
    def update_allocations_based_on_performance(self):
        """基于绩效动态调整资金分配"""
        # 简化版：近7天收益高的策略增加5%资金
        performances = []
        for name, strategy in self.strategies.items():
            perf = strategy.performance.recent_7d_return
            performances.append((name, perf))
        
        if not performances:
            return
        
        # 按收益排序
        performances.sort(key=lambda x: x[1], reverse=True)
        
        # 收益最高的策略增加资金（从最低的拿）
        if len(performances) >= 2 and performances[0][1] > performances[-1][1]:
            best_strategy = performances[0][0]
            worst_strategy = performances[-1][0]
            
            # 转移5%资金
            transfer = Decimal('0.05')
            if self.strategy_allocations[worst_strategy] > transfer:
                self.strategy_allocations[best_strategy] += transfer
                self.strategy_allocations[worst_strategy] -= transfer
                
                print(f"[Orchestrator] 资金调整: {worst_strategy} -> {best_strategy} ({transfer})")
    
    def get_status_report(self) -> Dict:
        """获取策略状态报告"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_capital': float(self.total_capital),
            'strategies': {}
        }
        
        for name, strategy in self.strategies.items():
            report['strategies'][name] = {
                'type': strategy.strategy_type.value,
                'allocation': float(self.strategy_allocations[name]),
                'allocated_capital': float(self.get_strategy_capital(name)),
                'performance': {
                    'total_pnl': strategy.performance.total_pnl,
                    'win_rate': strategy.performance.win_rate,
                    'trade_count': strategy.performance.trade_count
                },
                'recent_signals': len(strategy.get_recent_signals(24))
            }
        
        return report


# ============ 集成到V5的适配器 ============

class MultiStrategyAdapter:
    """
    V5多策略适配器
    
    将多策略系统适配到V5现有的execution_pipeline
    """
    
    def __init__(self, orchestrator: StrategyOrchestrator):
        self.orchestrator = orchestrator
    
    def run_strategy_cycle(self, market_data: pd.DataFrame) -> List[Dict]:
        """运行一个策略周期，返回目标持仓"""

        # 生成融合信号
        signals = self.orchestrator.generate_combined_signals(market_data)

        # 转换为V5格式的目标持仓
        targets = []
        for signal in signals:
            position_size = Decimal('0')

            # 常规单策略信号
            strategy = self.orchestrator.strategies.get(signal.strategy)
            if strategy is not None:
                capital = self.orchestrator.get_strategy_capital(signal.strategy)
                position_size = strategy.calculate_position_size(signal, capital)

            # 融合信号：使用总资金计算仓位（修复资金碎片化问题）
            elif signal.strategy == "FUSED":
                # 融合信号代表多策略共识，应使用总资金计算仓位
                # 而不是按来源策略拆分资金
                total_capital = self.orchestrator.total_capital
                
                # 根据信号置信度和来源策略数量调整仓位比例
                source_names = signal.metadata.get('source_strategies', []) if signal.metadata else []
                n_sources = len(source_names)
                
                # 基础仓位比例：30%，每增加一个策略来源增加5%，最高50%
                base_position_pct = Decimal('0.30')
                bonus_per_source = Decimal('0.05')
                max_position_pct = Decimal('0.50')
                
                position_pct = base_position_pct + (bonus_per_source * n_sources)
                position_pct = min(position_pct, max_position_pct)
                
                # 根据置信度微调
                confidence_factor = Decimal(str(signal.confidence))
                position_size = total_capital * position_pct * confidence_factor
                
                print(f"[MultiStrategyAdapter] FUSED信号 {signal.symbol}: "
                      f"来源={source_names}, 总资金={total_capital}, "
                      f"仓位比例={float(position_pct):.0%}, 置信度={float(confidence_factor):.2f}, "
                      f"计算仓位={float(position_size):.2f} USDT")

                # 兜底：来源缺失时使用总资金的小比例
                if position_size <= 0:
                    position_size = total_capital * Decimal(str(max(0.0, min(signal.confidence, 1.0)))) * Decimal('0.2')

            if position_size > 0:
                targets.append({
                    'symbol': signal.symbol,
                    'side': signal.side,
                    'target_position_usdt': float(position_size),
                    'signal_score': float(signal.score),
                    'confidence': float(signal.confidence),
                    'source_strategy': signal.strategy,
                    'metadata': signal.metadata
                })

        return targets


# ============ 使用示例 ============

def demo():
    """多策略系统演示"""
    print("=" * 60)
    print("V5 多策略并行系统演示")
    print("=" * 60)
    
    # 创建策略编排器
    orchestrator = StrategyOrchestrator(total_capital=Decimal('20'))  # 20 USDT
    
    # 注册趋势跟踪策略
    trend_strategy = TrendFollowingStrategy(config={
        'fast_ma': 20,
        'slow_ma': 60,
        'position_size_pct': 0.5  # 该策略内单信号50%仓位
    })
    orchestrator.register_strategy(trend_strategy, allocation=Decimal('0.6'))  # 60%资金
    
    # 注册均值回归策略
    mean_revert_strategy = MeanReversionStrategy(config={
        'rsi_oversold': 30,
        'rsi_overbought': 70,
        'position_size_pct': 0.3
    })
    orchestrator.register_strategy(mean_revert_strategy, allocation=Decimal('0.4'))  # 40%资金
    
    # 模拟市场数据
    print("\n模拟市场数据...")
    np.random.seed(42)
    
    # 生成模拟K线数据
    symbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT']
    market_data_list = []
    
    for symbol in symbols:
        # 生成随机价格序列
        n = 100
        trend = np.cumsum(np.random.randn(n) * 0.02)
        close = 100 + trend * 10
        high = close * (1 + np.abs(np.random.randn(n) * 0.01))
        low = close * (1 - np.abs(np.random.randn(n) * 0.01))
        volume = np.random.randint(1000, 10000, n)
        
        df = pd.DataFrame({
            'symbol': symbol,
            'close': close,
            'high': high,
            'low': low,
            'volume': volume
        })
        market_data_list.append(df)
    
    market_data = pd.concat(market_data_list, ignore_index=True)
    
    # 运行策略
    print("\n运行策略...")
    signals = orchestrator.generate_combined_signals(market_data)
    
    print(f"\n生成 {len(signals)} 个交易信号:")
    for sig in signals:
        print(f"  {sig.symbol}: {sig.side.upper()} | 评分: {sig.score:.2f} | 策略: {sig.strategy}")
    
    # 获取状态报告
    print("\n" + "=" * 60)
    print("策略状态报告")
    print("=" * 60)
    
    report = orchestrator.get_status_report()
    print(f"总资金: {report['total_capital']} USDT")
    print(f"策略数量: {len(report['strategies'])}")
    
    for name, info in report['strategies'].items():
        print(f"\n{name}:")
        print(f"  资金分配: {info['allocation']*100:.0f}% ({info['allocated_capital']:.2f} USDT)")
        print(f"  总盈亏: {info['performance']['total_pnl']:.2f}")
        print(f"  胜率: {info['performance']['win_rate']*100:.1f}%")
    
    # 适配器演示
    print("\n" + "=" * 60)
    print("V5集成适配器输出")
    print("=" * 60)
    
    adapter = MultiStrategyAdapter(orchestrator)
    targets = adapter.run_strategy_cycle(market_data)
    
    print(f"\n目标持仓 ({len(targets)} 个):")
    for t in targets:
        print(f"  {t['symbol']}: {t['side']} ${t['target_position_usdt']:.2f} "
              f"(策略: {t['source_strategy']}, 置信度: {t['confidence']:.2f})")
    
    print("\n" + "=" * 60)
    print("演示完成！")
    print("=" * 60)


if __name__ == "__main__":
    demo()
