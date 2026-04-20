"""
多策略并行交易系统 - Multi-Strategy Parallel Trading System

设计目标:
1. 同时运行多个策略（趋势跟踪 + 均值回归 + 动量）
2. 动态资金分配（基于策略近期表现）
3. 策略间信号融合
4. 独立风控（每个策略有自己的止损）
"""

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
import pandas as pd
import numpy as np
from decimal import Decimal
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from configs.schema import (
    ALPHA158_OVERLAY_FACTOR_KEYS,
    ALPHA_BASE_FACTOR_INPUT_TO_RUNTIME,
    normalize_alpha_base_factor_mapping,
    normalize_alpha158_overlay_factor_mapping,
)
from src.alpha.qlib_factors import compute_alpha158_style_factors


def _coalesce(value: Any, default: Any) -> Any:
    return default if value is None else value
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
            'volume_dry_ratio': 0.8,
            'buy_score_multiplier': 1.0,
            'sell_score_multiplier': 1.0,
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
            volume_dry_ratio = float(_coalesce(self.config.get('volume_dry_ratio', 0.8), 0.8))
            volume_dry_up = latest['volume'] < latest['volume_ma'] * volume_dry_ratio
            
            if oversold and volume_dry_up:
                score = (self.config['rsi_oversold'] - latest['rsi']) / self.config['rsi_oversold']
                score *= float(_coalesce(self.config.get('buy_score_multiplier', 1.0), 1.0))
                score = min(max(score, 0.0), 1.0)
                signal = Signal(
                    symbol=symbol,
                    side='buy',
                    score=score,
                    confidence=min(score * 0.9 + 0.1, 1.0),
                    strategy=self.name,
                    timestamp=datetime.now(),
                    metadata={
                        'rsi': latest['rsi'],
                        'deviation': deviation,
                        'bb_lower': latest['bb_lower'],
                        'volume_dry_ratio': volume_dry_ratio,
                        'side_weight_multiplier': float(
                            _coalesce(self.config.get('buy_score_multiplier', 1.0), 1.0)
                        ),
                    }
                )
                signals.append(signal)
                self.signals_history.append(signal)
            
            elif overbought and volume_dry_up:
                score = (latest['rsi'] - self.config['rsi_overbought']) / (100 - self.config['rsi_overbought'])
                score *= float(_coalesce(self.config.get('sell_score_multiplier', 1.0), 1.0))
                score = min(max(score, 0.0), 1.0)
                signal = Signal(
                    symbol=symbol,
                    side='sell',
                    score=score,
                    confidence=min(score * 0.9 + 0.1, 1.0),
                    strategy=self.name,
                    timestamp=datetime.now(),
                    metadata={
                        'rsi': latest['rsi'],
                        'deviation': deviation,
                        'bb_upper': latest['bb_upper'],
                        'volume_dry_ratio': volume_dry_ratio,
                        'side_weight_multiplier': float(
                            _coalesce(self.config.get('sell_score_multiplier', 1.0), 1.0)
                        ),
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
        default_overlay = {
            'enabled': True,
            'blend_weight': 0.35,
            'weights': {
                'f6_corr_pv_10': 0.15,
                'f7_cord_10': 0.15,
                'f8_rsqr_10': 0.20,
                'f9_rank_20': 0.15,
                'f10_imax_14': -0.05,
                'f11_imin_14': 0.05,
                'f12_imxd_14': 0.35,
            },
        }
        default_config = {
            'weights': {
                'f1_mom_5d': 0.15,
                'f2_mom_20d': 0.25,
                'f3_vol_adj_ret': 0.15,
                'f4_volume_expansion': 0.15,
                'f5_rsi_trend_confirm': 0.15,
                'f6_sentiment': 0.15,
                **default_overlay['weights'],
            },
            'factor_zscore_caps': {
                'f1_mom_5d': 6.0,
                'f2_mom_20d': 6.0,
                'f3_vol_adj_ret': 4.0,
                'f4_volume_expansion': 3.0,
                'f5_rsi_trend_confirm': 3.0,
                'f6_sentiment': 3.0,
                'f6_corr_pv_10': 4.0,
                'f7_cord_10': 4.0,
                'f8_rsqr_10': 4.0,
                'f9_rank_20': 4.0,
                'f10_imax_14': 4.0,
                'f11_imin_14': 4.0,
                'f12_imxd_14': 4.0,
            },
            'position_size_pct': 0.25,
            'score_threshold': 0.3,
            'use_sentiment': True,
            'alpha158_enabled': True,
            'alpha158_blend_weight': 0.35,
            'alpha158_overlay': deepcopy(default_overlay),
            'dynamic_ic_weighting': {
                'enabled': False,
                'ic_monitor_path': 'reports/alpha_ic_monitor.json',
                'min_abs_ic': 0.003,
                'fallback_to_static': True,
            },
        }
        user_config = dict(config or {})
        merged_config = deepcopy(default_config)
        for key, value in user_config.items():
            if key in {'weights', 'alpha158_overlay'}:
                continue
            merged_config[key] = value

        overlay_cfg = self._resolve_alpha158_overlay_settings(user_config, default_overlay)
        merged_weights = dict(default_config['weights'])
        if isinstance(user_config.get('weights'), dict):
            merged_weights.update(
                self._normalize_runtime_factor_weights(
                    user_config.get('weights'),
                    context='Alpha6FactorStrategy weights',
                )
            )
        merged_weights.update(overlay_cfg.get('weights') or {})

        merged_config['weights'] = merged_weights
        merged_config['alpha158_overlay'] = deepcopy(overlay_cfg)
        merged_config['alpha158_enabled'] = bool(overlay_cfg.get('enabled', True))
        merged_config['alpha158_blend_weight'] = float(overlay_cfg.get('blend_weight', 0.35))

        super().__init__(
            name="Alpha6Factor",
            strategy_type=StrategyType.ALPHA_6FACTOR,
            config=merged_config,
        )

        self.factor_weights = dict(self.config['weights'])
        self.sentiment_cache_dir = Path(__file__).resolve().parents[2] / 'data' / 'sentiment_cache'
        self.last_factor_snapshot: Dict[str, Dict[str, Any]] = {}
        self.last_resolved_weights: Dict[str, float] = dict(self.factor_weights)

    @staticmethod
    def _normalize_runtime_factor_weights(weights: Dict[str, Any], *, context: str) -> Dict[str, float]:
        if not isinstance(weights, dict):
            return {}

        allowed_keys = set(ALPHA_BASE_FACTOR_INPUT_TO_RUNTIME.keys()) | set(ALPHA158_OVERLAY_FACTOR_KEYS) | {'f6_sentiment'}
        unknown = sorted(str(key) for key in weights.keys() if str(key) not in allowed_keys)
        if unknown:
            raise ValueError(
                f"Unknown {context} keys: {unknown}. Allowed keys: {sorted(allowed_keys)}"
            )

        base_weights = normalize_alpha_base_factor_mapping(
            {k: v for k, v in weights.items() if str(k) in ALPHA_BASE_FACTOR_INPUT_TO_RUNTIME},
            context=context,
            output='runtime',
        )
        overlay_weights = normalize_alpha158_overlay_factor_mapping(
            {k: v for k, v in weights.items() if str(k) in ALPHA158_OVERLAY_FACTOR_KEYS},
            context=f"{context} alpha158_overlay",
            output='runtime',
        )
        resolved: Dict[str, float] = {
            str(key): float(value) for key, value in base_weights.items()
        }
        resolved.update({str(key): float(value) for key, value in overlay_weights.items()})
        if 'f6_sentiment' in weights:
            resolved['f6_sentiment'] = float(weights['f6_sentiment'])
        return resolved

    @classmethod
    def _resolve_alpha158_overlay_settings(
        cls,
        config: Dict[str, Any],
        default_overlay: Dict[str, Any],
    ) -> Dict[str, Any]:
        overlay_cfg = deepcopy(default_overlay)
        nested_cfg = config.get('alpha158_overlay') if isinstance(config, dict) else None

        if isinstance(nested_cfg, dict):
            if 'enabled' in nested_cfg:
                overlay_cfg['enabled'] = bool(nested_cfg.get('enabled'))
            if 'blend_weight' in nested_cfg:
                overlay_cfg['blend_weight'] = float(nested_cfg.get('blend_weight'))
            if isinstance(nested_cfg.get('weights'), dict):
                overlay_cfg['weights'].update(
                    {
                        str(key): float(value)
                        for key, value in normalize_alpha158_overlay_factor_mapping(
                            nested_cfg.get('weights'),
                            context='Alpha6FactorStrategy alpha158_overlay.weights',
                            output='runtime',
                        ).items()
                    }
                )

        if 'alpha158_enabled' in config and not (isinstance(nested_cfg, dict) and 'enabled' in nested_cfg):
            overlay_cfg['enabled'] = bool(config.get('alpha158_enabled'))
        if 'alpha158_blend_weight' in config and not (isinstance(nested_cfg, dict) and 'blend_weight' in nested_cfg):
            overlay_cfg['blend_weight'] = float(config.get('alpha158_blend_weight'))

        overlay_cfg['blend_weight'] = float(max(0.0, min(1.0, overlay_cfg.get('blend_weight', 0.35))))
        overlay_cfg['weights'] = {
            str(key): float(value)
            for key, value in (overlay_cfg.get('weights') or {}).items()
            if str(key) in ALPHA158_OVERLAY_FACTOR_KEYS
        }
        return overlay_cfg

    def _alpha158_settings(self) -> Dict[str, Any]:
        nested_cfg = self.config.get('alpha158_overlay') or {}
        enabled = bool(_coalesce(nested_cfg.get('enabled'), self.config.get('alpha158_enabled', True)))
        blend_weight = float(_coalesce(nested_cfg.get('blend_weight'), self.config.get('alpha158_blend_weight', 0.35)))
        weights = {
            str(key): float(value)
            for key, value in ((nested_cfg.get('weights') or {}).items())
            if str(key) in ALPHA158_OVERLAY_FACTOR_KEYS
        }
        return {
            'enabled': enabled,
            'blend_weight': float(max(0.0, min(1.0, blend_weight))),
            'weights': weights,
        }

    def _compress_signal_score(self, raw_score: float) -> float:
        """Compress raw cross-sectional strength into a stable display/routing scale."""
        magnitude = max(0.0, abs(float(raw_score)))
        mode = str(self.config.get('score_transform', 'tanh') or 'tanh').strip().lower()
        scale = max(float(self.config.get('score_transform_scale', 1.0) or 1.0), 1e-6)
        if mode == 'none':
            return magnitude
        if mode == 'clip':
            return min(magnitude, 1.0)
        return float(np.tanh(magnitude / scale))

    def set_factor_weights(self, weights: Dict[str, float]) -> None:
        merged = dict(self.config.get('weights') or {})
        merged.update(
            self._normalize_runtime_factor_weights(
                weights or {},
                context='Alpha6FactorStrategy weights',
            )
        )
        self.config['weights'] = merged
        self.factor_weights = merged

    def get_latest_factor_snapshot(self) -> Dict[str, Dict[str, Any]]:
        return deepcopy(self.last_factor_snapshot)

    @staticmethod
    def _extract_ic_mean(rec: Dict[str, Any], short_key: str, long_key: str) -> Tuple[Optional[float], Optional[float]]:
        def _nested_mean(bucket_key: str) -> Optional[float]:
            try:
                bucket = rec.get(bucket_key) or {}
                if isinstance(bucket, dict) and bucket.get("count", 0):
                    return float(bucket.get("mean"))
            except Exception:
                return None
            return None

        return _nested_mean(short_key), _nested_mean(long_key)

    def _resolve_dynamic_weights(self, static_weights: Dict[str, float]) -> Dict[str, float]:
        """根据 IC monitor 动态修正因子权重（可选）。"""
        try:
            cfg = self.config.get('dynamic_ic_weighting', {}) or {}
            if not bool(cfg.get('enabled', False)):
                return dict(static_weights)

            p = Path(str(cfg.get('ic_monitor_path', 'reports/alpha_ic_monitor.json')))
            if not p.exists():
                return dict(static_weights)

            obj = json.loads(p.read_text(encoding='utf-8'))
            factor_ic = obj.get('factor_ic') if isinstance(obj, dict) else None
            if not isinstance(factor_ic, dict):
                return dict(static_weights)

            min_abs_ic = float(_coalesce(cfg.get('min_abs_ic', 0.003), 0.003))
            dyn: Dict[str, float] = {}
            for k in static_weights.keys():
                rec = factor_ic.get(k) or {}
                short_ic, long_ic = self._extract_ic_mean(rec, 'rank_ic_short', 'rank_ic_long')
                if short_ic is None and long_ic is None:
                    short_ic, long_ic = self._extract_ic_mean(rec, 'ic_short', 'ic_long')

                base_mag = abs(float(static_weights.get(k, 0.0)))
                if base_mag <= 1e-12:
                    continue

                positive_short = max(0.0, float(short_ic or 0.0))
                positive_long = max(0.0, float(long_ic or 0.0))
                quality = 0.65 * positive_short + 0.35 * positive_long

                if quality >= min_abs_ic:
                    multiplier = 1.0 + min(2.0, quality / max(min_abs_ic, 1e-9))
                else:
                    multiplier = 0.35
                    if (short_ic is not None and float(short_ic) <= -min_abs_ic) or (
                        long_ic is not None and float(long_ic) <= -min_abs_ic
                    ):
                        multiplier = 0.15
                    elif short_ic is not None and float(short_ic) < 0:
                        multiplier = 0.25
                    elif long_ic is not None and float(long_ic) < 0:
                        multiplier = 0.20

                dyn[k] = base_mag * multiplier

            if not dyn:
                return dict(static_weights)

            l1_static = float(sum(abs(float(v)) for v in static_weights.values()))
            l1_dyn = float(sum(abs(float(v)) for v in dyn.values()))
            if l1_dyn <= 1e-12:
                return dict(static_weights)
            scale = l1_static / l1_dyn if l1_static > 0 else 1.0

            out = dict(static_weights)
            for k in out.keys():
                if k in dyn:
                    sign = -1.0 if float(out[k]) < 0 else 1.0
                    out[k] = round(sign * float(dyn[k]) * scale, 12)
            return out
        except Exception:
            return dict(static_weights)
    
    def generate_signals(self, market_data: pd.DataFrame) -> List[Signal]:
        """生成6因子Alpha信号

        根治修复：
        - 先计算全截面分数，再做截面中心化(rel_score = score - mean)
        - 避免在“全市场同向偏空”时出现 0 buy / 全 sell 的退化输出
        """
        signals: List[Signal] = []
        per_symbol = []
        weights_resolved = self._resolve_dynamic_weights(self.factor_weights)
        self.last_resolved_weights = dict(weights_resolved)
        self.last_factor_snapshot = {}

        for symbol in market_data['symbol'].unique():
            df = market_data[market_data['symbol'] == symbol].copy()
            if len(df) < 60:
                continue

            factors = self._calculate_factors(df, symbol)
            z_factors = self._zscore_factors(factors)
            score = self._calculate_score(z_factors, weights_resolved)
            per_symbol.append((symbol, factors, z_factors, float(score)))

        if not per_symbol:
            return signals

        # 截面中心化：解决“绝对分数全负 => 全卖无买”
        cs_mean = float(np.mean([x[3] for x in per_symbol]))
        threshold = float(self.config['score_threshold'])

        for symbol, factors, z_factors, score in per_symbol:
            rel_score = float(score - cs_mean)
            display_score = self._compress_signal_score(rel_score)
            self.last_factor_snapshot[symbol] = {
                'raw_factors': {str(k): float(v) for k, v in (factors or {}).items()},
                'z_factors': {str(k): float(v) for k, v in (z_factors or {}).items()},
                'final_score': float(score),
                'cross_section_mean': cs_mean,
                'relative_score': rel_score,
                'raw_score': abs(rel_score),
                'display_score': float(display_score),
            }

        buy_count = 0
        for symbol, factors, z_factors, score in per_symbol:
            telemetry = self.last_factor_snapshot.get(symbol, {})
            rel_score = float(telemetry.get('relative_score', score - cs_mean))
            display_score = float(telemetry.get('display_score', self._compress_signal_score(rel_score)))
            raw_score = float(telemetry.get('raw_score', abs(rel_score)))
            if display_score <= threshold:
                continue

            side = 'buy' if rel_score > 0 else 'sell'
            if side == 'buy':
                buy_count += 1
            confidence = min(raw_score, 1.0)

            signal = Signal(
                symbol=symbol,
                side=side,
                score=display_score,
                confidence=confidence,
                strategy=self.name,
                timestamp=datetime.now(),
                metadata={
                    'raw_factors': telemetry.get('raw_factors', factors),
                    'z_factors': telemetry.get('z_factors', z_factors),
                    'final_score': float(telemetry.get('final_score', score)),
                    'cross_section_mean': cs_mean,
                    'relative_score': rel_score,
                    'relative_score_raw': rel_score,
                    'raw_score': raw_score,
                    'display_score': display_score,
                    'score_transform': str(self.config.get('score_transform', 'tanh') or 'tanh'),
                    'score_transform_scale': float(self.config.get('score_transform_scale', 1.0) or 1.0),
                }
            )
            signals.append(signal)
            self.signals_history.append(signal)

        # 兜底：如果仍无buy，强制给最强相对分一个低置信度buy，防止长期全空仓
        if buy_count == 0 and per_symbol:
            top = max(per_symbol, key=lambda x: x[3] - cs_mean)
            rel_top = float(top[3] - cs_mean)
            display_top = self._compress_signal_score(rel_top)
            if rel_top > -threshold:
                telemetry = self.last_factor_snapshot.get(top[0], {})
                signals.append(
                    Signal(
                        symbol=top[0],
                        side='buy',
                        score=max(0.05, display_top),
                        confidence=0.35,
                        strategy=self.name,
                        timestamp=datetime.now(),
                        metadata={
                            'fallback_buy': True,
                            'raw_factors': telemetry.get('raw_factors', top[1]),
                            'z_factors': telemetry.get('z_factors', top[2]),
                            'final_score': float(telemetry.get('final_score', top[3])),
                            'cross_section_mean': cs_mean,
                            'relative_score': rel_top,
                            'relative_score_raw': rel_top,
                            'raw_score': abs(rel_top),
                            'display_score': max(0.05, display_top),
                            'score_transform': str(self.config.get('score_transform', 'tanh') or 'tanh'),
                            'score_transform_scale': float(self.config.get('score_transform_scale', 1.0) or 1.0),
                        }
                    )
                )

        return signals
    
    def _calculate_factors(self, df: pd.DataFrame, symbol: str) -> Dict[str, float]:
        """计算6个原始因子"""
        close = df['close'].values
        high = df['high'].values if 'high' in df.columns else close
        low = df['low'].values if 'low' in df.columns else close
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

        out = {
            'f1_mom_5d': f1,
            'f2_mom_20d': f2,
            'f3_vol_adj_ret': f3,
            'f4_volume_expansion': f4,
            'f5_rsi_trend_confirm': f5,
            'f6_sentiment': f6,
        }

        # Alpha158 overlay factors
        if self._alpha158_settings().get('enabled', True):
            try:
                qf = compute_alpha158_style_factors(
                    close.tolist(), high.tolist(), low.tolist(), volume.tolist()
                )
                out.update(qf)
            except Exception:
                pass

        return out
    
    def _zscore_factors(self, factors: Dict[str, float]) -> Dict[str, float]:
        """对因子进行z-score标准化 (简化版，实际应该用历史均值/std)"""
        # 这里使用简化的标准化：除以典型值范围
        typical_ranges = {
            'f1_mom_5d': 0.10,  # 10%波动
            'f2_mom_20d': 0.20,  # 20%波动
            'f3_vol_adj_ret': 2.0,
            'f4_volume_expansion': 0.50,
            'f5_rsi_trend_confirm': 1.0,
            'f6_sentiment': 1.0,
            'f6_corr_pv_10': 0.30,
            'f7_cord_10': 0.30,
            'f8_rsqr_10': 0.30,
            'f9_rank_20': 0.30,
            'f10_imax_14': 0.30,
            'f11_imin_14': 0.30,
            'f12_imxd_14': 0.30,
        }
        
        factor_caps = self.config.get('factor_zscore_caps', {}) or {}
        z_factors = {}
        for name, value in factors.items():
            z_value = float(value) / float(typical_ranges.get(name, 1.0) or 1.0)
            cap = factor_caps.get(name)
            if cap is not None:
                z_cap = abs(float(cap))
                z_value = float(np.clip(z_value, -z_cap, z_cap))
            z_factors[name] = z_value
        
        return z_factors
    
    def _calculate_score(self, z_factors: Dict[str, float], resolved_weights: Dict[str, float]) -> float:
        """计算综合评分：base + Alpha158 overlay blending。"""
        base_keys = [
            'f1_mom_5d',
            'f2_mom_20d',
            'f3_vol_adj_ret',
            'f4_volume_expansion',
            'f5_rsi_trend_confirm',
            'f6_sentiment',
        ]
        ov_keys = [
            'f6_corr_pv_10',
            'f7_cord_10',
            'f8_rsqr_10',
            'f9_rank_20',
            'f10_imax_14',
            'f11_imin_14',
            'f12_imxd_14',
        ]

        base_score = 0.0
        for k in base_keys:
            base_score += float(resolved_weights.get(k, 0.0)) * float(z_factors.get(k, 0.0))

        alpha158_cfg = self._alpha158_settings()
        if alpha158_cfg.get('enabled', True):
            ov_score = 0.0
            for k in ov_keys:
                ov_score += float(resolved_weights.get(k, 0.0)) * float(z_factors.get(k, 0.0))
            blend = float(alpha158_cfg.get('blend_weight', 0.35))
            return (1.0 - blend) * base_score + blend * ov_score

        return base_score
    
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

    def _latest_sentiment_cache_file(self, pattern: str) -> Optional[Path]:
        files = list(self.sentiment_cache_dir.glob(pattern))
        if not files:
            return None

        def _sort_epoch(path: Path) -> float:
            match = re.search(r'(?<!\d)(20\d{6}_\d{2})(?!\d)', path.stem)
            if match:
                try:
                    return datetime.strptime(match.group(1), '%Y%m%d_%H').timestamp()
                except Exception:
                    pass
            return path.stat().st_mtime

        return max(files, key=_sort_epoch)

    def _load_sentiment_factor(self, symbol: str) -> float:
        """从本地缓存读取情绪分值（-1~1）；支持多种数据源。"""
        try:
            s = symbol.replace('/', '-').replace('_', '-')
            data = None
            
            # 1. 优先尝试 funding_rate（资金费率，最实时）
            funding_file = self._latest_sentiment_cache_file(f"funding_{s}_*.json")
            if funding_file is not None:
                data = json.loads(funding_file.read_text())
            
            # 2. 尝试 deepseek AI分析
            if data is None:
                deepseek_file = self._latest_sentiment_cache_file(f"deepseek_{s}_*.json")
                if deepseek_file is not None:
                    data = json.loads(deepseek_file.read_text())
            
            # 3. 尝试其他格式
            if data is None:
                other_file = self._latest_sentiment_cache_file(f"{s}_*.json")
                if other_file is not None:
                    data = json.loads(other_file.read_text())
            
            # 4. 若该币种没有缓存，尝试用DeepSeek生成一次
            if data is None:
                try:
                    from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor
                    factor = DeepSeekSentimentFactor(cache_dir=str(self.sentiment_cache_dir))
                    factor.calculate(s)
                    # 重新尝试读取
                    funding_file = self._latest_sentiment_cache_file(f"funding_{s}_*.json")
                    if funding_file is not None:
                        data = json.loads(funding_file.read_text())
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
    
    def __init__(
        self,
        total_capital: Decimal = Decimal('100'),
        *,
        audit_root: Optional[Path] = None,
        conflict_penalty_enabled: bool = True,
        conflict_dominance_ratio: float = 1.35,
        conflict_min_confidence: float = 0.60,
        conflict_penalty_strength: float = 0.65,
    ):
        self.strategies: Dict[str, BaseStrategy] = {}
        self.total_capital = total_capital
        self.strategy_allocations: Dict[str, Decimal] = {}  # 资金分配比例
        self.conflict_penalty_enabled = bool(conflict_penalty_enabled)
        self.conflict_dominance_ratio = max(1.0, float(conflict_dominance_ratio))
        self.conflict_min_confidence = float(max(0.0, min(conflict_min_confidence, 1.0)))
        self.conflict_penalty_strength = float(max(0.0, min(conflict_penalty_strength, 1.0)))
        self.audit_root = Path(audit_root) if audit_root is not None else None
        self._latest_strategy_signal_payload: Dict[str, Any] = {}
        
        # 默认资金分配
        self.default_allocations = {
            'TrendFollowing': Decimal('0.5'),      # 50% 趋势策略
            'MeanReversion': Decimal('0.3'),       # 30% 均值回归
            'Momentum': Decimal('0.2')             # 20% 动量 (预留)
        }
        
        self.performance_history: List[Dict] = []
        self.run_id: str = ""

    @staticmethod
    def _signal_raw_score(signal: Signal) -> float:
        try:
            meta = signal.metadata or {}
            return float(meta.get("raw_score", signal.score))
        except Exception:
            return float(signal.score)

    @staticmethod
    def _build_fused_signal(
        *,
        symbol: str,
        side: str,
        signals: List[Signal],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Signal:
        avg_score = float(np.mean([float(s.score) for s in signals])) if signals else 0.0
        avg_raw_score = float(np.mean([StrategyOrchestrator._signal_raw_score(s) for s in signals])) if signals else 0.0
        avg_confidence = float(np.mean([float(s.confidence) for s in signals])) if signals else 0.0
        display_score = max(0.0, min(avg_score, 1.0))
        fused_metadata = {
            "source_strategies": [s.strategy for s in signals],
            "original_scores": [float(s.score) for s in signals],
            "original_raw_scores": [StrategyOrchestrator._signal_raw_score(s) for s in signals],
            "original_confidences": [float(s.confidence) for s in signals],
            "conflict_detected": False,
            "conflict_penalty_factor": 1.0,
            "raw_score": max(0.0, avg_raw_score),
            "display_score": display_score,
        }
        if metadata:
            fused_metadata.update(metadata)
        return Signal(
            symbol=symbol,
            side=side,
            score=display_score,
            confidence=max(0.0, min(avg_confidence, 1.0)),
            strategy="FUSED",
            timestamp=datetime.now(),
            metadata=fused_metadata,
        )
    
    def register_strategy(self, strategy: BaseStrategy, allocation: Optional[Decimal] = None):
        """注册策略"""
        self.strategies[strategy.name] = strategy
        
        if allocation is None:
            allocation = self.default_allocations.get(strategy.name, Decimal('0.1'))
        
        self.strategy_allocations[strategy.name] = allocation
        print(f"[Orchestrator] 注册策略: {strategy.name}, 资金分配: {allocation}")
    
    def set_run_id(self, run_id: Optional[str]) -> None:
        self.run_id = str(run_id or "").strip()

    def strategy_signals_path(self) -> Optional[Path]:
        if not self.run_id:
            return None
        if self.audit_root is not None:
            return self.audit_root / "runs" / self.run_id / "strategy_signals.json"
        return Path("reports") / "runs" / self.run_id / "strategy_signals.json"

    def set_strategy_allocation(self, strategy_name: str, allocation: Decimal) -> None:
        if strategy_name not in self.strategies:
            return
        self.strategy_allocations[strategy_name] = allocation

    def latest_strategy_signal_payload(self) -> Dict[str, Any]:
        return deepcopy(self._latest_strategy_signal_payload)

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
                    'raw_score': StrategyOrchestrator._signal_raw_score(s),
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
        fused_audit = []
        for s in combined:
            fused_audit.append({
                'symbol': s.symbol,
                'direction': s.side,
                'score': float(s.score),
                'raw_score': StrategyOrchestrator._signal_raw_score(s),
                'confidence': float(s.confidence),
                'strategy': s.strategy,
                'metadata': s.metadata,
                'rank': 0,  # Will be calculated later
            })

        payload = {
            'timestamp': datetime.now().isoformat(),
            'run_id': self.run_id,
            'strategies': strategy_signal_audit,
            'fused': {s['symbol']: s for s in fused_audit},  # Add fused signals
        }
        self._latest_strategy_signal_payload = deepcopy(payload)

        try:
            audit_file = self.strategy_signals_path()
            if audit_file is not None:
                audit_file.parent.mkdir(parents=True, exist_ok=True)

                tmp_file = audit_file.with_suffix('.tmp')
                tmp_file.write_text(
                    json.dumps(payload, indent=2, ensure_ascii=False, default=str),
                    encoding='utf-8',
                )
                tmp_file.replace(audit_file)
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
                fused = self._build_fused_signal(
                    symbol=symbol,
                    side='buy',
                    signals=buy_signals,
                    metadata={
                        'opposing_strategies': [],
                        'opposing_scores': [],
                        'opposing_confidences': [],
                    },
                )
                fused_signals.append(fused)
            
            elif sell_signals and not buy_signals:
                fused = self._build_fused_signal(
                    symbol=symbol,
                    side='sell',
                    signals=sell_signals,
                    metadata={
                        'opposing_strategies': [],
                        'opposing_scores': [],
                        'opposing_confidences': [],
                    },
                )
                fused_signals.append(fused)
            
            else:
                # 多空冲突 - 只保留显著占优的一方，同时对分数/置信度做降权
                buy_conf = max([s.confidence for s in buy_signals])
                sell_conf = max([s.confidence for s in sell_signals])

                dominant_side = None
                dominant_signals: List[Signal] = []
                opposing_signals: List[Signal] = []
                dominant_conf = 0.0
                opposing_conf = 0.0
                if buy_conf > sell_conf * self.conflict_dominance_ratio:
                    dominant_side = 'buy'
                    dominant_signals = buy_signals
                    opposing_signals = sell_signals
                    dominant_conf = float(buy_conf)
                    opposing_conf = float(sell_conf)
                elif sell_conf > buy_conf * self.conflict_dominance_ratio:
                    dominant_side = 'sell'
                    dominant_signals = sell_signals
                    opposing_signals = buy_signals
                    dominant_conf = float(sell_conf)
                    opposing_conf = float(buy_conf)
                else:
                    print(f"[Orchestrator] {symbol} 多空冲突且置信度接近，放弃交易")
                    continue

                if dominant_conf < self.conflict_min_confidence:
                    print(
                        f"[Orchestrator] {symbol} 冲突后主导侧置信度不足，放弃交易: "
                        f"dominant_conf={dominant_conf:.2f}"
                    )
                    continue

                penalty_factor = 1.0
                if self.conflict_penalty_enabled and opposing_signals:
                    conflict_ratio = min(1.0, opposing_conf / max(dominant_conf, 1e-9))
                    penalty_factor = max(0.05, 1.0 - conflict_ratio * self.conflict_penalty_strength)

                fused = self._build_fused_signal(
                    symbol=symbol,
                    side=str(dominant_side),
                    signals=dominant_signals,
                    metadata={
                        'conflict_detected': True,
                        'dominant_confidence': dominant_conf,
                        'opposing_confidence': opposing_conf,
                        'conflict_penalty_factor': float(penalty_factor),
                        'opposing_strategies': [s.strategy for s in opposing_signals],
                        'opposing_scores': [float(s.score) for s in opposing_signals],
                        'opposing_confidences': [float(s.confidence) for s in opposing_signals],
                    },
                )
                fused.score = float(max(0.0, fused.score * penalty_factor))
                fused.confidence = float(max(0.0, min(fused.confidence * penalty_factor, 1.0)))
                fused.metadata["display_score"] = float(fused.score)
                fused_signals.append(fused)

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
    
    def set_run_id(self, run_id: Optional[str]) -> None:
        self.orchestrator.set_run_id(run_id)

    def strategy_signals_path(self) -> Optional[Path]:
        return self.orchestrator.strategy_signals_path()

    def latest_strategy_signal_payload(self) -> Dict[str, Any]:
        return self.orchestrator.latest_strategy_signal_payload()

    def _normalize_targets_to_total_capital(self, targets: List[Dict]) -> List[Dict]:
        positive_targets = [
            t for t in targets
            if float(t.get('target_position_usdt', 0.0) or 0.0) > 0.0
        ]
        if not positive_targets:
            return targets

        total_requested = sum(float(t.get('target_position_usdt', 0.0) or 0.0) for t in positive_targets)
        max_capital = float(self.orchestrator.total_capital)
        if total_requested <= 0.0 or total_requested <= max_capital + 1e-9:
            return targets

        scale = max_capital / total_requested
        for target in positive_targets:
            target['target_position_usdt'] = float(target.get('target_position_usdt', 0.0) or 0.0) * scale
        return targets

    def run_strategy_cycle(self, market_data: pd.DataFrame) -> List[Dict]:
        """运行一个策略周期，返回目标持仓"""

        # 生成融合信号
        signals = self.orchestrator.generate_combined_signals(market_data)

        # 转换为V5格式的目标持仓
        targets = []
        for signal in signals:
            position_size = Decimal('0')
            strategy_weight = Decimal('0')
            source_weight_sum = Decimal('0')

            # 常规单策略信号
            strategy = self.orchestrator.strategies.get(signal.strategy)
            if strategy is not None:
                strategy_weight = self.orchestrator.strategy_allocations.get(signal.strategy, Decimal('0.1'))
                source_weight_sum = strategy_weight
                capital = self.orchestrator.get_strategy_capital(signal.strategy)
                position_size = strategy.calculate_position_size(signal, capital)

            # 融合信号：使用“来源策略权重”驱动仓位大小
            elif signal.strategy == "FUSED":
                total_capital = self.orchestrator.total_capital

                source_names = signal.metadata.get('source_strategies', []) if signal.metadata else []
                source_allocs = {
                    name: self.orchestrator.strategy_allocations.get(name, Decimal('0'))
                    for name in source_names
                }
                source_weight_sum = sum(source_allocs.values(), Decimal('0'))

                # FUSED 权重：优先使用来源策略分配之和，缺失时退化到confidence
                if source_weight_sum > 0:
                    strategy_weight = source_weight_sum
                else:
                    strategy_weight = Decimal(str(max(0.0, min(signal.confidence, 1.0))))

                # 仓位比例与来源权重挂钩（让 3 策略权重真正生效）
                # 例如：来源权重和=0.55 -> 仓位约47.5%；和=0.20 -> 仓位约30%
                base_position_pct = Decimal('0.20')
                weight_scale = Decimal('0.50')
                max_position_pct = Decimal('0.55')

                position_pct = base_position_pct + (source_weight_sum * weight_scale)
                position_pct = min(position_pct, max_position_pct)

                confidence_factor = Decimal(str(signal.confidence))
                position_size = total_capital * position_pct * confidence_factor

                print(f"[MultiStrategyAdapter] FUSED信号 {signal.symbol}: "
                      f"来源={source_names}, 来源权重和={float(source_weight_sum):.2f}, "
                      f"总资金={total_capital}, 仓位比例={float(position_pct):.0%}, "
                      f"置信度={float(confidence_factor):.2f}, 计算仓位={float(position_size):.2f} USDT")

                # 兜底：来源缺失时使用总资金的小比例
                if position_size <= 0:
                    position_size = total_capital * Decimal(str(max(0.0, min(signal.confidence, 1.0)))) * Decimal('0.2')

            if position_size > 0:
                targets.append({
                    'symbol': signal.symbol,
                    'side': signal.side,
                    'target_position_usdt': float(position_size),
                    'signal_score': float(signal.score),
                    'raw_signal_score': float((signal.metadata or {}).get('raw_score', signal.score)),
                    'confidence': float(signal.confidence),
                    'source_strategy': signal.strategy,
                    'strategy_weight': float(strategy_weight),
                    'source_weight_sum': float(source_weight_sum),
                    'metadata': signal.metadata
                })

        return self._normalize_targets_to_total_capital(targets)


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
