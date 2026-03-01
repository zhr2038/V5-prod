from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import numpy as np

from src.core.models import MarketSeries
from src.utils.math import safe_pct_change, zscore_cross_section
from configs.schema import AlphaConfig
from src.reporting.alpha_evaluation import robust_zscore_cross_section, compute_quote_volume

# 多策略集成
try:
    from src.strategy.multi_strategy_system import (
        StrategyOrchestrator,
        TrendFollowingStrategy,
        MeanReversionStrategy,
        MultiStrategyAdapter
    )
    MULTI_STRATEGY_AVAILABLE = True
except ImportError:
    MULTI_STRATEGY_AVAILABLE = False


def _rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    deltas = np.diff(np.array(closes[-(period + 1) :], dtype=float))
    gains = np.clip(deltas, 0, None)
    losses = -np.clip(deltas, None, 0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


@dataclass
class AlphaSnapshot:
    """Alpha因子快照
    
    包含原始因子、标准化因子和最终评分
    """
    raw_factors: Dict[str, Dict[str, float]]  # symbol -> factor -> value
    z_factors: Dict[str, Dict[str, float]]
    scores: Dict[str, float]


class AlphaEngine:
    """Alpha因子引擎
    
    计算多因子Alpha评分，支持：
    - 传统5因子Alpha (动量、波动率、成交量、RSI等)
    - 多策略模式 (趋势跟踪 + 均值回归 + 6因子Alpha)
    """
    
    def __init__(self, cfg: AlphaConfig):
        """初始化Alpha引擎
        
        Args:
            cfg: Alpha配置
        """
        self.cfg = cfg

        # 初始化多策略系统（如果启用）
        self.use_multi_strategy = getattr(cfg, 'use_multi_strategy', False)
        self.multi_strategy_adapter = None

        if self.use_multi_strategy and MULTI_STRATEGY_AVAILABLE:
            self._init_multi_strategy()

    def _init_multi_strategy(self):
        """初始化多策略系统 (趋势跟踪 + 均值回归 + 6因子Alpha)"""
        from decimal import Decimal
        from src.strategy.multi_strategy_system import Alpha6FactorStrategy

        # 从配置获取资金限制 - 修复：尝试多种方式获取 live_equity_cap_usdt
        total_capital = Decimal('20.0')  # 默认20 USDT

        # 尝试从 AlphaConfig 读取（旧方式，保持兼容）
        if hasattr(self.cfg, 'live_equity_cap_usdt') and self.cfg.live_equity_cap_usdt:
            total_capital = Decimal(str(self.cfg.live_equity_cap_usdt))
        # 尝试从 budget 配置读取（正确路径）
        elif hasattr(self.cfg, 'budget') and hasattr(self.cfg.budget, 'live_equity_cap_usdt'):
            if self.cfg.budget.live_equity_cap_usdt:
                total_capital = Decimal(str(self.cfg.budget.live_equity_cap_usdt))
        # 尝试从 account 配置读取（备选路径）
        elif hasattr(self.cfg, 'account') and hasattr(self.cfg.account, 'live_equity_cap_usdt'):
            if self.cfg.account.live_equity_cap_usdt:
                total_capital = Decimal(str(self.cfg.account.live_equity_cap_usdt))

        # 创建策略编排器
        orchestrator = StrategyOrchestrator(total_capital=total_capital)

        # 注册趋势跟踪策略 (15%资金，降低熊市噪声影响)
        trend_strategy = TrendFollowingStrategy(config={
            'fast_ma': 20,
            'slow_ma': 60,
            'adx_threshold': 28,
            'position_size_pct': 0.35,
            'trailing_stop': 0.04
        })
        orchestrator.register_strategy(trend_strategy, allocation=Decimal('0.15'))

        # 注册均值回归策略 (35%资金)
        mean_revert_strategy = MeanReversionStrategy(config={
            'rsi_period': 14,
            'rsi_oversold': 28,
            'rsi_overbought': 72,
            'bb_period': 20,
            'bb_std': 2,
            'position_size_pct': 0.25,
            'mean_rev_threshold': 0.025
        })
        orchestrator.register_strategy(mean_revert_strategy, allocation=Decimal('0.35'))

        # 注册6因子Alpha策略 (50%资金，主策略)
        alpha6_strategy = Alpha6FactorStrategy(config={
            'weights': {
                'f1_mom_5d': 0.15,
                'f2_mom_20d': 0.25,
                'f3_vol_adj_ret': 0.15,
                'f4_volume_expansion': 0.15,
                'f5_rsi_trend_confirm': 0.15,
                'f6_sentiment': 0.15
            },
            'position_size_pct': 0.30,
            'score_threshold': 0.10
        })
        orchestrator.register_strategy(alpha6_strategy, allocation=Decimal('0.50'))

        # 创建适配器
        self.multi_strategy_adapter = MultiStrategyAdapter(orchestrator)
        print(f"[AlphaEngine] 多策略融合已启用:")
        print(f"              - 趋势跟踪: 15%")
        print(f"              - 均值回归: 35%")
        print(f"              - 6因子Alpha: 50%")

    def compute_scores(self, market_data: Dict[str, MarketSeries]) -> Dict[str, float]:
        """计算Alpha评分
        
        Args:
            market_data: 市场数据 {symbol: MarketSeries}
            
        Returns:
            评分字典 {symbol: score}
        """
        # 如果使用多策略，返回多策略信号
        if self.use_multi_strategy and self.multi_strategy_adapter:
            return self._compute_multi_strategy_scores(market_data)

        # 否则使用原有的6因子Alpha
        snap = self.compute_snapshot(market_data)
        return snap.scores

    def _compute_multi_strategy_scores(self, market_data: Dict[str, MarketSeries]) -> Dict[str, float]:
        """
        使用多策略系统计算评分
        """
        import pandas as pd
        from datetime import datetime

        # 将 MarketSeries 转换为 DataFrame
        all_data = []
        for sym, series in market_data.items():
            if len(series.close) < 25:
                continue

            # 构建DataFrame
            df = pd.DataFrame({
                'symbol': sym,
                'close': list(series.close),
                'high': list(series.high) if hasattr(series, 'high') else list(series.close),
                'low': list(series.low) if hasattr(series, 'low') else list(series.close),
                'volume': list(series.volume) if hasattr(series, 'volume') else [0] * len(series.close)
            })
            all_data.append(df)

        if not all_data:
            return {}

        market_df = pd.concat(all_data, ignore_index=True)

        # 运行多策略
        targets = self.multi_strategy_adapter.run_strategy_cycle(market_df)
        
        # 转换为评分格式 (0-1之间的分数)
        # 同一symbol可能出现多个信号（多策略/多阶段），使用加权平均而非简单覆盖
        from collections import defaultdict
        
        symbol_signals = defaultdict(list)  # symbol -> [(score, weight, side), ...]
        
        for target in targets:
            sym = target['symbol'].replace('-', '/')
            # 买入信号为正分，卖出为负分
            score = float(target['signal_score']) * float(target['confidence'])
            if target['side'] == 'sell':
                score = -score
            
            # 获取策略权重（如果可用），否则默认使用confidence作为权重
            strategy_weight = float(target.get('strategy_weight', target['confidence']))
            symbol_signals[sym].append({
                'score': score,
                'weight': strategy_weight,
                'side': target['side']
            })
        
        # 加权平均合并同symbol的多个信号
        # 取反：IC为负，反向使用因子
        scores = {}
        for sym, signals in symbol_signals.items():
            if len(signals) == 1:
                scores[sym] = -signals[0]['score']  # 取反
            else:
                # 分离买入和卖出信号
                buy_signals = [s for s in signals if s['side'] == 'buy']
                sell_signals = [s for s in signals if s['side'] == 'sell']
                
                # 计算加权平均
                if buy_signals and sell_signals:
                    # 有冲突信号时，按权重加权平均
                    total_weight = sum(s['weight'] for s in signals)
                    weighted_score = sum(s['score'] * s['weight'] for s in signals) / total_weight
                    scores[sym] = -weighted_score  # 取反
                elif buy_signals:
                    # 只有买入信号
                    total_weight = sum(s['weight'] for s in buy_signals)
                    weighted_score = sum(s['score'] * s['weight'] for s in buy_signals) / total_weight
                    scores[sym] = -weighted_score  # 取反
                else:
                    # 只有卖出信号
                    total_weight = sum(s['weight'] for s in sell_signals)
                    weighted_score = sum(s['score'] * s['weight'] for s in sell_signals) / total_weight
                    scores[sym] = -weighted_score  # 取反
        
        return scores

    def compute_snapshot(self, market_data: Dict[str, MarketSeries], use_robust_zscore: bool = True) -> AlphaSnapshot:
        """计算完整的Alpha快照（包含原始因子、标准化因子和评分）
        
        Args:
            market_data: 市场数据 {symbol: MarketSeries}
            use_robust_zscore: 是否使用稳健标准化（去极值）
            
        Returns:
            Alpha快照
        """
        # 如果使用多策略，直接返回多策略结果
        if self.use_multi_strategy and self.multi_strategy_adapter:
            scores = self._compute_multi_strategy_scores(market_data)
            # 构建简化的AlphaSnapshot（多策略模式下部分字段为空）
            return AlphaSnapshot(
                raw_factors={},
                z_factors={},
                scores=scores
            )

        # 否则使用传统的5因子Alpha计算
        # Compute raw factors
        f1: Dict[str, float] = {}
        f2: Dict[str, float] = {}
        f3: Dict[str, float] = {}
        f4: Dict[str, float] = {}
        f5: Dict[str, float] = {}

        for sym, s in (market_data or {}).items():
            c = list(s.close)
            v = list(s.volume)
            if len(c) < 25:
                continue

            # 5d momentum and 20d momentum: on 1h data, treat 24 bars/day
            # For this scaffold: assume 1h bars and approximate.
            mom_5d = safe_pct_change(c[-1 - 24 * 5], c[-1]) if len(c) > 24 * 5 else safe_pct_change(c[0], c[-1])
            mom_20d = safe_pct_change(c[-1 - 24 * 20], c[-1]) if len(c) > 24 * 20 else safe_pct_change(c[0], c[-1])

            # 20d vol-adjusted return: mom_20d / vol_20d
            rets = np.diff(np.array(c[-(24 * 20 + 1) :], dtype=float)) / np.array(c[-(24 * 20 + 1) : -1], dtype=float)
            vol = float(np.std(rets)) if len(rets) > 10 else 0.0
            vol_adj = mom_20d / (vol + 1e-12)

            # volume expansion: last 24h QUOTE volume vs prev 7d average daily QUOTE volume
            # Quote volume = volume * close (USDT value)
            if len(v) >= 24 and len(c) >= 24:
                # 计算最近24小时的quote volume
                vol_1d = compute_quote_volume(v[-24:], c[-24:])

                # 计算过去7天的平均daily quote volume
                daily_quote = []
                if len(v) >= 24 * 8 and len(c) >= 24 * 8:
                    for k in range(1, 8):
                        start = -24 * (k + 1)
                        end = -24 * k
                        daily_quote.append(compute_quote_volume(v[start:end], c[start:end]))

                avg_7d = float(np.mean(daily_quote)) if daily_quote else vol_1d
                vol_exp = (vol_1d / (avg_7d + 1e-12)) - 1.0
            else:
                vol_exp = 0.0

            # RSI trend confirm: RSI - 50 (positive is bullish)
            rsi = _rsi(c, 14)
            rsi_trend = (rsi - 50.0) / 50.0

            f1[sym] = float(mom_5d)
            f2[sym] = float(mom_20d)
            f3[sym] = float(vol_adj)
            f4[sym] = float(vol_exp)
            f5[sym] = float(rsi_trend)

        # 使用稳健的z-score或标准z-score
        if use_robust_zscore:
            z1 = robust_zscore_cross_section(f1, winsorize_pct=0.05)
            z2 = robust_zscore_cross_section(f2, winsorize_pct=0.05)
            z3 = robust_zscore_cross_section(f3, winsorize_pct=0.05)
            z4 = robust_zscore_cross_section(f4, winsorize_pct=0.05)
            z5 = robust_zscore_cross_section(f5, winsorize_pct=0.05)
        else:
            z1 = zscore_cross_section(f1)
            z2 = zscore_cross_section(f2)
            z3 = zscore_cross_section(f3)
            z4 = zscore_cross_section(f4)
            z5 = zscore_cross_section(f5)

        raw_factors: Dict[str, Dict[str, float]] = {}
        z_factors: Dict[str, Dict[str, float]] = {}
        scores: Dict[str, float] = {}

        w = self.cfg.weights
        for sym in z1.keys():
            raw_factors[sym] = {
                "f1_mom_5d": f1.get(sym, 0.0),
                "f2_mom_20d": f2.get(sym, 0.0),
                "f3_vol_adj_ret_20d": f3.get(sym, 0.0),
                "f4_volume_expansion": f4.get(sym, 0.0),
                "f5_rsi_trend_confirm": f5.get(sym, 0.0),
            }
            z_factors[sym] = {
                "f1_mom_5d": z1.get(sym, 0.0),
                "f2_mom_20d": z2.get(sym, 0.0),
                "f3_vol_adj_ret_20d": z3.get(sym, 0.0),
                "f4_volume_expansion": z4.get(sym, 0.0),
                "f5_rsi_trend_confirm": z5.get(sym, 0.0),
            }
            # 原始因子评分（IC为负，因子与未来收益反向）
            raw_score = (
                w.f1_mom_5d * z1.get(sym, 0.0)
                + w.f2_mom_20d * z2.get(sym, 0.0)
                + w.f3_vol_adj_ret_20d * z3.get(sym, 0.0)
                + w.f4_volume_expansion * z4.get(sym, 0.0)
                + w.f5_rsi_trend_confirm * z5.get(sym, 0.0)
            )
            # 反向使用因子：IC为负，取反后高分=买入低因子值币
            scores[sym] = float(-raw_score)

        return AlphaSnapshot(raw_factors=raw_factors, z_factors=z_factors, scores=scores)
