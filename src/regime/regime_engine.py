from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path
import json

import numpy as np

from configs.schema import RegimeConfig, RegimeState
from src.core.models import MarketSeries


# 尝试导入HMM检测器（可选）
try:
    from src.regime.hmm_regime_detector import HMMRegimeDetector
    HMM_AVAILABLE = True
except ImportError:
    HMM_AVAILABLE = False


def _sma(xs: List[float], n: int) -> float:
    if len(xs) < n:
        return float(np.mean(xs)) if xs else 0.0
    return float(np.mean(np.array(xs[-n:], dtype=float)))


def _atr_pct(series: MarketSeries, n: int = 14) -> float:
    """ATR as percent of close."""
    if len(series.close) < n + 1:
        return 0.0
    h = np.array(series.high[-n:], dtype=float)
    l = np.array(series.low[-n:], dtype=float)
    c_prev = np.array(series.close[-n - 1 : -1], dtype=float)
    tr = np.maximum(h - l, np.maximum(np.abs(h - c_prev), np.abs(l - c_prev)))
    atr = float(np.mean(tr))
    last = float(series.close[-1])
    return atr / last if last else 0.0


@dataclass
class RegimeResult:
    state: RegimeState
    atr_pct: float
    ma20: float
    ma60: float
    multiplier: float
    # HMM新增字段
    hmm_state: Optional[str] = None
    hmm_probability: Optional[float] = None
    hmm_probs: Optional[dict] = None


class RegimeEngine:
    def __init__(self, cfg: RegimeConfig, use_hmm: bool = False):
        self.cfg = cfg
        self.use_hmm = use_hmm and HMM_AVAILABLE
        self.sentiment_cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/sentiment_cache')
        
        # 初始化HMM检测器
        self.hmm_detector = None
        if self.use_hmm:
            try:
                self.hmm_detector = HMMRegimeDetector(n_components=3)
                # 尝试加载预训练模型
                model_path = Path('/home/admin/clawd/v5-trading-bot/models/hmm_regime.pkl')
                if model_path.exists():
                    self.hmm_detector.model.load(model_path)
                    print("[RegimeEngine] HMM模型已加载")
                else:
                    print("[RegimeEngine] HMM模型未找到，将使用MA方法")
                    self.use_hmm = False
            except Exception as e:
                print(f"[RegimeEngine] HMM初始化失败: {e}")
                self.use_hmm = False
    
    def _load_market_sentiment(self) -> float:
        """读取市场情绪（-1~1），优先 BTC/ETH/SOL/BNB 的最新平均值。"""
        try:
            vals = []
            for sym in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']:
                data = None
                
                # 1. 优先尝试 RSS+DeepSeek（新闻情报）
                rss_files = sorted(self.sentiment_cache_dir.glob(f'rss_{sym}_*.json'))
                if rss_files:
                    data = json.loads(rss_files[-1].read_text())
                
                # 2. 尝试 funding_rate（资金费率，最实时）
                if data is None:
                    funding_files = sorted(self.sentiment_cache_dir.glob(f'funding_{sym}_*.json'))
                    if funding_files:
                        data = json.loads(funding_files[-1].read_text())
                
                # 3. 尝试 deepseek AI分析
                if data is None:
                    deepseek_files = sorted(self.sentiment_cache_dir.glob(f'deepseek_{sym}_*.json'))
                    if deepseek_files:
                        data = json.loads(deepseek_files[-1].read_text())
                
                # 4. 尝试其他格式
                if data is None:
                    other_files = sorted(self.sentiment_cache_dir.glob(f'{sym}_*.json'))
                    if other_files:
                        data = json.loads(other_files[-1].read_text())
                
                if data:
                    v = float(data.get('f6_sentiment', 0.0))
                    vals.append(max(-1.0, min(1.0, v)))
            
            if not vals:
                return 0.0
            return float(np.mean(vals))
        except Exception:
            return 0.0
    
    def _detect_hmm(self, btc_data: MarketSeries) -> Optional[RegimeResult]:
        """使用HMM检测市场状态"""
        if not self.use_hmm or self.hmm_detector is None:
            return None
        
        try:
            prices = np.array(btc_data.close)
            volumes = np.array(btc_data.volume) if hasattr(btc_data, 'volume') else None
            
            result = self.hmm_detector.detect_regime(prices, volumes)
            
            # 将HMM状态映射到RegimeState
            hmm_state = result['state']
            if hmm_state == 'TrendingUp':
                state = RegimeState.TRENDING
                mult = float(self.cfg.pos_mult_trending)
            elif hmm_state == 'TrendingDown':
                state = RegimeState.RISK_OFF  # 下跌趋势 = Risk-Off
                mult = float(self.cfg.pos_mult_risk_off)
            else:  # Sideways
                state = RegimeState.SIDEWAYS
                mult = float(self.cfg.pos_mult_sideways)
            
            ma20 = _sma(list(btc_data.close), 20)
            ma60 = _sma(list(btc_data.close), 60)
            atrp = _atr_pct(btc_data, 14)
            
            return RegimeResult(
                state=state,
                atr_pct=float(atrp),
                ma20=float(ma20),
                ma60=float(ma60),
                multiplier=float(mult),
                hmm_state=hmm_state,
                hmm_probability=result['probability'],
                hmm_probs=result['all_states']
            )
        except Exception as e:
            print(f"[RegimeEngine] HMM检测失败: {e}")
            return None
    
    def _detect_ma(self, btc_data: MarketSeries) -> RegimeResult:
        """使用传统MA方法检测市场状态"""
        closes = list(btc_data.close)
        ma20 = _sma(closes, 20)
        ma60 = _sma(closes, 60)
        atrp = _atr_pct(btc_data, 14)

        if ma20 > ma60 and atrp > float(self.cfg.atr_threshold):
            st = RegimeState.TRENDING
            mult = float(self.cfg.pos_mult_trending)
        elif atrp < float(self.cfg.atr_very_low):
            st = RegimeState.SIDEWAYS
            mult = float(self.cfg.pos_mult_sideways)
        else:
            st = RegimeState.RISK_OFF
            mult = float(self.cfg.pos_mult_risk_off)

        # 情绪驱动的 Risk-Off 修正
        if getattr(self.cfg, 'sentiment_regime_override_enabled', True):
            sent = self._load_market_sentiment()
            ma_gap = ((ma60 - ma20) / ma60) if ma60 else 1.0

            if st == RegimeState.RISK_OFF and sent >= float(self.cfg.sentiment_riskoff_relax_threshold) and ma_gap <= float(self.cfg.ma_gap_relax_threshold):
                st = RegimeState.SIDEWAYS
                mult = float(self.cfg.pos_mult_sideways)

            if sent <= float(self.cfg.sentiment_riskoff_harden_threshold):
                st = RegimeState.RISK_OFF
                mult = float(self.cfg.pos_mult_risk_off)

        return RegimeResult(
            state=st,
            atr_pct=float(atrp),
            ma20=float(ma20),
            ma60=float(ma60),
            multiplier=float(mult),
            hmm_state=None,
            hmm_probability=None,
            hmm_probs=None
        )

    def detect(self, btc_data: MarketSeries) -> RegimeResult:
        """
        检测市场状态
        
        优先使用HMM（如果可用且已训练），否则回退到MA方法
        """
        # 尝试HMM检测
        if self.use_hmm:
            hmm_result = self._detect_hmm(btc_data)
            if hmm_result is not None:
                return hmm_result
        
        # 回退到MA方法
        return self._detect_ma(btc_data)
