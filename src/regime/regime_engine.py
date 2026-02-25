from __future__ import annotations

from dataclasses import dataclass
from typing import List
from pathlib import Path
import json

import numpy as np

from configs.schema import RegimeConfig, RegimeState
from src.core.models import MarketSeries


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


class RegimeEngine:
    def __init__(self, cfg: RegimeConfig):
        self.cfg = cfg
        self.sentiment_cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/sentiment_cache')

    def _load_market_sentiment(self) -> float:
        """读取市场情绪（-1~1），优先 BTC/ETH/SOL/BNB 的最新平均值。
        
        支持多种数据源：
        1. funding_rate（资金费率，实时）
        2. deepseek（AI分析）
        3. 其他缓存文件
        """
        try:
            vals = []
            for sym in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']:
                data = None
                
                # 1. 优先尝试 funding_rate（资金费率，最实时）
                funding_files = sorted(self.sentiment_cache_dir.glob(f'funding_{sym}_*.json'))
                if funding_files:
                    data = json.loads(funding_files[-1].read_text())
                
                # 2. 尝试 deepseek AI分析
                if data is None:
                    deepseek_files = sorted(self.sentiment_cache_dir.glob(f'deepseek_{sym}_*.json'))
                    if deepseek_files:
                        data = json.loads(deepseek_files[-1].read_text())
                
                # 3. 尝试其他格式
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

    def detect(self, btc_data: MarketSeries) -> RegimeResult:
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

        # 情绪驱动的 Risk-Off 修正：
        # - 强乐观且MA20/MA60缺口不大 -> 放松到 Sideways
        # - 强悲观 -> 强化 Risk-Off
        if getattr(self.cfg, 'sentiment_regime_override_enabled', True):
            sent = self._load_market_sentiment()
            ma_gap = ((ma60 - ma20) / ma60) if ma60 else 1.0

            if st == RegimeState.RISK_OFF and sent >= float(self.cfg.sentiment_riskoff_relax_threshold) and ma_gap <= float(self.cfg.ma_gap_relax_threshold):
                st = RegimeState.SIDEWAYS
                mult = float(self.cfg.pos_mult_sideways)

            if sent <= float(self.cfg.sentiment_riskoff_harden_threshold):
                st = RegimeState.RISK_OFF
                mult = float(self.cfg.pos_mult_risk_off)

        return RegimeResult(state=st, atr_pct=float(atrp), ma20=float(ma20), ma60=float(ma60), multiplier=float(mult))
