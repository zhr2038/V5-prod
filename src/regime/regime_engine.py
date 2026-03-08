from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import numpy as np

from configs.schema import RegimeConfig, RegimeState
from src.core.models import MarketSeries

try:
    from src.regime.hmm_regime_detector import HMMRegimeDetector

    HMM_AVAILABLE = True
except ImportError:
    HMMRegimeDetector = None
    HMM_AVAILABLE = False


def _sma(xs: List[float], n: int) -> float:
    if len(xs) < n:
        return float(np.mean(xs)) if xs else 0.0
    return float(np.mean(np.array(xs[-n:], dtype=float)))


def _atr_pct(series: MarketSeries, n: int = 14) -> float:
    """ATR as a fraction of the latest close."""
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
    hmm_state: Optional[str] = None
    hmm_probability: Optional[float] = None
    hmm_probs: Optional[dict] = None


class RegimeEngine:
    """Detect market regime from BTC market data."""

    def __init__(self, cfg: RegimeConfig, use_hmm: bool = False):
        self.cfg = cfg
        self.use_hmm = bool(use_hmm and HMM_AVAILABLE)
        self.repo_root = Path(__file__).resolve().parents[2]
        self.sentiment_cache_dir = self.repo_root / "data" / "sentiment_cache"
        self.hmm_detector = None

        if self.use_hmm:
            try:
                self.hmm_detector = HMMRegimeDetector(n_components=3)
                model_path = self.repo_root / "models" / "hmm_regime.pkl"
                if model_path.exists():
                    self.hmm_detector.model.load(model_path)
                else:
                    print("[RegimeEngine] HMM model not found, fallback to MA+ATR")
                    self.use_hmm = False
                    self.hmm_detector = None
            except Exception as e:
                print(f"[RegimeEngine] HMM init failed: {e}")
                self.use_hmm = False
                self.hmm_detector = None

    def _load_market_sentiment(self) -> float:
        """Load the latest cached sentiment snapshot and average core majors."""
        try:
            vals = []
            if not self.sentiment_cache_dir.exists():
                return 0.0

            for sym in ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT"]:
                data = None
                patterns = [
                    f"rss_{sym}_*.json",
                    f"funding_{sym}_*.json",
                    f"deepseek_{sym}_*.json",
                    f"{sym}_*.json",
                ]
                for pattern in patterns:
                    files = sorted(self.sentiment_cache_dir.glob(pattern))
                    if files:
                        data = json.loads(files[-1].read_text(encoding="utf-8"))
                        break

                if data:
                    v = float(data.get("f6_sentiment", 0.0))
                    vals.append(max(-1.0, min(1.0, v)))

            if not vals:
                return 0.0
            return float(np.mean(vals))
        except Exception:
            return 0.0

    def _detect_hmm(self, btc_data: MarketSeries) -> Optional[RegimeResult]:
        if not self.use_hmm or self.hmm_detector is None:
            return None

        try:
            closes = list(btc_data.close)
            features = []
            for i in range(len(closes)):
                if i < 14:
                    continue

                prev_close = closes[i - 1]
                ret_1h = (closes[i] - prev_close) / prev_close if prev_close > 0 else 0.0

                lag_idx = max(0, i - 6)
                lag_close = closes[lag_idx]
                ret_6h = (closes[i] - lag_close) / lag_close if lag_close > 0 else 0.0

                window = closes[max(0, i - 14) : i + 1]
                vol = np.std(np.diff(window) / np.array(window[:-1], dtype=float)) if len(window) > 1 else 0.0

                gains = [
                    closes[j] - closes[j - 1]
                    for j in range(max(1, i - 14), i + 1)
                    if closes[j] > closes[j - 1]
                ]
                losses = [
                    closes[j - 1] - closes[j]
                    for j in range(max(1, i - 14), i + 1)
                    if closes[j] < closes[j - 1]
                ]
                avg_gain = np.mean(gains) if gains else 0.0
                avg_loss = np.mean(losses) if losses else 0.001
                rsi = 100 - (100 / (1 + avg_gain / avg_loss))

                features.append([ret_1h, ret_6h, vol, rsi])

            if len(features) < 10:
                return None

            result = self.hmm_detector.predict(np.array(features, dtype=float))
            hmm_state = result["state"]

            if hmm_state == "TrendingUp":
                state = RegimeState.TRENDING
                mult = float(self.cfg.pos_mult_trending)
            elif hmm_state == "TrendingDown":
                state = RegimeState.RISK_OFF
                mult = float(self.cfg.pos_mult_risk_off)
            else:
                state = RegimeState.SIDEWAYS
                mult = float(self.cfg.pos_mult_sideways)

            ma20 = _sma(closes, 20)
            ma60 = _sma(closes, 60)
            atrp = _atr_pct(btc_data, 14)

            return RegimeResult(
                state=state,
                atr_pct=float(atrp),
                ma20=float(ma20),
                ma60=float(ma60),
                multiplier=float(mult),
                hmm_state=hmm_state,
                hmm_probability=result.get("probability"),
                hmm_probs=result.get("all_states"),
            )
        except Exception as e:
            print(f"[RegimeEngine] HMM detect failed: {e}")
            return None

    def _detect_ma(self, btc_data: MarketSeries) -> RegimeResult:
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

        if getattr(self.cfg, "sentiment_regime_override_enabled", True):
            sent = self._load_market_sentiment()
            ma_gap = ((ma60 - ma20) / ma60) if ma60 else 1.0

            if (
                st == RegimeState.RISK_OFF
                and sent >= float(self.cfg.sentiment_riskoff_relax_threshold)
                and ma_gap <= float(self.cfg.ma_gap_relax_threshold)
            ):
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
            hmm_probs=None,
        )

    def detect(self, btc_data: MarketSeries) -> RegimeResult:
        if self.use_hmm:
            hmm_result = self._detect_hmm(btc_data)
            if hmm_result is not None:
                return hmm_result
            print("[RegimeEngine] HMM unavailable, fallback to MA+ATR")

        return self._detect_ma(btc_data)
