from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict
from pathlib import Path
import json
import numpy as np

from configs.schema import RegimeConfig, RegimeState
from src.core.models import MarketSeries

# 导入HMM检测器
try:
    from src.regime.hmm_regime_detector import HMMRegimeDetector
    HMM_AVAILABLE = True
except ImportError:
    HMM_AVAILABLE = False


@dataclass
class RegimeResult:
    """RegimeResult类"""
    state: RegimeState
    atr_pct: float
    ma20: float
    ma60: float
    multiplier: float
    # 详细投票信息
    votes: Dict[str, dict] = None
    final_score: float = 0.0


class EnsembleRegimeEngine:
    """
    多方法 ensemble 市场状态检测器
    
    集成三种方法：
    1. HMM (数据驱动) - 权重: hmm_weight
    2. 资金费率情绪 (实时) - 权重: funding_weight  
    3. RSS新闻情绪 (深度) - 权重: rss_weight
    
    加权投票决定最终状态
    """
    
    def __init__(self, cfg: RegimeConfig):
        self.cfg = cfg
        self.sentiment_cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/sentiment_cache')
        
        # 权重配置（可调整）
        self.weights = {
            'hmm': getattr(cfg, 'hmm_weight', 0.40),      # HMM: 40%
            'funding': getattr(cfg, 'funding_weight', 0.35),  # 资金费率: 35%
            'rss': getattr(cfg, 'rss_weight', 0.25)       # RSS: 25%
        }
        
        # 初始化HMM检测器
        self.hmm_detector = None
        if HMM_AVAILABLE:
            try:
                # 尝试加载GMM模型（新的训练方式）
                model_path = Path('/home/admin/clawd/v5-trading-bot/models/hmm_regime.pkl')
                if model_path.exists():
                    import pickle
                    with open(model_path, 'rb') as f:
                        model = pickle.load(f)
                    # 检查模型类型
                    if type(model).__name__ == 'GaussianMixture':
                        # 使用GMM包装器
                        from dataclasses import dataclass
                        @dataclass
                        class GMMWrapper:
                            gmm: any
                            def predict(self, features):
                                # GMM predict返回状态ID
                                state_id = self.gmm.predict(features[-1:])[0]  # 取最后一个
                                # 获取概率
                                probs = self.gmm.predict_proba(features[-1:])[0]
                                # 映射状态
                                state_map = {0: 'Sideways', 1: 'TrendingUp', 2: 'TrendingDown'}
                                return {
                                    'state': state_map.get(state_id, 'Sideways'),
                                    'probability': float(max(probs)),
                                    'all_states': {
                                        'Sideways': float(probs[0]),
                                        'TrendingUp': float(probs[1]), 
                                        'TrendingDown': float(probs[2])
                                    }
                                }
                        self.hmm_detector = GMMWrapper(gmm=model)
                        print(f"[EnsembleRegime] GMM模型已加载")
                    else:
                        # 传统HMM方式
                        from src.regime.hmm_regime_detector import HMMRegimeDetector
                        self.hmm_detector = HMMRegimeDetector(n_components=3)
                        self.hmm_detector.model.load(model_path)
                        print(f"[EnsembleRegime] HMM模型已加载")
            except Exception as e:
                print(f"[EnsembleRegime] HMM/GMM初始化失败: {e}")
    
    def _get_hmm_vote(self, btc_data: MarketSeries) -> dict:
        """HMM投票"""
        if self.hmm_detector is None:
            return {'state': None, 'confidence': 0, 'weight': 0}
        
        try:
            from src.regime.hmm_regime_detector import HMMRegimeDetector
            
            # 构造特征
            features = []
            closes = list(btc_data.close)
            for i in range(len(closes)):
                if i < 14:
                    continue
                ret_1h = (closes[i] - closes[i-1]) / closes[i-1] if closes[i-1] > 0 else 0
                ret_6h = (closes[i] - closes[max(0,i-6)]) / closes[max(0,i-6)] if closes[max(0,i-6)] > 0 else 0
                
                window = closes[max(0,i-14):i+1]
                vol = np.std(np.diff(window) / window[:-1]) if len(window) > 1 else 0
                
                gains = [closes[j]-closes[j-1] for j in range(max(0,i-14), i+1) if closes[j] > closes[j-1]]
                losses = [closes[j-1]-closes[j] for j in range(max(0,i-14), i+1) if closes[j] < closes[j-1]]
                avg_gain = np.mean(gains) if gains else 0
                avg_loss = np.mean(losses) if losses else 0.001
                rsi = 100 - (100 / (1 + avg_gain/avg_loss))
                
                features.append([ret_1h, ret_6h, vol, rsi])
            
            if len(features) < 10:
                return {'state': None, 'confidence': 0, 'weight': 0}
            
            result = self.hmm_detector.predict(np.array(features))
            
            # 映射HMM状态到RegimeState
            hmm_state = result['state']
            state_map = {
                'TrendingUp': 'TRENDING',
                'TrendingDown': 'RISK_OFF',
                'Sideways': 'SIDEWAYS'
            }
            
            # 从状态概率计算情绪分数
            # 正确口径：sentiment = P(TrendingUp) - P(TrendingDown)，天然在[-1, 1]
            all_states = result.get('all_states', {})
            sentiment = all_states.get('TrendingUp', 0) - all_states.get('TrendingDown', 0)
            sentiment = max(-1.0, min(1.0, float(sentiment)))
            
            return {
                'state': state_map.get(hmm_state, 'SIDEWAYS'),
                'confidence': result['probability'],
                'weight': self.weights['hmm'],
                'sentiment': sentiment,
                'raw_state': hmm_state,
                'probs': all_states
            }
        except Exception as e:
            print(f"[EnsembleRegime] HMM投票失败: {e}")
            return {'state': None, 'confidence': 0, 'weight': 0}
    
    def _get_funding_vote(self) -> dict:
        """资金费率投票（使用综合情绪）"""
        try:
            # 优先读取综合资金费率情绪文件
            composite_files = sorted(self.sentiment_cache_dir.glob('funding_COMPOSITE_*.json'))
            
            if composite_files:
                data = json.loads(composite_files[-1].read_text())
                sentiment = float(data.get('f6_sentiment', 0.0))
                
                # 映射到状态
                if sentiment > 0.3:
                    state = 'TRENDING'
                elif sentiment < -0.3:
                    state = 'RISK_OFF'
                else:
                    state = 'SIDEWAYS'
                
                confidence = min(abs(sentiment) * 2, 1.0)
                
                return {
                    'state': state,
                    'confidence': confidence,
                    'weight': self.weights['funding'],
                    'sentiment': sentiment,
                    'composite': True,
                    'details': data.get('tier_breakdown', {})
                }
            
            # 回退：读取旧格式（单个币种）
            vals = []
            for sym in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']:
                files = sorted(self.sentiment_cache_dir.glob(f'funding_{sym}_*.json'))
                if files:
                    data = json.loads(files[-1].read_text())
                    v = float(data.get('f6_sentiment', 0.0))
                    vals.append(max(-1.0, min(1.0, v)))
            
            if not vals:
                return {'state': None, 'confidence': 0, 'weight': 0}
            
            avg_sentiment = np.mean(vals)
            
            if avg_sentiment > 0.3:
                state = 'TRENDING'
            elif avg_sentiment < -0.3:
                state = 'RISK_OFF'
            else:
                state = 'SIDEWAYS'
            
            confidence = min(abs(avg_sentiment) * 2, 1.0)
            
            return {
                'state': state,
                'confidence': confidence,
                'weight': self.weights['funding'],
                'sentiment': avg_sentiment,
                'composite': False,
                'details': {sym: v for sym, v in zip(['BTC','ETH','SOL','BNB'], vals)}
            }
        except Exception as e:
            print(f"[EnsembleRegime] 资金费率投票失败: {e}")
            return {'state': None, 'confidence': 0, 'weight': 0}
    
    def _get_rss_vote(self) -> dict:
        """RSS新闻投票"""
        try:
            # 读取RSS市场情绪
            rss_files = sorted(self.sentiment_cache_dir.glob('rss_MARKET_*.json'))
            if not rss_files:
                # 尝试币种特定文件
                rss_files = sorted(self.sentiment_cache_dir.glob('rss_BTC-USDT_*.json'))
            
            if not rss_files:
                return {'state': None, 'confidence': 0, 'weight': 0}
            
            data = json.loads(rss_files[-1].read_text())
            sentiment = float(data.get('f6_sentiment', 0.0))
            
            # 新闻情绪映射到状态
            # > 0.5: 极度乐观 = TRENDING
            # 0.2 ~ 0.5: 谨慎乐观 = TRENDING (但权重低)
            # -0.2 ~ 0.2: 中性 = SIDEWAYS
            # < -0.2: 悲观 = RISK_OFF
            if sentiment > 0.3:
                state = 'TRENDING'
            elif sentiment < -0.2:
                state = 'RISK_OFF'
            else:
                state = 'SIDEWAYS'
            
            # 置信度
            confidence = min(abs(sentiment) * 1.5 + 0.3, 1.0)
            
            return {
                'state': state,
                'confidence': confidence,
                'weight': self.weights['rss'],
                'sentiment': sentiment,
                'summary': data.get('f6_sentiment_summary', '')[:100]
            }
        except Exception as e:
            print(f"[EnsembleRegime] RSS投票失败: {e}")
            return {'state': None, 'confidence': 0, 'weight': 0}
    
    def _weighted_vote(self, votes: List[dict]) -> tuple:
        """
        加权投票决定最终状态
        
        返回: (state, confidence, score)
        """
        # 统计各状态加权得分
        state_scores = {'TRENDING': 0, 'SIDEWAYS': 0, 'RISK_OFF': 0}
        total_weight = 0
        
        for vote in votes:
            if vote['state'] is None:
                continue
            # 加权: 方法权重 × 置信度
            weighted_score = vote['weight'] * vote['confidence']
            state_scores[vote['state']] += weighted_score
            total_weight += vote['weight']
        
        if total_weight == 0:
            return 'SIDEWAYS', 0.5, 0
        
        # 归一化
        for state in state_scores:
            state_scores[state] /= total_weight
        
        # 选择最高分状态
        final_state = max(state_scores, key=state_scores.get)
        final_score = state_scores[final_state]
        
        # 置信度 = 领先程度 (最高分 - 次高分)
        sorted_scores = sorted(state_scores.values(), reverse=True)
        confidence = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else sorted_scores[0]
        
        return final_state, confidence, final_score
    
    def detect(self, btc_data: MarketSeries) -> RegimeResult:
        """
        Ensemble检测市场状态
        
        流程:
        1. 三种方法各自投票
        2. 加权计算各状态得分
        3. 选择最高分的作为最终状态
        """
        # 1. 收集投票
        hmm_vote = self._get_hmm_vote(btc_data)
        funding_vote = self._get_funding_vote()
        rss_vote = self._get_rss_vote()
        
        votes = {
            'hmm': hmm_vote,
            'funding': funding_vote,
            'rss': rss_vote
        }
        
        # 2. 加权投票
        final_state_str, confidence, score = self._weighted_vote([
            hmm_vote, funding_vote, rss_vote
        ])
        
        # 3. 映射到RegimeState
        state_map = {
            'TRENDING': RegimeState.TRENDING,
            'SIDEWAYS': RegimeState.SIDEWAYS,
            'RISK_OFF': RegimeState.RISK_OFF
        }
        final_state = state_map.get(final_state_str, RegimeState.SIDEWAYS)
        
        # 4. 计算倍数
        mult_map = {
            RegimeState.TRENDING: float(self.cfg.pos_mult_trending),
            RegimeState.SIDEWAYS: float(self.cfg.pos_mult_sideways),
            RegimeState.RISK_OFF: float(self.cfg.pos_mult_risk_off)
        }
        multiplier = mult_map.get(final_state, 0.6)
        
        # 5. 计算MA（用于输出兼容）
        closes = list(btc_data.close)
        ma20 = np.mean(closes[-20:]) if len(closes) >= 20 else np.mean(closes)
        ma60 = np.mean(closes[-60:]) if len(closes) >= 60 else np.mean(closes)
        
        # 计算ATR%
        if len(closes) > 14:
            recent = closes[-15:]
            returns = [(recent[i] - recent[i-1]) / recent[i-1] for i in range(1, len(recent))]
            atrp = np.std(returns)
        else:
            atrp = 0.01
        
        # 6. 根据confidence调整multiplier（可选）
        # 高置信度可以略微提高倍数，低置信度降低倍数
        confidence_adjustment = (confidence - 0.5) * 0.2  # -0.1 ~ +0.1
        multiplier = max(0, min(1.5, multiplier * (1 + confidence_adjustment)))
        
        return RegimeResult(
            state=final_state,
            atr_pct=float(atrp),
            ma20=float(ma20),
            ma60=float(ma60),
            multiplier=float(multiplier),
            votes=votes,
            final_score=float(score)
        )
