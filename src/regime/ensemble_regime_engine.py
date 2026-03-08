from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict
from pathlib import Path
import json
import sqlite3
import time
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
    alerts: List[str] = None


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
        self.project_root = Path(__file__).resolve().parents[2]
        self.sentiment_cache_dir = self.project_root / 'data' / 'sentiment_cache'
        self.funding_signal_max_age_minutes = int(getattr(cfg, 'funding_signal_max_age_minutes', 180))
        self.rss_signal_max_age_minutes = int(getattr(cfg, 'rss_signal_max_age_minutes', 180))

        # 权重配置（可调整）
        self.weights = {
            'hmm': getattr(cfg, 'hmm_weight', 0.40),      # HMM: 40%
            'funding': getattr(cfg, 'funding_weight', 0.35),  # 资金费率: 35%
            'rss': getattr(cfg, 'rss_weight', 0.25)       # RSS: 25%
        }

        # 运行期健康监控（学习记录补齐项）
        self.monitor_enabled = bool(getattr(cfg, 'regime_monitor_enabled', True))
        db_path_raw = str(getattr(cfg, 'regime_history_db_path', 'reports/regime_history.db'))
        db_path = Path(db_path_raw)
        if not db_path.is_absolute():
            db_path = self.project_root / db_path
        self.regime_history_db = db_path
        self.monitor_sideways_prob_warn_threshold = float(
            getattr(cfg, 'regime_sideways_prob_warn_threshold', 0.8)
        )
        self.monitor_sideways_consecutive_warn = int(
            getattr(cfg, 'regime_sideways_consecutive_warn', 10)
        )
        self.monitor_keep_rows = int(getattr(cfg, 'regime_monitor_keep_rows', 5000))

        self.startup_alerts: List[str] = []
        self.last_runtime_alerts: List[str] = []
        self._model_type_mismatch = False

        if self.monitor_enabled:
            self._ensure_history_schema()

        # 初始化HMM检测器
        self.hmm_detector = None
        if HMM_AVAILABLE:
            try:
                model_path = self.project_root / 'models' / 'hmm_regime.pkl'
                info_path = self.project_root / 'models' / 'hmm_regime_info.json'
                expected_model_class = None
                if info_path.exists():
                    try:
                        info = json.loads(info_path.read_text(encoding='utf-8'))
                        expected_model_class = info.get('model_class')
                    except Exception:
                        pass

                if model_path.exists():
                    import pickle

                    with open(model_path, 'rb') as f:
                        model = pickle.load(f)

                    model_class = type(model).__name__
                    if expected_model_class:
                        expected = str(expected_model_class)
                        compatible = (
                            expected == model_class
                            or (expected in {'SimpleGaussianHMM', 'HMMRegimeDetector', 'SimpleGaussianHMMPayload'} and model_class == 'dict')
                        )
                        if not compatible:
                            self._model_type_mismatch = True
                            msg = f"model_type_mismatch(expected={expected_model_class}, actual={model_class})"
                            self.startup_alerts.append(msg)
                            print(f"[EnsembleRegime] ⚠️ {msg}")

                    if model_class == 'GaussianMixture':
                        class GMMWrapper:
                            def __init__(self, gmm):
                                self.gmm = gmm

                            def predict(self, features):
                                state_id = self.gmm.predict(features[-1:])[0]
                                probs = self.gmm.predict_proba(features[-1:])[0]
                                state_map = {0: 'Sideways', 1: 'TrendingUp', 2: 'TrendingDown'}
                                return {
                                    'state': state_map.get(state_id, 'Sideways'),
                                    'probability': float(max(probs)),
                                    'all_states': {
                                        'Sideways': float(probs[0]),
                                        'TrendingUp': float(probs[1]),
                                        'TrendingDown': float(probs[2]),
                                    },
                                }

                        self.hmm_detector = GMMWrapper(gmm=model)
                        print("[EnsembleRegime] GMM模型已加载")
                    else:
                        from src.regime.hmm_regime_detector import HMMRegimeDetector

                        self.hmm_detector = HMMRegimeDetector(n_components=3)
                        self.hmm_detector.model.load(model_path)
                        print("[EnsembleRegime] HMM模型已加载")
                else:
                    self.startup_alerts.append('hmm_model_missing')
            except Exception as e:
                self.startup_alerts.append('hmm_init_failed')
                print(f"[EnsembleRegime] HMM/GMM初始化失败: {e}")
    
    def _ensure_history_schema(self) -> None:
        """Create regime history DB schema (best-effort)."""
        try:
            self.regime_history_db.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.regime_history_db))
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS regime_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    final_state TEXT NOT NULL,
                    final_score REAL,
                    confidence REAL,
                    multiplier REAL,
                    hmm_state TEXT,
                    hmm_confidence REAL,
                    hmm_trending_up_prob REAL,
                    hmm_trending_down_prob REAL,
                    hmm_sideways_prob REAL,
                    funding_state TEXT,
                    funding_confidence REAL,
                    funding_sentiment REAL,
                    rss_state TEXT,
                    rss_confidence REAL,
                    rss_sentiment REAL,
                    alerts_json TEXT,
                    weights_json TEXT
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_regime_history_ts ON regime_history(ts_ms DESC)")
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EnsembleRegime] regime_history schema init failed: {e}")

    def _recent_column_values(self, column: str, limit: int) -> List:
        if not self.monitor_enabled or limit <= 0:
            return []
        try:
            conn = sqlite3.connect(str(self.regime_history_db))
            cur = conn.cursor()
            cur.execute(
                f"SELECT {column} FROM regime_history WHERE {column} IS NOT NULL ORDER BY ts_ms DESC LIMIT ?",
                (int(limit),),
            )
            rows = [r[0] for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception:
            return []

    def _is_sideways_stuck(self, current_sideways_prob: Optional[float]) -> bool:
        if current_sideways_prob is None:
            return False
        n = max(2, int(self.monitor_sideways_consecutive_warn))
        prev = self._recent_column_values('hmm_sideways_prob', n - 1)
        if len(prev) < n - 1:
            return False
        vals = [float(current_sideways_prob)] + [float(v) for v in prev]
        th = float(self.monitor_sideways_prob_warn_threshold)
        return all(v >= th for v in vals)

    def _is_final_state_stuck(self, current_final_state: str) -> Optional[str]:
        n = max(3, int(self.monitor_sideways_consecutive_warn))
        prev = self._recent_column_values('final_state', n - 1)
        if len(prev) < n - 1:
            return None
        vals = [str(current_final_state)] + [str(v) for v in prev]
        if len(set(vals)) == 1:
            return vals[0]
        return None

    def _collect_runtime_alerts(self, votes: Dict[str, dict], final_state: str) -> List[str]:
        alerts: List[str] = list(self.startup_alerts or [])

        hmm = votes.get('hmm', {}) if isinstance(votes, dict) else {}
        funding = votes.get('funding', {}) if isinstance(votes, dict) else {}
        rss = votes.get('rss', {}) if isinstance(votes, dict) else {}

        valid_states = [v.get('state') for v in (hmm, funding, rss) if v.get('state')]
        if not valid_states:
            alerts.append('all_votes_none')

        if not hmm.get('state'):
            alerts.append('hmm_predict_none')

        if self._model_type_mismatch:
            alerts.append('model_type_mismatch')

        # 漂移/卡住检测（基于历史 + 当前）
        hmm_probs = hmm.get('probs') if isinstance(hmm, dict) else None
        sideways_prob = None
        if isinstance(hmm_probs, dict):
            try:
                sideways_prob = float(hmm_probs.get('Sideways'))
            except Exception:
                sideways_prob = None

        if self._is_sideways_stuck(sideways_prob):
            alerts.append('hmm_sideways_stuck')

        stuck_state = self._is_final_state_stuck(final_state)
        if stuck_state:
            alerts.append(f'regime_stuck_{stuck_state.lower()}')

        # 去重并保持顺序
        seen = set()
        out = []
        for a in alerts:
            if not a or a in seen:
                continue
            seen.add(a)
            out.append(a)
        return out

    def _persist_history(
        self,
        *,
        votes: Dict[str, dict],
        final_state: str,
        final_score: float,
        confidence: float,
        multiplier: float,
        alerts: List[str],
    ) -> None:
        if not self.monitor_enabled:
            return
        try:
            ts_ms = int(time.time() * 1000)
            hmm = votes.get('hmm', {}) if isinstance(votes, dict) else {}
            funding = votes.get('funding', {}) if isinstance(votes, dict) else {}
            rss = votes.get('rss', {}) if isinstance(votes, dict) else {}
            probs = hmm.get('probs') if isinstance(hmm, dict) else {}
            if not isinstance(probs, dict):
                probs = {}

            conn = sqlite3.connect(str(self.regime_history_db))
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO regime_history (
                    ts_ms, final_state, final_score, confidence, multiplier,
                    hmm_state, hmm_confidence, hmm_trending_up_prob, hmm_trending_down_prob, hmm_sideways_prob,
                    funding_state, funding_confidence, funding_sentiment,
                    rss_state, rss_confidence, rss_sentiment,
                    alerts_json, weights_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts_ms,
                    str(final_state),
                    float(final_score),
                    float(confidence),
                    float(multiplier),
                    hmm.get('state'),
                    float(hmm.get('confidence') or 0.0),
                    float(probs.get('TrendingUp') or 0.0),
                    float(probs.get('TrendingDown') or 0.0),
                    float(probs.get('Sideways') or 0.0),
                    funding.get('state'),
                    float(funding.get('confidence') or 0.0),
                    float(funding.get('sentiment') or 0.0),
                    rss.get('state'),
                    float(rss.get('confidence') or 0.0),
                    float(rss.get('sentiment') or 0.0),
                    json.dumps(alerts or [], ensure_ascii=False),
                    json.dumps(self.weights, ensure_ascii=False),
                ),
            )

            # 控制表大小，避免无限增长
            if int(self.monitor_keep_rows) > 0:
                cur.execute(
                    """
                    DELETE FROM regime_history
                    WHERE id NOT IN (
                        SELECT id FROM regime_history ORDER BY ts_ms DESC LIMIT ?
                    )
                    """,
                    (int(self.monitor_keep_rows),),
                )

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EnsembleRegime] persist regime history failed: {e}")

    def _latest_fresh_file(self, pattern: str, max_age_minutes: int) -> Optional[Path]:
        files = sorted(self.sentiment_cache_dir.glob(pattern))
        if not files:
            return None
        candidate = files[-1]
        max_age_sec = max(int(max_age_minutes), 1) * 60
        age_sec = max(0.0, time.time() - candidate.stat().st_mtime)
        if age_sec > max_age_sec:
            return None
        return candidate

    def _get_hmm_vote(self, btc_data: MarketSeries) -> dict:
        """HMM投票"""
        if self.hmm_detector is None:
            return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'hmm_detector_missing'}

        try:
            # 构造特征
            features = []
            closes = list(btc_data.close)
            for i in range(len(closes)):
                if i < 14:
                    continue
                ret_1h = (closes[i] - closes[i - 1]) / closes[i - 1] if closes[i - 1] > 0 else 0
                ret_6h = (
                    (closes[i] - closes[max(0, i - 6)]) / closes[max(0, i - 6)]
                    if closes[max(0, i - 6)] > 0
                    else 0
                )

                window = closes[max(0, i - 14): i + 1]
                vol = np.std(np.diff(window) / window[:-1]) if len(window) > 1 else 0

                gains = [
                    closes[j] - closes[j - 1]
                    for j in range(max(0, i - 14), i + 1)
                    if closes[j] > closes[j - 1]
                ]
                losses = [
                    closes[j - 1] - closes[j]
                    for j in range(max(0, i - 14), i + 1)
                    if closes[j] < closes[j - 1]
                ]
                avg_gain = np.mean(gains) if gains else 0
                avg_loss = np.mean(losses) if losses else 0.001
                rsi = 100 - (100 / (1 + avg_gain / avg_loss))

                features.append([ret_1h, ret_6h, vol, rsi])

            if len(features) < 10:
                return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'hmm_features_insufficient'}

            result = self.hmm_detector.predict(np.array(features))
            if not isinstance(result, dict):
                return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'hmm_predict_invalid'}

            hmm_state = result.get('state')
            if hmm_state is None:
                return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'hmm_predict_none'}

            state_map = {
                'TrendingUp': 'TRENDING',
                'TrendingDown': 'RISK_OFF',
                'Sideways': 'SIDEWAYS',
            }

            all_states = result.get('all_states', {})
            if not isinstance(all_states, dict):
                all_states = {}
            sentiment = all_states.get('TrendingUp', 0) - all_states.get('TrendingDown', 0)
            sentiment = max(-1.0, min(1.0, float(sentiment)))

            return {
                'state': state_map.get(hmm_state, 'SIDEWAYS'),
                'confidence': float(result.get('probability') or 0.0),
                'weight': self.weights['hmm'],
                'sentiment': sentiment,
                'raw_state': hmm_state,
                'probs': all_states,
            }
        except Exception as e:
            print(f"[EnsembleRegime] HMM投票失败: {e}")
            return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'hmm_predict_exception'}
    
    def _get_funding_vote(self) -> dict:
        """资金费率投票（使用综合情绪）"""
        try:
            # 优先读取综合资金费率情绪文件
            composite_file = self._latest_fresh_file(
                'funding_COMPOSITE_*.json',
                self.funding_signal_max_age_minutes,
            )

            if composite_file is not None:
                data = json.loads(composite_file.read_text())
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
                latest = self._latest_fresh_file(
                    f'funding_{sym}_*.json',
                    self.funding_signal_max_age_minutes,
                )
                if latest is not None:
                    data = json.loads(latest.read_text())
                    v = float(data.get('f6_sentiment', 0.0))
                    vals.append(max(-1.0, min(1.0, v)))
            
            if not vals:
                return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'funding_signal_stale_or_missing'}
            
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
            latest_rss = self._latest_fresh_file(
                'rss_MARKET_*.json',
                self.rss_signal_max_age_minutes,
            )
            if latest_rss is None:
                # 尝试币种特定文件
                latest_rss = self._latest_fresh_file(
                    'rss_BTC-USDT_*.json',
                    self.rss_signal_max_age_minutes,
                )

            if latest_rss is None:
                return {'state': None, 'confidence': 0, 'weight': 0, 'error': 'rss_signal_stale_or_missing'}

            data = json.loads(latest_rss.read_text())
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
        4. 持久化状态历史 + 运行期告警
        """
        # 1. 收集投票
        hmm_vote = self._get_hmm_vote(btc_data)
        funding_vote = self._get_funding_vote()
        rss_vote = self._get_rss_vote()

        votes = {
            'hmm': hmm_vote,
            'funding': funding_vote,
            'rss': rss_vote,
        }

        # 2. 加权投票
        final_state_str, confidence, score = self._weighted_vote([
            hmm_vote, funding_vote, rss_vote
        ])

        # 3. 映射到RegimeState
        state_map = {
            'TRENDING': RegimeState.TRENDING,
            'SIDEWAYS': RegimeState.SIDEWAYS,
            'RISK_OFF': RegimeState.RISK_OFF,
        }
        final_state = state_map.get(final_state_str, RegimeState.SIDEWAYS)

        # 4. 计算倍数
        mult_map = {
            RegimeState.TRENDING: float(self.cfg.pos_mult_trending),
            RegimeState.SIDEWAYS: float(self.cfg.pos_mult_sideways),
            RegimeState.RISK_OFF: float(self.cfg.pos_mult_risk_off),
        }
        multiplier = mult_map.get(final_state, 0.6)

        # 5. 计算MA（用于输出兼容）
        closes = list(btc_data.close)
        ma20 = np.mean(closes[-20:]) if len(closes) >= 20 else np.mean(closes)
        ma60 = np.mean(closes[-60:]) if len(closes) >= 60 else np.mean(closes)

        # 计算ATR%
        if len(closes) > 14:
            recent = closes[-15:]
            returns = [(recent[i] - recent[i - 1]) / recent[i - 1] for i in range(1, len(recent))]
            atrp = np.std(returns)
        else:
            atrp = 0.01

        # 6. 根据confidence调整multiplier（可选）
        confidence_adjustment = (confidence - 0.5) * 0.2  # -0.1 ~ +0.1
        multiplier = max(0, min(1.5, multiplier * (1 + confidence_adjustment)))

        # 7. 运行期告警 + 状态历史持久化
        alerts = self._collect_runtime_alerts(votes, final_state_str)
        self.last_runtime_alerts = alerts
        if alerts:
            votes['alerts'] = alerts
            votes['monitor'] = {
                'enabled': bool(self.monitor_enabled),
                'db_path': str(self.regime_history_db),
                'sideways_prob_warn_threshold': float(self.monitor_sideways_prob_warn_threshold),
                'sideways_consecutive_warn': int(self.monitor_sideways_consecutive_warn),
            }

        self._persist_history(
            votes=votes,
            final_state=final_state_str,
            final_score=float(score),
            confidence=float(confidence),
            multiplier=float(multiplier),
            alerts=alerts,
        )

        return RegimeResult(
            state=final_state,
            atr_pct=float(atrp),
            ma20=float(ma20),
            ma60=float(ma60),
            multiplier=float(multiplier),
            votes=votes,
            final_score=float(score),
            alerts=alerts,
        )
