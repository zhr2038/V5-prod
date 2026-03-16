"""
V5 情绪分析因子模块

功能:
1. 从Twitter/Reddit抓取加密货币相关讨论
2. 使用FinBERT模型进行情感分析
3. 生成情绪得分作为V5的第6个因子

安装依赖:
pip install transformers torch textblob tweepy praw
"""

import json
import re
from datetime import datetime, timedelta
from typing import Dict, List
from pathlib import Path
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE_DIR = PROJECT_ROOT / 'data' / 'sentiment_cache'

# 尝试导入可选依赖
try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    print("[SentimentFactor] transformers未安装，使用备用方案")

try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False


class SentimentFactor:
    """
    情绪分析因子
    
    输出:
    - f6_sentiment: 情绪得分 (-1.0 ~ +1.0)
    - f6_sentiment_magnitude: 情绪强度 (0.0 ~ 1.0)
    - f6_fear_greed_index: 恐惧贪婪指数 (0 ~ 100)
    """
    
    def __init__(self, cache_dir: str = str(DEFAULT_CACHE_DIR)):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化情感分析模型
        self.sentiment_analyzer = None
        if TRANSFORMERS_AVAILABLE:
            try:
                # 使用FinBERT（金融情感分析专用）
                self.sentiment_analyzer = pipeline(
                    "sentiment-analysis",
                    model="ProsusAI/finbert",
                    tokenizer="ProsusAI/finbert"
                )
                print("[SentimentFactor] FinBERT模型加载成功")
            except Exception as e:
                print(f"[SentimentFactor] FinBERT加载失败: {e}，使用备用方案")
        
        # 关键词映射
        self.symbol_keywords = {
            'BTC-USDT': ['bitcoin', 'btc', '比特币'],
            'ETH-USDT': ['ethereum', 'eth', '以太'],
            'SOL-USDT': ['solana', 'sol'],
            'BNB-USDT': ['binance', 'bnb'],
            'ADA-USDT': ['cardano', 'ada'],
            'DOT-USDT': ['polkadot', 'dot'],
            'AVAX-USDT': ['avalanche', 'avax'],
            'XRP-USDT': ['ripple', 'xrp'],
            'DOGE-USDT': ['dogecoin', 'doge', '狗狗'],
            'LINK-USDT': ['chainlink', 'link'],
        }
    
    def calculate(self, symbol: str) -> Dict:
        """
        计算指定币种的情绪因子
        
        返回:
        {
            'f6_sentiment': float,  # -1.0 ~ +1.0
            'f6_sentiment_magnitude': float,  # 0.0 ~ 1.0
            'f6_fear_greed_index': float,  # 0 ~ 100
            'f6_sentiment_source': str,  # 'finbert' | 'textblob' | 'cache' | 'neutral'
        }
        """
        # 1. 检查缓存
        cache_file = self.cache_dir / f"{symbol}_{datetime.now().strftime('%Y%m%d')}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    cached = json.load(f)
                    # 检查缓存是否过期（超过1小时）
                    cache_time = datetime.fromisoformat(cached.get('timestamp', ''))
                    if datetime.now() - cache_time < timedelta(hours=1):
                        return {
                            'f6_sentiment': cached['sentiment'],
                            'f6_sentiment_magnitude': abs(cached['sentiment']),
                            'f6_fear_greed_index': self._sentiment_to_fear_greed(cached['sentiment']),
                            'f6_sentiment_source': 'cache'
                        }
            except:
                pass
        
        # 2. 获取文本数据
        texts = self._fetch_texts(symbol)
        
        if not texts:
            # 无数据时返回中性
            return {
                'f6_sentiment': 0.0,
                'f6_sentiment_magnitude': 0.0,
                'f6_fear_greed_index': 50.0,
                'f6_sentiment_source': 'neutral'
            }
        
        # 3. 情感分析
        if self.sentiment_analyzer:
            sentiment, source = self._analyze_with_finbert(texts)
        elif TEXTBLOB_AVAILABLE:
            sentiment, source = self._analyze_with_textblob(texts)
        else:
            sentiment, source = 0.0, 'neutral'
        
        # 4. 保存缓存
        try:
            with open(cache_file, 'w') as f:
                json.dump({
                    'symbol': symbol,
                    'sentiment': sentiment,
                    'texts_count': len(texts),
                    'timestamp': datetime.now().isoformat()
                }, f)
        except:
            pass
        
        return {
            'f6_sentiment': round(sentiment, 4),
            'f6_sentiment_magnitude': round(abs(sentiment), 4),
            'f6_fear_greed_index': round(self._sentiment_to_fear_greed(sentiment), 2),
            'f6_sentiment_source': source
        }
    
    def _fetch_texts(self, symbol: str) -> List[str]:
        """
        获取相关文本
        
        TODO: 实际接入Twitter/Reddit API
        当前使用模拟数据演示
        """
        keywords = self.symbol_keywords.get(symbol, [symbol.split('-')[0].lower()])
        
        # 模拟文本数据（实际使用时替换为API调用）
        mock_texts = self._generate_mock_texts(symbol)
        
        return mock_texts
    
    def _generate_mock_texts(self, symbol: str) -> List[str]:
        """生成模拟文本用于测试"""
        base = symbol.split('-')[0]
        
        # 根据当前时间生成不同的模拟情绪
        hour = datetime.now().hour
        
        if 9 <= hour <= 16:  # 美股交易时间，情绪积极
            texts = [
                f"{base} looking bullish today! 🚀",
                f"Just bought more {base}, expecting pump",
                f"{base} breaking resistance, moon soon",
                f"Strong volume on {base}, whales accumulating",
                f"{base} chart looks great, holding long",
            ]
        elif 0 <= hour <= 5:  # 深夜，情绪恐慌
            texts = [
                f"{base} dumping hard, scared",
                f"Sold all my {base}, too risky",
                f"{base} crashing, what's happening?",
                f"Bear market for {base}, going to zero",
                f"Lost money on {base}, never again",
            ]
        else:  # 其他时间，混合
            texts = [
                f"{base} sideways, waiting for breakout",
                f"Not sure about {base}, mixed signals",
                f"{base} consolidating, patience needed",
                f"Small pump on {base}, watching closely",
                f"{base} volume low, no clear direction",
            ]
        
        return texts
    
    def _analyze_with_finbert(self, texts: List[str]) -> tuple:
        """使用FinBERT分析"""
        if not self.sentiment_analyzer:
            return 0.0, 'neutral'
        
        scores = []
        for text in texts:
            try:
                # FinBERT输出: [{'label': 'positive', 'score': 0.95}]
                result = self.sentiment_analyzer(text[:512])[0]  # 限制长度
                label = result['label']
                score = result['score']
                
                # 转换为 -1 ~ +1
                if label == 'positive':
                    sentiment = score
                elif label == 'negative':
                    sentiment = -score
                else:
                    sentiment = 0.0
                
                scores.append(sentiment)
            except:
                continue
        
        if scores:
            # 使用加权平均（最近文本权重更高）
            weights = np.exp(np.linspace(-1, 0, len(scores)))
            avg_sentiment = np.average(scores, weights=weights)
            return float(avg_sentiment), 'finbert'
        
        return 0.0, 'neutral'
    
    def _analyze_with_textblob(self, texts: List[str]) -> tuple:
        """使用TextBlob备用分析"""
        if not TEXTBLOB_AVAILABLE:
            return 0.0, 'neutral'
        
        scores = []
        for text in texts:
            try:
                blob = TextBlob(text)
                # TextBlob输出: polarity (-1 ~ +1)
                scores.append(blob.sentiment.polarity)
            except:
                continue
        
        if scores:
            return float(np.mean(scores)), 'textblob'
        
        return 0.0, 'neutral'
    
    def _sentiment_to_fear_greed(self, sentiment: float) -> float:
        """
        将情绪得分转换为恐惧贪婪指数
        -1.0 (极度恐惧) ~ +1.0 (极度贪婪) -> 0 ~ 100
        """
        return (sentiment + 1) * 50
    
    def get_market_sentiment_summary(self) -> Dict:
        """
        获取市场整体情绪摘要
        """
        symbols = list(self.symbol_keywords.keys())
        sentiments = {}
        
        for symbol in symbols[:5]:  # 只分析前5个主要币种
            result = self.calculate(symbol)
            sentiments[symbol] = result['f6_sentiment']
        
        avg_sentiment = np.mean(list(sentiments.values()))
        
        return {
            'average_sentiment': round(avg_sentiment, 4),
            'fear_greed_index': round(self._sentiment_to_fear_greed(avg_sentiment), 2),
            'market_mood': 'greedy' if avg_sentiment > 0.5 else 'fearful' if avg_sentiment < -0.5 else 'neutral',
            'by_symbol': sentiments
        }


# ============================================================
# V5集成示例
# ============================================================

def integrate_with_v5():
    """
    集成到V5的示例代码
    
    修改: src/strategy/alpha_calculator.py
    """
    
    code = '''
# 在AlphaCalculator中添加:

from src.factors.sentiment_factor import SentimentFactor

class AlphaCalculator:
    def __init__(self, config):
        ...
        self.sentiment_factor = SentimentFactor()
    
    def calculate_alphas(self, market_data):
        ...
        # 原有因子 f1-f5
        
        # 新增情绪因子 f6
        for symbol in symbols:
            sentiment_scores = self.sentiment_factor.calculate(symbol)
            scores[symbol]['f6_sentiment'] = sentiment_scores['f6_sentiment']
            scores[symbol]['f6_sentiment_magnitude'] = sentiment_scores['f6_sentiment_magnitude']
            scores[symbol]['f6_fear_greed_index'] = sentiment_scores['f6_fear_greed_index']
        
        return scores
    
    def apply_sentiment_adjustment(self, portfolio_weights, sentiment_scores):
        """
        根据情绪调整仓位
        
        规则:
        - 情绪 > 0.7 (极度贪婪): 降低仓位50% (FOMO预警)
        - 情绪 < -0.7 (极度恐惧): 增加仓位20% (逆势抄底)
        - 其他: 维持原仓位
        """
        adjusted_weights = {}
        
        for symbol, weight in portfolio_weights.items():
            sentiment = sentiment_scores.get(symbol, {}).get('f6_sentiment', 0)
            
            if sentiment > 0.7:
                # 极度贪婪，降低仓位
                adjusted_weights[symbol] = weight * 0.5
            elif sentiment < -0.7:
                # 极度恐惧，增加仓位
                adjusted_weights[symbol] = min(weight * 1.2, 0.1)  # 最大10%
            else:
                adjusted_weights[symbol] = weight
        
        return adjusted_weights
'''
    return code


# ============================================================
# 测试运行
# ============================================================

if __name__ == "__main__":
    print("="*70)
    print("V5 情绪分析因子 - 测试运行")
    print("="*70)
    
    factor = SentimentFactor()
    
    # 测试单个币种
    print("\n测试 BTC-USDT:")
    result = factor.calculate('BTC-USDT')
    print(f"  情绪得分: {result['f6_sentiment']:+.4f}")
    print(f"  情绪强度: {result['f6_sentiment_magnitude']:.4f}")
    print(f"  恐惧贪婪指数: {result['f6_fear_greed_index']:.1f}")
    print(f"  数据来源: {result['f6_sentiment_source']}")
    
    # 测试市场整体情绪
    print("\n市场整体情绪:")
    summary = factor.get_market_sentiment_summary()
    print(f"  平均情绪: {summary['average_sentiment']:+.4f}")
    print(f"  市场情绪: {summary['market_mood']}")
    print(f"  恐惧贪婪指数: {summary['fear_greed_index']:.1f}")
    
    print("\n" + "="*70)
    print("说明:")
    print("- 当前使用模拟数据")
    print("- 实际使用时需要接入Twitter/Reddit API")
    print("- 建议每小时更新一次情绪数据")
    print("="*70)
