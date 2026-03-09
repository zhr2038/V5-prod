"""
V5 GPT情绪分析模块

使用OpenAI GPT-3.5/4进行加密货币情绪分析
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List
from pathlib import Path
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE_DIR = PROJECT_ROOT / 'data' / 'sentiment_cache'


class GPTSentimentFactor:
    """
    GPT情绪分析因子
    
    使用OpenAI API进行高质量情感分析
    支持自定义prompt，理解币圈黑话
    
    环境变量:
    - OPENAI_API_KEY: OpenAI API密钥
    - OPENAI_BASE_URL: 可选，用于代理(如https://api.openai-proxy.com/v1)
    """
    
    def __init__(self, 
                 cache_dir: str = str(DEFAULT_CACHE_DIR),
                 api_key: str = None,
                 base_url: str = None,
                 model: str = "gpt-3.5-turbo"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.base_url = base_url or os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
        self.model = model
        
        if not self.api_key:
            print("[GPTSentiment] 警告: 未设置OPENAI_API_KEY")
        else:
            print(f"[GPTSentiment] 使用模型: {model}")
    
    def analyze_sentiment(self, texts: List[str], symbol: str = "BTC") -> Dict:
        """
        使用GPT分析情绪
        
        Args:
            texts: 社交媒体文本列表
            symbol: 币种名称
            
        Returns:
            {
                'sentiment_score': float,  # -1.0 ~ +1.0
                'confidence': float,       # 0.0 ~ 1.0
                'summary': str,            # GPT分析摘要
                'key_points': List[str],   # 关键观点
                'fear_greed_index': int,   # 0 ~ 100
            }
        """
        if not texts:
            return self._neutral_result()
        
        # 合并文本（限制token数）
        combined_text = "\n---\n".join(texts[:20])  # 最多20条
        
        # 构建prompt
        prompt = self._build_prompt(combined_text, symbol)
        
        try:
            response = self._call_gpt(prompt)
            result = self._parse_response(response)
            return result
            
        except Exception as e:
            print(f"[GPTSentiment] API调用失败: {e}")
            return self._neutral_result()
    
    def _build_prompt(self, texts: str, symbol: str) -> str:
        """构建GPT prompt"""
        
        prompt = f"""你是一位专业的加密货币市场情绪分析师。请分析以下关于{symbol}的社交媒体评论，并给出情绪分析结果。

评论内容:
{texts}

请严格按照以下JSON格式输出（不要输出其他内容）:
{{
    "sentiment_score": 0.0,  // 情绪得分，范围-1.0(极度恐慌)到+1.0(极度贪婪)
    "confidence": 0.0,       // 置信度，范围0.0到1.0
    "summary": "",           // 一句话总结当前情绪
    "key_points": [""],      // 3-5个关键观点
    "fear_greed_index": 50   // 恐惧贪婪指数，范围0-100
}}

分析要点:
1. 识别"to the moon"、"diamond hands"、"paper hands"等币圈黑话
2. 区分真实情绪和 sarcasm/讽刺
3. 关注大户/鲸鱼相关讨论
4. 识别FOMO和恐慌抛售迹象
"""
        return prompt
    
    def _call_gpt(self, prompt: str) -> str:
        """调用OpenAI API"""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "你是一个专业的加密货币市场情绪分析师。只输出JSON格式结果。"},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,  # 低温度，更确定性的输出
            "max_tokens": 500
        }
        
        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"API错误: {response.status_code}, {response.text}")
        
        result = response.json()
        return result['choices'][0]['message']['content']
    
    def _parse_response(self, response: str) -> Dict:
        """解析GPT响应"""
        
        # 提取JSON部分
        try:
            # 找到JSON开始和结束
            start = response.find('{')
            end = response.rfind('}') + 1
            json_str = response[start:end]
            
            data = json.loads(json_str)
            
            return {
                'sentiment_score': float(data.get('sentiment_score', 0)),
                'confidence': float(data.get('confidence', 0)),
                'summary': data.get('summary', ''),
                'key_points': data.get('key_points', []),
                'fear_greed_index': int(data.get('fear_greed_index', 50)),
                'source': 'gpt'
            }
            
        except Exception as e:
            print(f"[GPTSentiment] JSON解析失败: {e}")
            return self._neutral_result()
    
    def _neutral_result(self) -> Dict:
        """中性结果"""
        return {
            'sentiment_score': 0.0,
            'confidence': 0.0,
            'summary': '无数据',
            'key_points': [],
            'fear_greed_index': 50,
            'source': 'neutral'
        }
    
    def calculate(self, symbol: str) -> Dict:
        """
        V5因子接口
        
        返回格式与SentimentFactor一致
        """
        # 检查缓存
        cache_file = self.cache_dir / f"gpt_{symbol}_{datetime.now().strftime('%Y%m%d_%H')}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    cached = json.load(f)
                    return cached
            except:
                pass
        
        # 获取文本（这里用模拟数据，实际接入Twitter/Reddit API）
        texts = self._fetch_mock_texts(symbol)
        
        # GPT分析
        result = self.analyze_sentiment(texts, symbol)
        
        # 转换为V5因子格式
        factor_result = {
            'f6_sentiment': round(result['sentiment_score'], 4),
            'f6_sentiment_magnitude': round(abs(result['sentiment_score']), 4),
            'f6_fear_greed_index': float(result['fear_greed_index']),
            'f6_sentiment_summary': result['summary'],
            'f6_sentiment_confidence': round(result['confidence'], 4),
            'f6_sentiment_source': 'gpt',
            'f6_sentiment_key_points': result['key_points'],
        }
        
        # 缓存
        with open(cache_file, 'w') as f:
            json.dump(factor_result, f, indent=2, ensure_ascii=False)
        
        return factor_result
    
    def _fetch_mock_texts(self, symbol: str) -> List[str]:
        """模拟文本数据"""
        base = symbol.split('-')[0]
        
        # 根据时间生成不同情绪
        hour = datetime.now().hour
        
        if hour in [9, 10, 11]:  # 上午活跃
            return [
                f"{base} looking bullish today! 🚀",
                f"Just bought more {base}, expecting pump",
                f"{base} breaking resistance, moon soon",
                f"Whales accumulating {base}, don't miss out",
                f"{base} to the moon! Diamond hands 💎🙌",
            ]
        elif hour in [0, 1, 2]:  # 深夜恐慌
            return [
                f"{base} crashing, what's happening?",
                f"Sold all my {base}, too risky now",
                f"Bear market confirmed for {base}",
                f"{base} going to zero, exit now",
                f"Lost everything on {base}, never again",
            ]
        else:
            return [
                f"{base} consolidating, patience needed",
                f"Not sure about {base} direction",
                f"{base} volume low today",
                f"Waiting for {base} breakout",
                f"{base} looks neutral",
            ]
    
    def get_cost_estimate(self, queries_per_day: int = 24) -> Dict:
        """
        成本估算
        
        Args:
            queries_per_day: 每天查询次数（默认每小时1次）
        """
        # GPT-3.5: $0.0015 / 1K tokens
        # 每次约500 tokens (prompt) + 200 tokens (response) = 700 tokens
        
        tokens_per_query = 700
        cost_per_1k = 0.0015  # $0.0015 for gpt-3.5-turbo
        
        daily_cost = (tokens_per_query * queries_per_day / 1000) * cost_per_1k
        monthly_cost = daily_cost * 30
        
        return {
            'model': self.model,
            'queries_per_day': queries_per_day,
            'tokens_per_query': tokens_per_query,
            'daily_cost_usd': round(daily_cost, 4),
            'monthly_cost_usd': round(monthly_cost, 4),
            'monthly_cost_cny': round(monthly_cost * 7.2, 2),  # 按7.2汇率
        }


# ============================================================
# 使用示例
# ============================================================

if __name__ == "__main__":
    print("="*70)
    print("V5 GPT情绪分析 - 测试")
    print("="*70)
    
    # 成本估算
    factor = GPTSentimentFactor()
    cost = factor.get_cost_estimate(queries_per_day=24)  # 每小时1次
    
    print("\n💰 成本估算:")
    print(f"  模型: {cost['model']}")
    print(f"  查询频率: {cost['queries_per_day']}次/天")
    print(f"  每日成本: ${cost['daily_cost_usd']}")
    print(f"  每月成本: ${cost['monthly_cost_usd']} (约¥{cost['monthly_cost_cny']})")
    
    # 测试分析
    print("\n📝 情绪分析测试 (BTC-USDT):")
    
    # 设置API key后取消注释下面代码
    # factor.api_key = "your-api-key-here"
    # result = factor.calculate('BTC-USDT')
    # print(f"  情绪得分: {result['f6_sentiment']:+.4f}")
    # print(f"  恐惧贪婪: {result['f6_fear_greed_index']}")
    # print(f"  摘要: {result['f6_sentiment_summary']}")
    # print(f"  关键点: {', '.join(result['f6_sentiment_key_points'][:3])}")
    
    print("\n" + "="*70)
    print("⚠️  注意: 需要设置 OPENAI_API_KEY 环境变量")
    print("    export OPENAI_API_KEY='sk-...'")
    print("="*70)
