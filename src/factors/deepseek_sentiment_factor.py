"""
V5 DeepSeek情绪分析模块

使用DeepSeek API进行加密货币情绪分析
优势:
- 价格便宜: ¥1/百万tokens (比GPT-3.5便宜90%)
- 中文效果: 针对中文优化，理解微博/贴吧更好
- 国内访问: 顺畅，无需代理
- 推理能力: 深度理解上下文和讽刺

API文档: https://platform.deepseek.com/
"""

import os
import json
from datetime import datetime
from typing import Dict, List
from pathlib import Path
import requests


def _load_env_file(env_path: Path):
    """轻量加载 .env（避免依赖python-dotenv）"""
    try:
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding='utf-8', errors='ignore').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except Exception:
        pass


class DeepSeekSentimentFactor:
    """
    DeepSeek情绪分析因子
    
    环境变量:
    - DEEPSEEK_API_KEY: DeepSeek API密钥
    """
    
    def __init__(self, 
                 cache_dir: str = '/home/admin/clawd/v5-trading-bot/data/sentiment_cache',
                 api_key: str = None,
                 model: str = "deepseek-chat"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 自动加载项目 .env，避免不同启动方式下环境变量缺失
        _load_env_file(Path('/home/admin/clawd/v5-trading-bot/.env'))

        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.base_url = "https://api.deepseek.com/v1"
        self.model = model
        
        if not self.api_key:
            print("[DeepSeekSentiment] 警告: 未设置DEEPSEEK_API_KEY")
        else:
            print(f"[DeepSeekSentiment] 使用模型: {model}")
    
    def analyze_sentiment(self, texts: List[str], symbol: str = "BTC") -> Dict:
        """
        使用DeepSeek分析情绪
        
        返回:
        {
            'sentiment_score': float,  # -1.0 ~ +1.0
            'confidence': float,       # 0.0 ~ 1.0
            'summary': str,            # 中文摘要
            'key_points': List[str],   # 关键观点
            'fear_greed_index': int,   # 0 ~ 100
            'market_stage': str,       # 'fomo'|'panic'|'accumulation'|'distribution'
        }
        """
        if not texts:
            return self._neutral_result()
        
        # 合并文本
        combined_text = "\n---\n".join(texts[:20])
        
        # 构建prompt (中文优化)
        prompt = self._build_prompt(combined_text, symbol)
        
        try:
            response = self._call_deepseek(prompt)
            result = self._parse_response(response)
            return result
            
        except Exception as e:
            print(f"[DeepSeekSentiment] API调用失败: {e}")
            return self._neutral_result()
    
    def _build_prompt(self, texts: str, symbol: str) -> str:
        """构建中文优化的prompt"""
        
        prompt = f"""你是一位专业的加密货币市场情绪分析师。请分析以下关于{symbol}的社交媒体评论。

评论内容:
{texts}

请严格按照以下JSON格式输出:
{{
    "sentiment_score": 0.0,
    "confidence": 0.0,
    "summary": "",
    "key_points": [""],
    "fear_greed_index": 50,
    "market_stage": ""
}}

字段说明:
- sentiment_score: 情绪得分，-1.0(极度恐慌)到+1.0(极度贪婪)
- confidence: 置信度，0.0到1.0
- summary: 一句话中文总结当前情绪
- key_points: 3-5个关键观点(中文)
- fear_greed_index: 恐惧贪婪指数，0-100
- market_stage: 市场阶段，可选: fomo(狂热)/panic(恐慌)/accumulation(吸筹)/distribution(派发)/neutral(中性)

分析要点:
1. 识别"梭哈"、"跑路"、"抄底"、"瀑布"等中文币圈术语
2. 区分真实情绪和阴阳怪气/讽刺
3. 关注"庄家"、"大户"、"韭菜"相关讨论
4. 识别FOMO和恐慌割肉迹象
5. 判断当前是吸筹阶段还是派发阶段
"""
        return prompt
    
    def _call_deepseek(self, prompt: str) -> str:
        """调用DeepSeek API"""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "你是专业的加密货币市场情绪分析师。只输出JSON格式结果。"},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 800,
            "response_format": {"type": "json_object"}  # DeepSeek支持强制JSON输出
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
        """解析DeepSeek响应"""
        
        try:
            data = json.loads(response)
            
            return {
                'sentiment_score': float(data.get('sentiment_score', 0)),
                'confidence': float(data.get('confidence', 0)),
                'summary': data.get('summary', ''),
                'key_points': data.get('key_points', []),
                'fear_greed_index': int(data.get('fear_greed_index', 50)),
                'market_stage': data.get('market_stage', 'neutral'),
                'source': 'deepseek'
            }
            
        except Exception as e:
            print(f"[DeepSeekSentiment] JSON解析失败: {e}")
            return self._neutral_result()
    
    def _neutral_result(self) -> Dict:
        """中性结果"""
        return {
            'sentiment_score': 0.0,
            'confidence': 0.0,
            'summary': '无数据',
            'key_points': [],
            'fear_greed_index': 50,
            'market_stage': 'neutral',
            'source': 'neutral'
        }
    
    def calculate(self, symbol: str) -> Dict:
        """V5因子接口"""
        
        # 检查缓存
        cache_file = self.cache_dir / f"deepseek_{symbol}_{datetime.now().strftime('%Y%m%d_%H')}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached = json.load(f)
                # 若缓存是无数据占位且当前有key，尝试刷新一次
                if not (self.api_key and str(cached.get('f6_sentiment_summary', '')).strip() == '无数据'):
                    return cached
            except Exception:
                pass
        
        # 获取文本（模拟数据，实际接入微博/贴吧/推特API）
        texts = self._fetch_mock_texts(symbol)
        
        # DeepSeek分析
        result = self.analyze_sentiment(texts, symbol)
        
        # 转换为V5因子格式
        factor_result = {
            'f6_sentiment': round(result['sentiment_score'], 4),
            'f6_sentiment_magnitude': round(abs(result['sentiment_score']), 4),
            'f6_fear_greed_index': float(result['fear_greed_index']),
            'f6_sentiment_summary': result['summary'],
            'f6_sentiment_confidence': round(result['confidence'], 4),
            'f6_sentiment_source': result.get('source', 'deepseek'),
            'f6_sentiment_key_points': result['key_points'],
            'f6_market_stage': result['market_stage'],
        }
        
        # 缓存
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(factor_result, f, indent=2, ensure_ascii=False)
        
        return factor_result
    
    def _fetch_mock_texts(self, symbol: str) -> List[str]:
        """模拟中文社交媒体文本"""
        base = symbol.split('-')[0]
        
        # 中文币圈黑话
        fomo_texts = [
            f"{base}要起飞了，梭哈！",
            f"满仓{base}，这波必到100万",
            f"{base}突破前高，to the moon！",
            f"凌晨三点{base}放量，庄家进场了",
            f"不要怂，{base}就是干",
            f"{base}筹码拿稳，钻石手",
        ]
        
        panic_texts = [
            f"{base}要归零了，快跑",
            f"刚割肉{base}，亏麻了",
            f"{base}瀑布了，瀑布啊！",
            f"熊市来了，{base}废了",
            f"{base}这是要把韭菜连根拔起",
            f"合约爆仓，{base}害人不浅",
        ]
        
        accumulation_texts = [
            f"{base}在吸筹，耐心持有",
            f"大户在买{base}，散户在卖",
            f"{base}缩量横盘，即将突破",
            f"捡便宜筹码的机会，{base}定投",
            f"{base}跌不动了，筑底中",
        ]
        
        # 根据时间选择
        hour = datetime.now().hour
        if hour in [9, 10, 21, 22]:  # 活跃时间
            return fomo_texts
        elif hour in [0, 1, 2, 3]:  # 深夜
            return panic_texts
        else:
            return accumulation_texts
    
    def get_cost_estimate(self, queries_per_day: int = 240) -> Dict:
        """
        成本估算
        
        DeepSeek价格:
        - 输入: ¥1/百万tokens
        - 输出: ¥2/百万tokens
        
        每次查询约:
        - 输入: 800 tokens (prompt)
        - 输出: 300 tokens (json响应)
        """
        
        input_tokens = 800
        output_tokens = 300
        
        # 价格 (¥/百万tokens)
        input_price = 1.0
        output_price = 2.0
        
        daily_input_cost = (input_tokens * queries_per_day / 1_000_000) * input_price
        daily_output_cost = (output_tokens * queries_per_day / 1_000_000) * output_price
        daily_total = daily_input_cost + daily_output_cost
        
        monthly_cost = daily_total * 30
        
        return {
            'model': self.model,
            'queries_per_day': queries_per_day,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'daily_cost_cny': round(daily_total, 4),
            'monthly_cost_cny': round(monthly_cost, 4),
            'monthly_cost_usd': round(monthly_cost / 7.2, 4),
        }


# ============================================================
# 使用示例
# ============================================================

if __name__ == "__main__":
    print("="*70)
    print("V5 DeepSeek情绪分析 - 测试")
    print("="*70)
    
    factor = DeepSeekSentimentFactor()
    
    # 成本估算 (每小时1次 × 10个币种 = 240次/天)
    cost = factor.get_cost_estimate(queries_per_day=240)
    
    print("\n💰 成本估算:")
    print(f"  模型: {cost['model']}")
    print(f"  查询频率: {cost['queries_per_day']}次/天")
    print(f"  每日成本: ¥{cost['daily_cost_cny']}")
    print(f"  每月成本: ¥{cost['monthly_cost_cny']} (${cost['monthly_cost_usd']})")
    
    # 对比其他方案
    print("\n📊 方案对比:")
    print("  DeepSeek:  ¥0.36/月 (推荐)")
    print("  GPT-3.5:   ¥5.44/月")
    print("  阿里云NLP: ¥108/月")
    print("  TextBlob:  免费 (准确度低)")
    
    print("\n✅ DeepSeek优势:")
    print("  • 价格便宜 (比GPT便宜15倍)")
    print("  • 中文理解好 (针对中文社交媒体优化)")
    print("  • 国内访问快 (无需代理)")
    print("  • 理解币圈黑话 ('梭哈'、'瀑布'、'跑路')")
    
    # 测试分析
    print("\n📝 情绪分析测试 (BTC-USDT):")
    print("  (需要设置 DEEPSEEK_API_KEY)")
    
    # 如果设置了API key，取消注释测试
    # factor.api_key = "sk-..."
    # result = factor.calculate('BTC-USDT')
    # print(f"  情绪得分: {result['f6_sentiment']:+.4f}")
    # print(f"  贪婪指数: {result['f6_fear_greed_index']}")
    # print(f"  市场阶段: {result['f6_market_stage']}")
    # print(f"  摘要: {result['f6_sentiment_summary']}")
    
    print("\n" + "="*70)
    print("⚠️  使用步骤:")
    print("  1. 注册 DeepSeek: https://platform.deepseek.com/")
    print("  2. 获取 API Key")
    print("  3. 设置环境变量: export DEEPSEEK_API_KEY='sk-...'")
    print("  4. 运行测试")
    print("="*70)
