#!/usr/bin/env python3
"""
V5 资金费率情绪指标收集器

使用OKX资金费率作为市场情绪代理指标：
- 高正资金费率 (>0.01%) = 多头过度乐观，情绪偏高，可能见顶
- 高负资金费率 (<-0.01%) = 空头过度悲观，情绪偏低，可能见底
- 接近0 = 情绪中性

资金费率每8小时结算一次，但API可以获取实时预测资金费率
"""

import os
import sys
import json
import requests
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')


def get_okx_funding_rate(inst_id: str) -> dict:
    """获取OKX资金费率"""
    try:
        url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if data.get('code') == '0' and data.get('data'):
            item = data['data'][0]
            return {
                'funding_rate': float(item.get('fundingRate', 0)),
                'next_funding_time': item.get('nextFundingTime', ''),
                'method': 'okx_api'
            }
    except Exception as e:
        print(f"[FundingRate] 获取 {inst_id} 失败: {e}")
    
    return {'funding_rate': 0, 'method': 'fallback'}


def funding_rate_to_sentiment(funding_rate: float) -> dict:
    """
    将资金费率转换为情绪值 (-1 ~ +1)
    
    逻辑：
    - 资金费率 > 0.01% (0.0001): 多头付空头，市场过度乐观，情绪 > 0
    - 资金费率 < -0.01% (-0.0001): 空头付多头，市场过度悲观，情绪 < 0
    - 资金费率接近0: 情绪中性
    """
    # 归一化：将资金费率映射到 -1 ~ +1
    # 假设极值：+0.0005 (0.05%) = 极度乐观 (+1)
    # 假设极值：-0.0005 (-0.05%) = 极度悲观 (-1)
    
    max_fr = 0.0005  # 0.05%
    min_fr = -0.0005  # -0.05%
    
    if funding_rate >= 0:
        sentiment = min(funding_rate / max_fr, 1.0)
    else:
        sentiment = max(funding_rate / abs(min_fr), -1.0)
    
    # 恐惧贪婪指数 (0-100)
    fear_greed = int((sentiment + 1) * 50)
    
    # 市场阶段判断
    if sentiment > 0.6:
        stage = "fomo"
        summary = "资金费率极高，多头过度乐观，市场处于狂热阶段"
    elif sentiment > 0.2:
        stage = "optimistic"
        summary = "资金费率偏高，市场情绪乐观"
    elif sentiment > -0.2:
        stage = "neutral"
        summary = "资金费率接近平衡，情绪中性"
    elif sentiment > -0.6:
        stage = "pessimistic"
        summary = "资金费率为负，市场情绪悲观"
    else:
        stage = "panic"
        summary = "资金费率极低，空头过度悲观，市场处于恐慌阶段"
    
    return {
        'sentiment_score': round(sentiment, 4),
        'fear_greed_index': fear_greed,
        'market_stage': stage,
        'summary': summary,
        'raw_funding_rate': funding_rate,
        'confidence': 0.8  # 资金费率是客观数据，置信度较高
    }


def collect_funding_sentiment():
    """收集主要币种的资金费率情绪"""
    symbols = [
        ('BTC-USDT', 'BTC-USDT-SWAP'),
        ('ETH-USDT', 'ETH-USDT-SWAP'),
        ('SOL-USDT', 'SOL-USDT-SWAP'),
        ('BNB-USDT', 'BNB-USDT-SWAP')
    ]
    
    cache_dir = Path('/home/admin/clawd/v5-trading-bot/data/sentiment_cache')
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H')
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始收集资金费率情绪...")
    
    for symbol_name, inst_id in symbols:
        try:
            # 获取资金费率
            fr_data = get_okx_funding_rate(inst_id)
            funding_rate = fr_data['funding_rate']
            
            # 转换为情绪值
            sentiment_data = funding_rate_to_sentiment(funding_rate)
            
            # 构建缓存文件格式（与DeepSeek格式兼容）
            cache_data = {
                'f6_sentiment': sentiment_data['sentiment_score'],
                'f6_sentiment_magnitude': abs(sentiment_data['sentiment_score']),
                'f6_fear_greed_index': sentiment_data['fear_greed_index'],
                'f6_sentiment_summary': sentiment_data['summary'],
                'f6_sentiment_confidence': sentiment_data['confidence'],
                'f6_sentiment_source': 'funding_rate',
                'f6_market_stage': sentiment_data['market_stage'],
                'raw_funding_rate': sentiment_data['raw_funding_rate'],
                'collected_at': datetime.now().isoformat()
            }
            
            # 保存缓存
            cache_file = cache_dir / f"funding_{symbol_name}_{timestamp}.json"
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            print(f"  {symbol_name}: 费率={funding_rate:.6f}, 情绪={sentiment_data['sentiment_score']:.2f}, 阶段={sentiment_data['market_stage']}")
            
        except Exception as e:
            print(f"  {symbol_name}: 处理失败 - {e}")
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 资金费率情绪收集完成")


if __name__ == '__main__':
    collect_funding_sentiment()
