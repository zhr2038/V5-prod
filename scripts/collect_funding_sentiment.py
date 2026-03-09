#!/usr/bin/env python3
"""
V5 资金费率情绪指标收集器（扩展版）

使用OKX资金费率作为市场情绪代理指标：
- 覆盖更多币种（大盘+中盘+小盘），更全面反映市场情绪
- 按市值加权：大盘50%、中盘30%、小盘20%

资金费率每8小时结算一次，但API可以获取实时预测资金费率
"""

import os
import sys
import json
import requests
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def get_cache_dir() -> Path:
    return PROJECT_ROOT / "data" / "sentiment_cache"


# 扩展币种列表，按市值分层
SYMBOLS_BY_TIER = {
    'large': {  # 大盘币 50%权重
        'BTC-USDT': 0.25,  # BTC占大盘一半
        'ETH-USDT': 0.25,
    },
    'mid': {    # 中盘币 30%权重
        'SOL-USDT': 0.10,
        'ADA-USDT': 0.08,
        'AVAX-USDT': 0.07,
        'DOT-USDT': 0.05,
    },
    'small': {  # 小盘币 20%权重（反映散户情绪）
        'DOGE-USDT': 0.08,
        'UNI-USDT': 0.06,
        'PEPE-USDT': 0.04,
        'LTC-USDT': 0.02,
    }
}

# 合并所有币种
def get_all_symbols():
    """获取所有监控币种"""
    all_symbols = {}
    for tier, symbols in SYMBOLS_BY_TIER.items():
        for sym, weight_in_tier in symbols.items():
            # 计算总权重
            tier_weight = {'large': 0.50, 'mid': 0.30, 'small': 0.20}[tier]
            total_weight = tier_weight * weight_in_tier / sum(symbols.values())
            all_symbols[sym] = {
                'tier': tier,
                'tier_weight': tier_weight,
                'weight_in_tier': weight_in_tier,
                'total_weight': total_weight
            }
    return all_symbols


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
    # 小盘币波动更大，调整极值
    max_fr = 0.001  # 0.1% (放宽到应对小盘币高波动)
    min_fr = -0.001  # -0.1%
    
    if funding_rate >= 0:
        sentiment = min(funding_rate / max_fr, 1.0)
    else:
        sentiment = max(funding_rate / abs(min_fr), -1.0)
    
    # 恐惧贪婪指数 (0-100)
    fear_greed = int((sentiment + 1) * 50)
    
    # 市场阶段判断
    if sentiment > 0.7:
        stage = "fomo"
        summary = "资金费率极高，多头过度乐观，市场处于狂热阶段"
    elif sentiment > 0.3:
        stage = "optimistic"
        summary = "资金费率偏高，市场情绪乐观"
    elif sentiment > -0.3:
        stage = "neutral"
        summary = "资金费率接近平衡，情绪中性"
    elif sentiment > -0.7:
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
        'confidence': 0.8
    }


def collect_funding_sentiment():
    """收集资金费率情绪（扩展版，12个币种）"""
    
    all_symbols = get_all_symbols()
    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H')
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始收集资金费率情绪（{len(all_symbols)}个币种）...")
    
    # 按层级分组统计
    tier_sentiments = {'large': [], 'mid': [], 'small': []}
    
    for symbol_name, config in all_symbols.items():
        inst_id = symbol_name.replace('-USDT', '-USDT-SWAP')
        tier = config['tier']
        
        try:
            fr_data = get_okx_funding_rate(inst_id)
            funding_rate = fr_data['funding_rate']
            sentiment_data = funding_rate_to_sentiment(funding_rate)
            
            # 保存单个币种数据
            cache_data = {
                'f6_sentiment': sentiment_data['sentiment_score'],
                'f6_sentiment_magnitude': abs(sentiment_data['sentiment_score']),
                'f6_fear_greed_index': sentiment_data['fear_greed_index'],
                'f6_sentiment_summary': sentiment_data['summary'],
                'f6_sentiment_confidence': sentiment_data['confidence'],
                'f6_sentiment_source': 'funding_rate',
                'f6_market_stage': sentiment_data['market_stage'],
                'raw_funding_rate': sentiment_data['raw_funding_rate'],
                'tier': tier,
                'weight': config['total_weight'],
                'collected_at': datetime.now().isoformat()
            }
            
            cache_file = cache_dir / f"funding_{symbol_name}_{timestamp}.json"
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # 计入层级统计
            tier_sentiments[tier].append({
                'symbol': symbol_name,
                'sentiment': sentiment_data['sentiment_score'],
                'funding_rate': funding_rate,
                'weight': config['total_weight']
            })
            
            tier_cn = {'large': '大盘', 'mid': '中盘', 'small': '小盘'}[tier]
            print(f"  [{tier_cn}] {symbol_name}: 费率={funding_rate:.6f}, 情绪={sentiment_data['sentiment_score']:.2f}")
            
        except Exception as e:
            print(f"  {symbol_name}: 处理失败 - {e}")
    
    # 计算加权平均情绪
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 计算加权平均...")
    
    weighted_sum = 0
    total_weight = 0
    
    for tier, items in tier_sentiments.items():
        if not items:
            continue
        tier_weight = {'large': 0.50, 'mid': 0.30, 'small': 0.20}[tier]
        tier_avg = sum(item['sentiment'] for item in items) / len(items)
        weighted_sum += tier_avg * tier_weight
        total_weight += tier_weight
        
        tier_cn = {'large': '大盘', 'mid': '中盘', 'small': '小盘'}[tier]
        print(f"  {tier_cn}平均: {tier_avg:.3f} (权重{tier_weight*100}%)")
    
    overall_sentiment = weighted_sum / total_weight if total_weight > 0 else 0
    
    print(f"\n  综合资金费率情绪: {overall_sentiment:.3f}")
    
    # 保存综合市场情绪
    overall_data = {
        'f6_sentiment': round(overall_sentiment, 4),
        'f6_sentiment_magnitude': abs(round(overall_sentiment, 4)),
        'f6_fear_greed_index': int((overall_sentiment + 1) * 50),
        'f6_sentiment_summary': f'资金费率综合情绪: {overall_sentiment:.3f} (大盘{len(tier_sentiments["large"])}个+中盘{len(tier_sentiments["mid"])}个+小盘{len(tier_sentiments["small"])}个)',
        'f6_sentiment_confidence': 0.85,
        'f6_sentiment_source': 'funding_rate_composite',
        'f6_market_stage': 'optimistic' if overall_sentiment > 0.3 else 'neutral' if overall_sentiment > -0.3 else 'pessimistic',
        'tier_breakdown': {
            tier: {'avg': sum(i['sentiment'] for i in items)/len(items) if items else 0, 'count': len(items)}
            for tier, items in tier_sentiments.items()
        },
        'collected_at': datetime.now().isoformat()
    }
    
    overall_file = cache_dir / f"funding_COMPOSITE_{timestamp}.json"
    with open(overall_file, 'w') as f:
        json.dump(overall_data, f, indent=2)
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 资金费率情绪收集完成")


if __name__ == '__main__':
    collect_funding_sentiment()
