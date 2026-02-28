#!/usr/bin/env python3
"""
情绪数据定时收集脚本
每小时运行一次，获取最新市场情绪
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# 添加项目路径
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor


def collect_sentiment():
    """收集主要币种情绪数据"""
    symbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT']
    
    factor = DeepSeekSentimentFactor()
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始收集情绪数据...")
    
    for symbol in symbols:
        try:
            # 使用简化的提示词获取情绪
            texts = [f"{symbol} 最新市场情绪和价格走势分析"]
            result = factor.analyze_sentiment(texts, symbol=symbol)
            
            sentiment = result.get('sentiment_score', 0)
            summary = result.get('summary', '')[:100]
            
            print(f"  {symbol}: 情绪={sentiment:.2f}, 摘要={summary}...")
            
        except Exception as e:
            print(f"  {symbol}: 获取失败 - {e}")
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 情绪数据收集完成")


if __name__ == '__main__':
    collect_sentiment()
