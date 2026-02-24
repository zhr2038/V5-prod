#!/usr/bin/env python3
"""
V5 情绪因子集成测试

验证DeepSeek情绪分析能正确集成到V5中
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor
from datetime import datetime

def test_sentiment_integration():
    """测试情绪因子集成"""
    print("="*70)
    print("V5 DeepSeek情绪因子集成测试")
    print("="*70)
    
    # 初始化因子
    factor = DeepSeekSentimentFactor()
    
    # 测试币种列表
    test_symbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT']
    
    print(f"\n测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试币种: {', '.join(test_symbols)}")
    print()
    
    results = {}
    for symbol in test_symbols:
        try:
            result = factor.calculate(symbol)
            results[symbol] = result
            
            # 输出结果
            mood = "🟢 贪婪" if result['f6_sentiment'] > 0.5 else "🔴 恐慌" if result['f6_sentiment'] < -0.5 else "⚪ 中性"
            print(f"{symbol}:")
            print(f"  情绪得分: {result['f6_sentiment']:+.4f} {mood}")
            print(f"  恐惧贪婪指数: {result['f6_fear_greed_index']}/100")
            print(f"  市场阶段: {result['f6_market_stage']}")
            print(f"  置信度: {result['f6_sentiment_confidence']:.2f}")
            print()
            
        except Exception as e:
            print(f"❌ {symbol} 分析失败: {e}")
            return False
    
    # 计算平均情绪
    avg_sentiment = sum(r['f6_sentiment'] for r in results.values()) / len(results)
    avg_fear_greed = sum(r['f6_fear_greed_index'] for r in results.values()) / len(results)
    
    print("-"*70)
    print("市场整体情绪:")
    print(f"  平均情绪得分: {avg_sentiment:+.4f}")
    print(f"  平均恐惧贪婪: {avg_fear_greed:.1f}/100")
    
    if avg_fear_greed < 20:
        print(f"  判断: 🔴 极度恐慌 - 可能是抄底机会")
    elif avg_fear_greed > 80:
        print(f"  判断: 🟢 极度贪婪 - 注意回调风险")
    else:
        print(f"  判断: ⚪ 情绪中性 - 正常交易")
    
    print("="*70)
    print("✅ 情绪因子集成测试通过!")
    print("="*70)
    
    return True

if __name__ == "__main__":
    success = test_sentiment_integration()
    sys.exit(0 if success else 1)
