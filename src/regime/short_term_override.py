"""
短线交易增强 - Risk-Off 机会覆盖

短线交易需要更灵敏地捕捉反弹机会。
当以下信号出现时，应该覆盖 Risk-Off 状态，允许交易：

1. Alpha评分异常高 (Top 3 平均分 > 1.0)
2. 达标币种数量多 (>5个且评分>0.1)
3. 资金费率情绪转正 (>0.2)
4. 24h价格反弹明显 (BTC/ETH 24h > +2%)

"""

import json
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class ShortTermOverride:
    """短线覆盖信号"""
    should_override: bool
    reason: str
    confidence: float  # 0-1
    new_multiplier: float  # 建议仓位倍数


def check_short_term_opportunity(
    alpha_scores: Dict[str, float],
    btc_change_24h: float = 0.0,
    eth_change_24h: float = 0.0,
    funding_sentiment: float = None,  # None表示从cache读取
    cache_dir: Path = None,
    min_score_threshold: float = 0.05
) -> ShortTermOverride:
    """
    检查是否应该覆盖 Risk-Off 状态
    
    返回: ShortTermOverride 对象
    """
    
    # 如果未提供funding_sentiment，从cache读取
    if funding_sentiment is None:
        cache_dir = cache_dir or (Path(__file__).resolve().parents[2] / 'data' / 'sentiment_cache')
        try:
            funding_files = sorted(cache_dir.glob('funding_COMPOSITE_*.json'))
            if funding_files:
                data = json.loads(funding_files[-1].read_text())
                funding_sentiment = float(data.get('f6_sentiment', 0.0))
        except:
            funding_sentiment = 0.0
    
    signals = []
    confidence_scores = []
    
    # 1. Alpha 评分信号
    if alpha_scores:
        # 取前5名
        top_scores = sorted(alpha_scores.values(), reverse=True)[:5]
        avg_top3 = sum(top_scores[:3]) / 3 if len(top_scores) >= 3 else 0
        
        # Top 3 平均分 > 1.0 且都 > 0.5 = 强烈信号
        if avg_top3 > 1.0 and all(s > 0.5 for s in top_scores[:3]):
            signals.append(f"Alpha强势: Top3 avg={avg_top3:.2f}")
            confidence_scores.append(0.9)
        # Top 3 平均分 > 0.5 = 中等信号
        elif avg_top3 > 0.5:
            signals.append(f"Alpha正向: Top3 avg={avg_top3:.2f}")
            confidence_scores.append(0.6)
        
        # 达标币数 > 5 且都 > 0.1
        qualified = [s for s in alpha_scores.values() if s >= 0.1]
        if len(qualified) >= 5 and all(s > 0.1 for s in qualified[:5]):
            signals.append(f"广度充足: {len(qualified)}币>0.1")
            confidence_scores.append(0.7)
    
    # 2. 价格反弹信号
    if btc_change_24h > 3.0 and eth_change_24h > 3.0:
        signals.append(f"价格反弹: BTC+{btc_change_24h:.1f}%, ETH+{eth_change_24h:.1f}%")
        confidence_scores.append(0.8)
    elif btc_change_24h > 1.5 or eth_change_24h > 1.5:
        signals.append(f"价格回升: BTC+{btc_change_24h:.1f}%, ETH+{eth_change_24h:.1f}%")
        confidence_scores.append(0.5)
    
    # 3. 资金费率情绪
    if funding_sentiment > 0.3:
        signals.append(f"资金费率乐观: {funding_sentiment:.2f}")
        confidence_scores.append(0.7)
    elif funding_sentiment > 0.1:
        signals.append(f"资金费率转正: {funding_sentiment:.2f}")
        confidence_scores.append(0.4)
    
    # 决策逻辑
    if len(signals) >= 2 and sum(confidence_scores) / len(confidence_scores) > 0.6:
        # 多信号确认，覆盖 Risk-Off
        avg_confidence = sum(confidence_scores) / len(confidence_scores)
        return ShortTermOverride(
            should_override=True,
            reason="; ".join(signals),
            confidence=avg_confidence,
            new_multiplier=0.5  # Risk-Off下允许50%仓位（比正常30%更高）
        )
    elif len(signals) >= 1 and max(confidence_scores) > 0.7:
        # 单一强信号，谨慎覆盖
        return ShortTermOverride(
            should_override=True,
            reason=signals[0],
            confidence=max(confidence_scores),
            new_multiplier=0.3  # 标准试探仓位
        )
    
    # 不满足覆盖条件
    return ShortTermOverride(
        should_override=False,
        reason="无强短线信号",
        confidence=0.0,
        new_multiplier=0.0
    )


def apply_short_term_override(
    regime_result: 'RegimeResult',
    alpha_scores: Dict[str, float],
    cache_dir: Path = None
) -> 'RegimeResult':
    """
    应用短线覆盖逻辑到 regime 结果
    
    如果满足短线机会条件，将 Risk-Off 改为 Sideways 并提高仓位倍数
    """
    from configs.schema import RegimeState
    
    # 只有在 Risk-Off 时才检查覆盖
    if regime_result.state != RegimeState.RISK_OFF:
        return regime_result
    
    # 获取必要数据
    cache_dir = cache_dir or (Path(__file__).resolve().parents[2] / 'data' / 'sentiment_cache')
    
    # 读取资金费率情绪
    funding_sentiment = 0.0
    try:
        funding_files = sorted(cache_dir.glob('funding_COMPOSITE_*.json'))
        if funding_files:
            data = json.loads(funding_files[-1].read_text())
            funding_sentiment = float(data.get('f6_sentiment', 0.0))
    except:
        pass
    
    # 检查覆盖条件
    override = check_short_term_opportunity(
        alpha_scores=alpha_scores,
        funding_sentiment=funding_sentiment
    )
    
    if override.should_override:
        # 创建新的结果，覆盖 Risk-Off
        from dataclasses import replace
        new_result = replace(
            regime_result,
            state=RegimeState.SIDEWAYS,  # 改为震荡市，允许交易
            multiplier=override.new_multiplier,
        )
        # 记录覆盖原因（通过其他方式记录到audit）
        print(f"[ShortTermOverride] Risk-Off → Sideways: {override.reason} (conf={override.confidence:.2f})")
        return new_result
    
    return regime_result


if __name__ == '__main__':
    # 测试
    test_scores = {
        'BTC/USDT': 1.5,
        'ETH/USDT': 1.3,
        'SOL/USDT': 1.1,
        'DOT/USDT': 0.9,
        'UNI/USDT': 0.8,
        'AAVE/USDT': 0.6,
    }
    
    result = check_short_term_opportunity(
        alpha_scores=test_scores,
        btc_change_24h=3.5,
        eth_change_24h=4.2,
        funding_sentiment=0.35
    )
    
    print(f"Should override: {result.should_override}")
    print(f"Reason: {result.reason}")
    print(f"Confidence: {result.confidence:.2f}")
    print(f"New multiplier: {result.new_multiplier}")
