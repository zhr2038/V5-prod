"""
V5 情绪因子权重配置 - 生产环境推荐
"""

# ============================================================
# 推荐配置: 分层动态权重
# ============================================================

PRODUCTION_CONFIG = {
    # 基础技术因子权重 (70%)
    'base_weights': {
        'f1_mom_5d': 0.12,           # 短期动量 12%
        'f2_mom_20d': 0.20,          # 中期动量 20% (核心)
        'f3_vol_adj_ret_20d': 0.12,  # 波动率调整 12%
        'f4_volume_expansion': 0.13, # 成交量 13%
        'f5_rsi_trend_confirm': 0.13,# RSI趋势 13%
    },
    
    # 情绪因子权重 (15%基础 + 15%动态)
    'f6_sentiment': {
        'base_weight': 0.15,
        'dynamic_adjustment': True,
        'max_weight': 0.30,  # 最高可达30%
        'min_weight': 0.05,  # 最低5%
    },
    
    # 情绪反向操作策略
    'sentiment_contrarian': {
        'enabled': True,
        'greedy_threshold': 0.70,     # >0.7视为极度贪婪
        'fearful_threshold': -0.70,   # <-0.7视为极度恐惧
        'position_adjustment': 0.30,  # 调整30%仓位
        
        # 具体操作
        'actions': {
            'extreme_greed': {  # 情绪>0.7
                'factor_boost': -0.10,     # 降低情绪因子权重
                'position_scale': 0.70,    # 仓位降至70%
                'reason': 'FOMO预警:市场过热，降低风险敞口'
            },
            'extreme_fear': {   # 情绪<-0.7
                'factor_boost': +0.10,     # 提高情绪因子权重
                'position_scale': 1.20,    # 仓位增至120%(可加杠杆)
                'reason': '恐慌抄底:市场超跌，逆势加仓'
            },
            'neutral': {        # -0.7~0.7
                'factor_boost': 0,
                'position_scale': 1.0,
                'reason': '正常交易'
            }
        }
    },
    
    # 市场状态自适应
    'market_regime_adjustment': {
        'trending': {  # 趋势市场
            'f2_mom_20d_boost': +0.05,    # 加强趋势因子
            'f6_sentiment_boost': -0.05,  # 削弱情绪因子
            'note': '趋势明确，跟随趋势为主'
        },
        'sideways': {  # 震荡市场
            'f6_sentiment_boost': +0.10,  # 加强情绪因子
            'f1_mom_5d_boost': -0.03,     # 削弱短期动量
            'note': '趋势不明，情绪驱动明显'
        },
        'risk_off': {  # 风险规避
            'all_factors_scale': 0.0,     # 所有因子归零
            'f6_sentiment_override': True, # 情绪因子主导决策
            'note': '恐慌情绪决定是否抄底'
        }
    }
}


# ============================================================
# 实际权重计算示例
# ============================================================

def calculate_weights(market_state: str, sentiment_score: float) -> dict:
    """
    计算实际权重
    
    Args:
        market_state: 'trending' | 'sideways' | 'risk_off'
        sentiment_score: -1.0 ~ +1.0
    """
    config = PRODUCTION_CONFIG
    base = config['base_weights'].copy()
    
    # 基础情绪权重
    f6_weight = config['f6_sentiment']['base_weight']
    
    # 1. 应用市场状态调整
    regime_adj = config['market_regime_adjustment'].get(market_state, {})
    
    if 'f2_mom_20d_boost' in regime_adj:
        base['f2_mom_20d'] += regime_adj['f2_mom_20d_boost']
    if 'f6_sentiment_boost' in regime_adj:
        f6_weight += regime_adj['f6_sentiment_boost']
    
    # 2. 应用情绪反向操作
    contrarian = config['sentiment_contrarian']
    position_scale = 1.0
    
    if sentiment_score > contrarian['greedy_threshold']:
        # 极度贪婪
        f6_weight += contrarian['actions']['extreme_greed']['factor_boost']
        position_scale = contrarian['actions']['extreme_greed']['position_scale']
        action = 'reduce_position'
        
    elif sentiment_score < contrarian['fearful_threshold']:
        # 极度恐惧
        f6_weight += contrarian['actions']['extreme_fear']['factor_boost']
        position_scale = contrarian['actions']['extreme_fear']['position_scale']
        action = 'increase_position'
        
    else:
        action = 'normal'
    
    # 3. 限制权重范围
    f6_weight = max(config['f6_sentiment']['min_weight'],
                    min(config['f6_sentiment']['max_weight'], f6_weight))
    
    # 4. 构建最终权重
    weights = base.copy()
    weights['f6_sentiment'] = round(f6_weight, 4)
    
    # 5. 归一化
    total = sum(weights.values())
    weights = {k: round(v/total, 4) for k, v in weights.items()}
    
    return {
        'weights': weights,
        'position_scale': position_scale,
        'action': action,
        'market_state': market_state,
        'sentiment_score': sentiment_score
    }


# ============================================================
# 场景示例
# ============================================================

if __name__ == "__main__":
    print("="*70)
    print("V5 情绪因子权重配置 - 场景演示")
    print("="*70)
    
    scenarios = [
        ('trending', 0.20, '趋势上涨，情绪正常'),
        ('trending', 0.85, '趋势上涨，极度贪婪'),
        ('sideways', -0.10, '震荡市，情绪中性'),
        ('sideways', -0.80, '震荡市，极度恐慌'),
        ('risk_off', -0.90, 'Risk-Off，恐慌'),
    ]
    
    for market, sentiment, desc in scenarios:
        result = calculate_weights(market, sentiment)
        
        print(f"\n场景: {desc}")
        print(f"  市场状态: {market}")
        print(f"  情绪得分: {sentiment:+.2f}")
        print(f"  权重分配:")
        for factor, weight in result['weights'].items():
            bar = '█' * int(weight * 50)
            print(f"    {factor:20s}: {weight:.2%} {bar}")
        print(f"  仓位调整: {result['position_scale']:.0%}")
        print(f"  操作: {result['action']}")
    
    print("\n" + "="*70)
    print("核心逻辑:")
    print("  1. 趋势市场: 动量因子主导(40%), 情绪辅助(10%)")
    print("  2. 震荡市场: 情绪因子加强(25%), 捕捉情绪波动")
    print("  3. 极度贪婪: 减仓30%, 避开FOMO")
    print("  4. 极度恐慌: 加仓20%, 逆势抄底")
    print("="*70)
