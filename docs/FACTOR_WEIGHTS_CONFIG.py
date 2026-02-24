"""
V5 因子权重分配方案

新增 f6_sentiment (情绪因子) 后的权重配置
"""

# ============================================================
# 方案1: 保守分配 (推荐初始使用)
# ============================================================

CONSERVATIVE_WEIGHTS = {
    # 原有因子 - 保持主导
    'f1_mom_5d': 0.15,           # 短期动量 (略降)
    'f2_mom_20d': 0.25,          # 中期动量 (核心)
    'f3_vol_adj_ret_20d': 0.15,  # 波动率调整收益
    'f4_volume_expansion': 0.15, # 成交量扩张
    'f5_rsi_trend_confirm': 0.15, # RSI趋势确认
    
    # 新增因子
    'f6_sentiment': 0.15,        # 情绪因子 (保守起步)
}
# 总计: 1.00


# ============================================================
# 方案2: 动态权重 (根据市场状态调整)
# ============================================================

DYNAMIC_WEIGHTS_STRATEGY = """
动态权重逻辑:

1. Trending市场 (趋势明显)
   - 提高动量因子权重
   - 降低情绪因子权重 (情绪会滞后)
   
   weights = {
       'f1_mom_5d': 0.20,
       'f2_mom_20d': 0.30,      # 趋势核心
       'f3_vol_adj_ret_20d': 0.15,
       'f4_volume_expansion': 0.15,
       'f5_rsi_trend_confirm': 0.10,
       'f6_sentiment': 0.10,     # 情绪降权
   }

2. Sideways市场 (震荡)
   - 提高情绪因子权重
   - 提高均值回归因子权重
   
   weights = {
       'f1_mom_5d': 0.10,
       'f2_mom_20d': 0.15,
       'f3_vol_adj_ret_20d': 0.15,
       'f4_volume_expansion': 0.20,
       'f5_rsi_trend_confirm': 0.15,
       'f6_sentiment': 0.25,     # 情绪重要
   }

3. Risk-Off市场 (恐慌)
   - 情绪因子极端值时反向操作
   - 贪婪减仓，恐惧加仓
   
   special_logic = {
       'f6_sentiment': 'contrarian',  # 反向操作
       'threshold_greedy': 0.7,       # >0.7 减仓
       'threshold_fearful': -0.7,     # <-0.7 加仓
   }
"""


# ============================================================
# 方案3: ML自动学习 (LightGBM决定)
# ============================================================

ML_AUTO_WEIGHTS = """
LightGBM自动学习权重:

不需要手动设置权重，而是:
1. 提供所有6个因子作为特征
2. LightGBM自动学习每个因子的重要性
3. 根据历史表现自动调整

特征输入:
- X = [f1, f2, f3, f4, f5, f6]
- y = 未来1小时收益率

LightGBM输出:
- feature_importance: 自动排序因子重要性
- 自动给重要因子更高权重

优点:
- 无需人工调参
- 自适应市场变化
- 可发现非线性组合

缺点:
- 需要大量历史数据
- 可能过拟合
- 解释性差
"""


# ============================================================
# 方案4: 分层权重 (推荐最终方案)
# ============================================================

HIERARCHICAL_WEIGHTS = {
    # 第一层: 基础因子 (技术)
    'technical_factors': {
        'weight': 0.60,  # 技术因子占60%
        'factors': {
            'f1_mom_5d': 0.15,
            'f2_mom_20d': 0.25,
            'f3_vol_adj_ret_20d': 0.10,
            'f4_volume_expansion': 0.05,
            'f5_rsi_trend_confirm': 0.05,
        }
    },
    
    # 第二层: AI因子 (情绪)
    'ai_factors': {
        'weight': 0.25,  # AI因子占25%
        'factors': {
            'f6_sentiment': 0.25,
        }
    },
    
    # 第三层: 动态调整因子
    'dynamic_adjustment': {
        'weight': 0.15,  # 根据市场状态分配
        'allocation_rules': {
            'trending': 'add to f2',
            'sideways': 'add to f6',
            'risk_off': 'add to cash',
        }
    }
}


# ============================================================
# 实施方案代码
# ============================================================

WEIGHTS_CONFIG = """
# configs/live_20u_real.yaml

alpha_weights:
  # 方案选择: conservative | dynamic | ml_auto | hierarchical
  scheme: hierarchical
  
  # 保守方案权重
  conservative:
    f1_mom_5d: 0.15
    f2_mom_20d: 0.25
    f3_vol_adj_ret_20d: 0.15
    f4_volume_expansion: 0.15
    f5_rsi_trend_confirm: 0.15
    f6_sentiment: 0.15
  
  # 动态方案阈值
  dynamic:
    base_weights:
      f1_mom_5d: 0.15
      f2_mom_20d: 0.25
      f3_vol_adj_ret_20d: 0.15
      f4_volume_expansion: 0.15
      f5_rsi_trend_confirm: 0.15
      f6_sentiment: 0.15
    
    adjustments:
      trending:
        f2_mom_20d: +0.05
        f6_sentiment: -0.05
      sideways:
        f6_sentiment: +0.10
        f1_mom_5d: -0.05
      risk_off:
        f6_sentiment: special  # 反向操作
  
  # 情绪反向操作阈值
  sentiment_contrarian:
    enabled: true
    greedy_threshold: 0.7    # >0.7 触发减仓
    fearful_threshold: -0.7  # <-0.7 触发加仓
    position_adjustment: 0.3  # 调整30%仓位

# ML训练配置
ml_training:
  features:
    - f1_mom_5d
    - f2_mom_20d
    - f3_vol_adj_ret_20d
    - f4_volume_expansion
    - f5_rsi_trend_confirm
    - f6_sentiment          # 新增
  target: fwd_ret_1h
  min_samples: 100
"""


# ============================================================
# 实际代码实现
# ============================================================

class FactorWeightManager:
    """
    因子权重管理器
    """
    
    def __init__(self, config):
        self.scheme = config.get('scheme', 'conservative')
        self.weights = config.get(self.scheme, {})
        self.sentiment_config = config.get('sentiment_contrarian', {})
    
    def get_weights(self, market_state: str = 'sideways', sentiment_score: float = 0.0) -> dict:
        """
        获取当前权重
        
        Args:
            market_state: 'trending' | 'sideways' | 'risk_off'
            sentiment_score: 情绪得分 -1.0 ~ +1.0
        
        Returns:
            dict: 各因子权重
        """
        if self.scheme == 'conservative':
            return self.weights
        
        elif self.scheme == 'dynamic':
            return self._get_dynamic_weights(market_state)
        
        elif self.scheme == 'hierarchical':
            return self._get_hierarchical_weights(market_state, sentiment_score)
        
        return self.weights
    
    def _get_dynamic_weights(self, market_state: str) -> dict:
        """动态权重计算"""
        base = self.weights.get('base_weights', {}).copy()
        adjustments = self.weights.get('adjustments', {}).get(market_state, {})
        
        # 应用调整
        for factor, delta in adjustments.items():
            if factor in base:
                base[factor] = max(0.05, min(0.40, base[factor] + delta))
        
        # 归一化
        total = sum(base.values())
        return {k: v/total for k, v in base.items()}
    
    def _get_hierarchical_weights(self, market_state: str, sentiment: float) -> dict:
        """分层权重计算"""
        weights = {
            'f1_mom_5d': 0.15,
            'f2_mom_20d': 0.25,
            'f3_vol_adj_ret_20d': 0.10,
            'f4_volume_expansion': 0.05,
            'f5_rsi_trend_confirm': 0.05,
            'f6_sentiment': 0.15,
        }
        
        # 根据市场状态调整
        if market_state == 'trending':
            weights['f2_mom_20d'] += 0.05
            weights['f6_sentiment'] -= 0.05
        elif market_state == 'sideways':
            weights['f6_sentiment'] += 0.10
            weights['f1_mom_5d'] -= 0.05
        
        # 情绪反向操作
        if self.sentiment_config.get('enabled'):
            greedy = self.sentiment_config.get('greedy_threshold', 0.7)
            fearful = self.sentiment_config.get('fearful_threshold', -0.7)
            
            if sentiment > greedy:
                # 极度贪婪，降低情绪因子权重 (因为会触发减仓)
                weights['f6_sentiment'] *= 0.5
            elif sentiment < fearful:
                # 极度恐惧，提高情绪因子权重 (因为会触发加仓)
                weights['f6_sentiment'] *= 1.5
        
        # 归一化
        total = sum(weights.values())
        return {k: round(v/total, 4) for k, v in weights.items()}


# ============================================================
# 推荐配置 (最终建议)
# ============================================================

RECOMMENDED_CONFIG = {
    'scheme': 'hierarchical',
    'weights': {
        'f1_mom_5d': 0.15,
        'f2_mom_20d': 0.25,
        'f3_vol_adj_ret_20d': 0.10,
        'f4_volume_expansion': 0.05,
        'f5_rsi_trend_confirm': 0.05,
        'f6_sentiment': 0.15,
        # 剩余 0.25 根据市场状态动态分配
    },
    'sentiment_adjustment': {
        'enabled': True,
        'greedy_threshold': 0.7,
        'fearful_threshold': -0.7,
        'position_impact': 0.30,  # 调整30%仓位
    }
}


if __name__ == "__main__":
    print("="*70)
    print("V5 因子权重分配方案")
    print("="*70)
    
    print("\n【方案1】保守分配 (推荐初始使用)")
    for k, v in CONSERVATIVE_WEIGHTS.items():
        print(f"  {k}: {v:.2f}")
    
    print("\n【方案4】分层权重 (推荐最终方案)")
    print("  技术因子: 60% (f1-f5)")
    print("  AI因子: 25% (f6_sentiment)")
    print("  动态调整: 15% (根据市场状态)")
    
    print("\n【情绪反向操作】")
    print("  贪婪(>0.7): 自动减仓30%")
    print("  恐惧(<-0.7): 自动加仓30%")
    
    print("\n" + "="*70)
    print("建议: 先用保守方案测试，稳定后切换到分层权重")
    print("="*70)
