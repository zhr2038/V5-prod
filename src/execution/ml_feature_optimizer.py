"""
ML特征工程优化模块

优化内容：
1. 移除高相关性特征
2. 特征选择
3. 防止未来数据泄露
"""

import numpy as np
import pandas as pd
from typing import List, Tuple
from pathlib import Path
from sklearn.feature_selection import mutual_info_regression


class FeatureEngineeringOptimizer:
    """特征工程优化器"""
    
    # 高相关性特征组（只保留一个）
    HIGH_CORR_GROUPS = [
        ['returns_1h', 'returns_6h', 'returns_24h'],  # 只保留returns_24h
        ['volatility_6h', 'volatility_24h'],  # 只保留volatility_24h
    ]
    
    # 要移除的特征（低信息增益）
    LOW_INFO_FEATURES = [
        'returns_1h',  # 与returns_6h/24h高度相关
        'returns_6h',  # 与returns_24h高度相关
        'volatility_6h',  # 与volatility_24h高度相关
    ]
    
    @staticmethod
    def remove_high_correlation_features(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
        """
        移除高相关性特征
        
        Args:
            df: 特征DataFrame
            threshold: 相关性阈值
            
        Returns:
            移除高相关性特征后的DataFrame
        """
        df = df.copy()
        
        # 计算相关性矩阵
        corr_matrix = df.corr().abs()
        
        # 上三角矩阵（避免重复）
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        
        # 找到高相关性特征对
        to_drop = set()
        for col in upper.columns:
            high_corr = upper[col][upper[col] > threshold].index.tolist()
            if high_corr:
                # 保留第一个，移除其他
                to_drop.update(high_corr)
        
        # 移除特征
        if to_drop:
            print(f"Removing high correlation features: {to_drop}")
            df = df.drop(columns=list(to_drop), errors='ignore')
        
        return df
    
    @staticmethod
    def select_features_by_importance(X: pd.DataFrame, y: pd.Series, n_features: int = 10) -> List[str]:
        """
        使用互信息选择重要特征
        
        Args:
            X: 特征矩阵
            y: 目标变量
            n_features: 选择的特征数
            
        Returns:
            选中的特征名列表
        """
        # 处理NaN
        X_clean = X.fillna(X.median())
        y_clean = y.fillna(y.median())
        
        # 计算互信息
        mi_scores = mutual_info_regression(X_clean, y_clean, random_state=42)
        
        # 创建特征重要性DataFrame
        importance_df = pd.DataFrame({
            'feature': X.columns,
            'mi_score': mi_scores
        }).sort_values('mi_score', ascending=False)
        
        print("\nFeature Importance (Mutual Information):")
        for idx, row in importance_df.head(15).iterrows():
            print(f"  {row['feature']}: {row['mi_score']:.4f}")
        
        # 选择top N特征
        selected = importance_df.head(n_features)['feature'].tolist()
        
        return selected
    
    @staticmethod
    def create_time_aware_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        创建时间感知特征（避免未来数据泄露）
        
        Args:
            df: 原始DataFrame
            
        Returns:
            添加时间特征后的DataFrame
        """
        df = df.copy()
        
        # 时间衰减权重（近期数据更重要）
        if 'timestamp' in df.columns:
            df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour
            df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.dayofweek
        
        # 滚动特征（只用过去数据，不用未来）
        if 'close' in df.columns:
            # 过去N期的统计（不包含当期）
            df['past_5d_mean'] = df['close'].shift(1).rolling(5*24).mean()
            df['past_5d_std'] = df['close'].shift(1).rolling(5*24).std()
            
        return df
    
    @staticmethod
    def check_feature_leakage(df: pd.DataFrame, target_col: str = 'target') -> List[str]:
        """
        检查可能导致泄露的特征
        
        Args:
            df: 特征DataFrame
            target_col: 目标列名
            
        Returns:
            可能导致泄露的特征列表
        """
        leaky_features = []
        
        for col in df.columns:
            if col == target_col:
                continue
            
            # 检查特征名是否包含未来信息
            if any(keyword in col.lower() for keyword in ['future', 'next', 'lead', 'ahead']):
                leaky_features.append(col)
                print(f"⚠️  Potential leakage: {col}")
        
        return leaky_features


def optimize_features_for_training(df: pd.DataFrame, y: pd.Series = None) -> pd.DataFrame:
    """
    优化特征用于训练
    
    主函数，整合所有优化步骤
    """
    optimizer = FeatureEngineeringOptimizer()
    
    print("="*60)
    print("🔧 ML Feature Engineering Optimization")
    print("="*60)
    
    original_features = list(df.columns)
    print(f"\nOriginal features ({len(original_features)}): {original_features}")
    
    # 1. 检查标签泄漏
    leaky = optimizer.check_feature_leakage(df)
    if leaky:
        print(f"\n⚠️  Removing {len(leaky)} leaky features: {leaky}")
        df = df.drop(columns=leaky, errors='ignore')
    
    # 2. 移除预定义的低效特征
    low_info_cols = [c for c in optimizer.LOW_INFO_FEATURES if c in df.columns]
    if low_info_cols:
        print(f"\n🗑️  Removing low-info features: {low_info_cols}")
        df = df.drop(columns=low_info_cols, errors='ignore')
    
    # 3. 移除高相关性特征
    df = optimizer.remove_high_correlation_features(df, threshold=0.9)
    
    # 4. 添加时间感知特征
    df = optimizer.create_time_aware_features(df)
    
    # 5. 如果提供了目标变量，进行特征选择
    if y is not None:
        selected = optimizer.select_features_by_importance(df, y, n_features=12)
        df = df[selected]
    
    remaining_features = list(df.columns)
    print(f"\n✅ Optimized features ({len(remaining_features)}): {remaining_features}")
    
    return df


if __name__ == '__main__':
    # 测试
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    
    # 创建测试数据
    np.random.seed(42)
    n_samples = 1000
    
    test_df = pd.DataFrame({
        'returns_1h': np.random.randn(n_samples),
        'returns_6h': np.random.randn(n_samples) * 0.8 + np.random.randn(n_samples) * 0.2,
        'returns_24h': np.random.randn(n_samples) * 0.9 + np.random.randn(n_samples) * 0.1,
        'momentum_5d': np.random.randn(n_samples),
        'volatility_6h': np.abs(np.random.randn(n_samples)),
        'volatility_24h': np.abs(np.random.randn(n_samples)) * 0.8,
        'rsi': np.random.uniform(0, 100, n_samples),
        'macd': np.random.randn(n_samples),
    })
    
    y = np.random.randn(n_samples)
    
    optimized_df = optimize_features_for_training(test_df, y)
    print(f"\nFinal shape: {optimized_df.shape}")
