"""
时序交叉验证模块

针对金融时间序列数据的交叉验证，避免未来数据泄露
"""

import numpy as np
import pandas as pd
from typing import List, Tuple, Generator
from sklearn.model_selection import BaseCrossValidator


class TimeSeriesSplit(BaseCrossValidator):
    """
    时间序列交叉验证分割器
    
    特点：
    - 训练集只包含过去数据
    - 验证集只包含未来数据
    - 模拟真实交易场景
    """
    
    def __init__(self, n_splits: int = 5, test_size: int = None, gap: int = 0):
        """
        Args:
            n_splits: 分割次数
            test_size: 验证集大小，None则自动计算
            gap: 训练集和验证集之间的间隔（防止泄露）
        """
        self.n_splits = n_splits
        self.test_size = test_size
        self.gap = gap
    
    def split(self, X, y=None, groups=None):
        """生成训练和验证索引"""
        n_samples = len(X)
        indices = np.arange(n_samples)
        
        if self.test_size is None:
            test_size = n_samples // (self.n_splits + 1)
        else:
            test_size = self.test_size
        
        for i in range(self.n_splits):
            # 验证集结束位置
            test_end = n_samples - i * test_size
            # 验证集开始位置
            test_start = test_end - test_size
            # 训练集结束位置（考虑gap）
            train_end = test_start - self.gap
            
            if train_end <= 0:
                break
            
            train_indices = indices[:train_end]
            test_indices = indices[test_start:test_end]
            
            yield train_indices, test_indices
    
    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_splits


class PurgedKFold(BaseCrossValidator):
    """
    清洗K折交叉验证
    
    在训练集和验证集之间添加清洗期，防止重叠样本泄露
    参考：Advances in Financial Machine Learning (Marcos Lopez de Prado)
    """
    
    def __init__(self, n_splits: int = 5, purge_gap: int = 10):
        """
        Args:
            n_splits: 折数
            purge_gap: 清洗期大小（样本数）
        """
        self.n_splits = n_splits
        self.purge_gap = purge_gap
    
    def split(self, X, y=None, groups=None):
        """生成带清洗期的分割"""
        n_samples = len(X)
        fold_size = n_samples // self.n_splits
        
        for i in range(self.n_splits):
            # 验证集范围
            test_start = i * fold_size
            test_end = min((i + 1) * fold_size, n_samples)
            
            # 训练集范围（排除验证集和清洗期）
            train_indices = list(range(0, test_start - self.purge_gap)) + \
                          list(range(test_end + self.purge_gap, n_samples))
            
            test_indices = list(range(test_start, test_end))
            
            yield np.array(train_indices), np.array(test_indices)
    
    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_splits


def time_series_cv_score(model, X: pd.DataFrame, y: pd.Series, 
                         cv=None, metric='ic') -> dict:
    """
    时序交叉验证评分
    
    Args:
        model: 模型对象（需要有fit和predict方法）
        X: 特征矩阵
        y: 目标变量
        cv: 交叉验证分割器，None则使用默认TimeSeriesSplit
        metric: 评估指标 ('ic', 'rmse', 'mae')
        
    Returns:
        包含各折分数和统计信息的字典
    """
    if cv is None:
        cv = TimeSeriesSplit(n_splits=5, gap=24)  # 默认6小时间隔
    
    scores = []
    fold_details = []
    
    print("="*60)
    print("📊 Time-Series Cross Validation")
    print("="*60)
    
    for fold, (train_idx, test_idx) in enumerate(cv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        
        # 训练模型
        model.fit(X_train, y_train)
        
        # 预测
        y_pred = model.predict(X_test)
        
        # 计算指标
        if metric == 'ic':
            # 信息系数（Pearson相关系数）
            score = np.corrcoef(y_test, y_pred)[0, 1]
            if np.isnan(score):
                score = 0
        elif metric == 'rmse':
            score = -np.sqrt(np.mean((y_test - y_pred)**2))  # 负值越大越好
        elif metric == 'mae':
            score = -np.mean(np.abs(y_test - y_pred))
        else:
            raise ValueError(f"Unknown metric: {metric}")
        
        scores.append(score)
        fold_details.append({
            'fold': fold + 1,
            'train_size': len(train_idx),
            'test_size': len(test_idx),
            'score': score
        })
        
        print(f"  Fold {fold+1}: Train={len(train_idx)}, Test={len(test_idx)}, IC={score:.4f}")
    
    # 统计结果
    mean_score = np.mean(scores)
    std_score = np.std(scores)
    
    print(f"\n{'='*60}")
    print(f"CV Results ({metric.upper()}):")
    print(f"  Mean: {mean_score:.4f}")
    print(f"  Std:  {std_score:.4f}")
    print(f"  Min:  {np.min(scores):.4f}")
    print(f"  Max:  {np.max(scores):.4f}")
    print(f"{'='*60}")
    
    return {
        'mean_score': mean_score,
        'std_score': std_score,
        'scores': scores,
        'fold_details': fold_details
    }


def create_walk_forward_splits(df: pd.DataFrame, train_days: int = 30, 
                               test_days: int = 7, step_days: int = 7) -> Generator:
    """
    生成Walk-Forward分割
    
    每次滚动窗口，用过去N天训练，预测未来M天
    
    Args:
        df: DataFrame（需要有时间索引）
        train_days: 训练集天数
        test_days: 测试集天数
        step_days: 滚动步长
        
    Yields:
        (train_df, test_df, train_start, train_end, test_start, test_end)
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'timestamp' in df.columns:
            df = df.set_index('timestamp')
        else:
            raise ValueError("DataFrame needs DatetimeIndex or 'timestamp' column")
    
    start_date = df.index.min()
    end_date = df.index.max()
    
    current_train_start = start_date
    
    while True:
        train_start = current_train_start
        train_end = train_start + pd.Timedelta(days=train_days)
        test_start = train_end
        test_end = test_start + pd.Timedelta(days=test_days)
        
        if test_end > end_date:
            break
        
        train_df = df[(df.index >= train_start) & (df.index < train_end)]
        test_df = df[(df.index >= test_start) & (df.index < test_end)]
        
        if len(train_df) > 0 and len(test_df) > 0:
            yield train_df, test_df, train_start, train_end, test_start, test_end
        
        current_train_start += pd.Timedelta(days=step_days)


if __name__ == '__main__':
    # 测试
    from sklearn.linear_model import LinearRegression
    
    # 创建测试数据
    np.random.seed(42)
    n_samples = 1000
    X = pd.DataFrame(np.random.randn(n_samples, 5), columns=['f1', 'f2', 'f3', 'f4', 'f5'])
    y = pd.Series(X.sum(axis=1) + np.random.randn(n_samples) * 0.5)
    
    # 时序交叉验证
    model = LinearRegression()
    results = time_series_cv_score(model, X, y, metric='ic')
    
    print(f"\nFinal Score: {results['mean_score']:.4f} ± {results['std_score']:.4f}")
