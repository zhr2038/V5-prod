#!/usr/bin/env python3
"""
ML训练 - 修复版（移除泄露特征和高相关特征）
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path

# 加载数据
df = pd.read_csv('/home/admin/clawd/v5-trading-bot/reports/ml_training_data.csv')
print(f"Loaded {len(df)} samples")

# 修复后的特征列表 - 移除问题特征
# 移除: returns_1h/6h (与returns_24h相关), volatility_ratio (泄露嫌疑), rsi/bb_position (高度相关)
feature_cols = [
    'returns_24h',      # 只保留一个收益率特征
    'momentum_5d',
    'momentum_20d',
    'volatility_24h',   # 只保留一个波动率特征
    'volume_ratio',
    'obv',
    'macd',
    'macd_signal',
    'price_position',
]

# 移除NaN
df = df.dropna(subset=feature_cols + ['future_return_6h'])
print(f"After dropna: {len(df)} samples")

# 准备数据
X = df[feature_cols]
y = df['future_return_6h']

# 时间序列分割 (80/20)
split_idx = int(len(df) * 0.8)
X_train, X_valid = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_valid = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Train: {len(X_train)}, Valid: {len(X_valid)}")
print(f"Features: {feature_cols}")

# 训练模型 - 极简配置
model = lgb.LGBMRegressor(
    n_estimators=30,       # 极少树
    max_depth=3,           # 浅层
    num_leaves=7,          # 少叶子
    learning_rate=0.05,
    subsample=0.5,         # 50%采样
    colsample_bytree=0.5,  # 50%特征
    min_data_in_leaf=50,   # 最小样本
    reg_alpha=1.0,         # 强L1
    reg_lambda=1.0,        # 强L2
    random_state=42,
    verbose=-1
)

print("\nTraining...")
model.fit(
    X_train, y_train,
    eval_set=[(X_valid, y_valid)],
    callbacks=[lgb.early_stopping(stopping_rounds=5), lgb.log_evaluation(period=0)]
)

# 预测
train_pred = model.predict(X_train)
valid_pred = model.predict(X_valid)

# 计算IC
train_ic = np.corrcoef(y_train, train_pred)[0, 1]
valid_ic = np.corrcoef(y_valid, valid_pred)[0, 1]

print(f"\n{'='*60}")
print(f"Result:")
print(f"  Train IC: {train_ic:.4f}")
print(f"  Valid IC: {valid_ic:.4f}")
print(f"  Gap: {train_ic - valid_ic:.4f}")
print(f"{'='*60}")

# 特征重要性
importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\nFeature Importance:")
for _, row in importance.iterrows():
    print(f"  {row['feature']}: {row['importance']}")
