#!/usr/bin/env python3
"""
ML训练 - 线性回归版（避免树模型过拟合）
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from pathlib import Path

# 加载数据
df = pd.read_csv('/home/admin/clawd/v5-trading-bot/reports/ml_training_data.csv')
print(f"Loaded {len(df)} samples")

# 使用更保守的特征
feature_cols = [
    'returns_24h',
    'momentum_5d', 
    'momentum_20d',
    'volatility_24h',
    'volume_ratio',
    'rsi',
]

# 移除NaN
df = df.dropna(subset=feature_cols + ['future_return_6h'])
print(f"After dropna: {len(df)} samples")

# 准备数据
X = df[feature_cols]
y = df['future_return_6h']

# 时间序列分割
split_idx = int(len(df) * 0.8)
X_train, X_valid = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_valid = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Train: {len(X_train)}, Valid: {len(X_valid)}")

# 标准化
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_valid_scaled = scaler.transform(X_valid)

# Ridge回归（L2正则化）
model = Ridge(alpha=10.0)  # 强正则化
model.fit(X_train_scaled, y_train)

# 预测
train_pred = model.predict(X_train_scaled)
valid_pred = model.predict(X_valid_scaled)

# 计算IC
train_ic = np.corrcoef(y_train, train_pred)[0, 1]
valid_ic = np.corrcoef(y_valid, valid_pred)[0, 1]

print(f"\n{'='*60}")
print(f"Ridge Regression Result:")
print(f"  Train IC: {train_ic:.4f}")
print(f"  Valid IC: {valid_ic:.4f}")
print(f"  Gap: {train_ic - valid_ic:.4f}")
print(f"{'='*60}")

# 系数
print("\nCoefficients:")
for feat, coef in zip(feature_cols, model.coef_):
    print(f"  {feat}: {coef:.6f}")

# 尝试不同的alpha值
print("\n\nTrying different alpha values:")
for alpha in [0.1, 1.0, 10.0, 100.0]:
    model = Ridge(alpha=alpha)
    model.fit(X_train_scaled, y_train)
    valid_pred = model.predict(X_valid_scaled)
    valid_ic = np.corrcoef(y_valid, valid_pred)[0, 1]
    print(f"  alpha={alpha}: Valid IC = {valid_ic:.4f}")
