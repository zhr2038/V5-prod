"""
Phase 3: Machine Learning Factor Model
使用LightGBM替代线性因子加权
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from dataclasses import dataclass
import json
from pathlib import Path

# 尝试导入lightgbm，如果没有安装则给出提示
try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    print("Warning: lightgbm not installed. Run: pip install lightgbm")

@dataclass
class MLFactorConfig:
    """ML因子模型配置 - 优化后减少过拟合"""
    model_type: str = 'lightgbm'  # lightgbm, xgboost, sklearn
    n_estimators: int = 100       # 保持
    max_depth: int = 4            # 降低从5到4，限制树深度
    learning_rate: float = 0.05   # 保持
    subsample: float = 0.7        # 降低从0.8到0.7，增加样本随机性
    colsample_bytree: float = 0.7 # 降低从0.8到0.7，增加特征随机性
    random_state: int = 42
    
    # 新增正则化参数
    num_leaves: int = 15          # 限制叶子节点数（默认31太大）
    min_data_in_leaf: int = 50    # 增加最小样本数（防止过拟合小样本）
    reg_alpha: float = 0.1        # L1正则化
    reg_lambda: float = 0.1       # L2正则化
    
    # 训练参数
    train_lookback_days: int = 60
    prediction_horizon: int = 6  # 预测未来6小时收益
    min_train_samples: int = 100
    early_stopping_rounds: int = 20  # 增加早停轮数

class MLFactorModel:
    """
    机器学习因子组合模型
    
    功能：
    1. 多因子特征工程
    2. LightGBM模型训练
    3. 收益率预测
    4. 特征重要性分析
    """
    
    def __init__(self, config: MLFactorConfig = None):
        self.config = config or MLFactorConfig()
        self.model = None
        self.feature_names = []
        self.is_trained = False
        
        if not LIGHTGBM_AVAILABLE and self.config.model_type == 'lightgbm':
            raise ImportError("lightgbm is required. Install with: pip install lightgbm")
    
    def feature_engineering(self, market_data: Dict) -> pd.DataFrame:
        """
        特征工程 - 从原始数据构建ML特征
        
        Features:
        - 价格动量特征
        - 波动率特征
        - 成交量特征
        - 技术指标特征
        """
        features = pd.DataFrame()
        
        for symbol, data in market_data.items():
            if 'close' not in data or len(data['close']) < 30:
                continue
            
            close = pd.Series(data['close'])
            volume = pd.Series(data.get('volume', [0]*len(close)))
            high = pd.Series(data.get('high', close))
            low = pd.Series(data.get('low', close))
            
            # 1. 收益率特征
            returns_1h = close.pct_change(1)
            returns_6h = close.pct_change(6)
            returns_24h = close.pct_change(24)
            
            # 2. 动量特征
            momentum_5d = (close - close.shift(5*24)) / close.shift(5*24)
            momentum_20d = (close - close.shift(20*24)) / close.shift(20*24)
            
            # 3. 波动率特征
            volatility_6h = returns_1h.rolling(6).std()
            volatility_24h = returns_1h.rolling(24).std()
            volatility_ratio = volatility_6h / volatility_24h
            
            # 4. 成交量特征
            volume_sma = volume.rolling(24).mean()
            volume_ratio = volume / volume_sma
            obv = (np.sign(returns_1h) * volume).cumsum()
            
            # 5. 技术指标
            # RSI
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # MACD
            exp1 = close.ewm(span=12).mean()
            exp2 = close.ewm(span=26).mean()
            macd = exp1 - exp2
            macd_signal = macd.ewm(span=9).mean()
            
            # 布林带位置
            bb_middle = close.rolling(20).mean()
            bb_std = close.rolling(20).std()
            bb_position = (close - bb_middle) / (2 * bb_std)
            
            # 6. 价格位置特征
            high_20d = high.rolling(20*24).max()
            low_20d = low.rolling(20*24).min()
            price_position = (close - low_20d) / (high_20d - low_20d)
            
            # 组装特征
            symbol_features = pd.DataFrame({
                'symbol': symbol,
                'returns_1h': returns_1h,
                'returns_6h': returns_6h,
                'returns_24h': returns_24h,
                'momentum_5d': momentum_5d,
                'momentum_20d': momentum_20d,
                'volatility_6h': volatility_6h,
                'volatility_24h': volatility_24h,
                'volatility_ratio': volatility_ratio,
                'volume_ratio': volume_ratio,
                'obv': obv,
                'rsi': rsi,
                'macd': macd,
                'macd_signal': macd_signal,
                'bb_position': bb_position,
                'price_position': price_position,
            })
            
            features = pd.concat([features, symbol_features], ignore_index=True)
        
        # 记录特征名
        self.feature_names = [c for c in features.columns if c not in ['symbol', 'target']]
        
        return features
    
    def prepare_target(self, features: pd.DataFrame, horizon: int = 6) -> pd.DataFrame:
        """
        准备目标变量 - 未来收益率
        """
        features = features.copy()
        
        # 按币种分组计算未来收益
        for symbol in features['symbol'].unique():
            mask = features['symbol'] == symbol
            # 未来horizon小时的收益率
            features.loc[mask, 'target'] = features.loc[mask, 'returns_1h'].shift(-horizon)
        
        return features
    
    def train(self, X_train=None, y_train=None, X_valid=None, y_valid=None, 
              market_data: Dict = None, force_retrain: bool = False):
        """
        训练ML模型
        
        Args:
            X_train, y_train: 训练集特征和标签
            X_valid, y_valid: 验证集特征和标签
            market_data: 原始市场数据（如果没有提供X_train/y_train）
            force_retrain: 是否强制重新训练
        """
        if self.is_trained and not force_retrain:
            print("Model already trained. Use force_retrain=True to retrain.")
            return
        
        # 方式1: 直接使用提供的训练数据
        if X_train is not None and y_train is not None:
            print(f"Training with provided data: {len(X_train)} train, {len(X_valid)} valid")
            self._train_with_data(X_train, y_train, X_valid, y_valid)
            return
        
        # 方式2: 从market_data构建特征
        if market_data is None:
            raise ValueError("Must provide either (X_train, y_train) or market_data")
        
        print("Building features from market_data...")
        features = self.feature_engineering(market_data)
        features = self.prepare_target(features, self.config.prediction_horizon)
        
        # 删除NaN
        features = features.dropna()
        
        if len(features) < self.config.min_train_samples:
            raise ValueError(f"Insufficient samples: {len(features)} < {self.config.min_train_samples}")
        
        X = features[self.feature_names]
        y = features['target']
        
        # 时间序列分割（避免未来数据泄露）
        split_idx = int(len(features) * 0.8)
        X_train, X_valid = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_valid = y.iloc[:split_idx], y.iloc[split_idx:]
        
        print(f"Training samples: {len(X_train)}, Validation samples: {len(X_valid)}")
        self._train_with_data(X_train, y_train, X_valid, y_valid)
    
    def _train_with_data(self, X_train, y_train, X_valid, y_valid):
        """使用提供的数据训练模型"""
        # 确保feature_names被设置
        if not self.feature_names:
            self.feature_names = [c for c in X_train.columns if c not in ['symbol', 'target']]
        
        # 训练LightGBM
        if self.config.model_type == 'lightgbm':
            self.model = lgb.LGBMRegressor(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                num_leaves=self.config.num_leaves,           # 新增：限制叶子节点
                min_data_in_leaf=self.config.min_data_in_leaf,  # 新增：最小样本数
                reg_alpha=self.config.reg_alpha,              # 新增：L1正则化
                reg_lambda=self.config.reg_lambda,            # 新增：L2正则化
                random_state=self.config.random_state,
                verbose=-1
            )
            
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_valid, y_valid)],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=self.config.early_stopping_rounds),  # 使用配置中的早停轮数
                    lgb.log_evaluation(period=0)
                ]
            )
        
        self.is_trained = True
        
        # 打印特征重要性
        self.print_feature_importance()
        
        # 评估
        train_pred = self.model.predict(X_train)
        valid_pred = self.model.predict(X_valid)
        
        train_ic = np.corrcoef(y_train, train_pred)[0, 1]
        valid_ic = np.corrcoef(y_valid, valid_pred)[0, 1]
        
        print(f"\nModel Performance:")
        print(f"  Train IC: {train_ic:.4f}")
        print(f"  Valid IC: {valid_ic:.4f}")
    
    def predict(self, symbol_features: Dict[str, float]) -> float:
        """
        预测单个币种的收益率
        """
        if not self.is_trained:
            raise RuntimeError("Model not trained. Call train() first.")
        
        # 构建特征向量
        X = pd.DataFrame([symbol_features])
        X = X[self.feature_names]
        
        prediction = self.model.predict(X)[0]
        return prediction
    
    def predict_batch(self, features_df: pd.DataFrame) -> pd.Series:
        """
        批量预测
        """
        if not self.is_trained:
            raise RuntimeError("Model not trained. Call train() first.")
        
        X = features_df[self.feature_names]
        predictions = self.model.predict(X)
        
        return pd.Series(predictions, index=features_df.index)
    
    def print_feature_importance(self, top_n: int = 10):
        """
        打印特征重要性
        """
        if self.model is None:
            return
        
        importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\nTop {top_n} Important Features:")
        for idx, row in importance.head(top_n).iterrows():
            print(f"  {row['feature']}: {row['importance']:.4f}")
    
    def save_model(self, path: str):
        """保存模型"""
        if not self.is_trained:
            raise RuntimeError("Model not trained.")
        
        model_data = {
            'config': self.config.__dict__,
            'feature_names': self.feature_names,
            'model_type': self.config.model_type
        }
        
        # 保存LightGBM模型
        if self.config.model_type == 'lightgbm':
            self.model.booster_.save_model(f"{path}.txt")
        
        # 保存配置
        with open(f"{path}_config.json", 'w') as f:
            json.dump(model_data, f, indent=2)
        
        print(f"Model saved to {path}")
    
    def load_model(self, path: str):
        """加载模型"""
        # 加载配置
        with open(f"{path}_config.json", 'r') as f:
            model_data = json.load(f)
        
        self.config = MLFactorConfig(**model_data['config'])
        self.feature_names = model_data['feature_names']
        
        # 加载LightGBM模型
        if self.config.model_type == 'lightgbm' and LIGHTGBM_AVAILABLE:
            self.model = lgb.Booster(model_file=f"{path}.txt")
        
        self.is_trained = True
        print(f"Model loaded from {path}")


# 集成到AlphaEngine的示例
"""
# 在 alpha_engine.py 中使用

from src.execution.ml_factor_model import MLFactorModel, MLFactorConfig

class AlphaEngine:
    def __init__(self, cfg):
        # ... 现有代码 ...
        
        # Phase 3: ML因子模型
        self.use_ml = getattr(cfg, 'use_ml_factors', False)
        if self.use_ml:
            self.ml_model = MLFactorModel(
                config=MLFactorConfig(
                    n_estimators=100,
                    max_depth=5
                )
            )
            # 加载预训练模型或训练
            try:
                self.ml_model.load_model('models/ml_factor_model')
            except:
                print("ML model not found, will use linear weights")
    
    def compute_snapshot(self, market_data):
        # ... 现有因子计算 ...
        
        if self.use_ml and self.ml_model.is_trained:
            # 使用ML模型预测
            ml_scores = {}
            for symbol in market_data.keys():
                features = self.extract_features_for_symbol(symbol, market_data)
                ml_scores[symbol] = self.ml_model.predict(features)
            
            # 融合ML分数和传统因子分数
            for symbol in alpha.scores:
                alpha.scores[symbol] = (
                    0.7 * alpha.scores[symbol] +  # 传统因子
                    0.3 * ml_scores.get(symbol, 0)  # ML因子
                )
        
        return alpha
"""
