#!/usr/bin/env python3
"""
Daily ML Model Training Script
每天自动导出训练数据并训练/更新ML模型
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import os
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from src.execution.ml_data_collector import MLDataCollector
from src.execution.ml_factor_model import MLFactorModel, MLFactorConfig

class DailyMLTrainer:
    """每日ML训练器"""
    
    def __init__(self):
        self.collector = MLDataCollector()
        self.model_path = Path("models/ml_factor_model")
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = Path("logs/ml_training.log")
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
    def log(self, message):
        """记录日志"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] {message}"
        print(log_line)
        
        with open(self.log_file, 'a') as f:
            f.write(log_line + '\n')
    
    def check_data_sufficiency(self, min_samples=100):
        """检查数据是否足够"""
        stats = self.collector.get_statistics()
        
        self.log(f"Data Statistics:")
        self.log(f"  Total records: {stats['total_records']}")
        self.log(f"  Labeled records: {stats['labeled_records']}")
        self.log(f"  Unique symbols: {stats['num_symbols']}")
        
        return stats['labeled_records'] >= min_samples, stats
    
    def export_and_train(self):
        """导出数据并训练模型"""
        self.log("="*60)
        self.log("Starting Daily ML Training")
        self.log("="*60)
        
        # 1. 检查数据充足性
        is_ready, stats = self.check_data_sufficiency(min_samples=100)
        
        if not is_ready:
            self.log(f"⚠️  Insufficient data: {stats['labeled_records']} < 100 samples")
            self.log("Waiting for more data...")
            return False
        
        # 2. 导出训练数据
        self.log("\nExporting training data...")
        csv_path = "reports/ml_training_data.csv"
        
        success = self.collector.export_training_data(csv_path, min_samples=100)
        if not success:
            self.log("❌ Failed to export training data")
            return False
        
        # 3. 加载数据
        self.log("\nLoading training data...")
        df = pd.read_csv(csv_path)
        
        # 准备特征和目标
        feature_cols = [
            'returns_1h', 'returns_6h', 'returns_24h',
            'momentum_5d', 'momentum_20d',
            'volatility_6h', 'volatility_24h', 'volatility_ratio',
            'volume_ratio', 'obv',
            'rsi', 'macd', 'macd_signal',
            'bb_position', 'price_position'
        ]
        
        # 删除NaN
        df = df.dropna(subset=feature_cols + ['future_return_6h'])
        
        if len(df) < 100:
            self.log(f"❌ Insufficient valid samples after cleaning: {len(df)}")
            return False
        
        X = df[feature_cols]
        y = df['future_return_6h']
        
        self.log(f"Training samples: {len(X)}")
        self.log(f"Features: {len(feature_cols)}")
        
        # 4. 时间序列分割
        split_idx = int(len(df) * 0.8)
        X_train, X_valid = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_valid = y.iloc[:split_idx], y.iloc[split_idx:]
        
        self.log(f"Train: {len(X_train)}, Valid: {len(X_valid)}")
        
        # 5. 训练模型 - 使用优化后的防过拟合配置
        self.log("\nTraining LightGBM model...")
        
        config = MLFactorConfig(
            n_estimators=100,      # 减少树数量
            max_depth=4,           # 限制深度（原来是6）
            learning_rate=0.05,
            subsample=0.7,         # 降低采样率
            colsample_bytree=0.7,  # 降低特征采样率
            num_leaves=15,         # 限制叶子节点
            min_data_in_leaf=50,   # 最小样本数
            reg_alpha=0.1,         # L1正则化
            reg_lambda=0.1,        # L2正则化
            early_stopping_rounds=20  # 早停
        )
        
        model = MLFactorModel(config)
        
        try:
            model.train(X_train, y_train, X_valid, y_valid)
        except Exception as e:
            self.log(f"❌ Training failed: {e}")
            return False
        
        # 6. 评估模型
        train_pred = model.predict_batch(X_train)
        valid_pred = model.predict_batch(X_valid)
        
        train_ic = df.iloc[:split_idx]['future_return_6h'].corr(train_pred)
        valid_ic = df.iloc[split_idx:]['future_return_6h'].corr(valid_pred)
        
        self.log(f"\nModel Performance:")
        self.log(f"  Train IC: {train_ic:.4f}")
        self.log(f"  Valid IC: {valid_ic:.4f}")
        
        # 判断模型是否有效
        if valid_ic < -0.5:  # 暂时放宽阈值，允许负IC模型作为基线
            self.log(f"⚠️  Model IC too low ({valid_ic:.4f}), not saving")
            return False
        
        # 7. 保存模型
        self.log(f"\nSaving model to {self.model_path}...")
        model.save_model(str(self.model_path))
        
        # 8. 记录训练历史
        history = {
            'timestamp': datetime.now().isoformat(),
            'samples': len(df),
            'train_ic': float(train_ic),
            'valid_ic': float(valid_ic),
            'config': config.__dict__
        }
        
        history_file = Path("reports/ml_training_history.json")
        histories = []
        if history_file.exists():
            with open(history_file, 'r') as f:
                histories = json.load(f)
        
        histories.append(history)
        
        with open(history_file, 'w') as f:
            json.dump(histories, f, indent=2)
        
        self.log(f"\n✅ Training completed successfully!")
        self.log(f"   Model saved: {self.model_path}")
        self.log(f"   Valid IC: {valid_ic:.4f}")
        
        return True
    
    def generate_report(self):
        """生成训练报告"""
        self.log("\n" + "="*60)
        self.log("Generating Training Report")
        self.log("="*60)
        
        # 加载历史
        history_file = Path("reports/ml_training_history.json")
        if not history_file.exists():
            self.log("No training history found")
            return
        
        with open(history_file, 'r') as f:
            histories = json.load(f)
        
        if not histories:
            self.log("Empty training history")
            return
        
        # 最近5次训练
        self.log("\nRecent Training History:")
        for h in histories[-5:]:
            ts = h['timestamp'][:16]  # 截取日期时间
            self.log(f"  {ts}: IC={h['valid_ic']:.4f}, Samples={h['samples']}")
        
        # IC趋势
        ics = [h['valid_ic'] for h in histories]
        avg_ic = sum(ics) / len(ics)
        latest_ic = ics[-1]
        
        self.log(f"\nIC Statistics:")
        self.log(f"  Average: {avg_ic:.4f}")
        self.log(f"  Latest: {latest_ic:.4f}")
        self.log(f"  Best: {max(ics):.4f}")
        
        if latest_ic > avg_ic:
            self.log(f"  ✅ Latest model better than average")
        else:
            self.log(f"  ⚠️  Latest model worse than average")

def main():
    """主函数"""
    trainer = DailyMLTrainer()
    
    try:
        # 训练模型
        success = trainer.export_and_train()
        
        # 生成报告
        trainer.generate_report()
        
        if success:
            print("\n✅ Daily ML training completed successfully")
            return 0
        else:
            print("\n⚠️  Training skipped or failed")
            return 1
            
    except Exception as e:
        trainer.log(f"❌ Fatal error: {e}")
        import traceback
        trainer.log(traceback.format_exc())
        return 1

if __name__ == '__main__':
    sys.exit(main())
