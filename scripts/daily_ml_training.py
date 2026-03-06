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
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from sklearn.feature_selection import mutual_info_regression
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from src.execution.ml_data_collector import MLDataCollector
from src.execution.ml_factor_model import MLFactorModel, MLFactorConfig
from src.execution.ml_feature_optimizer import optimize_features_for_training
from src.execution.ml_time_series_cv import TimeSeriesSplit, time_series_cv_score

class DailyMLTrainer:
    """每日ML训练器"""
    
    def __init__(self):
        self.collector = MLDataCollector()
        self.model_path = Path("models/ml_factor_model")
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = Path("logs/ml_training.log")
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.last_outcome = 'unknown'  # saved|blocked|insufficient|error
        
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
    
    def _load_histories(self):
        history_file = Path("reports/ml_training_history.json")
        if not history_file.exists():
            return []
        try:
            with open(history_file, 'r') as f:
                obj = json.load(f)
            return obj if isinstance(obj, list) else []
        except Exception:
            return []

    def _append_history(self, history: dict):
        histories = self._load_histories()
        histories.append(history)
        with open("reports/ml_training_history.json", 'w') as f:
            json.dump(histories, f, indent=2)

    def _fallback_feature_cols(self, df: pd.DataFrame):
        cols = [
            'returns_24h',
            'momentum_5d',
            'momentum_20d',
            'volatility_24h',
            'volume_ratio',
            'obv',
            'rsi',
            'macd',
            'macd_signal',
            'bb_position',
            'price_position',
        ]
        return [c for c in cols if c in df.columns]

    def _select_features_dynamic(self, df: pd.DataFrame, y: pd.Series, fallback_cols: list):
        """动态特征选择（带白名单兜底）。"""
        excluded = {
            'future_return_6h', 'symbol', 'regime', 'created_at', 'label_filled',
            'timestamp', 'hour_of_day', 'day_of_week'
        }
        candidate_cols = []
        for c in df.columns:
            if c in excluded:
                continue
            if str(c).startswith('Unnamed'):
                continue
            if pd.api.types.is_numeric_dtype(df[c]):
                candidate_cols.append(c)

        # 至少包含 fallback 特征
        candidate_cols = list(dict.fromkeys([c for c in candidate_cols if c in df.columns] + fallback_cols))

        mode = 'fallback'
        selected_cols = list(fallback_cols)
        mi_scores = {}
        selector_reason = 'fallback_default'

        if len(candidate_cols) < 6:
            return selected_cols, mode, mi_scores, 'candidate_too_small'

        try:
            X_raw = df[candidate_cols].copy()
            X_raw = X_raw.replace([np.inf, -np.inf], np.nan)

            # 先做优化（去相关/去低信息/候选筛选）
            X_opt = optimize_features_for_training(X_raw, y)
            selected = [c for c in X_opt.columns if c in df.columns]

            # 计算 MI 分数用于可解释记录
            eval_df = pd.concat([X_opt, y.rename('future_return_6h')], axis=1).dropna()
            if len(eval_df) < 80 or len(selected) < 6:
                raise ValueError('insufficient_samples_after_feature_opt')

            X_eval = eval_df[selected].copy()
            X_eval = X_eval.fillna(X_eval.median())
            y_eval = eval_df['future_return_6h']

            mi = mutual_info_regression(X_eval, y_eval, random_state=42)
            mi_scores = {c: float(v) for c, v in zip(selected, mi)}

            # 稳定性判断：有效特征不足则回退
            positive_cnt = int(sum(1 for v in mi_scores.values() if float(v) > 1e-6))
            if positive_cnt < max(3, min(6, len(selected))):
                raise ValueError('mi_unstable_positive_count_low')

            selected_cols = selected
            mode = 'dynamic_active'
            selector_reason = 'ok'
        except Exception as e:
            selected_cols = list(fallback_cols)
            mode = 'fallback'
            selector_reason = f'fallback_on_error:{e}'

        return selected_cols, mode, mi_scores, selector_reason

    def _run_cv_gate(self, X: pd.DataFrame, y: pd.Series):
        """时序CV门禁：返回 (mean_ic, std_ic, scores)。"""
        # 小样本时自适应 folds
        n_samples = len(X)
        n_splits = 5 if n_samples >= 250 else (4 if n_samples >= 180 else 3)
        cv = TimeSeriesSplit(n_splits=n_splits, gap=24)

        model_cv = make_pipeline(
            StandardScaler(),
            Ridge(alpha=10.0)
        )
        cv_res = time_series_cv_score(model_cv, X, y, cv=cv, metric='ic')
        return float(cv_res['mean_score']), float(cv_res['std_score']), [float(s) for s in cv_res['scores']]

    def export_and_train(self):
        """导出数据并训练模型（时序CV门禁 + 动态特征选择）"""
        self.log("="*60)
        self.log("Starting Daily ML Training")
        self.log("="*60)
        self.last_outcome = 'unknown'

        # ---- Gate Config ----
        min_samples = int(os.getenv('V5_ML_MIN_SAMPLES', '100'))
        min_valid_ic = float(os.getenv('V5_ML_MIN_VALID_IC', '-0.10'))
        max_ic_gap = float(os.getenv('V5_ML_MAX_IC_GAP', '0.90'))
        min_cv_mean_ic = float(os.getenv('V5_ML_MIN_CV_MEAN_IC', '0.00'))
        max_cv_std = float(os.getenv('V5_ML_MAX_CV_STD', '0.35'))

        # 1) 数据充足性
        is_ready, stats = self.check_data_sufficiency(min_samples=min_samples)
        if not is_ready:
            self.log(f"⚠️  Insufficient data: {stats['labeled_records']} < {min_samples} samples")
            self.log("Waiting for more data...")
            self.last_outcome = 'insufficient'
            return False

        # 2) 导出训练数据
        self.log("\nExporting training data...")
        csv_path = "reports/ml_training_data.csv"
        success = self.collector.export_training_data(csv_path, min_samples=min_samples)
        if not success:
            self.log("❌ Failed to export training data")
            self.last_outcome = 'error'
            return False

        # 3) 加载并按时间排序
        self.log("\nLoading training data...")
        df = pd.read_csv(csv_path)
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp').reset_index(drop=True)

        # 4) 特征选择（动态 + 兜底）
        fallback_cols = self._fallback_feature_cols(df)
        if not fallback_cols:
            self.log("❌ No fallback features available")
            self.last_outcome = 'error'
            return False

        y_all = df['future_return_6h']
        selected_cols, feature_mode, mi_scores, selector_reason = self._select_features_dynamic(df, y_all, fallback_cols)

        self.log(f"Feature mode: {feature_mode}")
        self.log(f"Feature selector reason: {selector_reason}")
        self.log(f"Selected features ({len(selected_cols)}): {selected_cols}")

        # 5) 清洗样本
        work_df = df[selected_cols + ['future_return_6h']].copy()
        work_df = work_df.replace([np.inf, -np.inf], np.nan).dropna()

        if len(work_df) < min_samples:
            self.log(f"❌ Insufficient valid samples after cleaning: {len(work_df)} < {min_samples}")
            self.last_outcome = 'insufficient'
            return False

        X = work_df[selected_cols]
        y = work_df['future_return_6h']

        self.log(f"Training samples: {len(X)}")

        # 6) 时序 split（holdout）
        split_idx = int(len(work_df) * 0.8)
        X_train, X_valid = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_valid = y.iloc[:split_idx], y.iloc[split_idx:]

        if 'timestamp' in df.columns:
            # 与 work_df 对齐后无法直接索引原时间列，这里仅输出大致范围
            self.log(f"Train/Valid split: {len(X_train)} / {len(X_valid)}")

        # 7) 时序CV门禁（先卡质量）
        self.log("\nRunning time-series CV gate...")
        try:
            cv_mean_ic, cv_std_ic, cv_scores = self._run_cv_gate(X, y)
        except Exception as e:
            self.log(f"❌ CV gate failed: {e}")
            cv_mean_ic, cv_std_ic, cv_scores = -1.0, 99.0, []

        self.log(f"CV mean IC: {cv_mean_ic:.4f}")
        self.log(f"CV std IC:  {cv_std_ic:.4f}")

        # 8) 训练最终模型
        self.log("\nTraining Ridge Regression model...")
        config = MLFactorConfig(model_type='ridge', alpha=10.0)
        model = MLFactorModel(config)

        try:
            model.train(X_train, y_train, X_valid, y_valid)
        except Exception as e:
            self.log(f"❌ Training failed: {e}")
            self.last_outcome = 'error'
            return False

        # 9) Holdout评估
        train_pred = model.predict_batch(X_train)
        valid_pred = model.predict_batch(X_valid)
        train_ic = float(pd.Series(y_train).corr(pd.Series(train_pred, index=y_train.index)))
        valid_ic = float(pd.Series(y_valid).corr(pd.Series(valid_pred, index=y_valid.index)))

        self.log("\nModel Performance:")
        self.log(f"  Train IC: {train_ic:.4f}")
        self.log(f"  Valid IC: {valid_ic:.4f}")

        ic_gap = float(train_ic - valid_ic)

        # 10) 双闸门判定
        fail_reasons = []
        if valid_ic < min_valid_ic:
            fail_reasons.append(f"valid_ic<{min_valid_ic:.2f}")
        if ic_gap > max_ic_gap:
            fail_reasons.append(f"ic_gap>{max_ic_gap:.2f}")
        if cv_mean_ic < min_cv_mean_ic:
            fail_reasons.append(f"cv_mean_ic<{min_cv_mean_ic:.2f}")
        if cv_std_ic > max_cv_std:
            fail_reasons.append(f"cv_std_ic>{max_cv_std:.2f}")

        gate_passed = len(fail_reasons) == 0

        # 11) 记录历史（无论通过与否都记录）
        history = {
            'timestamp': datetime.now().isoformat(),
            'samples': int(len(work_df)),
            'train_ic': float(train_ic),
            'valid_ic': float(valid_ic),
            'cv_mean_ic': float(cv_mean_ic),
            'cv_std_ic': float(cv_std_ic),
            'cv_scores': cv_scores,
            'feature_mode': feature_mode,
            'selected_features': selected_cols,
            'mi_scores': mi_scores,
            'selector_reason': selector_reason,
            'gate': {
                'min_valid_ic': min_valid_ic,
                'max_ic_gap': max_ic_gap,
                'min_cv_mean_ic': min_cv_mean_ic,
                'max_cv_std': max_cv_std,
                'passed': gate_passed,
                'fail_reasons': fail_reasons,
            },
            'model_saved': gate_passed,
            'config': config.__dict__,
        }
        self._append_history(history)

        if not gate_passed:
            self.log(f"⚠️  Gate blocked model update: {', '.join(fail_reasons)}")
            self.log("Keeping previous model unchanged.")
            self.last_outcome = 'blocked'
            return False

        # 12) 保存模型
        self.log(f"\nSaving model to {self.model_path}...")
        model.save_model(str(self.model_path))

        self.log("\n✅ Training completed successfully!")
        self.log(f"   Model saved: {self.model_path}")
        self.log(f"   Valid IC: {valid_ic:.4f}")
        self.log(f"   CV Mean/Std: {cv_mean_ic:.4f} / {cv_std_ic:.4f}")

        self.last_outcome = 'saved'
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
            ts = str(h.get('timestamp', 'unknown'))[:16]
            vic = h.get('valid_ic', None)
            try:
                vic_txt = f"{float(vic):.4f}" if np.isfinite(float(vic)) else "nan"
            except Exception:
                vic_txt = "nan"
            self.log(f"  {ts}: IC={vic_txt}, Samples={h.get('samples', 'n/a')}")

        # IC趋势
        ics = []
        for h in histories:
            try:
                v = float(h.get('valid_ic'))
                if np.isfinite(v):
                    ics.append(v)
            except Exception:
                pass

        if not ics:
            self.log("\nIC Statistics: no finite IC records yet")
            return

        avg_ic = float(sum(ics) / len(ics))
        latest_ic = float(ics[-1])

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

        outcome = getattr(trainer, 'last_outcome', 'unknown')
        if outcome in ('blocked', 'insufficient'):
            print(f"\nℹ️  Daily ML training finished with no model update (outcome={outcome})")
            return 0

        print(f"\n⚠️  Training failed (outcome={outcome})")
        return 1
            
    except Exception as e:
        trainer.log(f"❌ Fatal error: {e}")
        import traceback
        trainer.log(traceback.format_exc())
        return 1

if __name__ == '__main__':
    sys.exit(main())
