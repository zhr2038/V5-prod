#!/usr/bin/env python3
"""
V5 ML标签泄漏检查工具

检查训练数据是否存在标签泄漏问题
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import pandas as pd
import numpy as np
from pathlib import Path
from scipy import stats

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')


class LabelLeakageChecker:
    """标签泄漏检查器"""
    
    def __init__(self):
        self.issues = []
    
    def log(self, msg, level='INFO'):
        prefix = {'INFO': 'ℹ️', 'WARN': '⚠️', 'CRITICAL': '🔴', 'OK': '✅'}
        print(f"{prefix.get(level, '•')} {msg}")
    
    def load_training_data(self) -> pd.DataFrame:
        """加载训练数据"""
        csv_path = REPORTS_DIR / 'ml_training_data.csv'
        
        if not csv_path.exists():
            self.log("训练数据文件不存在，请先运行数据收集", 'CRITICAL')
            return None
        
        self.log(f"加载训练数据: {csv_path}")
        df = pd.read_csv(csv_path)
        self.log(f"加载了 {len(df)} 条记录，{len(df.columns)} 个特征")
        return df
    
    def check_feature_target_correlation(self, df: pd.DataFrame, target_col: str = 'future_return_6h'):
        """检查特征与目标的同期相关性（可能是泄露）"""
        self.log("\n" + "="*60)
        self.log("检查特征-目标相关性（可能泄露）")
        self.log("="*60)
        
        feature_cols = [c for c in df.columns if c not in [target_col, 'timestamp', 'symbol']]
        
        correlations = []
        for col in feature_cols:
            # 跳过非数值列
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            corr = df[col].corr(df[target_col])
            if not np.isnan(corr):
                correlations.append((col, abs(corr)))
        
        # 按绝对相关性排序
        correlations.sort(key=lambda x: x[1], reverse=True)
        
        self.log("\n高相关性特征（可能泄露）:")
        leaked_features = []
        for col, corr in correlations[:10]:
            status = '🔴' if corr > 0.5 else ('🟡' if corr > 0.3 else '✅')
            self.log(f"  {status} {col}: {corr:.4f}")
            if corr > 0.5:
                leaked_features.append(col)
        
        if leaked_features:
            self.issues.append(f"高相关性特征可能泄露: {leaked_features}")
        
        return leaked_features
    
    def check_future_feature_names(self, df: pd.DataFrame):
        """检查特征名是否包含未来信息"""
        self.log("\n" + "="*60)
        self.log("检查特征名是否包含未来信息")
        self.log("="*60)
        
        future_keywords = ['future', 'next', 'lead', 'ahead', 'forward', 'target']
        leaky_cols = []
        
        for col in df.columns:
            if any(kw in col.lower() for kw in future_keywords):
                self.log(f"  🔴 {col}: 包含未来信息关键词", 'CRITICAL')
                leaky_cols.append(col)
        
        if leaky_cols:
            self.issues.append(f"特征名包含未来信息: {leaky_cols}")
        else:
            self.log("  ✅ 未发现明显的未来信息特征名")
        
        return leaky_cols
    
    def check_perfect_correlation(self, df: pd.DataFrame, target_col: str = 'future_return_6h'):
        """检查完美/接近完美相关（严重泄露）"""
        self.log("\n" + "="*60)
        self.log("检查完美相关性（严重泄露）")
        self.log("="*60)
        
        feature_cols = [c for c in df.columns if c != target_col]
        perfect_corr = []
        
        for col in feature_cols:
            # 跳过非数值列
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            if df[col].dtype in ['float64', 'float32', 'int64']:
                corr = df[col].corr(df[target_col])
                if abs(corr) > 0.99:
                    self.log(f"  🔴 {col}: r={corr:.6f} (完美相关!)", 'CRITICAL')
                    perfect_corr.append(col)
        
        if perfect_corr:
            self.issues.append(f"完美相关特征（严重泄露）: {perfect_corr}")
        else:
            self.log("  ✅ 未发现完美相关")
        
        return perfect_corr
    
    def check_target_distribution(self, df: pd.DataFrame, target_col: str = 'future_return_6h'):
        """检查目标变量分布"""
        self.log("\n" + "="*60)
        self.log("检查目标变量分布")
        self.log("="*60)
        
        y = df[target_col].dropna()
        
        self.log(f"\n目标变量统计:")
        self.log(f"  均值: {y.mean():.6f}")
        self.log(f"  标准差: {y.std():.6f}")
        self.log(f"  最小值: {y.min():.6f}")
        self.log(f"  最大值: {y.max():.6f}")
        self.log(f"  中位数: {y.median():.6f}")
        
        # 检查是否有异常值
        q1, q3 = y.quantile([0.25, 0.75])
        iqr = q3 - q1
        outliers = y[(y < q1 - 3*iqr) | (y > q3 + 3*iqr)]
        
        if len(outliers) > 0:
            self.log(f"\n  ⚠️ 发现 {len(outliers)} 个极端异常值")
        
        # Shapiro-Wilk正态性检验（样本太大时用子集）
        if len(y) > 5000:
            y_sample = y.sample(5000, random_state=42)
        else:
            y_sample = y
        
        _, p_value = stats.shapiro(y_sample)
        self.log(f"\n  正态性检验p值: {p_value:.6f}")
        if p_value < 0.05:
            self.log("  ⚠️ 目标变量非正态分布")
    
    def check_time_order(self, df: pd.DataFrame):
        """检查时间顺序是否正确"""
        if 'timestamp' not in df.columns:
            self.log("  ⚠️ 没有时间戳列，跳过时间顺序检查")
            return
        
        self.log("\n" + "="*60)
        self.log("检查时间顺序")
        self.log("="*60)
        
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # 检查是否单调递增
        is_sorted = df['timestamp'].is_monotonic_increasing
        
        if is_sorted:
            self.log("  ✅ 时间戳单调递增（正确）")
        else:
            self.log("  🔴 时间戳不是单调递增（可能有重复或乱序）", 'CRITICAL')
            self.issues.append("时间戳非单调递增")
        
        # 检查时间范围
        self.log(f"\n  时间范围:")
        self.log(f"    开始: {df['timestamp'].min()}")
        self.log(f"    结束: {df['timestamp'].max()}")
        self.log(f"    跨度: {df['timestamp'].max() - df['timestamp'].min()}")
    
    def generate_report(self):
        """生成检查报告"""
        print("\n" + "="*60)
        print("📋 标签泄漏检查报告")
        print("="*60)
        
        if self.issues:
            print(f"\n🔴 发现 {len(self.issues)} 个问题:")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue}")
            print("\n⚠️  建议: 修复上述问题后重新训练模型")
            return False
        else:
            print("\n✅ 未发现明显的标签泄漏问题")
            return True
    
    def run(self):
        """运行所有检查"""
        print("="*60)
        print("🔍 ML标签泄漏检查")
        print("="*60)
        
        df = self.load_training_data()
        if df is None:
            return False
        
        # 运行各项检查
        self.check_future_feature_names(df)
        self.check_feature_target_correlation(df)
        self.check_perfect_correlation(df)
        self.check_target_distribution(df)
        self.check_time_order(df)
        
        # 生成报告
        is_clean = self.generate_report()
        
        return is_clean


def main():
    checker = LabelLeakageChecker()
    is_clean = checker.run()
    
    sys.exit(0 if is_clean else 1)


if __name__ == '__main__':
    main()
