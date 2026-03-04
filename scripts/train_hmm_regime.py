#!/usr/bin/env python3
"""
训练HMM市场状态检测模型（带正确状态标记）

用法:
    python scripts/train_hmm_regime.py
    
输出:
    - models/hmm_regime.pkl: 训练好的HMM模型
    - models/hmm_regime_info.json: 模型信息和正确状态标签
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# 自动检测项目根目录
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.regime.hmm_regime_detector import HMMRegimeDetector

# 模型保存路径
MODEL_DIR = PROJECT_ROOT / 'models'
INFO_PATH = MODEL_DIR / 'hmm_regime_info.json'


def analyze_and_label_states(detector):
    """分析状态特征并返回正确的标签映射"""
    labels = {}
    
    print("\n[3] 分析状态特征并正确标记...")
    for i in range(detector.n_components):
        mean = detector.model.means_[i]
        mom_5d = mean[0]
        mom_20d = mean[1]
        vol = mean[2]
        rsi = mean[3]
        
        print(f"  State {i}: mom_5d={mom_5d:.5f}, mom_20d={mom_20d:.5f}, vol={vol:.5f}, rsi={rsi:.1f}")
        
        # 根据特征判断真实状态
        # 强上涨: mom_5d > 0 且 mom_20d > 0
        # 强下跌: mom_5d < 0 且 mom_20d < 0
        # 震荡: 波动率最高或动量混合
        
        if mom_5d > 0.0001 and mom_20d > -0.0001:
            label = "TrendingUp"
        elif mom_5d < -0.0001 and mom_20d < 0.0001:
            label = "TrendingDown"
        elif vol > 0.05:
            label = "Sideways"  # 高波动 = 震荡
        elif abs(mom_5d) < 0.0003:
            label = "Sideways"  # 低动量 = 震荡
        elif mom_5d > 0:
            label = "TrendingUp"
        else:
            label = "TrendingDown"
        
        labels[i] = label
        print(f"    -> 标记为: {label}")
    
    return labels


def main():
    print("=" * 60)
    print("V5 HMM Regime Detector Training")
    print("=" * 60)
    
    # 创建检测器
    detector = HMMRegimeDetector(n_components=3)
    
    # 加载训练数据（使用120天，包含更多市场周期）
    print("\n[1] 加载训练数据（120天）...")
    X = detector.load_training_data(lookback_days=400)
    
    if X is None or len(X) < 100:
        print("❌ 训练数据不足，需要至少100条记录")
        return 1
    
    print(f"✓ 加载了 {len(X)} 条训练样本")
    print(f"  特征统计:")
    print(f"    mom_5d: mean={X[:,0].mean():.5f}, std={X[:,0].std():.5f}")
    print(f"    mom_20d: mean={X[:,1].mean():.5f}, std={X[:,1].std():.5f}")
    print(f"    volatility: mean={X[:,2].mean():.5f}, std={X[:,2].std():.5f}")
    print(f"    rsi: mean={X[:,3].mean():.1f}, std={X[:,3].std():.1f}")
    
    # 训练模型
    print("\n[2] 训练HMM模型...")
    success = detector.train(X)
    
    if not success:
        print("❌ 训练失败")
        return 1
    
    # 分析并正确标记状态
    state_labels = analyze_and_label_states(detector)
    
    # 更新检测器的state_names
    detector.state_names = state_labels
    
    # 保存更新后的模型
    detector.model.save(detector.model_path)
    
    # 保存模型信息
    print("\n[4] 保存模型信息...")
    model_info = {
        'trained_at': datetime.now().isoformat(),
        'model_class': 'SimpleGaussianHMM',
        'model_payload_type': 'dict',
        'n_components': detector.n_components,
        'n_samples': len(X),
        'n_features': X.shape[1],
        'converged': detector.model.converged,
        'state_labels': state_labels,
        'state_means': {f"State_{i}": detector.model.means_[i].tolist() 
                       for i in range(detector.n_components)},
        'transition_matrix': detector.model.transmat_.tolist()
    }
    
    info_path = MODEL_DIR / 'hmm_regime_info.json'
    with open(info_path, 'w') as f:
        json.dump(model_info, f, indent=2)
    
    print(f"✓ 模型信息已保存到 {info_path}")
    
    # 测试预测
    print("\n[5] 测试模型...")
    result = detector.predict(X[-50:])
    
    print(f"当前市场状态: {result['state']}")
    print(f"原始状态ID: State {result['state_id']}")
    print(f"置信度: {result['probability']:.2%}")
    print(f"真实状态概率分布:")
    for state, prob in result['all_states'].items():
        print(f"  - {state}: {prob:.2%}")
    
    print("\n" + "=" * 60)
    print("✓ HMM模型训练完成")
    print("=" * 60)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
