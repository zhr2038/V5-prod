#!/usr/bin/env python3
"""
训练HMM市场状态检测模型

用法:
    python scripts/train_hmm_regime.py
    
输出:
    - models/hmm_regime.pkl: 训练好的HMM模型
    - models/hmm_regime_info.json: 模型信息和状态定义
"""

import sys
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

from src.regime.hmm_regime_detector import HMMRegimeDetector


def main():
    print("=" * 60)
    print("V5 HMM Regime Detector Training")
    print("=" * 60)
    
    # 创建检测器
    detector = HMMRegimeDetector(n_components=3)
    
    # 加载训练数据
    print("\n[1] 加载训练数据...")
    X = detector.load_training_data(lookback_days=60)
    
    if X is None or len(X) < 100:
        print("❌ 训练数据不足，需要至少100条记录")
        return 1
    
    print(f"✓ 加载了 {len(X)} 条训练样本")
    
    # 训练模型
    print("\n[2] 训练HMM模型...")
    success = detector.train(X)
    
    if not success:
        print("❌ 训练失败")
        return 1
    
    # 保存模型信息
    print("\n[3] 保存模型信息...")
    model_info = {
        'trained_at': datetime.now().isoformat(),
        'n_components': detector.n_components,
        'n_samples': len(X),
        'n_features': X.shape[1],
        'converged': detector.model.converged,
        'state_names': detector.state_names,
        'state_means': {detector.state_names[i]: detector.model.means_[i].tolist() 
                       for i in range(detector.n_components)},
        'transition_matrix': detector.model.transmat_.tolist()
    }
    
    info_path = Path('/home/admin/clawd/v5-trading-bot/models/hmm_regime_info.json')
    info_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(info_path, 'w') as f:
        json.dump(model_info, f, indent=2)
    
    print(f"✓ 模型信息已保存到 {info_path}")
    
    # 测试预测
    print("\n[4] 测试模型...")
    result = detector.predict(X[-50:])
    
    print(f"当前市场状态: {result['state']}")
    print(f"置信度: {result['probability']:.2%}")
    print(f"状态概率分布:")
    for state, prob in result['all_states'].items():
        print(f"  - {state}: {prob:.2%}")
    
    print("\n" + "=" * 60)
    print("✓ HMM模型训练完成")
    print("=" * 60)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
