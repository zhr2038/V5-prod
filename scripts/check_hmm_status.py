#!/usr/bin/env python3
"""
检查当前HMM市场状态
"""

import sys
from pathlib import Path

# 自动检测项目根目录
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import json

# 读取模型信息
model_info_path = PROJECT_ROOT / 'models' / 'hmm_regime_info.json'

print("="*60)
print("📊 HMM市场状态检测器")
print("="*60)

if model_info_path.exists():
    with open(model_info_path) as f:
        info = json.load(f)
    
    print(f"\n✅ 模型信息")
    print(f"   训练时间: {info.get('trained_at', 'unknown')}")
    print(f"   数据: {info.get('data_source', 'unknown')}")
    print(f"   样本数: {info.get('n_samples', 'unknown')}")
    print(f"   状态数: {info.get('n_components', 'unknown')}")
    print(f"   收敛: {info.get('converged', False)}")
    
    print(f"\n🏷️ 状态标签:")
    labels = info.get('state_labels', {})
    for k, v in labels.items():
        print(f"   State {k}: {v}")
    
    print(f"\n📈 状态特征均值:")
    means = info.get('state_means', {})
    for state, values in means.items():
        label = labels.get(state.replace('State_', ''), state)
        print(f"   {label}:")
        print(f"      mom_5d: {values[0]:.6f} ({values[0]*100:.2f}%)")
        print(f"      mom_20d: {values[1]:.6f} ({values[1]*100:.2f}%)")
        print(f"      volatility: {values[2]:.4f}")
        print(f"      rsi: {values[3]:.1f}")

# 尝试检测当前状态
print("\n" + "="*60)
print("🔍 当前市场状态 (BTC/USDT)")
print("="*60)

try:
    from src.regime.hmm_regime_detector import HMMRegimeDetector
    from src.data.okx_ccxt_provider import OKXCCXTProvider
    import os
    from dotenv import load_dotenv
    
    load_dotenv(PROJECT_ROOT / '.env')
    
    detector = HMMRegimeDetector(n_components=3)
    
    # 手动加载模型
    import pickle
    model_path = PROJECT_ROOT / 'models' / 'hmm_regime.pkl'
    if model_path.exists():
        with open(model_path, 'rb') as f:
            detector.model = pickle.load(f)
        print(f"✅ 模型加载成功")
        
        # 加载数据并检测
        provider = OKXCCXTProvider(
            api_key=os.getenv('EXCHANGE_API_KEY'),
            api_secret=os.getenv('EXCHANGE_API_SECRET'),
            passphrase=os.getenv('EXCHANGE_PASSPHRASE')
        )
        
        # 获取最近数据
        bars = provider.fetch_ohlcv('BTC-USDT', '1h', limit=120*24)  # 120天
        if bars and len(bars) > 100:
            df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 计算特征
            close = df['close']
            returns = close.pct_change()
            
            # 最新特征
            mom_5d = (close.iloc[-1] - close.iloc[-5*24]) / close.iloc[-5*24]
            mom_20d = (close.iloc[-1] - close.iloc[-20*24]) / close.iloc[-20*24]
            volatility = returns.rolling(24).std().iloc[-1]
            
            # RSI
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = (100 - (100 / (1 + rs))).iloc[-1]
            
            features = [mom_5d, mom_20d, volatility, rsi]
            
            print(f"\n📊 当前特征:")
            print(f"   mom_5d: {mom_5d:.6f} ({mom_5d*100:.2f}%)")
            print(f"   mom_20d: {mom_20d:.6f} ({mom_20d*100:.2f}%)")
            print(f"   volatility: {volatility:.6f}")
            print(f"   rsi: {rsi:.1f}")
            
            # 检测状态
            result = detector.detect_regime([features])
            
            print(f"\n🎯 HMM判断:")
            print(f"   状态: {result['state']}")
            print(f"   置信度: {result['probability']:.2%}")
            print(f"   状态概率分布:")
            for k, v in result['all_states'].items():
                print(f"      {k}: {v:.2%}")
        else:
            print(f"⚠️ 数据不足")
    else:
        print(f"❌ 模型文件不存在")
        
except Exception as e:
    print(f"⚠️ 检测失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("⏰ 下次重训练: 周日 02:00 (v5-hmm-retrain.timer)")
print("="*60)
