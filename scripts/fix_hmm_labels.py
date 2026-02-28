#!/usr/bin/env python3
"""
修复 HMM 模型状态标签
将重复的 TrendingDown 改为 TrendingUp
"""

import sys
import json
from pathlib import Path

# 项目根目录
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

INFO_PATH = PROJECT_ROOT / 'models' / 'hmm_regime_info.json'

def fix_hmm_labels():
    """修复 HMM 状态标签"""
    
    if not INFO_PATH.exists():
        print(f"❌ 模型信息文件不存在: {INFO_PATH}")
        return 1
    
    with open(INFO_PATH, 'r') as f:
        info = json.load(f)
    
    print("=" * 60)
    print("修复 HMM 状态标签")
    print("=" * 60)
    
    print("\n当前标签:")
    for state_id, label in info.get('state_labels', {}).items():
        print(f"  State {state_id}: {label}")
    
    # 检查是否有重复的 TrendingDown
    labels = info.get('state_labels', {})
    trending_down_states = [k for k, v in labels.items() if v == 'TrendingDown']
    
    if len(trending_down_states) >= 2:
        print(f"\n⚠️ 发现 {len(trending_down_states)} 个 TrendingDown 状态")
        
        # 策略：选择 rsi 最低的改为 TrendingUp（因为上涨通常 rsi 较高）
        state_stats = info.get('state_stats', {})
        
        # 找到 rsi 最高的 TrendingDown，改为 TrendingUp
        max_rsi = -1
        state_to_change = None
        
        for state_id in trending_down_states:
            rsi = state_stats.get(state_id, {}).get('rsi', 0)
            print(f"  State {state_id} RSI: {rsi:.1f}")
            if rsi > max_rsi:
                max_rsi = rsi
                state_to_change = state_id
        
        if state_to_change:
            print(f"\n✅ 将 State {state_to_change} (RSI={max_rsi:.1f}) 改为 TrendingUp")
            labels[state_to_change] = 'TrendingUp'
            
            # 保存
            info['state_labels'] = labels
            info['fix_date'] = str(datetime.now())
            info['fix_reason'] = '将重复的 TrendingDown 改为 TrendingUp'
            
            with open(INFO_PATH, 'w') as f:
                json.dump(info, f, indent=2)
            
            print(f"\n✅ 已保存到: {INFO_PATH}")
    
    elif 'TrendingUp' not in labels.values():
        print("\n⚠️ 没有 TrendingUp 状态")
        print("选择一个 Sideways 改为 TrendingUp")
        
        sideways_states = [k for k, v in labels.items() if v == 'Sideways']
        if sideways_states:
            # 选择 rsi 最高的 Sideways
            state_stats = info.get('state_stats', {})
            max_rsi = -1
            state_to_change = None
            
            for state_id in sideways_states:
                rsi = state_stats.get(state_id, {}).get('rsi', 0)
                print(f"  State {state_id} RSI: {rsi:.1f}")
                if rsi > max_rsi:
                    max_rsi = rsi
                    state_to_change = state_id
            
            if state_to_change:
                print(f"\n✅ 将 State {state_to_change} (RSI={max_rsi:.1f}) 改为 TrendingUp")
                labels[state_to_change] = 'TrendingUp'
                
                info['state_labels'] = labels
                info['fix_date'] = str(datetime.now())
                info['fix_reason'] = '添加 TrendingUp 状态'
                
                with open(INFO_PATH, 'w') as f:
                    json.dump(info, f, indent=2)
                
                print(f"\n✅ 已保存到: {INFO_PATH}")
    else:
        print("\n✅ 标签已正确，无需修复")
    
    print("\n修复后标签:")
    for state_id, label in labels.items():
        print(f"  State {state_id}: {label}")
    
    return 0


if __name__ == '__main__':
    from datetime import datetime
    sys.exit(fix_hmm_labels())
