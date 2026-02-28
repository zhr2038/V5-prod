#!/usr/bin/env python3
"""
币池守卫 - 防止奇怪币进入选币环节
运行时机：universe生成后、alpha计算前
"""

import json
import sys
from pathlib import Path

# 高风险币种特征
SUSPICIOUS_PATTERNS = [
    # Meme币
    'PEPE', 'DOGE', 'SHIB', 'FLOKI', 'BONK', 'WIF', 'BOME', 
    # 新币/低市值
    'PROMPT', 'SPACE', 'KITE', 'WLFI', 'MERL', 'J', 'AGLD',
    # 稳定币变体
    'USDT/USDT', 'USDG', 
    # 商品代币
    'XAUT',  # 黄金
]

# 必须有良好的历史
MIN_LISTING_DAYS = 90  # 至少上线90天

def check_universe(universe_path: str = 'reports/universe_cache.json'):
    """检查币池，标记可疑币种"""
    path = Path(universe_path)
    if not path.exists():
        print(f"❌ 币池文件不存在: {universe_path}")
        return False
    
    with open(path) as f:
        data = json.load(f)
    
    symbols = data.get('symbols', [])
    suspicious = []
    
    for sym in symbols:
        sym_upper = sym.upper()
        for pattern in SUSPICIOUS_PATTERNS:
            if pattern in sym_upper:
                suspicious.append((sym, pattern))
                break
    
    print("="*60)
    print("币池守卫检查报告")
    print("="*60)
    print(f"币池总数: {len(symbols)}")
    print(f"可疑币种: {len(suspicious)}")
    print()
    
    if suspicious:
        print("⚠️  发现可疑币种:")
        for sym, reason in suspicious:
            print(f"   - {sym} (匹配: {reason})")
        print()
        print("🛡️  建议操作:")
        print("   1. 将这些币加入 configs/blacklist.json")
        print("   2. 重新生成币池")
        return False
    else:
        print("✅ 币池检查通过，无可疑币种")
        return True

def auto_blacklist_suspicious():
    """自动将可疑币加入黑名单"""
    universe_path = Path('reports/universe_cache.json')
    blacklist_path = Path('configs/blacklist.json')
    
    if not universe_path.exists():
        return
    
    with open(universe_path) as f:
        data = json.load(f)
    
    symbols = data.get('symbols', [])
    
    # 加载现有黑名单
    if blacklist_path.exists():
        with open(blacklist_path) as f:
            bl = json.load(f)
    else:
        bl = {"symbols": []}
    
    existing = set(bl.get('symbols', []))
    added = []
    
    for sym in symbols:
        sym_upper = sym.upper()
        for pattern in SUSPICIOUS_PATTERNS:
            if pattern in sym_upper:
                if sym not in existing:
                    bl['symbols'].append(sym)
                    added.append(sym)
                break
    
    if added:
        with open(blacklist_path, 'w') as f:
            json.dump(bl, f, indent=2)
        print(f"🛡️  已自动将 {len(added)} 个可疑币加入黑名单:")
        for s in added:
            print(f"   - {s}")
    
    return added

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto-blacklist', action='store_true', help='自动加入黑名单')
    args = parser.parse_args()
    
    if args.auto_blacklist:
        auto_blacklist_suspicious()
    else:
        ok = check_universe()
        sys.exit(0 if ok else 1)
