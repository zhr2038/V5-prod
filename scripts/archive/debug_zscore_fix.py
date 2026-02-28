#!/usr/bin/env python3
"""
调试和修复Z-score计算问题
"""

import sys
from pathlib import Path
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_zscore_issue():
    """调试Z-score计算问题"""
    
    print("🔍 调试Z-score计算问题")
    print("=" * 60)
    
    try:
        from src.reporting.alpha_evaluation import robust_zscore_cross_section
        
        # 测试不同情况
        test_cases = [
            {"name": "单币种", "values": {"BTC/USDT": 0.5}},
            {"name": "两币种不同值", "values": {"BTC/USDT": 0.5, "ETH/USDT": 0.8}},
            {"name": "三币种", "values": {"BTC/USDT": 0.5, "ETH/USDT": 0.8, "SOL/USDT": 0.3}},
            {"name": "单币种负值", "values": {"BTC/USDT": -0.2}},
            {"name": "所有值相同", "values": {"BTC/USDT": 0.5, "ETH/USDT": 0.5, "SOL/USDT": 0.5}},
        ]
        
        for test_case in test_cases:
            print(f"\n📊 测试: {test_case['name']}")
            print(f"  输入值: {test_case['values']}")
            
            zscores = robust_zscore_cross_section(test_case['values'], winsorize_pct=0.05)
            
            print(f"  Z-scores: {zscores}")
            
            # 检查是否全为0
            all_zero = all(abs(v) < 1e-12 for v in zscores.values())
            if all_zero:
                print(f"  ⚠️ 所有Z-score为0!")
                
                # 分析原因
                if len(test_case['values']) == 1:
                    print(f"    原因: 只有1个币种，MAD=0")
                elif len(set(test_case['values'].values())) == 1:
                    print(f"    原因: 所有值相同，MAD=0")
        
        # 手动计算验证
        print(f"\n🔍 手动计算验证...")
        
        # 单币种情况
        single_values = {"BTC/USDT": 0.5}
        keys = list(single_values.keys())
        xs = np.array([float(single_values[k]) for k in keys], dtype=float)
        
        print(f"  单币种计算:")
        print(f"    值: {xs}")
        print(f"    median: {np.median(xs)}")
        print(f"    MAD: {np.median(np.abs(xs - np.median(xs)))}")
        print(f"    MAD < 1e-12: {np.median(np.abs(xs - np.median(xs))) < 1e-12}")
        
        # 多币种情况
        multi_values = {"BTC/USDT": 0.5, "ETH/USDT": 0.8, "SOL/USDT": 0.3}
        keys = list(multi_values.keys())
        xs = np.array([float(multi_values[k]) for k in keys], dtype=float)
        
        print(f"\n  多币种计算:")
        print(f"    值: {xs}")
        print(f"    median: {np.median(xs)}")
        print(f"    MAD: {np.median(np.abs(xs - np.median(xs)))}")
        print(f"    MAD * 1.4826: {np.median(np.abs(xs - np.median(xs))) * 1.4826}")
        
        # 计算z-score
        med = np.median(xs)
        mad = np.median(np.abs(xs - med))
        if mad < 1e-12:
            zs = np.zeros_like(xs)
        else:
            zs = (xs - med) / (mad * 1.4826)
        
        print(f"    Z-scores: {zs}")
        
        print(f"\n💡 Z-score问题分析完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def create_zscore_fix():
    """创建Z-score修复方案"""
    
    print("\n" + "=" * 60)
    print("🚀 创建Z-score修复方案")
    print("=" * 60)
    
    fix_code = """def robust_zscore_cross_section_fixed(values: Dict[str, float], winsorize_pct: float = 0.05) -> Dict[str, float]:
    \"\"\"修复版稳健截面z-score：处理单币种情况\"\"\"
    if not values:
        return {}
    
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    
    # 1. 缩尾处理
    if winsorize_pct > 0 and len(xs) > 1:
        lower = np.percentile(xs, winsorize_pct * 100)
        upper = np.percentile(xs, (1 - winsorize_pct) * 100)
        xs = np.clip(xs, lower, upper)
    
    # 2. 处理单币种情况
    if len(xs) == 1:
        # 单币种时，返回标准化值（例如除以绝对值或固定值）
        # 方案A: 返回原值（不标准化）
        # 方案B: 返回符号值（-1, 0, 1）
        # 方案C: 返回缩放值
        return {keys[0]: float(np.sign(xs[0]) if abs(xs[0]) > 1e-12 else 0.0)}
    
    # 3. 处理所有值相同的情况
    if len(set(xs)) == 1:
        # 所有值相同，返回0
        return {k: 0.0 for k in keys}
    
    # 4. 使用median和MAD（Median Absolute Deviation）
    med = np.median(xs)
    mad = np.median(np.abs(xs - med))
    
    # 5. 标准化：MAD -> 标准差近似 (MAD * 1.4826 ≈ std for normal)
    if mad < 1e-12:
        return {k: 0.0 for k in keys}
    
    zs = (xs - med) / (mad * 1.4826)
    return {k: float(z) for k, z in zip(keys, zs)}


def standard_zscore_cross_section_fixed(values: Dict[str, float]) -> Dict[str, float]:
    \"\"\"修复版标准z-score：处理单币种情况\"\"\"
    if not values:
        return {}
    
    keys = list(values.keys())
    xs = np.array([float(values[k]) for k in keys], dtype=float)
    
    # 处理单币种情况
    if len(xs) == 1:
        return {keys[0]: float(np.sign(xs[0]) if abs(xs[0]) > 1e-12 else 0.0)}
    
    # 处理所有值相同的情况
    if len(set(xs)) == 1:
        return {k: 0.0 for k in keys}
    
    # 标准z-score计算
    mu = float(np.mean(xs))
    sd = float(np.std(xs))
    if sd < 1e-12:
        return {k: 0.0 for k in keys}
    
    zs = (xs - mu) / sd
    return {k: float(z) for k, z in zip(keys, zs)}
"""
    
    print("📋 问题分析:")
    print("  1. 当只有1个币种时，MAD=0，z-score全部为0")
    print("  2. 当所有值相同时，MAD=0，z-score全部为0")
    print("  3. 导致Alpha分数为0，策略不交易")
    
    print(f"\n💡 修复方案:")
    print("  1. 单币种时返回符号值或原值")
    print("  2. 所有值相同时返回0")
    print("  3. 多币种时正常计算z-score")
    
    # 保存修复代码
    fix_path = Path("/home/admin/clawd/v5-trading-bot/scripts/zscore_fix.py")
    fix_path.write_text(fix_code, encoding="utf-8")
    
    print(f"\n✅ 修复代码已保存到: {fix_path}")
    
    print(f"\n🎯 立即测试修复:")
    print("  1. 修改AlphaEngine使用修复的z-score函数")
    print("  2. 或测试多币种配置")

def test_multi_symbol_config():
    """测试多币种配置"""
    
    print("\n" + "=" * 60)
    print("🎯 测试多币种配置")
    print("=" * 60)
    
    multi_config = """# 多币种测试配置
symbols:
  - BTC/USDT
  - ETH/USDT
  - SOL/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  testnet: false

alpha:
  long_top_pct: 0.50  # 选择前50%
  weights:
    f1_mom_5d: 0.5
    f2_mom_20d: 0.5
    f3_vol_adj_ret_20d: 0.0
    f4_volume_expansion: 0.0
    f5_rsi_trend_confirm: 0.0

regime:
  atr_threshold: 0.01
  atr_very_low: 0.004
  pos_mult_trending: 1.0
  pos_mult_sideways: 1.0
  pos_mult_risk_off: 1.0

risk:
  max_single_weight: 0.5
  max_gross_exposure: 1.0
  drawdown_trigger: 0.50
  drawdown_delever: 0.50

rebalance:
  interval_minutes: 60
  deadband_sideways: 0.001
  deadband_trending: 0.001
  deadband_riskoff: 0.001

execution:
  mode: dry_run
  dry_run: true
  fee_bps: 0
  slippage_bps: 0

backtest:
  start_date: "2026-01-19"
  end_date: "2026-02-17"
  fee_bps: 0
  slippage_bps: 0
  one_bar_delay: true
  walk_forward_folds: 2
  cost_model: fixed
  cost_stats_dir: ""
  fee_quantile: p75
  slippage_quantile: p90
  min_fills_global: 1
  min_fills_bucket: 1
  max_stats_age_days: 365
"""
    
    print("📋 多币种配置要点:")
    print("  1. 3个币种(BTC/USDT, ETH/USDT, SOL/USDT)")
    print("  2. 极简Alpha因子(只有动量)")
    print("  3. 极低deadband")
    print("  4. 零成本")
    
    # 保存配置
    config_path = Path("configs/multi_symbol_test.yaml")
    config_path.write_text(multi_config, encoding="utf-8")
    
    print(f"\n✅ 多币种配置已保存到: {config_path}")
    print(f"   使用命令测试: V5_CONFIG={config_path} python3 scripts/run_walk_forward.py")
    
    print(f"\n💡 测试目的:")
    print("  验证多币种时Z-score计算是否正常")
    print("  如果多币种有交易，说明是Z-score单币种问题")

def main():
    """主函数"""
    
    print("🚀 Z-score计算问题调试")
    print("=" * 60)
    
    # 调试Z-score问题
    debug_zscore_issue()
    
    # 创建修复方案
    create_zscore_fix()
    
    # 测试多币种配置
    test_multi_symbol_config()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 根本原因:")
    print("Z-score计算需要至少2个不同的值")
    print("单币种时MAD=0，导致z-score全部为0")
    print("Alpha分数为0，策略不生成订单")

if __name__ == "__main__":
    main()