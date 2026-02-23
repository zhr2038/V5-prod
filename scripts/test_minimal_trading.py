#!/usr/bin/env python3
"""
最小化交易测试：降低所有限制
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def create_ultra_low_deadband_config():
    """创建极低deadband配置"""
    
    config_content = """# 极低deadband测试配置
symbols:
  - BTC/USDT
  - ETH/USDT

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
  deadband_sideways: 0.0001  # 极低deadband (0.01%)
  deadband_trending: 0.0001
  deadband_riskoff: 0.0001

execution:
  mode: dry_run
  dry_run: true
  fee_bps: 0  # 零费用
  slippage_bps: 0  # 零滑点
  min_notional_usdt: 0.001  # 极低最小交易金额

backtest:
  start_date: "2026-01-19"
  end_date: "2026-01-25"  # 缩短测试周期
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
    
    config_path = Path("configs/ultra_low_deadband.yaml")
    config_path.write_text(config_content, encoding="utf-8")
    
    print(f"✅ 创建极低deadband配置: {config_path}")
    print(f"  deadband: 0.01% (原3%)")
    print(f"  最小交易金额: $0.001")
    print(f"  零费用/零滑点")
    
    return config_path

def run_minimal_test():
    """运行最小化测试"""
    
    print("\n" + "=" * 60)
    print("🚀 运行最小化交易测试")
    print("=" * 60)
    
    config_path = create_ultra_low_deadband_config()
    
    # 运行walk-forward测试
    import subprocess
    import json
    
    print(f"\n📊 运行walk-forward测试...")
    
    try:
        result = subprocess.run(
            ["python3", "scripts/run_walk_forward.py"],
            env={"V5_CONFIG": str(config_path), **dict(os.environ)},
            cwd="/home/admin/clawd/v5-trading-bot",
            capture_output=True,
            text=True,
            timeout=300
        )
        
        print(f"  退出码: {result.returncode}")
        
        if result.returncode == 0:
            print(f"  ✅ 测试运行成功")
            
            # 读取结果
            with open("reports/walk_forward.json", 'r') as f:
                data = json.load(f)
            
            folds = data['folds']
            total_trades = 0
            
            for i, fold in enumerate(folds):
                result_data = fold['result']
                trades = result_data.get('num_trades', 0)
                total_trades += trades
                
                print(f"\n  Fold {i+1}:")
                print(f"    交易次数: {trades}")
                print(f"    夏普: {result_data['sharpe']:.3f}")
                print(f"    收益: {result_data.get('total_return', 0)*100:.2f}%")
            
            print(f"\n📊 汇总:")
            print(f"  总交易次数: {total_trades}")
            
            if total_trades > 0:
                print(f"\n🎉 成功！降低deadband后产生交易！")
                print(f"  证明: 交易限制是主要问题")
            else:
                print(f"\n❌ 即使极低deadband仍然没有交易")
                print(f"  问题更深层: 可能是订单生成逻辑问题")
        
        else:
            print(f"  ❌ 测试运行失败")
            print(f"  错误输出: {result.stderr[:500]}")
            
    except Exception as e:
        print(f"  ❌ 测试错误: {e}")
        import traceback
        traceback.print_exc()

def check_order_generator_directly():
    """直接检查订单生成器"""
    
    print("\n" + "=" * 60)
    print("🔧 直接检查订单生成器")
    print("=" * 60)
    
    order_gen_path = Path("/home/admin/clawd/v5-trading-bot/src/execution/order_generator.py")
    
    if order_gen_path.exists():
        with open(order_gen_path, 'r') as f:
            content = f.read()
        
        print(f"📄 文件: {order_gen_path}")
        
        # 查找关键函数
        key_functions = [
            'def generate_orders',
            'def _should_trade',
            'min_notional',
            'deadband'
        ]
        
        for func in key_functions:
            if func in content:
                print(f"\n🔍 找到: {func}")
                
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if func in line:
                        print(f"  行 {i+1}: {line.strip()}")
                        
                        # 显示上下文
                        for j in range(max(0, i-2), min(len(lines), i+3)):
                            if j != i:
                                print(f"  行 {j+1}: {lines[j].strip()}")
                        print("")
    else:
        print(f"❌ 文件不存在: {order_gen_path}")

def main():
    """主函数"""
    
    print("🚀 最小化交易测试")
    print("=" * 60)
    
    import os
    
    # 运行最小化测试
    run_minimal_test()
    
    # 直接检查订单生成器
    check_order_generator_directly()
    
    print("\n✅ 测试完成")
    print("=" * 60)
    
    print("\n💡 诊断思路:")
    print("  1. 如果极低deadband有交易 → deadband是问题")
    print("  2. 如果仍然无交易 → 订单生成逻辑有问题")
    print("  3. 需要检查订单生成器的过滤条件")

if __name__ == "__main__":
    main()