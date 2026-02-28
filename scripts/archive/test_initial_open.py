#!/usr/bin/env python3
"""
测试初始开仓逻辑
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def test_initial_position():
    """测试初始持仓情况"""
    
    print("🔍 测试初始开仓逻辑")
    print("=" * 60)
    
    # 查看backtest或walk-forward的初始设置
    print("📋 分析可能的问题:")
    print("  1. 初始持仓为空")
    print("  2. 目标权重可能很小")
    print("  3. 持仓变化(drift)计算")
    print("  4. 初始开仓逻辑")
    
    # 检查backtest引擎
    backtest_path = Path("/home/admin/clawd/v5-trading-bot/src/backtest/backtest_engine.py")
    
    if backtest_path.exists():
        with open(backtest_path, 'r') as f:
            content = f.read()
        
        print(f"\n📄 Backtest引擎检查:")
        
        # 查找初始持仓设置
        if 'initial_positions' in content or 'initial_cash' in content:
            print("  找到初始设置相关代码")
            
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'initial' in line.lower() and ('position' in line.lower() or 'cash' in line.lower()):
                    print(f"  行 {i+1}: {line.strip()}")
    
    # 创建强制开仓的配置
    print(f"\n🚀 创建强制开仓配置...")
    
    config_content = """# 强制开仓测试配置
symbols:
  - BTC/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  testnet: false

alpha:
  long_top_pct: 1.0  # 选择100%的币种
  weights:
    f1_mom_5d: 1.0
    f2_mom_20d: 0.0
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
  max_single_weight: 1.0  # 100%仓位
  max_gross_exposure: 1.0
  drawdown_trigger: 1.0
  drawdown_delever: 1.0

rebalance:
  interval_minutes: 60
  deadband_sideways: 0.0  # 零deadband
  deadband_trending: 0.0
  deadband_riskoff: 0.0

execution:
  mode: dry_run
  dry_run: true
  fee_bps: 0
  slippage_bps: 0

backtest:
  start_date: "2026-01-19"
  end_date: "2026-01-20"
  fee_bps: 0
  slippage_bps: 0
  one_bar_delay: false
  walk_forward_folds: 1
  cost_model: fixed
  cost_stats_dir: ""
  fee_quantile: p75
  slippage_quantile: p90
  min_fills_global: 1
  min_fills_bucket: 1
  max_stats_age_days: 365

budget:
  action_enabled: false
  min_trade_notional_base: 0.000001

# 初始设置
initial_cash: 10000.0
initial_positions: []  # 空持仓，强制开仓
"""
    
    config_path = Path("configs/force_open.yaml")
    config_path.write_text(config_content, encoding="utf-8")
    
    print(f"✅ 创建配置: {config_path}")
    print(f"  关键设置:")
    print(f"  零deadband (强制交易)")
    print(f"  100%仓位权重")
    print(f"  空初始持仓")
    print(f"  选择100%币种")
    
    return config_path

def run_force_open_test():
    """运行强制开仓测试"""
    
    print("\n" + "=" * 60)
    print("🎯 运行强制开仓测试")
    print("=" * 60)
    
    config_path = test_initial_position()
    
    import subprocess
    import json
    
    print(f"\n📊 运行测试...")
    
    try:
        result = subprocess.run(
            ["python3", "scripts/run_walk_forward.py"],
            env={"V5_CONFIG": str(config_path)},
            cwd="/home/admin/clawd/v5-trading-bot",
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            print(f"  ✅ 测试运行成功")
            
            # 读取结果
            with open("reports/walk_forward.json", 'r') as f:
                data = json.load(f)
            
            fold = data['folds'][0]
            result_data = fold['result']
            trades = result_data.get('num_trades', 0)
            
            print(f"\n📊 结果:")
            print(f"  交易次数: {trades}")
            print(f"  夏普: {result_data['sharpe']}")
            print(f"  收益: {result_data.get('total_return', 0)*100:.6f}%")
            
            if trades > 0:
                print(f"\n🎉 🎉 🎉 终于有交易了！")
                print(f"  证明初始开仓逻辑是问题")
            else:
                print(f"\n❌ 即使强制开仓仍然没有交易")
                print(f"  问题在订单生成的最深层")
        
        else:
            print(f"  ❌ 测试运行失败")
            print(f"  错误: {result.stderr[:200]}")
            
    except Exception as e:
        print(f"  ❌ 测试错误: {e}")

def check_drift_calculation():
    """检查drift计算"""
    
    print("\n" + "=" * 60)
    print("📐 检查drift计算")
    print("=" * 60)
    
    pipeline_path = Path("/home/admin/clawd/v5-trading-bot/src/core/pipeline.py")
    
    with open(pipeline_path, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    print("Drift计算相关代码:")
    
    for i, line in enumerate(lines):
        if 'drift' in line and '=' in line:
            print(f"行 {i+1}: {line.strip()}")
            
            # 显示上下文
            context_start = max(0, i-1)
            context_end = min(len(lines), i+2)
            
            for j in range(context_start, context_end):
                if j != i:
                    print(f"行 {j+1}: {lines[j].strip()}")
            print("")
    
    print("💡 Drift计算公式:")
    print("  drift = target_weight - current_weight")
    print("  如果|drift| <= deadband，跳过交易")
    print("  初始持仓为空时，current_weight = 0")
    print("  如果target_weight很小，drift可能很小")

def main():
    """主函数"""
    
    print("🚀 测试初始开仓逻辑")
    print("=" * 60)
    
    # 运行强制开仓测试
    run_force_open_test()
    
    # 检查drift计算
    check_drift_calculation()
    
    print("\n✅ 测试完成")
    print("=" * 60)
    
    print("\n💡 最终诊断:")
    print("策略核心问题可能是:")
    print("  1. 目标权重计算太小")
    print("  2. Drift始终在deadband内")
    print("  3. 缺少初始开仓触发")

if __name__ == "__main__":
    main()