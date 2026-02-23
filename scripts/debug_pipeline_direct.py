#!/usr/bin/env python3
"""
直接调试pipeline逻辑
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_pipeline_min_notional():
    """调试pipeline中的min_notional逻辑"""
    
    print("🔍 直接调试pipeline min_notional逻辑")
    print("=" * 60)
    
    try:
        # 直接查看pipeline代码
        pipeline_path = Path("/home/admin/clawd/v5-trading-bot/src/core/pipeline.py")
        
        with open(pipeline_path, 'r') as f:
            content = f.read()
        
        # 查找min_notional相关代码
        lines = content.split('\n')
        
        print("📋 min_notional相关代码:")
        
        for i, line in enumerate(lines):
            if 'min_notional' in line:
                print(f"行 {i+1}: {line.strip()}")
                
                # 显示上下文
                context_start = max(0, i-2)
                context_end = min(len(lines), i+3)
                
                for j in range(context_start, context_end):
                    if j != i:
                        print(f"行 {j+1}: {lines[j].strip()}")
                print("")
        
        # 查找配置加载
        print("📋 配置加载相关代码:")
        
        for i, line in enumerate(lines):
            if 'self.cfg.budget' in line or 'self.cfg.min_trade_notional_base' in line:
                print(f"行 {i+1}: {line.strip()}")
                
                # 显示上下文
                context_start = max(0, i-2)
                context_end = min(len(lines), i+3)
                
                for j in range(context_start, context_end):
                    if j != i:
                        print(f"行 {j+1}: {lines[j].strip()}")
                print("")
        
        # 检查配置schema
        print("📋 检查配置schema...")
        
        schema_path = Path("/home/admin/clawd/v5-trading-bot/configs/schema.py")
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                schema_content = f.read()
            
            if 'min_trade_notional_base' in schema_content:
                print("✅ schema中包含min_trade_notional_base")
                
                # 查找定义
                lines = schema_content.split('\n')
                for i, line in enumerate(lines):
                    if 'min_trade_notional_base' in line:
                        print(f"行 {i+1}: {line.strip()}")
            else:
                print("❌ schema中不包含min_trade_notional_base")
        
        print(f"\n💡 问题分析:")
        print("pipeline从self.cfg.budget.min_trade_notional_base获取min_notional")
        print("但配置中可能没有正确设置这个值")
        
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def create_working_config():
    """创建能工作的配置"""
    
    print("\n" + "=" * 60)
    print("🚀 创建能工作的配置")
    print("=" * 60)
    
    # 查看schema结构
    schema_path = Path("/home/admin/clawd/v5-trading-bot/configs/schema.py")
    
    if schema_path.exists():
        with open(schema_path, 'r') as f:
            content = f.read()
        
        # 查找BudgetConfig
        if 'class BudgetConfig' in content:
            print("✅ 找到BudgetConfig类")
            
            lines = content.split('\n')
            budget_start = None
            
            for i, line in enumerate(lines):
                if 'class BudgetConfig' in line:
                    budget_start = i
                    break
            
            if budget_start:
                print("BudgetConfig字段:")
                for i in range(budget_start, min(budget_start+20, len(lines))):
                    if ':' in lines[i] and '=' in lines[i]:
                        print(f"  {lines[i].strip()}")
    
    # 创建正确配置
    config_content = """# 正确配置：包含所有必要字段
symbols:
  - BTC/USDT
  - ETH/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  testnet: false

alpha:
  long_top_pct: 0.50
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
  deadband_sideways: 0.0001
  deadband_trending: 0.0001
  deadband_riskoff: 0.0001

execution:
  mode: dry_run
  dry_run: true
  fee_bps: 0
  slippage_bps: 0

backtest:
  start_date: "2026-01-19"
  end_date: "2026-01-20"  # 非常短
  fee_bps: 0
  slippage_bps: 0
  one_bar_delay: true
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
  min_trade_notional_base: 0.000001  # 极小的值
  deadband_multiplier_exceeded: 2.0
  deadband_cap: 0.1
"""
    
    config_path = Path("configs/working_test.yaml")
    config_path.write_text(config_content, encoding="utf-8")
    
    print(f"\n✅ 创建配置: {config_path}")
    print(f"  关键设置:")
    print(f"  budget.min_trade_notional_base: 0.000001")
    print(f"  deadband: 0.01%")
    print(f"  测试周期: 1天")
    
    return config_path

def test_working_config():
    """测试能工作的配置"""
    
    print("\n" + "=" * 60)
    print("🎯 测试能工作的配置")
    print("=" * 60)
    
    config_path = create_working_config()
    
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
                print(f"  证明配置正确性很重要")
            else:
                print(f"\n❌ 仍然没有交易")
                print(f"  需要更深入的调试")
        
        else:
            print(f"  ❌ 测试运行失败")
            print(f"  错误: {result.stderr[:200]}")
            
    except Exception as e:
        print(f"  ❌ 测试错误: {e}")

def main():
    """主函数"""
    
    print("🚀 直接调试pipeline逻辑")
    print("=" * 60)
    
    # 调试min_notional逻辑
    debug_pipeline_min_notional()
    
    # 测试能工作的配置
    test_working_config()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 关键发现:")
    print("pipeline从self.cfg.budget.min_trade_notional_base获取最小交易金额")
    print("配置必须包含正确的budget部分")

if __name__ == "__main__":
    main()