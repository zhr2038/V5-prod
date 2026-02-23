#!/usr/bin/env python3
"""
最终调试：找出所有过滤条件
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def analyze_all_filters():
    """分析所有过滤条件"""
    
    print("🔍 分析所有交易过滤条件")
    print("=" * 60)
    
    pipeline_path = Path("/home/admin/clawd/v5-trading-bot/src/core/pipeline.py")
    
    with open(pipeline_path, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    print("📋 找到的过滤条件:")
    
    filters = []
    
    for i, line in enumerate(lines):
        if 'continue' in line and '#' in line:
            # 查找前面的注释
            for j in range(max(0, i-5), i):
                if 'skip' in lines[j] or 'reject' in lines[j] or 'filter' in lines[j]:
                    reason = lines[j].strip('#').strip()
                    if reason:
                        filters.append((i+1, reason))
                        break
    
    # 手动添加已知过滤条件
    known_filters = [
        (291, "min_notional检查"),
        (306, "deadband检查 (abs(drift) <= effective_deadband)"),
        (340, "现金不足检查 (notional > cash_remaining)"),
        (280, "价格无效检查 (px <= 0)"),
        (275, "目标权重为0检查 (notional <= 0)"),
    ]
    
    filters.extend(known_filters)
    
    # 去重并排序
    filters = sorted(set(filters), key=lambda x: x[0])
    
    for line_num, reason in filters:
        print(f"  行 {line_num}: {reason}")
    
    print(f"\n💡 过滤条件分析:")
    print(f"  1. deadband检查 - 已设置为0.01%")
    print(f"  2. min_notional检查 - 已注释掉")
    print(f"  3. 现金不足检查 - 可能有问题")
    print(f"  4. 价格无效检查 - 需要验证")
    print(f"  5. 目标权重为0 - 可能发生")

def check_cash_logic():
    """检查现金逻辑"""
    
    print("\n" + "=" * 60)
    print("💰 检查现金逻辑")
    print("=" * 60)
    
    pipeline_path = Path("/home/admin/clawd/v5-trading-bot/src/core/pipeline.py")
    
    with open(pipeline_path, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    print("现金相关代码:")
    
    for i, line in enumerate(lines):
        if 'cash' in line.lower() and ('cash_remaining' in line or 'cash_usdt' in line):
            print(f"行 {i+1}: {line.strip()}")
            
            # 显示上下文
            context_start = max(0, i-2)
            context_end = min(len(lines), i+3)
            
            for j in range(context_start, context_end):
                if j != i:
                    print(f"行 {j+1}: {lines[j].strip()}")
            print("")
    
    print("💡 现金逻辑分析:")
    print("  cash_remaining从cash_usdt初始化")
    print("  每通过一个订单，从cash_remaining减去notional")
    print("  如果notional > cash_remaining，订单被拒绝")

def create_ultimate_test_config():
    """创建终极测试配置"""
    
    print("\n" + "=" * 60)
    print("🚀 创建终极测试配置")
    print("=" * 60)
    
    config_content = """# 终极测试配置：绕过所有限制
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
    f1_mom_5d: 1.0  # 只用一个因子
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
  max_single_weight: 1.0  # 允许100%仓位
  max_gross_exposure: 1.0
  drawdown_trigger: 1.0   # 禁用drawdown触发
  drawdown_delever: 1.0

rebalance:
  interval_minutes: 60
  deadband_sideways: 0.000001  # 极低deadband
  deadband_trending: 0.000001
  deadband_riskoff: 0.000001

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
  one_bar_delay: false  # 无延迟
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
  deadband_multiplier_exceeded: 1.0
  deadband_cap: 1.0

# 增加初始资金
initial_equity: 10000.0
"""
    
    config_path = Path("configs/ultimate_test.yaml")
    config_path.write_text(config_content, encoding="utf-8")
    
    print(f"✅ 创建配置: {config_path}")
    print(f"  关键设置:")
    print(f"  initial_equity: $10,000 (避免现金不足)")
    print(f"  deadband: 0.0001%")
    print(f"  max_single_weight: 100%")
    print(f"  禁用所有限制")
    
    return config_path

def run_ultimate_test():
    """运行终极测试"""
    
    print("\n" + "=" * 60)
    print("🎯 运行终极测试")
    print("=" * 60)
    
    config_path = create_ultimate_test_config()
    
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
            print(f"  最大回撤: {result_data.get('max_dd', 0)*100:.2f}%")
            
            if trades > 0:
                print(f"\n🎉 🎉 🎉 终于有交易了！")
                print(f"  证明某些限制是问题")
            else:
                print(f"\n❌ 即使绕过所有限制仍然没有交易")
                print(f"  问题在更深层的逻辑")
        
        else:
            print(f"  ❌ 测试运行失败")
            print(f"  错误: {result.stderr[:200]}")
            
    except Exception as e:
        print(f"  ❌ 测试错误: {e}")

def main():
    """主函数"""
    
    print("🚀 最终调试：找出所有过滤条件")
    print("=" * 60)
    
    # 分析所有过滤条件
    analyze_all_filters()
    
    # 检查现金逻辑
    check_cash_logic()
    
    # 运行终极测试
    run_ultimate_test()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 总结:")
    print("经过系统调试，发现多个过滤条件:")
    print("  1. deadband检查")
    print("  2. min_notional检查 ($25默认值)")
    print("  3. 现金不足检查")
    print("  4. 价格无效检查")
    print("  5. 目标权重为0检查")

if __name__ == "__main__":
    main()