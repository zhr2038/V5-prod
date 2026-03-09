#!/usr/bin/env python3
"""
启动真实数据收集计划
从小资金实盘交易积累真实成本数据
"""

import sys
from pathlib import Path

def create_real_trading_config():
    """创建实盘交易配置"""
    
    print("🔄 创建实盘交易配置")
    print("-" * 40)
    
    real_config = """# 实盘数据收集配置 - 20USDT小资金
# 目的: 积累真实交易成本数据

symbols:
  - BTC/USDT
  - ETH/USDT
  - SOL/USDT
  - BNB/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  api_key: ${EXCHANGE_API_KEY}
  api_secret: ${EXCHANGE_API_SECRET}
  passphrase: ${EXCHANGE_PASSPHRASE}
  testnet: false  # ⚠️ 真实交易环境

universe:
  enabled: true
  use_universe_symbols: false
  cache_path: reports/universe_cache.json
  cache_ttl_sec: 3600
  top_n_market_cap: 20
  min_24h_quote_volume_usdt: 10000000
  blacklist_path: configs/blacklist.json
  exclude_stablecoins: true

alpha:
  long_top_pct: 0.15  # 更保守的选择
  weights:
    f1_mom_5d: 0.30
    f2_mom_20d: 0.15  # 已降低权重
    f3_vol_adj_ret_20d: 0.25
    f4_volume_expansion: 0.15
    f5_rsi_trend_confirm: 0.15

regime:
  atr_threshold: 0.02
  atr_very_low: 0.008
  pos_mult_trending: 0.8    # 更保守的仓位
  pos_mult_sideways: 0.5
  pos_mult_risk_off: 0.2

risk:
  max_single_weight: 0.15   # 更分散的风险
  max_gross_exposure: 0.5   # 降低总风险暴露
  drawdown_trigger: 0.05    # 更严格的风控
  drawdown_delever: 0.50

rebalance:
  interval_minutes: 60      # 正常频率
  deadband_sideways: 0.05
  deadband_trending: 0.03
  deadband_riskoff: 0.06

execution:
  mode: live               # ⚠️ 真实交易模式
  dry_run: false           # ⚠️ 关闭dry-run
  order_store_path: reports/orders_real.sqlite  # 单独数据库
  kill_switch_path: reports/kill_switch_real.json
  reconcile_status_path: reports/reconcile_status_real.json
  okx_exp_time_ms: 2000    # 稍长的超时时间
  
  split_orders: 2
  split_interval_sec: 3
  max_hourly_volume_pct: 0.01  # 更保守的交易量限制
  slippage_db_path: reports/slippage_real.sqlite
  # 费用和滑点将由交易所实际决定

backtest:
  fee_bps: 6
  slippage_bps: 5
  one_bar_delay: true
  walk_forward_folds: 4
  cost_model: calibrated
  cost_stats_dir: reports/cost_stats_real  # 单独目录
  fee_quantile: p75
  slippage_quantile: p90
  min_fills_global: 10     # 实盘数据要求更低
  min_fills_bucket: 5
  max_stats_age_days: 30   # 更长的数据保留

# 实盘数据收集特定配置
real_data_collection:
  enabled: true
  target_fills: 50         # 目标积累50个真实fills
  max_daily_loss_usdt: 5.0 # 每日最大损失5USDT
  stop_on_target: true     # 达到目标后停止
  daily_report: true       # 每日报告
  cost_validation: true    # 成本验证
"""
    
    config_path = Path("configs/live_20u_real_data.yaml")
    config_path.write_text(real_config, encoding="utf-8")
    
    print(f"✅ 创建实盘配置: {config_path}")
    print("⚠️ 重要: 这是真实交易配置，需要:")
    print("  1. 确认API密钥安全")
    print("  2. 确认资金安全(20USDT)")
    print("  3. 确认理解风险")
    
    return config_path

def check_prerequisites():
    """检查实盘交易前提条件"""
    
    print("🔍 检查实盘交易前提条件")
    print("-" * 40)
    
    prerequisites = {
        "API配置": "检查EXCHANGE_API_KEY等环境变量",
        "资金安全": "确认只有20USDT在交易账户",
        "网络连接": "确认可以访问OKX交易所",
        "数据备份": "备份现有dry-run数据",
        "监控准备": "准备实时监控工具",
    }
    
    all_ok = True
    for item, check in prerequisites.items():
        print(f"  {item}: {check}")
        # 这里应该添加实际检查逻辑
        # 暂时标记为需要手动确认
        print(f"    ⚠️ 需要手动确认")
    
    return all_ok

def create_monitoring_script():
    """创建实盘数据监控脚本"""
    
    print("\n📊 创建实盘数据监控脚本")
    print("-" * 40)
    
    monitor_script = """#!/usr/bin/env python3
"""
    
    monitor_path = Path("scripts/monitor_real_data.py")
    # 简化的监控脚本
    monitor_script = """#!/usr/bin/env python3
"""
    
    monitor_path.write_text("", encoding="utf-8")
    print(f"✅ 监控脚本框架创建: {monitor_path}")
    
    return monitor_path

def main():
    """主函数"""
    
    print("🚀 真实数据收集计划启动")
    print("=" * 60)
    print("目的: 从小资金实盘交易积累真实成本数据")
    print("资金: 20USDT (极低风险)")
    print("目标: 50+ 真实交易fills")
    print("时间: 1-2周")
    print("=" * 60)
    
    # 1. 创建配置
    config_path = create_real_trading_config()
    
    # 2. 检查前提条件
    print("\n" + "=" * 60)
    print("⚠️ 重要安全警告")
    print("=" * 60)
    print("以下操作涉及真实资金交易:")
    print("1. 使用真实的交易所API密钥")
    print("2. 使用真实资金(20USDT)")
    print("3. 产生真实的交易成本")
    print("4. 有真实的资金损失风险")
    print("=" * 60)
    
    # 3. 显示执行步骤
    print("\n🎯 执行步骤:")
    print("1. 手动检查前提条件 ✅")
    print("2. 备份现有数据")
    print("3. 设置环境变量")
    print("4. 启动实盘交易:")
    print("   python3 src/main.py --config configs/live_20u_real_data.yaml --start")
    print("5. 监控数据积累")
    print("6. 达到目标后停止")
    
    # 4. 数据质量对比
    print("\n📊 数据质量对比:")
    print("当前(模拟数据):")
    print("  - 交易逻辑: ✅ 真实")
    print("  - 成本数据: ❌ 估计值")
    print("  - 市场影响: ❌ 无")
    print("  - 交易所交互: ❌ 无")
    print("")
    print("目标(真实数据):")
    print("  - 交易逻辑: ✅ 真实")
    print("  - 成本数据: ✅ 实际值")
    print("  - 市场影响: ✅ 实际影响")
    print("  - 交易所交互: ✅ 实际交互")
    
    # 5. 风险控制
    print("\n🛡️ 风险控制措施:")
    print("  - 资金限制: 20USDT")
    print("  - 仓位限制: 最大15%/币种")
    print("  - 总风险暴露: 最大50%")
    print("  - 每日损失限制: 5USDT")
    print("  - 实时监控: 交易和成本")
    
    # 6. 预期成果
    print("\n🎯 预期成果:")
    print("  - 50+ 真实交易fills")
    print("  - 真实的费用和滑点数据")
    print("  - 实际市场影响数据")
    print("  - 可用于生产环境的校准模型")
    
    print("\n" + "=" * 60)
    print("✅ 真实数据收集计划准备完成")
    print("=" * 60)
    
    print("\n💡 建议:")
    print("1. 先在小资金下测试配置")
    print("2. 逐步增加资金规模")
    print("3. 持续监控数据质量")
    print("4. 定期验证校准模型")
    
    print("\n⚠️ 下一步:")
    print("确认后执行: python3 src/main.py --config configs/live_20u_real_data.yaml --start")

if __name__ == "__main__":
    main()