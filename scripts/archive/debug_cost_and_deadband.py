#!/usr/bin/env python3
"""
深度调试：成本模型和deadband影响
"""

import sys
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_cost_model_impact():
    """调试成本模型影响"""
    
    print("🔍 深度调试：成本模型影响")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.backtest.cost_factory import make_cost_model_from_cfg
        
        # 测试不同成本模型
        test_configs = [
            {"name": "当前配置", "cost_model": "calibrated", "desc": "校准成本模型"},
            {"name": "固定成本", "cost_model": "fixed", "desc": "固定成本模型"},
            {"name": "无成本", "cost_model": "zero", "desc": "零成本模型"},
        ]
        
        for test_cfg in test_configs:
            print(f"\n🎯 测试 {test_cfg['name']}: {test_cfg['desc']}")
            
            # 创建临时配置
            import yaml
            base_cfg = load_config("configs/config.yaml", env_path=".env")
            
            # 修改成本模型
            cfg_dict = base_cfg.model_dump()
            cfg_dict["backtest"]["cost_model"] = test_cfg["cost_model"]
            
            # 创建新配置
            from configs.schema import AppConfig
            temp_cfg = AppConfig.model_validate(cfg_dict)
            
            # 创建成本模型
            try:
                cost_model = make_cost_model_from_cfg(temp_cfg)
                print(f"  ✅ 成本模型创建成功")
                print(f"     类型: {type(cost_model).__name__}")
                
                # 测试成本估计
                test_symbol = "BTC/USDT"
                test_amounts = [100, 1000, 10000]
                
                for amount in test_amounts:
                    try:
                        fee = cost_model.estimate_fee(test_symbol, amount)
                        slippage = cost_model.estimate_slippage(test_symbol, amount)
                        total_cost = fee + slippage
                        
                        print(f"     测试金额 ${amount}:")
                        print(f"       费用: {fee*10000:.2f}bps")
                        print(f"       滑点: {slippage*10000:.2f}bps")
                        print(f"       总成本: {total_cost*10000:.2f}bps")
                        
                    except Exception as e:
                        print(f"     成本估计错误: {e}")
                
            except Exception as e:
                print(f"  ❌ 成本模型创建失败: {e}")
        
        # 检查成本数据状态
        print(f"\n📊 成本数据状态检查...")
        
        cost_stats_dir = Path("reports/cost_stats_clean")
        if cost_stats_dir.exists():
            files = list(cost_stats_dir.glob("*.json"))
            print(f"  成本数据文件: {len(files)}个")
            
            if files:
                # 读取最新文件
                latest_file = max(files, key=lambda x: x.stat().st_mtime)
                import json
                data = json.loads(latest_file.read_text())
                
                print(f"  最新文件: {latest_file.name}")
                print(f"  数据日期: {data.get('day_utc', 'N/A')}")
                print(f"  全局fills: {data.get('global_fills', 0)}")
                
                # 检查币种数据
                symbols = list(data.get("fee_stats", {}).keys())
                print(f"  币种数量: {len(symbols)}")
                
                if symbols:
                    sample_symbol = symbols[0]
                    fee_stats = data["fee_stats"].get(sample_symbol, {})
                    print(f"  示例币种({sample_symbol})统计:")
                    print(f"    样本数: {fee_stats.get('count', 0)}")
                    print(f"    中位数: {fee_stats.get('median', 0)*10000:.2f}bps")
        
        print(f"\n💡 成本模型调试完成")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def debug_deadband_impact():
    """调试deadband影响"""
    
    print("\n" + "=" * 60)
    print("🎯 调试Deadband影响")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 当前Deadband配置:")
        print(f"  横盘状态: {cfg.rebalance.deadband_sideways} ({cfg.rebalance.deadband_sideways*100:.1f}%)")
        print(f"  趋势状态: {cfg.rebalance.deadband_trending} ({cfg.rebalance.deadband_trending*100:.1f}%)")
        print(f"  风险规避: {cfg.rebalance.deadband_riskoff} ({cfg.rebalance.deadband_riskoff*100:.1f}%)")
        
        print(f"\n💡 Deadband对交易的影响:")
        print(f"  Deadband = 调仓阈值")
        print(f"  当仓位变化 < deadband时，不调仓")
        print(f"  较高的deadband → 较少调仓")
        print(f"  较低的deadband → 较多调仓")
        
        print(f"\n🎯 基于市场分析的调整建议:")
        print(f"  当前市场: 下降趋势，波动率0.75-1.22%/小时")
        print(f"  建议调整:")
        print(f"    - 降低横盘deadband: {cfg.rebalance.deadband_sideways*100:.1f}% → 3.0%")
        print(f"    - 降低趋势deadband: {cfg.rebalance.deadband_trending*100:.1f}% → 2.0%")
        print(f"    - 降低风险规避deadband: {cfg.rebalance.deadband_riskoff*100:.1f}% → 4.0%")
        
        # 模拟deadband影响
        print(f"\n📊 Deadband影响模拟:")
        
        test_portfolio_changes = [0.02, 0.03, 0.04, 0.05, 0.06]  # 2-6%的仓位变化
        
        for change in test_portfolio_changes:
            would_rebalance = change > cfg.rebalance.deadband_sideways
            print(f"  仓位变化 {change*100:.1f}%: {'调仓' if would_rebalance else '不调仓'}")
        
    except Exception as e:
        print(f"❌ Deadband调试错误: {e}")

def create_optimized_config():
    """创建优化配置"""
    
    print("\n" + "=" * 60)
    print("🚀 创建综合优化配置")
    print("=" * 60)
    
    optimized_config = """# 综合优化配置 - 基于深度调试结果
symbols:
  - BTC/USDT
  - ETH/USDT
  - SOL/USDT
  - BNB/USDT
  - ADA/USDT
  - DOGE/USDT

timeframe_main: 1h
timeframe_aux: 4h

exchange:
  name: okx
  testnet: false

alpha:
  long_top_pct: 0.25  # 选择前25% (原20%)
  weights:
    f1_mom_5d: 0.30   # 增加短期动量权重
    f2_mom_20d: 0.25  # 增加长期动量权重
    f3_vol_adj_ret_20d: 0.20
    f4_volume_expansion: 0.15
    f5_rsi_trend_confirm: 0.10

regime:
  atr_threshold: 0.01      # 降低阈值到1% (原2%)
  atr_very_low: 0.004      # 降低极低阈值到0.4% (原0.8%)
  pos_mult_trending: 1.2
  pos_mult_sideways: 0.7   # 提高横盘仓位 (原0.6)
  pos_mult_risk_off: 0.7   # 提高Risk-Off仓位 (原0.3)

risk:
  max_single_weight: 0.25
  max_gross_exposure: 1.0
  drawdown_trigger: 0.08
  drawdown_delever: 0.50

rebalance:
  interval_minutes: 60
  deadband_sideways: 0.03  # 降低deadband到3% (原5%)
  deadband_trending: 0.02  # 降低deadband到2% (原3%)
  deadband_riskoff: 0.04   # 降低deadband到4% (原5%)

execution:
  mode: dry_run
  dry_run: true
  fee_bps: 6
  slippage_bps: 5

backtest:
  start_date: "2026-01-19"
  end_date: "2026-02-17"
  fee_bps: 6
  slippage_bps: 5
  one_bar_delay: true
  walk_forward_folds: 4
  cost_model: fixed        # 使用固定成本避免限制
  cost_stats_dir: ""
  fee_quantile: p75
  slippage_quantile: p90
  min_fills_global: 1      # 极低要求
  min_fills_bucket: 1
  max_stats_age_days: 365  # 不限制数据年龄
"""
    
    print("📋 优化配置要点:")
    print("  1. Regime阈值调整: 2% → 1%")
    print("  2. Risk-Off仓位提高: 0.3 → 0.7")
    print("  3. Deadband降低: 5% → 3%")
    print("  4. 成本模型: calibrated → fixed (避免限制)")
    print("  5. Alpha权重调整: 增加动量因子权重")
    print("  6. Top选择: 20% → 25%")
    
    # 保存配置
    config_path = Path("configs/optimized_debug.yaml")
    config_path.write_text(optimized_config, encoding="utf-8")
    
    print(f"\n✅ 优化配置已保存到: {config_path}")
    print(f"   使用命令: V5_CONFIG={config_path} python3 scripts/run_walk_forward.py")

def main():
    """主函数"""
    
    print("🚀 成本模型和Deadband深度调试")
    print("=" * 60)
    
    # 调试成本模型影响
    debug_cost_model_impact()
    
    # 调试deadband影响
    debug_deadband_impact()
    
    # 创建优化配置
    create_optimized_config()
    
    print("\n✅ 深度调试完成")
    print("=" * 60)
    
    print("\n💡 下一步行动:")
    print("1. 使用优化配置运行walk-forward测试")
    print("2. 如果仍无交易，需要端到端调试")
    print("3. 检查完整的策略执行流程")

if __name__ == "__main__":
    main()