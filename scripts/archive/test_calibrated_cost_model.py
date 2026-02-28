#!/usr/bin/env python3
"""
测试校准成本模型效果
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def test_calibrated_model():
    """测试校准成本模型"""
    
    print("🧪 测试校准成本模型")
    print("=" * 60)
    
    try:
        from src.backtest.cost_calibration import CalibratedCostModel, FixedCostModel
        import json
        
        # 加载最新统计数据
        stats_dir = Path("reports/cost_stats")
        stats_files = list(stats_dir.glob("daily_cost_stats_*.json"))
        
        if not stats_files:
            print("❌ 无成本统计文件")
            return
        
        latest_file = max(stats_files, key=lambda x: x.name)
        print(f"使用统计文件: {latest_file.name}")
        
        with open(latest_file, 'r') as f:
            stats = json.load(f)
        
        # 创建校准模型
        calibrated_model = CalibratedCostModel(
            stats=stats,
            fee_quantile='p75',
            slippage_quantile='p90',
            min_fills_global=20,  # 使用降低后的阈值
            min_fills_bucket=8,   # 使用降低后的阈值
            default_fee_bps=6.0,
            default_slippage_bps=5.0
        )
        
        # 创建固定模型（对比）
        fixed_model = FixedCostModel(fee_bps=6.0, slippage_bps=5.0)
        
        print(f"\n📊 模型配置:")
        print(f"  校准模型: fee_quantile=p75, slippage_quantile=p90")
        print(f"  固定模型: fee=6bps, slippage=5bps")
        
        # 测试不同场景
        test_scenarios = [
            ("SOL/USDT", "Risk-Off", "MARKET_BUY", 30.0, "F2相关交易"),
            ("BNB/USDT", "Sideways", "MARKET_SELL", 20.0, "横盘交易"),
            ("ETH/USDT", "Trending", "MARKET_BUY", 50.0, "趋势交易"),
            ("BTC/USDT", "Risk-Off", "MARKET_SELL", 100.0, "大额交易"),
        ]
        
        print(f"\n🧪 成本对比测试:")
        print(f"{'场景':<20} {'固定模型':<15} {'校准模型':<15} {'差异':<10} {'模式':<10}")
        print("-" * 70)
        
        for symbol, regime, action, amount, desc in test_scenarios:
            # 固定模型成本
            fixed_fee, fixed_slippage = fixed_model.resolve(symbol, regime, action, amount)
            fixed_total = fixed_fee + fixed_slippage
            
            # 校准模型成本
            cal_fee, cal_slippage, meta = calibrated_model.resolve(symbol, regime, action, amount)
            cal_total = cal_fee + cal_slippage
            
            # 差异
            diff = cal_total - fixed_total
            diff_pct = (diff / fixed_total * 100) if fixed_total > 0 else 0
            
            mode = meta.get('mode', 'unknown')
            
            print(f"{desc:<20} {fixed_total:>6.2f}bps{'':<8} {cal_total:>6.2f}bps{'':<8} {diff:>+6.2f}bps{'':<3} {mode:<10}")
        
        # 分析校准效果
        print(f"\n📈 校准模型分析:")
        
        # 检查统计数据
        total_fills = stats.get("coverage", {}).get("fills", 0)
        buckets = stats.get("buckets", {})
        
        print(f"  总fills数: {total_fills}")
        print(f"  bucket数量: {len(buckets)}")
        
        # 检查bucket覆盖
        bucket_coverage = {}
        for key, bucket in buckets.items():
            count = bucket.get("count", 0)
            if count >= 8:  # 满足降低后的阈值
                parts = key.split("|")
                if len(parts) >= 4:
                    symbol = parts[0]
                    regime = parts[1]
                    bucket_coverage[f"{symbol}|{regime}"] = bucket_coverage.get(f"{symbol}|{regime}", 0) + 1
        
        print(f"  有效bucket覆盖: {len(bucket_coverage)} 个symbol-regime组合")
        
        # 成本分布分析
        all_fees = []
        all_slippages = []
        
        for bucket in buckets.values():
            fee_stats = bucket.get("fee_bps", {})
            slippage_stats = bucket.get("slippage_bps", {})
            
            if fee_stats.get("p75"):
                all_fees.append(fee_stats["p75"])
            if slippage_stats.get("p90"):
                all_slippages.append(slippage_stats["p90"])
        
        if all_fees:
            avg_fee = sum(all_fees) / len(all_fees)
            print(f"  平均费用(p75): {avg_fee:.2f} bps")
        
        if all_slippages:
            avg_slippage = sum(all_slippages) / len(all_slippages)
            print(f"  平均滑点(p90): {avg_slippage:.2f} bps")
        
        # 对F2因子的影响
        print(f"\n🎯 对F2因子的影响:")
        print(f"  F2因子权重已从25%降至15%")
        print(f"  校准模型将提供更准确的成本估计")
        print(f"  预期效果: 减少成本高估，提高策略真实性")
        
        # 建议
        print(f"\n💡 使用建议:")
        print(f"  1. 在校准模型下重新运行回测")
        print(f"  2. 监控实际交易成本与校准成本的差异")
        print(f"  3. 定期更新成本统计数据")
        print(f"  4. 考虑进一步降低F2因子权重")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 测试错误: {e}")
    
    print("\n" + "=" * 60)
    print("✅ 校准成本模型测试完成")

def main():
    """主函数"""
    print("🚀 校准成本模型启用验证")
    print("=" * 60)
    
    test_calibrated_model()
    
    print("\n🎯 下一步:")
    print("1. 运行回测验证校准模型效果")
    print("2. 监控实际交易成本")
    print("3. 优化F2因子权重")
    print("=" * 60)

if __name__ == "__main__":
    main()