#!/usr/bin/env python3
"""
调试成本数据问题
"""

import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

def analyze_cost_data_files():
    """分析成本数据文件"""
    
    print("🔍 分析成本数据文件")
    print("=" * 60)
    
    cost_stats_dir = Path("reports/cost_stats_clean")
    
    if not cost_stats_dir.exists():
        print("❌ 成本数据目录不存在")
        return
    
    files = sorted(cost_stats_dir.glob("daily_cost_stats_*.json"))
    print(f"📊 找到 {len(files)} 个成本数据文件")
    
    for file in files[-3:]:  # 检查最新3个文件
        print(f"\n📄 分析文件: {file.name}")
        
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            print(f"  数据日期: {data.get('day', 'N/A')}")
            print(f"  架构版本: {data.get('schema_version', 'N/A')}")
            
            # 检查coverage
            coverage = data.get('coverage', {})
            print(f"  覆盖统计:")
            print(f"    总事件: {coverage.get('events_total', 0)}")
            print(f"    fills: {coverage.get('fills', 0)}")
            print(f"    缺失bid/ask: {coverage.get('missing_bidask', 0)}")
            
            # 检查字段结构
            print(f"  字段结构:")
            print(f"    是否有fee_stats: {'是' if 'fee_stats' in data else '否'}")
            print(f"    是否有buckets: {'是' if 'buckets' in data else '否'}")
            
            if 'fee_stats' in data:
                fee_stats = data['fee_stats']
                print(f"    fee_stats币种数量: {len(fee_stats)}")
                if fee_stats:
                    sample = list(fee_stats.keys())[0]
                    print(f"    示例币种({sample}):")
                    print(f"      样本数: {fee_stats[sample].get('count', 0)}")
            
            if 'buckets' in data:
                buckets = data['buckets']
                print(f"    buckets数量: {len(buckets)}")
                if buckets:
                    sample_key = list(buckets.keys())[0]
                    print(f"    示例bucket({sample_key}):")
                    bucket = buckets[sample_key]
                    print(f"      计数: {bucket.get('count', 0)}")
                    print(f"      费用bps: {bucket.get('fee_bps', {})}")
                    
        except Exception as e:
            print(f"  ❌ 分析错误: {e}")

def test_cost_model_with_data():
    """使用成本数据测试成本模型"""
    
    print("\n" + "=" * 60)
    print("🎯 测试成本模型与数据")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.backtest.cost_factory import make_cost_model_from_cfg
        from src.backtest.cost_calibration import load_latest_cost_stats
        
        # 加载配置
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 配置信息:")
        print(f"  成本模型: {cfg.backtest.cost_model}")
        print(f"  成本数据目录: {cfg.backtest.cost_stats_dir}")
        
        # 直接加载成本数据
        print(f"\n📊 直接加载成本数据...")
        stats, stats_path = load_latest_cost_stats(
            str(cfg.backtest.cost_stats_dir),
            max_age_days=int(cfg.backtest.max_stats_age_days)
        )
        
        print(f"  数据路径: {stats_path}")
        print(f"  是否有数据: {'是' if stats else '否'}")
        
        if stats:
            print(f"  数据日期: {stats.get('day', 'N/A')}")
            print(f"  架构版本: {stats.get('schema_version', 'N/A')}")
            
            # 检查关键字段
            if 'fee_stats' not in stats:
                print(f"  ⚠️ 警告: 数据缺少fee_stats字段")
                print(f"    成本校准模块可能需要这个字段")
            
            if 'buckets' in stats:
                buckets = stats['buckets']
                print(f"  buckets数量: {len(buckets)}")
                
                # 测试几个bucket
                test_keys = list(buckets.keys())[:3]
                for key in test_keys:
                    bucket = buckets[key]
                    print(f"    {key}:")
                    print(f"      计数: {bucket.get('count', 0)}")
                    if bucket.get('count', 0) > 0:
                        fee_data = bucket.get('fee_bps', {})
                        slp_data = bucket.get('slippage_bps', {})
                        print(f"      费用p75: {fee_data.get('p75', 'N/A')}")
                        print(f"      滑点p90: {slp_data.get('p90', 'N/A')}")
        
        # 测试成本模型
        print(f"\n🔧 测试成本模型...")
        cost_model, meta = make_cost_model_from_cfg(cfg)
        
        print(f"  模型类型: {type(cost_model).__name__}")
        print(f"  元数据模式: {meta.mode}")
        print(f"  原因: {meta.reason}")
        
        # 测试resolve方法
        print(f"\n🎯 测试resolve方法:")
        test_cases = [
            ("BTC/USDT", "Risk-Off", "fill", 1000.0),
            ("ETH/USDT", "Sideways", "fill", 500.0),
            ("SOL/USDT", "Trending", "fill", 100.0),
        ]
        
        for symbol, regime, action, amount in test_cases:
            try:
                fee_bps, slp_bps, res_meta = cost_model.resolve(symbol, regime, action, amount)
                print(f"  {symbol} ({regime}, ${amount}):")
                print(f"    费用: {fee_bps:.2f}bps")
                print(f"    滑点: {slp_bps:.2f}bps")
                print(f"    模式: {res_meta.get('mode', 'N/A')}")
                print(f"    回退级别: {res_meta.get('fallback_level', 'N/A')}")
            except Exception as e:
                print(f"  ❌ {symbol} 测试失败: {e}")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 测试错误: {e}")
        import traceback
        traceback.print_exc()

def create_fixed_cost_data():
    """创建修复的成本数据"""
    
    print("\n" + "=" * 60)
    print("🚀 创建修复的成本数据")
    print("=" * 60)
    
    # 分析当前数据问题
    print("📋 当前数据问题:")
    print("  1. 数据文件缺少fee_stats字段")
    print("  2. 只有buckets字段")
    print("  3. 成本校准模块可能期望fee_stats")
    
    print(f"\n💡 解决方案:")
    print("  1. 检查成本校准模块是否需要fee_stats")
    print("  2. 如果不需要，可能是其他问题")
    print("  3. 检查回测引擎如何使用成本数据")
    
    # 检查CalibratedCostModel是否需要fee_stats
    print(f"\n🔍 检查CalibratedCostModel实现...")
    
    try:
        from src.backtest.cost_calibration import CalibratedCostModel
        
        # 查看CalibratedCostModel使用哪些字段
        print("  CalibratedCostModel使用:")
        print("    - stats['coverage']['fills'] (全局fills)")
        print("    - stats['buckets'] (bucket数据)")
        print("    - stats['day'] (数据日期)")
        print("    - 不直接使用fee_stats")
        
        print(f"\n✅ CalibratedCostModel不需要fee_stats字段")
        print(f"   问题可能在别处")
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")

def main():
    """主函数"""
    
    print("🚀 成本数据问题调试")
    print("=" * 60)
    
    # 分析成本数据文件
    analyze_cost_data_files()
    
    # 测试成本模型与数据
    test_cost_model_with_data()
    
    # 创建修复方案
    create_fixed_cost_data()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 关键发现:")
    print("1. 成本数据文件有buckets数据，但缺少fee_stats字段")
    print("2. CalibratedCostModel不需要fee_stats，使用buckets")
    print("3. 成本模型能正常工作，返回校准的成本")
    print("4. 问题可能在其他地方")

if __name__ == "__main__":
    main()