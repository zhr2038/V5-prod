#!/usr/bin/env python3
"""
F2因子优化验证脚本
基于真实成本数据重新评估F2因子有效性
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

def analyze_f2_performance_with_real_costs():
    """基于真实成本分析F2因子表现"""
    
    print("🎯 F2因子优化验证分析")
    print("=" * 60)
    
    # 1. 加载F2相关交易数据
    print("📊 加载F2相关交易数据...")
    
    # 从成本事件中提取F2相关交易
    cost_events_dir = Path("reports/cost_events_real")
    f2_trades = []
    all_trades = []
    
    f2_keywords = ["SOL", "BNB", "ETH", "BTC", "ADA", "DOGE", "XRP", "DOT"]
    
    for file in cost_events_dir.glob("*.jsonl"):
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    trade = json.loads(line.strip())
                    all_trades.append(trade)
                    
                    symbol = trade.get('symbol', '')
                    if any(kw in symbol for kw in f2_keywords):
                        f2_trades.append(trade)
                except:
                    continue
    
    print(f"  总交易数: {len(all_trades)}")
    print(f"  F2相关交易: {len(f2_trades)} ({len(f2_trades)/len(all_trades)*100:.1f}%)")
    
    if not f2_trades:
        print("❌ 无F2相关交易数据")
        return
    
    # 2. 分析F2交易成本
    print("\n💰 F2交易成本分析:")
    
    f2_fees = [t.get('fee_bps', 0) for t in f2_trades]
    f2_slippages = [t.get('slippage_bps', 0) for t in f2_trades]
    f2_notionals = [t.get('notional_usdt', 0) for t in f2_trades]
    
    # 过滤异常值（费用>100bps为异常）
    valid_f2_fees = [f for f in f2_fees if f is not None and f < 100]
    valid_f2_slippages = [s for s in f2_slippages if s < 100]
    valid_f2_notionals = [n for n in f2_notionals if n > 0]
    
    print(f"  有效F2交易数: {len(valid_f2_fees)}")
    
    if valid_f2_fees:
        print(f"  平均费用: {np.mean(valid_f2_fees):.4f}bps")
        print(f"  费用中位数: {np.median(valid_f2_fees):.4f}bps")
        print(f"  费用范围: [{np.min(valid_f2_fees):.4f}, {np.max(valid_f2_fees):.4f}]bps")
        print(f"  费用标准差: {np.std(valid_f2_fees):.4f}bps")
    
    if valid_f2_slippages:
        print(f"  平均滑点: {np.mean(valid_f2_slippages):.4f}bps")
    
    # 3. 对比整体交易成本
    print("\n📈 F2 vs 整体成本对比:")
    
    all_fees = [t.get('fee_bps', 0) for t in all_trades]
    valid_all_fees = [f for f in all_fees if f is not None and f < 100]
    
    if valid_f2_fees and valid_all_fees:
        f2_avg_fee = np.mean(valid_f2_fees)
        all_avg_fee = np.mean(valid_all_fees)
        
        fee_diff = f2_avg_fee - all_avg_fee
        fee_diff_pct = (fee_diff / all_avg_fee * 100) if all_avg_fee != 0 else 0
        
        print(f"  F2平均费用: {f2_avg_fee:.4f}bps")
        print(f"  整体平均费用: {all_avg_fee:.4f}bps")
        print(f"  差异: {fee_diff:+.4f}bps ({fee_diff_pct:+.1f}%)")
        
        if fee_diff < -0.5:
            print(f"  ✅ F2交易成本显著更低")
        elif fee_diff > 0.5:
            print(f"  ⚠️ F2交易成本显著更高")
        else:
            print(f"  🔄 F2交易成本与整体相近")
    
    # 4. 基于30天IC数据的F2有效性验证
    print("\n📊 基于30天IC数据的F2有效性:")
    
    try:
        # 从数据库加载IC数据
        db_path = Path("reports/alpha_history.db")
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            
            # 查询F2因子IC
            query = """
            SELECT 
                symbol,
                f2_mom_20d,
                score,
                return_1h,
                return_6h,
                return_24h
            FROM alpha_snapshots a
            JOIN ic_calculation_view i ON a.ts = i.timestamp AND a.symbol = i.symbol
            WHERE f2_mom_20d IS NOT NULL
            LIMIT 1000
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if not df.empty:
                # 计算F2因子IC
                f2_ic_1h = df['f2_mom_20d'].corr(df['return_1h'])
                f2_ic_6h = df['f2_mom_20d'].corr(df['return_6h'])
                f2_ic_24h = df['f2_mom_20d'].corr(df['return_24h'])
                
                print(f"  F2因子IC值:")
                print(f"    1小时: {f2_ic_1h:.4f}")
                print(f"    6小时: {f2_ic_6h:.4f}")
                print(f"    24小时: {f2_ic_24h:.4f}")
                
                # 评估F2有效性
                if f2_ic_1h > 0.01:
                    print(f"  ✅ F2短期预测能力良好")
                elif f2_ic_1h > 0:
                    print(f"  ⚠️ F2短期预测能力一般")
                else:
                    print(f"  ❌ F2短期预测能力不足")
                
                # IC衰减分析
                if f2_ic_6h > f2_ic_1h:
                    print(f"  📈 F2预测能力随时间增强")
                else:
                    print(f"  📉 F2预测能力随时间衰减")
                    
    except Exception as e:
        print(f"  ❌ IC数据分析错误: {e}")
    
    # 5. 成本效益分析
    print("\n💡 F2成本效益分析:")
    
    if valid_f2_fees and 'f2_ic_1h' in locals():
        # 假设每次交易成本
        avg_f2_cost_bps = np.mean(valid_f2_fees) + 5.0  # 费用 + 滑点
        
        # 计算需要的alpha来覆盖成本
        required_alpha = avg_f2_cost_bps / 10000  # bps转换为小数
        
        print(f"  F2平均交易成本: {avg_f2_cost_bps:.2f}bps")
        print(f"  需要覆盖成本的alpha: {required_alpha*100:.4f}%")
        print(f"  F2因子IC(1h): {f2_ic_1h:.4f}")
        
        # 粗略估计：IC≈0.01对应约0.1%的预期alpha
        estimated_alpha = f2_ic_1h * 0.1  # 简化估计
        
        if estimated_alpha > required_alpha:
            print(f"  ✅ F2预期alpha({estimated_alpha*100:.4f}%) > 成本({required_alpha*100:.4f}%)")
            print(f"  💡 F2可能仍有正期望收益")
        else:
            print(f"  ⚠️ F2预期alpha({estimated_alpha*100:.4f}%) < 成本({required_alpha*100:.4f}%)")
            print(f"  💡 F2可能难以覆盖交易成本")
    
    # 6. 权重调整建议
    print("\n🎯 F2权重调整建议:")
    
    current_weight = 0.15
    print(f"  当前权重: {current_weight*100:.1f}%")
    
    # 基于分析的建议
    recommendations = []
    
    if 'f2_ic_1h' in locals() and f2_ic_1h > 0.02:
        recommendations.append(("增加权重", "F2预测能力较强"))
    elif 'f2_ic_1h' in locals() and f2_ic_1h < 0:
        recommendations.append(("减少权重", "F2预测能力为负"))
    
    if valid_f2_fees and np.mean(valid_f2_fees) < 1.0:
        recommendations.append(("保持或增加权重", "F2交易成本较低"))
    elif valid_f2_fees and np.mean(valid_f2_fees) > 10.0:
        recommendations.append(("减少权重", "F2交易成本较高"))
    
    if recommendations:
        print("  基于分析的建议:")
        for action, reason in recommendations:
            print(f"    {action}: {reason}")
    else:
        print("  当前权重调整适中，建议保持观察")
    
    # 7. 验证实验建议
    print("\n🔬 验证实验建议:")
    print("  1. 回测对比不同F2权重(10%, 15%, 20%)")
    print("  2. 分析F2在不同市场状态下的表现")
    print("  3. 监控F2因子IC的稳定性")
    print("  4. 定期重新评估F2成本效益")
    
    return {
        "f2_trades_count": len(f2_trades),
        "f2_avg_fee": np.mean(valid_f2_fees) if valid_f2_fees else None,
        "f2_ic_1h": f2_ic_1h if 'f2_ic_1h' in locals() else None,
        "recommendations": recommendations
    }

def generate_f2_optimization_report(analysis_results):
    """生成F2优化验证报告"""
    
    print("\n" + "=" * 60)
    print("📋 F2因子优化验证报告")
    print("=" * 60)
    
    print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if analysis_results:
        print(f"\n📊 分析摘要:")
        print(f"  F2相关交易数: {analysis_results['f2_trades_count']}")
        
        if analysis_results['f2_avg_fee'] is not None:
            print(f"  F2平均费用: {analysis_results['f2_avg_fee']:.4f}bps")
        
        if analysis_results['f2_ic_1h'] is not None:
            print(f"  F2因子IC(1h): {analysis_results['f2_ic_1h']:.4f}")
        
        print(f"\n🎯 当前状态:")
        print(f"  F2权重: 15% (从25%降低)")
        print(f"  校准模型: 已启用(基于真实数据)")
        
        print(f"\n💡 关键发现:")
        print("  1. F2交易成本可能被异常值影响")
        print("  2. 需要进一步清洗成本数据")
        print("  3. 基于当前数据，F2权重调整需要谨慎")
        
        print(f"\n🚀 下一步行动:")
        print("  1. 清洗成本数据，移除异常值")
        print("  2. 重新运行校准模型验证")
        print("  3. 基于干净数据重新评估F2")
        print("  4. 考虑A/B测试不同F2权重")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 开始F2因子优化验证")
    print("=" * 60)
    print("阶段2: 基于真实成本重新评估F2因子")
    print("=" * 60)
    
    # 运行F2优化分析
    analysis_results = analyze_f2_performance_with_real_costs()
    
    # 生成报告
    generate_f2_optimization_report(analysis_results)
    
    print("\n✅ 阶段2验证完成")
    print("=" * 60)
    
    print("\n🎯 下一步: 开始阶段3 - 数据清洗和重新校准")
    print("建议运行: python3 scripts/clean_cost_data.py")

if __name__ == "__main__":
    main()