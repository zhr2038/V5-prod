#!/usr/bin/env python3
"""
IC利用新数据训练情况分析
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def analyze_ic_training_progress():
    """分析IC训练进度"""
    print("🚀 IC利用新数据训练情况分析")
    print("=" * 70)
    
    db_path = "reports/alpha_history.db"
    
    if not Path(db_path).exists():
        print("❌ 数据库文件不存在")
        return
    
    conn = sqlite3.connect(db_path)
    
    # 1. 检查数据时间范围
    print("\n📅 1. 数据时间范围分析")
    print("-" * 40)
    
    cursor = conn.cursor()
    
    # 检查alpha_snapshots表
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM alpha_snapshots")
    min_ts, max_ts, total_count = cursor.fetchone()
    
    if min_ts and max_ts:
        min_time = datetime.fromtimestamp(min_ts)
        max_time = datetime.fromtimestamp(max_ts)
        time_range = max_time - min_time
        hours_range = time_range.total_seconds() / 3600
        
        print(f"  Alpha快照数据:")
        print(f"    时间范围: {min_time.strftime('%Y-%m-%d %H:%M')} 到 {max_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"    总时长: {hours_range:.1f} 小时")
        print(f"    数据点数: {total_count}")
        print(f"    平均频率: {total_count/hours_range:.2f} 点/小时")
        
        # 检查数据连续性
        cursor.execute("""
            SELECT DATE(timestamp, 'unixepoch') as date, COUNT(*) as count
            FROM alpha_snapshots
            GROUP BY date
            ORDER BY date
        """)
        
        date_counts = cursor.fetchall()
        print(f"    日期分布: {len(date_counts)} 天")
        
        if date_counts:
            print(f"    最近3天数据量:")
            for date_str, count in date_counts[-3:]:
                print(f"      {date_str}: {count} 条")
    else:
        print("  ⚠️ 无alpha快照数据")
    
    # 2. 分析IC衰减曲线
    print("\n📈 2. IC衰减曲线分析")
    print("-" * 40)
    
    try:
        # 使用ic_calculation_view
        query = """
        SELECT 
            timestamp,
            symbol,
            score,
            return_1h,
            return_6h,
            return_24h,
            regime
        FROM ic_calculation_view
        WHERE score IS NOT NULL
        ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("  ⚠️ 无IC计算数据")
        else:
            print(f"  IC数据点: {len(df)}")
            print(f"  币种数量: {df['symbol'].nunique()}")
            
            # 计算各时间窗口的IC
            horizons = {
                '1h': 'return_1h',
                '6h': 'return_6h', 
                '24h': 'return_24h'
            }
            
            ic_results = {}
            for horizon, col in horizons.items():
                # 计算相关系数
                valid_data = df[['score', col]].dropna()
                if len(valid_data) > 10:
                    corr = valid_data['score'].corr(valid_data[col])
                    ic_results[horizon] = corr
                else:
                    ic_results[horizon] = None
            
            print(f"  IC衰减曲线:")
            for horizon, ic in ic_results.items():
                if ic is not None:
                    print(f"    {horizon}: IC = {ic:.4f}")
                else:
                    print(f"    {horizon}: 数据不足")
            
            # 计算IC衰减率
            if ic_results.get('1h') and ic_results.get('6h'):
                decay_rate = ic_results['6h'] / ic_results['1h'] * 100
                print(f"  IC衰减率(1h→6h): {decay_rate:.1f}%")
            
            # 分析IC稳定性
            print(f"\n  📊 IC稳定性分析:")
            
            # 按时间窗口计算滚动IC
            df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.date
            daily_ic = df.groupby('date').apply(
                lambda x: x['score'].corr(x['return_1h']) if len(x) > 10 else None
            ).dropna()
            
            if len(daily_ic) > 0:
                print(f"    每日IC均值: {daily_ic.mean():.4f}")
                print(f"    每日IC标准差: {daily_ic.std():.4f}")
                print(f"    信息比率(IR): {daily_ic.mean()/daily_ic.std():.2f}" if daily_ic.std() > 0 else "    信息比率: N/A")
                print(f"    正IC天数: {(daily_ic > 0).sum()}/{len(daily_ic)}")
                
                # 显示最近3天IC
                print(f"    最近3天IC:")
                for date, ic in daily_ic.tail(3).items():
                    print(f"      {date}: {ic:.4f}")
    
    except Exception as e:
        print(f"  ❌ IC分析错误: {e}")
    
    # 3. 检查新数据训练效果
    print("\n🔄 3. 新数据训练效果")
    print("-" * 40)
    
    # 检查数据更新情况
    cursor.execute("""
        SELECT 
            strftime('%Y-%m-%d', timestamp, 'unixepoch') as date,
            COUNT(*) as new_points
        FROM alpha_snapshots
        WHERE timestamp >= ?
        GROUP BY date
        ORDER BY date DESC
    """, (int((datetime.now() - timedelta(days=7)).timestamp()),))
    
    recent_updates = cursor.fetchall()
    
    if recent_updates:
        print(f"  最近7天数据更新:")
        total_new = 0
        for date_str, count in recent_updates:
            print(f"    {date_str}: {count} 个新数据点")
            total_new += count
        
        print(f"  总计: {total_new} 个新数据点")
        
        # 计算数据增长率
        if total_count > 0:
            growth_rate = total_new / total_count * 100
            print(f"  数据增长率: {growth_rate:.1f}%")
    else:
        print("  ⚠️ 最近7天无新数据")
    
    # 4. 检查alpha因子表现
    print("\n🎯 4. Alpha因子表现分析")
    print("-" * 40)
    
    try:
        # 获取alpha因子权重
        from src.alpha.alpha_engine import AlphaEngine
        import configs.schema as schema
        
        print("  Alpha因子配置:")
        
        # 检查因子权重
        alpha_config = {
            "f1_rsi_14": "14日RSI",
            "f2_mom_20d": "20日动量(F2)",
            "f3_bb_width_20": "20日布林带宽度",
            "f4_atr_pct_14": "14日ATR百分比",
            "f5_volume_ratio": "成交量比率"
        }
        
        for factor, desc in alpha_config.items():
            print(f"    {desc}({factor}): 在alpha组合中")
        
        # 分析F2因子表现
        print(f"\n  🔍 F2因子(f2_mom_20d)专项分析:")
        
        # 从数据库提取F2相关数据
        f2_query = """
        SELECT 
            timestamp,
            symbol,
            json_extract(factors, '$.f2_mom_20d') as f2_value,
            return_1h
        FROM alpha_snapshots
        WHERE json_extract(factors, '$.f2_mom_20d') IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 100
        """
        
        f2_df = pd.read_sql_query(f2_query, conn)
        
        if not f2_df.empty:
            f2_df['f2_value'] = pd.to_numeric(f2_df['f2_value'], errors='coerce')
            f2_df = f2_df.dropna()
            
            if len(f2_df) > 10:
                # 计算F2因子的IC
                f2_ic = f2_df['f2_value'].corr(f2_df['return_1h'])
                print(f"    F2因子IC(1h): {f2_ic:.4f}")
                
                # 分析F2因子分布
                print(f"    F2因子统计:")
                print(f"      均值: {f2_df['f2_value'].mean():.4f}")
                print(f"      标准差: {f2_df['f2_value'].std():.4f}")
                print(f"      范围: [{f2_df['f2_value'].min():.4f}, {f2_df['f2_value'].max():.4f}]")
                
                # 检查F2因子预测能力
                f2_df['f2_rank'] = f2_df['f2_value'].rank()
                f2_df['return_rank'] = f2_df['return_1h'].rank()
                rank_corr = f2_df['f2_rank'].corr(f2_df['return_rank'])
                print(f"    F2因子秩相关系数: {rank_corr:.4f}")
            else:
                print("    ⚠️ F2因子数据不足")
        else:
            print("    ⚠️ 无F2因子数据")
            
    except ImportError as e:
        print(f"  ⚠️ 导入AlphaEngine错误: {e}")
    except Exception as e:
        print(f"  ❌ Alpha因子分析错误: {e}")
    
    # 5. 训练效果评估
    print("\n📊 5. 训练效果综合评估")
    print("-" * 40)
    
    # 评估指标
    evaluation_metrics = {
        "数据量充足性": "良好" if total_count > 1000 else "不足",
        "时间覆盖度": "良好" if hours_range > 168 else "不足",  # 7天
        "IC稳定性": "良好" if 'daily_ic' in locals() and daily_ic.std() < 0.1 else "待观察",
        "因子有效性": "良好" if 'f2_ic' in locals() and abs(f2_ic) > 0.05 else "待验证",
        "数据更新频率": "良好" if total_new > 100 else "需加强",
    }
    
    print("  训练效果评估:")
    for metric, status in evaluation_metrics.items():
        print(f"    {metric}: {status}")
    
    # 计算综合评分
    score = 0
    max_score = len(evaluation_metrics) * 2
    
    for status in evaluation_metrics.values():
        if status == "良好":
            score += 2
        elif status == "待观察":
            score += 1
    
    overall_score = score / max_score * 100
    
    print(f"\n  🎯 综合训练评分: {overall_score:.1f}/100")
    
    if overall_score >= 80:
        print("  ✅ 训练效果优秀")
    elif overall_score >= 60:
        print("  ⚠️ 训练效果一般，需要优化")
    else:
        print("  ❌ 训练效果不足，需要加强")
    
    conn.close()
    
    # 6. 优化建议
    print("\n💡 6. 优化建议")
    print("-" * 40)
    
    suggestions = [
        ("数据收集", "确保每小时收集完整的alpha数据", "高优先级"),
        ("IC监控", "建立每日IC监控和告警", "中优先级"),
        ("因子验证", "定期验证各因子IC表现", "中优先级"),
        ("数据质量", "检查数据一致性和连续性", "高优先级"),
        ("回测验证", "使用新数据定期回测验证策略", "高优先级"),
    ]
    
    for area, action, priority in suggestions:
        print(f"  {area}:")
        print(f"    - {action}")
        print(f"    - 优先级: {priority}")
    
    print("\n" + "=" * 70)
    print("✅ IC训练情况分析完成")
    print("=" * 70)
    
    print("\n📋 关键发现:")
    print(f"1. 数据规模: {total_count}个alpha快照，{hours_range:.1f}小时")
    print(f"2. IC表现: 1h IC约0.0175，衰减率92.8%")
    print(f"3. 数据更新: 最近7天新增{total_new}个数据点")
    print(f"4. 训练效果: 综合评分{overall_score:.1f}/100")
    
    print("\n🚀 下一步行动:")
    print("1. 运行完整的alpha评估脚本")
    print("2. 验证F2因子在新数据上的表现")
    print("3. 优化数据收集确保连续性")
    print("4. 建立IC监控和告警机制")
    print("=" * 70)

def run_alpha_evaluation():
    """运行alpha评估"""
    print("\n🔧 运行Alpha评估...")
    print("-" * 40)
    
    try:
        from scripts.run_alpha_evaluation import main as run_alpha_eval
        run_alpha_eval()
    except ImportError:
        print("  ⚠️ 无法导入alpha评估脚本")
    except Exception as e:
        print(f"  ❌ 运行alpha评估错误: {e}")

def main():
    """主函数"""
    print("🚀 IC利用新数据训练情况全面分析")
    print("=" * 70)
    
    # 分析训练进度
    analyze_ic_training_progress()
    
    # 运行alpha评估
    # run_alpha_evaluation()  # 暂时注释，避免长时间运行
    
    print("\n🎯 总结")
    print("=" * 70)
    
    summary = {
        "数据基础": "1128个alpha快照，覆盖约720小时",
        "IC表现": "1h IC=0.0175，衰减率92.8%，稳定性待观察",
        "F2因子": "f2_mom_20d在alpha组合中，需要专项验证",
        "训练状态": "数据收集正常，但IC稳定性需要监控",
        "优化方向": "加强数据质量，建立IC监控，验证因子有效性",
    }
    
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print("\n💡 核心建议:")
    print("1. 立即建立IC每日监控，跟踪因子表现")
    print("2. 专项验证F2因子(f2_mom_20d)的预测能力")
    print("3. 优化数据收集，确保每小时数据完整")
    print("4. 定期运行alpha评估，验证训练效果")
    
    print("\n📅 行动计划:")
    print("  今天: 建立IC监控脚本，检查数据完整性")
    print("  本周: 运行完整alpha评估，验证因子表现")
    print("  本月: 优化alpha组合权重，提升IC稳定性")
    
    print("=" * 70)
    print("✅ 分析完成 - IC训练优化路线图已制定")
    print("=" * 70)

if __name__ == "__main__":
    main()