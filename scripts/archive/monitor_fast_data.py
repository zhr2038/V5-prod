#!/usr/bin/env python3
"""
高频数据积累监控脚本
实时监控fills数据积累进度
"""

import sqlite3
import time
from datetime import datetime, timedelta
import sys
from pathlib import Path

def monitor_fills_progress(target_fills=30, check_interval=300):
    """监控fills数据积累进度"""
    
    db_path = Path("reports/orders.sqlite")
    if not db_path.exists():
        print("❌ 数据库文件不存在")
        return
    
    print("🚀 高频数据积累监控启动")
    print("=" * 60)
    print(f"目标fills数: {target_fills}")
    print(f"检查间隔: {check_interval}秒")
    print("=" * 60)
    
    start_time = datetime.now()
    last_check_count = 0
    
    while True:
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            # 获取当前订单总数（作为fills代理）
            cursor.execute("SELECT COUNT(*) FROM orders WHERE state = 'filled'")
            current_fills = cursor.fetchone()[0]
            
            # 获取今日新增订单
            today_start = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
            cursor.execute("SELECT COUNT(*) FROM orders WHERE state = 'filled' AND created_ts >= ?", (today_start,))
            today_fills = cursor.fetchone()[0]
            
            # 获取订单分布（简化版本）
            cursor.execute("""
                SELECT 
                    inst_id as symbol,
                    'Unknown' as regime,  -- 实际应从其他地方获取regime
                    CASE 
                        WHEN notional_usdt < 25 THEN 'lt25'
                        WHEN notional_usdt BETWEEN 25 AND 50 THEN '25_50'
                        WHEN notional_usdt BETWEEN 50 AND 100 THEN '50_100'
                        ELSE 'gt100'
                    END as size_bucket,
                    COUNT(*) as count
                FROM orders 
                WHERE state = 'filled'
                GROUP BY inst_id, size_bucket
                ORDER BY count DESC
            """)
            
            distribution = cursor.fetchall()
            
            conn.close()
            
            # 计算进度
            elapsed = datetime.now() - start_time
            progress = (current_fills / target_fills) * 100 if target_fills > 0 else 0
            fills_since_last = current_fills - last_check_count
            
            # 显示进度
            print(f"\n📊 [{datetime.now().strftime('%H:%M:%S')}] 数据积累进度")
            print(f"  当前fills: {current_fills}/{target_fills} ({progress:.1f}%)")
            print(f"  今日新增: {today_fills} 个")
            print(f"  上次检查后新增: {fills_since_last} 个")
            print(f"  运行时间: {elapsed}")
            
            # 显示分布
            if distribution:
                print(f"\n  📈 Fills分布:")
                for symbol, regime, size_bucket, count in distribution[:5]:  # 显示前5个
                    print(f"    {symbol} | {regime} | {size_bucket}: {count}次")
                
                if len(distribution) > 5:
                    print(f"    ... 还有{len(distribution)-5}个分布")
            
            # 检查是否达到目标
            if current_fills >= target_fills:
                print(f"\n🎯 目标达成! 已积累 {current_fills} 个fills")
                print("=" * 60)
                print("✅ 可以启用校准成本模型了!")
                print("=" * 60)
                
                # 建议下一步操作
                print("\n🚀 下一步操作:")
                print("1. 停止高频交易: kill相关进程")
                print("2. 运行成本汇总: python3 scripts/rollup_costs.py")
                print("3. 启用校准模型: 修改config中的cost_model为calibrated")
                print("4. 验证校准效果: 运行回测验证")
                
                break
            
            # 更新上次检查计数
            last_check_count = current_fills
            
            # 计算预计完成时间
            if fills_since_last > 0 and progress < 100:
                fills_per_hour = fills_since_last / (check_interval / 3600)
                remaining_fills = target_fills - current_fills
                if fills_per_hour > 0:
                    hours_remaining = remaining_fills / fills_per_hour
                    print(f"  预计完成时间: {hours_remaining:.1f} 小时后")
            
            # 等待下一次检查
            print(f"\n⏳ 下次检查: {check_interval}秒后...")
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            print("\n⏹️ 监控已停止")
            break
        except Exception as e:
            print(f"❌ 监控错误: {e}")
            time.sleep(check_interval)

def check_data_quality():
    """检查数据质量"""
    
    print("\n🔍 数据质量检查")
    print("-" * 40)
    
    db_path = Path("reports/orders.sqlite")
    if not db_path.exists():
        print("❌ 数据库文件不存在")
        return
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # 检查基本统计
    cursor.execute("SELECT COUNT(*) FROM orders WHERE state = 'filled'")
    total_fills = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT inst_id) FROM orders WHERE state = 'filled'")
    unique_symbols = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT strftime('%Y-%m-%d', created_ts/1000, 'unixepoch')) FROM orders WHERE state = 'filled'")
    unique_days = cursor.fetchone()[0]
    
    # regime信息需要从其他地方获取，这里设为1
    unique_regimes = 1
    
    print(f"📊 基本统计:")
    print(f"  总fills数: {total_fills}")
    print(f"  币种覆盖: {unique_symbols}个")
    print(f"  市场状态覆盖: {unique_regimes}种")
    print(f"  天数覆盖: {unique_days}天")
    
    # 检查bucket分布
    cursor.execute("""
        SELECT 
            inst_id || '|Unknown|fill|' || 
            CASE 
                WHEN notional_usdt < 25 THEN 'lt25'
                WHEN notional_usdt BETWEEN 25 AND 50 THEN '25_50'
                WHEN notional_usdt BETWEEN 50 AND 100 THEN '50_100'
                ELSE 'gt100'
            END as bucket_key,
            COUNT(*) as count
        FROM orders 
        WHERE state = 'filled'
        GROUP BY bucket_key
        ORDER BY count DESC
    """)
    
    buckets = cursor.fetchall()
    
    print(f"\n📦 Bucket分布:")
    for bucket_key, count in buckets[:10]:  # 显示前10个
        print(f"  {bucket_key}: {count}次")
    
    # 检查是否满足校准要求
    print(f"\n🎯 校准模型要求检查:")
    
    # 全局fills要求
    if total_fills >= 20:  # 使用降低后的阈值
        print(f"  ✅ 全局fills: {total_fills} >= 20 (满足)")
    else:
        print(f"  ❌ 全局fills: {total_fills} < 20 (不满足)")
    
    # bucket fills要求
    sufficient_buckets = sum(1 for _, count in buckets if count >= 8)  # 使用降低后的阈值
    if sufficient_buckets > 0:
        print(f"  ✅ 有{sufficient_buckets}个bucket满足>=8要求")
    else:
        print(f"  ❌ 无bucket满足>=8要求")
    
    conn.close()
    
    return total_fills >= 20 and sufficient_buckets > 0

def main():
    """主函数"""
    print("🚀 高频数据积累监控系统")
    print("=" * 60)
    
    # 检查当前状态
    quality_ok = check_data_quality()
    
    if quality_ok:
        print("\n✅ 当前数据质量满足校准要求")
        print("建议立即启用校准成本模型")
    else:
        print("\n⚠️ 当前数据质量不满足校准要求")
        print("开始监控数据积累进度...")
        
        # 开始监控
        try:
            monitor_fills_progress(target_fills=30, check_interval=300)  # 5分钟检查一次
        except KeyboardInterrupt:
            print("\n⏹️ 监控已停止")
    
    print("=" * 60)
    print("监控结束")

if __name__ == "__main__":
    main()