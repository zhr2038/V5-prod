#!/usr/bin/env python3
"""
自动化运行监控面板
"""

import os
import csv
import json
from datetime import datetime, timedelta
import pandas as pd
import sys

def monitor_auto_runs():
    """监控自动化运行状态"""
    print("📊 V5 自动化运行监控面板")
    print("=" * 60)
    
    workdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_file = os.path.join(workdir, "reports/auto_runs.csv")
    runs_dir = os.path.join(workdir, "reports/runs")
    
    # 1. 检查运行记录
    print("1. 运行记录统计:")
    print("-" * 40)
    
    if os.path.exists(csv_file):
        try:
            df = pd.read_csv(csv_file, header=None, 
                           names=["timestamp", "run_id", "equity_start", "equity_end", 
                                  "return_pct", "num_trades", "duration"])
            
            print(f"   总运行次数: {len(df)}")
            
            if len(df) > 0:
                # 最近24小时运行
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                last_24h = df[df['timestamp'] > (datetime.now() - timedelta(hours=24))]
                print(f"   最近24小时运行: {len(last_24h)} 次")
                
                # 平均运行时间
                avg_duration = df['duration'].mean()
                print(f"   平均运行时间: {avg_duration:.1f} 秒")
                
                # 最后运行时间
                last_run = df['timestamp'].max()
                hours_ago = (datetime.now() - last_run).total_seconds() / 3600
                print(f"   最后运行: {last_run.strftime('%Y-%m-%d %H:%M:%S')} ({hours_ago:.1f} 小时前)")
                
                # 显示最近5次运行
                print(f"\n   最近5次运行:")
                for _, row in df.tail(5).iterrows():
                    print(f"     {row['timestamp'].strftime('%H:%M')} - {row['run_id']} - "
                          f"{row['return_pct']:.1f}% - {row['num_trades']} trades")
        except Exception as e:
            print(f"   读取运行记录失败: {e}")
    else:
        print("   ⚠️  无运行记录文件")
    
    # 2. 检查运行目录
    print(f"\n2. 运行目录状态:")
    print("-" * 40)
    
    if os.path.exists(runs_dir):
        run_dirs = [d for d in os.listdir(runs_dir) if os.path.isdir(os.path.join(runs_dir, d))]
        print(f"   运行目录数: {len(run_dirs)}")
        
        if run_dirs:
            # 按修改时间排序
            run_dirs.sort(key=lambda x: os.path.getmtime(os.path.join(runs_dir, x)), reverse=True)
            
            latest_dir = run_dirs[0]
            latest_path = os.path.join(runs_dir, latest_dir)
            mtime = os.path.getmtime(latest_path)
            hours_ago = (datetime.now().timestamp() - mtime) / 3600
            
            print(f"   最新运行目录: {latest_dir}")
            print(f"   最后修改: {datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')} "
                  f"({hours_ago:.1f} 小时前)")
            
            # 检查最新运行的 summary.json
            summary_file = os.path.join(latest_path, "summary.json")
            if os.path.exists(summary_file):
                try:
                    with open(summary_file, 'r') as f:
                        summary = json.load(f)
                    
                    equity_start = summary.get('equity_start', 0)
                    equity_end = summary.get('equity_end', 0)
                    return_pct = summary.get('total_return_pct', 0)
                    num_trades = summary.get('num_trades', 0)
                    
                    print(f"   最新运行结果:")
                    print(f"     equity_start: {equity_start:.2f} USDT")
                    print(f"     equity_end: {equity_end:.2f} USDT")
                    print(f"     计算回报: {return_pct:.2f}%")
                    print(f"     交易数量: {num_trades}")
                except Exception as e:
                    print(f"   读取最新运行结果失败: {e}")
    else:
        print("   ⚠️  运行目录不存在")
    
    # 3. 检查数据状态
    print(f"\n3. 数据状态:")
    print("-" * 40)
    
    try:
        # 导入 data_monitor 函数
        sys.path.append(workdir)
        from scripts.data_monitor import monitor_data_status
        
        # 简化版数据检查
        db_path = os.path.join(workdir, "reports/alpha_history.db")
        if os.path.exists(db_path):
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Alpha 数据
            cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
            alpha_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT run_id) FROM alpha_snapshots")
            run_count = cursor.fetchone()[0]
            
            print(f"   Alpha 数据: {alpha_count} 条记录")
            print(f"   运行次数: {run_count} 次")
            
            # 市场数据
            cursor.execute("SELECT COUNT(*) FROM market_data_1h")
            market_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM market_data_1h")
            symbol_count = cursor.fetchone()[0]
            
            print(f"   市场数据: {market_count} 条记录")
            print(f"   币种数量: {symbol_count}")
            
            conn.close()
        else:
            print("   ⚠️  数据库文件不存在")
            
    except Exception as e:
        print(f"   数据检查失败: {e}")
    
    # 4. 检查自动化状态
    print(f"\n4. 自动化状态:")
    print("-" * 40)
    
    # 检查 crontab
    try:
        import subprocess
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        if "auto_run_v5.sh" in result.stdout:
            print("   ✅ crontab 已配置")
            
            # 统计配置的行数
            lines = [line for line in result.stdout.split('\n') if line.strip() and not line.startswith('#')]
            print(f"   crontab 任务数: {len(lines)}")
        else:
            print("   ⚠️  crontab 未配置")
            print("   运行: ./scripts/setup_auto_cron.sh")
    except Exception as e:
        print(f"   检查 crontab 失败: {e}")
    
    # 5. 建议
    print(f"\n5. 建议:")
    print("-" * 40)
    
    recommendations = []
    
    # 检查最后运行时间
    if os.path.exists(csv_file):
        try:
            df = pd.read_csv(csv_file, header=None)
            if len(df) > 0:
                last_timestamp = pd.to_datetime(df.iloc[-1, 0])
                hours_ago = (datetime.now() - last_timestamp).total_seconds() / 3600
                
                if hours_ago > 2:
                    recommendations.append(f"   最后运行 {hours_ago:.1f} 小时前，建议检查自动化")
        except:
            pass
    
    # 检查数据量
    try:
        import sqlite3
        db_path = os.path.join(workdir, "reports/alpha_history.db")
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
            alpha_count = cursor.fetchone()[0]
            conn.close()
            
            if alpha_count < 500:
                recommendations.append(f"   Alpha 数据不足 ({alpha_count}/500)，继续积累")
            else:
                recommendations.append(f"   ✅ Alpha 数据充足 ({alpha_count} 条)")
    except:
        pass
    
    if recommendations:
        for rec in recommendations:
            print(rec)
    else:
        print("   ✅ 系统运行正常")
    
    print(f"\n" + "=" * 60)
    print("📋 维护命令:")
    print("1. 手动运行: ./scripts/auto_run_v5.sh")
    print("2. 设置自动化: ./scripts/setup_auto_cron.sh")
    print("3. 数据监控: python3 scripts/data_monitor.py")
    print("4. IC 分析: python3 scripts/quick_ic_analysis.py")
    print("=" * 60)


def main():
    monitor_auto_runs()


if __name__ == "__main__":
    main()