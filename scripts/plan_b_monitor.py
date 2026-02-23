#!/usr/bin/env python3
"""
方案B监控脚本
监控真实数据积累进度，自适应调整参数
"""

import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys

class PlanBMonitor:
    """方案B监控器"""
    
    def __init__(self, config_path="configs/live_normal_accelerated.yaml"):
        self.config_path = Path(config_path)
        self.db_path = Path("reports/orders_accelerated.sqlite")
        self.start_time = datetime.now()
        self.daily_target = 17  # 每日目标fills
        self.total_target = 50  # 总目标fills
        
        # 状态跟踪
        self.status = {
            "start_time": self.start_time.isoformat(),
            "daily_fills": {},
            "total_fills": 0,
            "adjustments_made": [],
            "last_check": None
        }
        
    def check_current_fills(self):
        """检查当前fills数量"""
        
        if not self.db_path.exists():
            return 0, 0
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        if not cursor.fetchone():
            conn.close()
            return 0, 0
        
        # 获取今日fills
        today_start = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
        cursor.execute("SELECT COUNT(*) FROM orders WHERE state = 'FILLED' AND created_ts >= ?", (today_start,))
        today_fills = cursor.fetchone()[0]
        
        # 获取总fills
        cursor.execute("SELECT COUNT(*) FROM orders WHERE state = 'FILLED'")
        total_fills = cursor.fetchone()[0]
        
        conn.close()
        
        return today_fills, total_fills
    
    def analyze_fills_distribution(self):
        """分析fills分布"""
        
        if not self.db_path.exists():
            return {}
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        if not cursor.fetchone():
            conn.close()
            return {}
        
        # 获取币种分布
        cursor.execute("""
            SELECT 
                inst_id,
                COUNT(*) as count
            FROM orders 
            WHERE state = 'FILLED'
            GROUP BY inst_id
            ORDER BY count DESC
        """)
        
        symbol_dist = dict(cursor.fetchall())
        
        # 获取时间分布
        cursor.execute("""
            SELECT 
                strftime('%H', created_ts/1000, 'unixepoch') as hour,
                COUNT(*) as count
            FROM orders 
            WHERE state = 'FILLED'
            GROUP BY hour
            ORDER BY hour
        """)
        
        hour_dist = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            "symbols": symbol_dist,
            "hours": hour_dist,
            "total_symbols": len(symbol_dist)
        }
    
    def calculate_progress(self):
        """计算进度"""
        
        today_fills, total_fills = self.check_current_fills()
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 更新状态
        if today not in self.status["daily_fills"]:
            self.status["daily_fills"][today] = 0
        self.status["daily_fills"][today] = today_fills
        self.status["total_fills"] = total_fills
        self.status["last_check"] = datetime.now().isoformat()
        
        # 计算进度
        days_elapsed = (datetime.now() - self.start_time).days + 1
        daily_progress = (today_fills / self.daily_target) * 100
        total_progress = (total_fills / self.total_target) * 100
        
        # 计算预计完成时间
        if today_fills > 0:
            fills_per_hour = today_fills / ((datetime.now().hour + 1) if datetime.now().hour > 0 else 1)
            remaining_fills = self.total_target - total_fills
            if fills_per_hour > 0:
                hours_remaining = remaining_fills / fills_per_hour
                estimated_completion = datetime.now() + timedelta(hours=hours_remaining)
            else:
                estimated_completion = None
        else:
            estimated_completion = None
        
        return {
            "today": today,
            "today_fills": today_fills,
            "total_fills": total_fills,
            "daily_target": self.daily_target,
            "total_target": self.total_target,
            "daily_progress": daily_progress,
            "total_progress": total_progress,
            "days_elapsed": days_elapsed,
            "estimated_completion": estimated_completion.isoformat() if estimated_completion else None,
            "fills_per_hour": fills_per_hour if 'fills_per_hour' in locals() else 0
        }
    
    def suggest_adjustments(self, progress):
        """根据进度建议参数调整"""
        
        suggestions = []
        today_fills = progress["today_fills"]
        fills_per_hour = progress.get("fills_per_hour", 0)
        
        # 基于当前表现建议调整
        if today_fills < 5 and fills_per_hour < 1:
            # 交易频率太低，需要加速
            suggestions.append({
                "action": "reduce_deadband",
                "parameter": "deadband_sideways",
                "current": 0.04,
                "suggested": 0.035,
                "reason": f"交易频率低({fills_per_hour:.1f}/小时)，降低deadband增加敏感度"
            })
            suggestions.append({
                "action": "reduce_interval",
                "parameter": "rebalance.interval_minutes",
                "current": 45,
                "suggested": 40,
                "reason": "增加调仓频率"
            })
            
        elif today_fills > 25 or fills_per_hour > 3:
            # 交易频率太高，需要减速
            suggestions.append({
                "action": "increase_deadband",
                "parameter": "deadband_sideways",
                "current": 0.04,
                "suggested": 0.045,
                "reason": f"交易频率高({fills_per_hour:.1f}/小时)，增加deadband减少过度交易"
            })
            
        # 检查币种分布
        distribution = self.analyze_fills_distribution()
        if distribution.get("total_symbols", 0) < 5 and today_fills > 10:
            # 币种分布太集中
            suggestions.append({
                "action": "increase_universe",
                "parameter": "universe.top_n_market_cap",
                "current": 25,
                "suggested": 30,
                "reason": f"币种分布不足({distribution['total_symbols']}个)，增加币种选择"
            })
        
        return suggestions
    
    def generate_report(self, progress, suggestions):
        """生成进度报告"""
        
        report = []
        report.append("=" * 60)
        report.append("📊 方案B数据积累进度报告")
        report.append("=" * 60)
        report.append(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"运行时间: {progress['days_elapsed']}天")
        report.append("")
        
        # 今日进度
        report.append("🎯 今日进度:")
        report.append(f"  Fills数量: {progress['today_fills']}/{progress['daily_target']} ({progress['daily_progress']:.1f}%)")
        report.append(f"  速度: {progress.get('fills_per_hour', 0):.1f} fills/小时")
        
        # 总进度
        report.append(f"\n📈 总进度:")
        report.append(f"  累计Fills: {progress['total_fills']}/{progress['total_target']} ({progress['total_progress']:.1f}%)")
        
        if progress.get("estimated_completion"):
            est_time = datetime.fromisoformat(progress["estimated_completion"])
            report.append(f"  预计完成: {est_time.strftime('%Y-%m-%d %H:%M')}")
        
        # 分布分析
        distribution = self.analyze_fills_distribution()
        if distribution:
            report.append(f"\n📋 分布分析:")
            report.append(f"  币种数量: {distribution.get('total_symbols', 0)}个")
            
            if distribution.get("symbols"):
                top_symbols = sorted(distribution["symbols"].items(), key=lambda x: x[1], reverse=True)[:3]
                report.append(f"  主要币种: {', '.join([f'{s}({c})' for s, c in top_symbols])}")
        
        # 调整建议
        if suggestions:
            report.append(f"\n🔧 调整建议:")
            for suggestion in suggestions:
                report.append(f"  {suggestion['reason']}")
                report.append(f"    调整: {suggestion['parameter']} {suggestion['current']} → {suggestion['suggested']}")
        else:
            report.append(f"\n✅ 当前参数合适，无需调整")
        
        # 下一步
        report.append(f"\n🚀 下一步:")
        if progress["total_progress"] >= 100:
            report.append("  ✅ 目标已达成！可以停止加速模式")
        elif progress["daily_progress"] < 50:
            report.append("  ⚠️ 进度较慢，考虑实施上述调整建议")
        else:
            report.append("  🔄 保持当前参数，继续监控")
        
        report.append("=" * 60)
        
        return "\n".join(report)
    
    def save_status(self):
        """保存状态到文件"""
        
        status_file = Path("reports/plan_b_status.json")
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump(self.status, f, indent=2, default=str)
    
    def run_monitoring_cycle(self, check_interval_minutes=30):
        """运行监控周期"""
        
        print("🚀 启动方案B数据积累监控")
        print("=" * 60)
        print(f"目标: {self.total_target}个真实fills")
        print(f"时间: 2-3天")
        print(f"监控间隔: {check_interval_minutes}分钟")
        print("=" * 60)
        
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                print(f"\n📊 监控周期 #{cycle_count}")
                print(f"时间: {datetime.now().strftime('%H:%M:%S')}")
                
                # 检查进度
                progress = self.calculate_progress()
                
                # 分析并建议
                suggestions = self.suggest_adjustments(progress)
                
                # 生成报告
                report = self.generate_report(progress, suggestions)
                print(report)
                
                # 保存状态
                self.save_status()
                
                # 检查是否达到目标
                if progress["total_progress"] >= 100:
                    print("\n🎉 方案B目标达成！")
                    print(f"累计 {progress['total_fills']} 个真实fills")
                    print("建议停止加速模式，恢复正常参数")
                    break
                
                # 等待下一个周期
                print(f"\n⏳ 下次检查: {check_interval_minutes}分钟后...")
                time.sleep(check_interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\n⏹️ 监控已停止")
        except Exception as e:
            print(f"\n❌ 监控错误: {e}")

def main():
    """主函数"""
    
    monitor = PlanBMonitor()
    
    print("方案B：正常交易加速积累真实数据")
    print("=" * 60)
    print("策略: 优化参数，适度加速，2-3天积累50+真实fills")
    print("安全: 保持dry-run模式，无资金风险")
    print("监控: 自适应调整参数，确保效率")
    print("=" * 60)
    
    # 立即检查当前状态
    progress = monitor.calculate_progress()
    suggestions = monitor.suggest_adjustments(progress)
    report = monitor.generate_report(progress, suggestions)
    print(report)
    
    # 询问是否启动监控
    print("\n是否启动持续监控？(y/N)")
    response = input().lower()
    
    if response == 'y':
        monitor.run_monitoring_cycle(check_interval_minutes=30)
    else:
        print("\n⏹️ 单次检查完成")
        print("可以手动运行监控: python3 scripts/plan_b_monitor.py")
    
    print("=" * 60)

if __name__ == "__main__":
    main()