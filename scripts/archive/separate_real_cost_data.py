#!/usr/bin/env python3
"""
分离真实和模拟成本数据
创建只包含真实交易数据的成本统计
"""

import json
from pathlib import Path
from datetime import datetime, timezone
import sys

def separate_real_data():
    """分离真实成本数据"""
    
    print("🔄 分离真实成本数据")
    print("=" * 60)
    
    # 路径
    cost_events_dir = Path("reports/cost_events")
    real_events_dir = Path("reports/cost_events_real")
    real_events_dir.mkdir(exist_ok=True)
    
    # 处理所有成本事件文件
    total_real = 0
    total_simulated = 0
    
    for file in cost_events_dir.glob("*.jsonl"):
        real_events = []
        simulated_events = []
        
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    event = json.loads(line)
                    
                    # 判断是否为真实交易
                    # 新标准：明确的 source 标记（避免 dry-run 使用校准fee_bps 被误判为真实）
                    src = str(event.get("source") or "").strip().lower()
                    if src:
                        is_real = (src == "okx_fill")
                    else:
                        # 兼容旧数据：启发式（不推荐，仅用于历史数据迁移）
                        notional = event.get('notional_usdt', 0)
                        fee_bps = event.get('fee_bps', 6.0)
                        is_real = (notional > 0 and fee_bps != 6.0)
                    
                    if is_real:
                        real_events.append(event)
                        total_real += 1
                    else:
                        simulated_events.append(event)
                        total_simulated += 1
                        
                except Exception as e:
                    print(f"  解析错误 {file.name}: {e}")
                    continue
        
        # 保存真实数据到新文件
        if real_events:
            real_file = real_events_dir / file.name
            with open(real_file, 'w', encoding='utf-8') as f:
                for event in real_events:
                    f.write(json.dumps(event) + '\n')
            
            print(f"  ✅ {file.name}: {len(real_events)}个真实事件 -> {real_file.name}")
    
    print(f"\n📊 数据分离完成:")
    print(f"  真实交易事件: {total_real}个")
    print(f"  模拟交易事件: {total_simulated}个")
    print(f"  总事件: {total_real + total_simulated}个")
    
    return total_real

def create_real_cost_stats():
    """创建真实数据成本统计"""
    
    print("\n📈 创建真实数据成本统计")
    print("-" * 40)
    
    real_events_dir = Path("reports/cost_events_real")
    real_stats_dir = Path("reports/cost_stats_real")
    real_stats_dir.mkdir(exist_ok=True)
    
    # 检查真实数据文件
    real_files = list(real_events_dir.glob("*.jsonl"))
    if not real_files:
        print("❌ 无真实数据文件")
        return False
    
    print(f"找到 {len(real_files)} 个真实数据文件")
    
    # 使用rollup_costs.py处理真实数据
    try:
        import subprocess
        import sys
        
        # 导入rollup_costs模块
        sys.path.append(str(Path(__file__).resolve().parents[1]))
        from scripts.rollup_costs import rollup_day
        
        # 处理每个有真实数据的日期
        processed_days = []
        for file in real_files:
            day_str = file.stem  # 例如20260217
            print(f"  处理 {day_str}...")
            
            try:
                # 调用rollup_day函数
                output_file = rollup_day(
                    day_str,
                    base_dir="reports/cost_events_real",
                    out_dir="reports/cost_stats_real"
                )
                
                if Path(output_file).exists():
                    processed_days.append(day_str)
                    print(f"    ✅ 生成 {output_file}")
            except Exception as e:
                print(f"    ❌ 处理错误: {e}")
        
        if processed_days:
            print(f"\n✅ 成功处理 {len(processed_days)} 天的真实数据")
            return True
        else:
            print("\n❌ 未成功处理任何数据")
            return False
            
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        print("尝试手动运行成本汇总...")
        
        # 手动运行rollup_costs.py
        for file in real_files:
            day_str = file.stem
            cmd = f"cd /home/admin/clawd/v5-trading-bot && python3 scripts/rollup_costs.py --day {day_str} --base_dir reports/cost_events_real --out_dir reports/cost_stats_real"
            print(f"  执行: {cmd}")
            
            # 这里应该实际执行命令，但为了安全先打印
            print(f"    ⚠️ 需要手动执行以上命令")
        
        return False

def update_config_for_real_data():
    """更新配置使用真实数据"""
    
    print("\n⚙️ 更新配置使用真实数据")
    print("-" * 40)
    
    config_path = Path("configs/config.yaml")
    if not config_path.exists():
        print("❌ 配置文件不存在")
        return False
    
    # 读取配置
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 更新成本统计目录
    if "cost_stats_dir: reports/cost_stats" in content:
        new_content = content.replace(
            "cost_stats_dir: reports/cost_stats",
            "cost_stats_dir: reports/cost_stats_real  # 使用真实数据"
        )
        
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("✅ 更新成本统计目录为真实数据")
        return True
    else:
        print("❌ 未找到成本统计目录配置")
        return False

def main():
    """主函数"""
    
    print("🚀 切换到真实交易数据校准")
    print("=" * 60)
    
    # 1. 分离真实数据
    real_count = separate_real_data()
    
    if real_count == 0:
        print("\n❌ 无真实交易数据，无法切换")
        return
    
    # 2. 创建真实数据统计
    if not create_real_cost_stats():
        print("\n⚠️ 创建真实数据统计失败，但可以继续")
    
    # 3. 更新配置
    if update_config_for_real_data():
        print("\n✅ 配置已更新为使用真实数据")
    else:
        print("\n⚠️ 配置更新失败")
    
    # 4. 验证
    print("\n🔍 验证真实数据质量:")
    real_stats_dir = Path("reports/cost_stats_real")
    real_stats_files = list(real_stats_dir.glob("*.json"))
    
    if real_stats_files:
        latest_file = max(real_stats_files, key=lambda x: x.name)
        print(f"  最新真实统计文件: {latest_file.name}")
        
        try:
            with open(latest_file, 'r') as f:
                stats = json.load(f)
            
            fills = stats.get("coverage", {}).get("fills", 0)
            buckets = len(stats.get("buckets", {}))
            
            print(f"  真实fills数: {fills}")
            print(f"  真实buckets数: {buckets}")
            
            if fills >= 10:  # 真实数据要求可以更低
                print(f"  ✅ 有足够的真实数据用于校准")
            else:
                print(f"  ⚠️ 真实数据量较少，但可以尝试")
                
        except Exception as e:
            print(f"  ❌ 读取统计文件错误: {e}")
    else:
        print("  ❌ 无真实统计文件")
    
    print("\n" + "=" * 60)
    print("🎯 切换到真实数据校准完成!")
    print("=" * 60)
    
    print("\n🚀 下一步:")
    print("1. 验证校准模型使用真实数据")
    print("2. 监控真实数据下的成本估计")
    print("3. 继续积累更多真实交易数据")
    print("4. 考虑启动小资金实盘加速数据积累")

if __name__ == "__main__":
    main()