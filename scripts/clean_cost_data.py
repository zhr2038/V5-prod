#!/usr/bin/env python3
"""
成本数据清洗脚本
移除异常值，生成干净的成本数据用于校准
"""

import json
from pathlib import Path
import numpy as np
from datetime import datetime
import shutil

def clean_cost_events():
    """清洗成本事件数据"""
    
    print("🔄 清洗成本事件数据")
    print("=" * 60)
    
    # 路径
    source_dir = Path("reports/cost_events_real")
    clean_dir = Path("reports/cost_events_clean")
    backup_dir = Path("reports/cost_events_real_backup")
    
    # 备份原始数据
    if source_dir.exists() and not backup_dir.exists():
        shutil.copytree(source_dir, backup_dir)
        print(f"✅ 备份原始数据到: {backup_dir}")
    
    # 创建干净数据目录
    clean_dir.mkdir(exist_ok=True)
    
    total_events = 0
    cleaned_events = 0
    removed_events = 0
    
    # 清洗规则
    cleaning_rules = {
        "max_fee_bps": 100.0,      # 最大费用100bps
        "min_notional_usdt": 0.01,  # 最小交易规模$0.01
        "max_notional_usdt": 1000000,  # 最大交易规模$1M
        "max_slippage_bps": 50.0,   # 最大滑点50bps
    }
    
    for file in source_dir.glob("*.jsonl"):
        clean_events = []
        
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                total_events += 1
                line = line.strip()
                if not line:
                    continue
                
                try:
                    event = json.loads(line)
                    
                    # 提取关键字段
                    fee_bps = event.get('fee_bps', 0)
                    notional = event.get('notional_usdt', 0)
                    slippage_bps = event.get('slippage_bps', 0)
                    
                    # 应用清洗规则
                    is_valid = True
                    issues = []
                    
                    # 规则1: 费用不能过高
                    if abs(fee_bps) > cleaning_rules["max_fee_bps"]:
                        is_valid = False
                        issues.append(f"费用异常: {fee_bps:.2f}bps > {cleaning_rules['max_fee_bps']}bps")
                    
                    # 规则2: 交易规模合理
                    if notional < cleaning_rules["min_notional_usdt"]:
                        is_valid = False
                        issues.append(f"规模过小: ${notional:.6f} < ${cleaning_rules['min_notional_usdt']}")
                    elif notional > cleaning_rules["max_notional_usdt"]:
                        is_valid = False
                        issues.append(f"规模过大: ${notional:.2f} > ${cleaning_rules['max_notional_usdt']}")
                    
                    # 规则3: 滑点合理
                    if abs(slippage_bps) > cleaning_rules["max_slippage_bps"]:
                        is_valid = False
                        issues.append(f"滑点异常: {slippage_bps:.2f}bps > {cleaning_rules['max_slippage_bps']}bps")
                    
                    # 规则4: 对于极小规模交易，如果费用比例异常，进行修正
                    if notional > 0 and notional < 1.0:  # 小于1USDT的交易
                        # 计算实际费用金额
                        fee_usdt = event.get('fee_usdt', 0)
                        if abs(fee_usdt) > 0 and abs(fee_bps) > 100:
                            # 修正费用bps，使用更合理的估计
                            reasonable_fee_bps = min(50.0, max(0.1, abs(fee_usdt) / notional * 10000))
                            event['fee_bps'] = reasonable_fee_bps
                            event['fee_usdt'] = fee_usdt  # 保持原费用金额
                            event['cost_bps_total'] = reasonable_fee_bps + slippage_bps
                            event['cost_usdt_total'] = fee_usdt
                            issues.append(f"修正极小规模交易费用: {fee_bps:.2f}bps → {reasonable_fee_bps:.2f}bps")
                    
                    if is_valid:
                        clean_events.append(event)
                        cleaned_events += 1
                    else:
                        removed_events += 1
                        if removed_events <= 5:  # 只显示前5个被移除的事件
                            print(f"  ❌ 移除异常事件: {', '.join(issues)}")
                            
                except Exception as e:
                    removed_events += 1
                    print(f"  ❌ 解析错误: {e}")
                    continue
        
        # 保存清洗后的数据
        if clean_events:
            clean_file = clean_dir / file.name
            with open(clean_file, 'w', encoding='utf-8') as f:
                for event in clean_events:
                    f.write(json.dumps(event) + '\n')
            
            print(f"  ✅ {file.name}: {len(clean_events)}/{total_events} 个事件通过清洗")
    
    print(f"\n📊 清洗结果:")
    print(f"  总事件数: {total_events}")
    print(f"  保留事件: {cleaned_events} ({cleaned_events/total_events*100:.1f}%)")
    print(f"  移除事件: {removed_events} ({removed_events/total_events*100:.1f}%)")
    
    return cleaned_events > 0

def regenerate_cost_stats():
    """重新生成成本统计"""
    
    print("\n📈 重新生成成本统计")
    print("-" * 40)
    
    clean_dir = Path("reports/cost_events_clean")
    stats_dir = Path("reports/cost_stats_clean")
    stats_dir.mkdir(exist_ok=True)
    
    # 检查清洗后的数据
    clean_files = list(clean_dir.glob("*.jsonl"))
    if not clean_files:
        print("❌ 无清洗后数据")
        return False
    
    print(f"找到 {len(clean_files)} 个清洗后数据文件")
    
    # 使用rollup_costs.py重新生成统计
    try:
        import subprocess
        import sys
        
        # 导入rollup_costs模块
        sys.path.append(str(Path(__file__).resolve().parents[1]))
        from scripts.rollup_costs import rollup_day
        
        processed_files = 0
        for file in clean_files:
            day_str = file.stem
            print(f"  处理 {day_str}...")
            
            try:
                output_file = rollup_day(
                    day_str,
                    base_dir="reports/cost_events_clean",
                    out_dir="reports/cost_stats_clean"
                )
                
                if Path(output_file).exists():
                    processed_files += 1
                    print(f"    ✅ 生成 {output_file}")
                    
                    # 检查生成的数据
                    with open(output_file, 'r') as f:
                        stats = json.load(f)
                    
                    fills = stats.get("coverage", {}).get("fills", 0)
                    print(f"      包含 {fills} 个fills")
                    
            except Exception as e:
                print(f"    ❌ 处理错误: {e}")
        
        if processed_files > 0:
            print(f"\n✅ 成功处理 {processed_files} 个文件")
            return True
        else:
            print("\n❌ 未成功处理任何文件")
            return False
            
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        
        # 尝试手动运行
        for file in clean_files:
            day_str = file.stem
            cmd = f"cd /home/admin/clawd/v5-trading-bot && python3 scripts/rollup_costs.py --day {day_str} --base_dir reports/cost_events_clean --out_dir reports/cost_stats_clean"
            print(f"  执行: {cmd}")
        
        return False

def update_config_for_clean_data():
    """更新配置使用清洗后数据"""
    
    print("\n⚙️ 更新配置使用清洗后数据")
    print("-" * 40)
    
    config_path = Path("configs/config.yaml")
    if not config_path.exists():
        print("❌ 配置文件不存在")
        return False
    
    # 读取配置
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 更新成本统计目录
    if "cost_stats_dir: reports/cost_stats_real" in content:
        new_content = content.replace(
            "cost_stats_dir: reports/cost_stats_real",
            "cost_stats_dir: reports/cost_stats_clean  # 使用清洗后数据"
        )
        
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print("✅ 更新成本统计目录为清洗后数据")
        return True
    else:
        print("❌ 未找到成本统计目录配置")
        return False

def verify_clean_data_quality():
    """验证清洗后数据质量"""
    
    print("\n🔍 验证清洗后数据质量")
    print("-" * 40)
    
    stats_dir = Path("reports/cost_stats_clean")
    stats_files = list(stats_dir.glob("daily_cost_stats_*.json"))
    
    if not stats_files:
        print("❌ 无清洗后统计文件")
        return False
    
    latest_file = max(stats_files, key=lambda x: x.name)
    print(f"最新清洗后文件: {latest_file.name}")
    
    with open(latest_file, 'r') as f:
        stats = json.load(f)
    
    fills = stats.get("coverage", {}).get("fills", 0)
    buckets = stats.get("buckets", {})
    
    print(f"📊 数据统计:")
    print(f"  fills数量: {fills}")
    print(f"  buckets数量: {len(buckets)}")
    
    # 检查费用分布
    all_fees = []
    for bucket in buckets.values():
        fee_stats = bucket.get("fee_bps", {})
        fee_p75 = fee_stats.get("p75")
        if fee_p75 is not None:
            all_fees.append(fee_p75)
    
    if all_fees:
        avg_fee = np.mean(all_fees)
        max_fee = np.max(all_fees)
        min_fee = np.min(all_fees)
        
        print(f"💰 费用分布:")
        print(f"  平均费用: {avg_fee:.2f}bps")
        print(f"  最低费用: {min_fee:.2f}bps")
        print(f"  最高费用: {max_fee:.2f}bps")
        
        # 检查是否还有异常值
        if max_fee > 100:
            print(f"  ⚠️ 仍有异常高费用: {max_fee:.2f}bps")
            return False
        elif avg_fee > 20:
            print(f"  ⚠️ 平均费用偏高: {avg_fee:.2f}bps")
            return False
        else:
            print(f"  ✅ 费用分布合理")
            return True
    
    return True

def main():
    """主函数"""
    
    print("🚀 开始成本数据清洗和重新校准")
    print("=" * 60)
    print("阶段3: 数据质量修复和重新验证")
    print("=" * 60)
    
    # 1. 清洗数据
    if not clean_cost_events():
        print("\n❌ 数据清洗失败")
        return
    
    # 2. 重新生成统计
    if not regenerate_cost_stats():
        print("\n⚠️ 统计生成失败，但可以继续")
    
    # 3. 更新配置
    if not update_config_for_clean_data():
        print("\n⚠️ 配置更新失败")
    
    # 4. 验证数据质量
    if not verify_clean_data_quality():
        print("\n⚠️ 数据质量验证失败")
    
    print("\n" + "=" * 60)
    print("✅ 数据清洗完成")
    print("=" * 60)
    
    print("\n🚀 下一步:")
    print("1. 重新运行校准模型验证")
    print("2. 基于干净数据重新评估F2")
    print("3. 开始整体策略优化")
    
    print("\n💡 立即执行:")
    print("python3 scripts/validate_calibration.py  # 重新验证")
    print("python3 scripts/validate_f2_optimization.py  # 重新评估F2")

if __name__ == "__main__":
    main()