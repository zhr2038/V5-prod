#!/usr/bin/env python3
"""
从orders表生成成本事件数据
用于启用成本校准模型
"""

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

def generate_cost_events():
    """从orders表生成成本事件"""
    
    print("🔄 从orders表生成成本事件数据")
    print("=" * 60)
    
    # 路径
    orders_db = Path("reports/orders.sqlite")
    cost_events_dir = Path("reports/cost_events")
    cost_events_dir.mkdir(exist_ok=True)
    
    if not orders_db.exists():
        print("❌ orders数据库不存在")
        return
    
    # 连接数据库
    conn = sqlite3.connect(str(orders_db))
    cursor = conn.cursor()
    
    # 获取所有FILLED订单
    cursor.execute("""
        SELECT 
            cl_ord_id,
            run_id,
            inst_id,
            side,
            notional_usdt,
            fee,
            avg_px,
            created_ts
        FROM orders 
        WHERE state = 'FILLED'
        ORDER BY created_ts
    """)
    
    orders = cursor.fetchall()
    
    print(f"找到 {len(orders)} 个FILLED订单")
    
    if len(orders) == 0:
        print("❌ 无FILLED订单数据")
        return
    
    # 按日期分组
    events_by_date = {}
    
    for order in orders:
        cl_ord_id, run_id, inst_id, side, notional_usdt, fee_str, avg_px_str, created_ts = order
        
        # 转换时间戳为日期
        dt = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc)
        date_str = dt.strftime("%Y%m%d")
        
        # 解析fee和avg_px
        try:
            fee_usdt = abs(float(fee_str)) if fee_str else 0.0
            avg_px = float(avg_px_str) if avg_px_str else 0.0
        except:
            fee_usdt = 0.0
            avg_px = 0.0
        
        # 计算费用bps
        fee_bps = (fee_usdt / notional_usdt * 10000) if notional_usdt > 0 else 6.0
        
        # 使用固定滑点5bps（因为dry-run没有实际滑点数据）
        slippage_bps = 5.0
        
        # 创建成本事件
        cost_event = {
            "schema_version": 1,
            "event_type": "fill",
            "ts": int(created_ts / 1000),  # 转换为秒
            "run_id": run_id or "unknown",
            "window_start_ts": int(created_ts / 1000) - 3600,  # 假设1小时前
            "window_end_ts": int(created_ts / 1000),
            "symbol": inst_id.replace("-", "/"),  # 转换格式
            "side": side.lower(),
            "intent": "OPEN_LONG" if side.upper() == "BUY" else "CLOSE_LONG",
            "regime": "Unknown",  # 需要从其他地方获取
            "router_action": "fill",
            "notional_usdt": float(notional_usdt),
            "mid_px": float(avg_px),
            "bid": None,
            "ask": None,
            "spread_bps": None,
            "fill_px": float(avg_px),
            "slippage_bps": float(slippage_bps),
            "fee_usdt": float(fee_usdt),
            "fee_bps": float(fee_bps),
            "cost_usdt_total": float(fee_usdt),  # 只有费用，无滑点成本
            "cost_bps_total": float(fee_bps + slippage_bps),
            "deadband_pct": None,
            "drift": None
        }
        
        # 添加到对应日期的列表
        if date_str not in events_by_date:
            events_by_date[date_str] = []
        events_by_date[date_str].append(cost_event)
    
    conn.close()
    
    # 写入文件
    files_written = 0
    for date_str, events in events_by_date.items():
        file_path = cost_events_dir / f"{date_str}.jsonl"
        
        # 读取现有事件（如果有）
        existing_events = []
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            existing_events.append(json.loads(line))
                        except:
                            continue
        
        # 合并事件，避免重复
        existing_ids = {e.get('ts', 0) for e in existing_events}
        new_events = [e for e in events if e['ts'] not in existing_ids]
        
        if new_events:
            with open(file_path, 'a', encoding='utf-8') as f:
                for event in new_events:
                    f.write(json.dumps(event) + '\n')
            
            print(f"  ✅ {date_str}.jsonl: 新增 {len(new_events)} 个事件")
            files_written += 1
        else:
            print(f"  ⚠️ {date_str}.jsonl: 无新事件")
    
    print(f"\n📊 统计:")
    total_events = sum(len(events) for events in events_by_date.values())
    print(f"  总事件数: {total_events}")
    print(f"  覆盖日期: {len(events_by_date)} 天")
    print(f"  写入文件: {files_written} 个")
    
    # 检查数据质量
    print(f"\n🔍 数据质量检查:")
    
    all_events = []
    for events in events_by_date.values():
        all_events.extend(events)
    
    if all_events:
        # 检查notional分布
        notionals = [e['notional_usdt'] for e in all_events]
        valid_notionals = [n for n in notionals if n > 0]
        
        print(f"  有效notional事件: {len(valid_notionals)}/{len(all_events)}")
        if valid_notionals:
            print(f"  notional范围: ${min(valid_notionals):.2f} - ${max(valid_notionals):.2f}")
            print(f"  平均notional: ${sum(valid_notionals)/len(valid_notionals):.2f}")
        
        # 检查费用分布
        fees = [e['fee_bps'] for e in all_events if e['notional_usdt'] > 0]
        if fees:
            print(f"  费用bps范围: {min(fees):.1f} - {max(fees):.1f}")
            print(f"  平均费用bps: {sum(fees)/len(fees):.1f}")
    
    print("\n" + "=" * 60)
    print("✅ 成本事件生成完成")
    
    # 建议下一步
    if files_written > 0:
        print("\n🚀 下一步:")
        print("1. 运行成本汇总: python3 scripts/rollup_costs.py")
        print("2. 启用校准模型: 修改config中的cost_model为calibrated")
        print("3. 验证校准效果: 运行回测验证")
    
    return files_written > 0

def main():
    """主函数"""
    print("🚀 从orders表生成成本事件")
    print("=" * 60)
    
    success = generate_cost_events()
    
    if success:
        print("\n🎯 可以继续执行成本校准流程了!")
    else:
        print("\n⚠️ 未生成新事件，可能需要检查数据源")
    
    print("=" * 60)

if __name__ == "__main__":
    main()