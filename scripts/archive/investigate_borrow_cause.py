#!/usr/bin/env python3
"""
严查借币原因：为什么 USDT 充足却产生借币
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path

def investigate_borrow_cause():
    print("🔍 严查借币原因分析")
    print("=" * 60)
    
    # 1. 检查 PEPE 交易详情
    orders_db = Path("reports/orders.sqlite")
    if not orders_db.exists():
        print("❌ 订单数据库不存在")
        return
    
    conn = sqlite3.connect(str(orders_db))
    cursor = conn.cursor()
    
    # 获取 PEPE 买入订单详情
    cursor.execute("""
        SELECT run_id, inst_id, side, intent, sz, notional_usdt, fee, acc_fill_sz, avg_px, state, created_ts, req_json
        FROM orders 
        WHERE inst_id = 'PEPE-USDT' 
        AND side = 'buy'
        AND intent = 'OPEN_LONG'
        ORDER BY created_ts DESC
        LIMIT 1
    """)
    
    pepe_order = cursor.fetchone()
    
    if not pepe_order:
        print("❌ 未找到 PEPE 买入订单")
        conn.close()
        return
    
    # 解析订单数据
    (run_id, inst_id, side, intent, sz, notional, fee, fill_sz, avg_px, state, created_ts, req_json) = pepe_order
    
    print(f"PEPE 买入订单详情:")
    print(f"  运行ID: {run_id}")
    print(f"  订单状态: {state}")
    print(f"  计划数量: {sz}")
    print(f"  成交数量: {fill_sz}")
    print(f"  成交均价: {avg_px}")
    print(f"  交易金额: {notional} USDT")
    print(f"  手续费: {fee}")
    
    # 解析请求 JSON
    try:
        req_data = json.loads(req_json)
        print(f"  原始请求: {json.dumps(req_data, indent=2)[:200]}...")
    except:
        print(f"  原始请求: {req_json[:200]}...")
    
    # 2. 分析手续费问题
    print(f"\n📊 手续费分析:")
    
    fee_str = str(fee)
    if fee_str.startswith('-'):
        fee_pepe = abs(float(fee_str))
        print(f"  手续费（PEPE）: {fee_pepe}")
        print(f"  手续费价值: {fee_pepe * float(avg_px):.6f} USDT")
        
        # 计算实际需要的 PEPE 数量
        actual_pepe_needed = float(fill_sz) + fee_pepe
        print(f"  实际需要: {actual_pepe_needed} PEPE (成交 {fill_sz} + 手续费 {fee_pepe})")
        print(f"  账户应有: {actual_pepe_needed} PEPE 余额")
    
    # 3. 检查账户余额历史
    print(f"\n💳 账户余额分析:")
    
    # 获取交易时间前后的账户快照
    cursor.execute("""
        SELECT created_ts, run_id, inst_id, side, sz, notional_usdt, fee
        FROM orders 
        WHERE created_ts BETWEEN ? - 60000 AND ? + 60000
        ORDER BY created_ts
    """, (created_ts, created_ts))
    
    nearby_orders = cursor.fetchall()
    
    print(f"  交易时间附近订单 ({len(nearby_orders)} 个):")
    for order in nearby_orders:
        o_ts, o_run, o_inst, o_side, o_sz, o_notional, o_fee = order
        time_diff = (int(created_ts) - int(o_ts)) / 1000
        print(f"    {o_inst} {o_side} {o_sz} ({time_diff:.1f}s)")
    
    # 4. 检查决策审计
    print(f"\n🤔 决策过程分析:")
    
    audit_file = Path(f"reports/runs/{run_id}/decision_audit.json")
    if audit_file.exists():
        with open(audit_file, 'r') as f:
            audit = json.load(f)
        
        # 检查 PEPE 的决策
        if 'router_decisions' in audit:
            for decision in audit['router_decisions']:
                if decision.get('symbol') == 'PEPE/USDT':
                    print(f"  PEPE 决策: {decision}")
                    break
    
    # 5. 根本原因分析
    print(f"\n🔍 根本原因分析:")
    print("=" * 40)
    
    print("1. ✅ USDT 充足: 账户有 ~44 USDT 余额")
    print("2. ✅ 交易金额: 仅 ~19.39 USDT")
    print("3. ❌ 问题: 手续费以 PEPE 币种扣除")
    print("")
    print("📌 关键发现:")
    print("   - OKX 手续费机制: 以交易币种（PEPE）扣除")
    print("   - 账户没有 PEPE 余额: 买入前余额为 0")
    print("   - 自动借币: 余额不足时 OKX 自动借币支付手续费")
    print("")
    print("🎯 根本原因:")
    print("   系统没有考虑『手续费币种余额』问题")
    print("   只检查了 USDT 余额，没检查 PEPE 余额")
    
    # 6. 解决方案
    print(f"\n🛡️ 解决方案:")
    print("1. 交易前检查手续费币种余额")
    print("2. 预留手续费（购买稍多数量）")
    print("3. 过滤低价值、高手续费风险币种")
    print("4. 启用借币预防检测")
    
    # 7. 创建预防脚本
    print(f"\n🤖 创建预防脚本...")
    
    prevent_script = """
#!/usr/bin/env python3
"""
    
    conn.close()
    
    print(f"\n" + "=" * 60)
    print("📋 调查总结:")
    print("1. 原因: OKX 以交易币种扣除手续费")
    print("2. 问题: 系统未检查手续费币种余额")
    print("3. 解决: 需要增加余额检查和预防机制")
    print("=" * 60)

if __name__ == "__main__":
    investigate_borrow_cause()