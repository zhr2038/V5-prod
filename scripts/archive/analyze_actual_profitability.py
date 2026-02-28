#!/usr/bin/env python3
"""
基于实际交易数据分析盈利能力
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json

def analyze_actual_trades():
    """分析实际交易数据"""
    
    print("📊 基于实际交易数据分析盈利能力")
    print("=" * 60)
    
    # 连接到订单数据库
    db_path = Path("reports/orders.sqlite")
    if not db_path.exists():
        print("❌ 订单数据库不存在")
        return None
    
    conn = sqlite3.connect(str(db_path))
    
    # 查询实际交易数据
    query = """
    SELECT 
        inst_id as symbol,
        side,
        notional_usdt,
        fee,
        created_ts,
        state,
        last_query_json
    FROM orders 
    WHERE state = 'FILLED'
    ORDER BY created_ts DESC
    LIMIT 100
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("❌ 无实际交易数据")
        return None
    
    print(f"✅ 找到 {len(df)} 个实际交易记录")
    
    # 分析交易表现
    analysis = {
        "total_trades": len(df),
        "unique_symbols": df['symbol'].nunique(),
        "buy_trades": len(df[df['side'] == 'buy']),
        "sell_trades": len(df[df['side'] == 'sell']),
        "total_pnl": 0.0,
        "winning_trades": 0,
        "losing_trades": 0,
        "avg_trade_size_usdt": 0.0,
        "trade_dates": []
    }
    
    # 计算PNL
    if 'realized_pnl_usdt' in df.columns:
        pnl_values = df['realized_pnl_usdt'].dropna()
        if not pnl_values.empty:
            analysis["total_pnl"] = pnl_values.sum()
            analysis["winning_trades"] = len(pnl_values[pnl_values > 0])
            analysis["losing_trades"] = len(pnl_values[pnl_values < 0])
            analysis["break_even_trades"] = len(pnl_values[pnl_values == 0])
    
    # 计算交易规模
    if 'fill_px' in df.columns and 'fill_sz' in df.columns:
        df['notional_usdt'] = df['fill_px'] * df['fill_sz']
        analysis["avg_trade_size_usdt"] = df['notional_usdt'].mean()
    
    # 交易日期分布
    if 'created_ts' in df.columns:
        df['date'] = pd.to_datetime(df['created_ts'], unit='ms').dt.date
        analysis["trade_dates"] = df['date'].unique().tolist()
        analysis["trading_days"] = len(analysis["trade_dates"])
    
    return analysis

def analyze_cost_impact():
    """分析成本对盈利的影响"""
    
    print("\n💰 成本影响分析")
    print("-" * 40)
    
    # 检查成本事件数据
    cost_events_dir = Path("reports/cost_events_clean")
    if not cost_events_dir.exists():
        print("❌ 无成本事件数据")
        return None
    
    cost_files = list(cost_events_dir.glob("*.jsonl"))
    if not cost_files:
        print("❌ 无成本事件文件")
        return None
    
    total_fees_usdt = 0.0
    total_slippage_usdt = 0.0
    total_notional_usdt = 0.0
    total_trades = 0
    
    for file in cost_files:
        with open(file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line.strip())
                    total_fees_usdt += event.get('fee_usdt', 0)
                    total_slippage_usdt += event.get('slippage_usdt', 0)
                    total_notional_usdt += event.get('notional_usdt', 0)
                    total_trades += 1
                except:
                    continue
    
    if total_trades == 0:
        print("❌ 无成本事件记录")
        return None
    
    analysis = {
        "total_trades": total_trades,
        "total_fees_usdt": total_fees_usdt,
        "total_slippage_usdt": total_slippage_usdt,
        "total_cost_usdt": total_fees_usdt + total_slippage_usdt,
        "total_notional_usdt": total_notional_usdt,
        "avg_fee_bps": (total_fees_usdt / total_notional_usdt * 10000) if total_notional_usdt > 0 else 0,
        "avg_slippage_bps": (total_slippage_usdt / total_notional_usdt * 10000) if total_notional_usdt > 0 else 0,
        "avg_total_cost_bps": ((total_fees_usdt + total_slippage_usdt) / total_notional_usdt * 10000) if total_notional_usdt > 0 else 0,
    }
    
    return analysis

def generate_profitability_report(trade_analysis, cost_analysis):
    """生成盈利能力报告"""
    
    print("\n" + "=" * 60)
    print("📋 实际交易盈利能力报告")
    print("=" * 60)
    
    print(f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if trade_analysis:
        print(f"\n📊 交易表现分析:")
        print(f"  总交易次数: {trade_analysis['total_trades']}")
        print(f"  交易币种数: {trade_analysis['unique_symbols']}")
        print(f"  买入交易: {trade_analysis['buy_trades']}")
        print(f"  卖出交易: {trade_analysis['sell_trades']}")
        print(f"  交易天数: {trade_analysis.get('trading_days', 'N/A')}")
        
        if trade_analysis['total_pnl'] != 0:
            print(f"\n💰 盈亏分析:")
            print(f"  总盈亏: ${trade_analysis['total_pnl']:.2f}")
            print(f"  盈利交易: {trade_analysis['winning_trades']}")
            print(f"  亏损交易: {trade_analysis['losing_trades']}")
            print(f"  持平交易: {trade_analysis.get('break_even_trades', 0)}")
            
            if trade_analysis['total_trades'] > 0:
                win_rate = trade_analysis['winning_trades'] / trade_analysis['total_trades'] * 100
                print(f"  胜率: {win_rate:.1f}%")
        
        if trade_analysis['avg_trade_size_usdt'] > 0:
            print(f"\n📈 交易规模:")
            print(f"  平均交易规模: ${trade_analysis['avg_trade_size_usdt']:.2f}")
    
    if cost_analysis:
        print(f"\n💰 成本分析:")
        print(f"  总交易次数: {cost_analysis['total_trades']}")
        print(f"  总费用: ${cost_analysis['total_fees_usdt']:.4f}")
        print(f"  总滑点: ${cost_analysis['total_slippage_usdt']:.4f}")
        print(f"  总成本: ${cost_analysis['total_cost_usdt']:.4f}")
        print(f"  总交易额: ${cost_analysis['total_notional_usdt']:.2f}")
        print(f"  平均费用: {cost_analysis['avg_fee_bps']:.2f}bps")
        print(f"  平均滑点: {cost_analysis['avg_slippage_bps']:.2f}bps")
        print(f"  平均总成本: {cost_analysis['avg_total_cost_bps']:.2f}bps")
        
        # 成本占比分析
        if trade_analysis and trade_analysis['total_pnl'] != 0:
            cost_ratio = cost_analysis['total_cost_usdt'] / abs(trade_analysis['total_pnl']) * 100
            print(f"  成本占盈亏比例: {cost_ratio:.1f}%")
    
    # 优化效果评估
    print(f"\n🎯 优化效果评估:")
    
    # 基于实际数据的评估
    if cost_analysis:
        print(f"  1. 实际平均成本: {cost_analysis['avg_total_cost_bps']:.2f}bps")
        print(f"  2. 固定成本假设: 11.00bps (6+5)")
        print(f"  3. 差异: {cost_analysis['avg_total_cost_bps'] - 11.00:+.2f}bps")
        
        if cost_analysis['avg_total_cost_bps'] < 11.00:
            print(f"  ✅ 实际成本低于固定假设")
        elif cost_analysis['avg_total_cost_bps'] > 11.00:
            print(f"  ⚠️ 实际成本高于固定假设")
        else:
            print(f"  🔄 实际成本与假设一致")
    
    # F2优化评估
    print(f"\n🎯 F2优化评估:")
    print(f"  1. 原权重: 25%")
    print(f"  2. 优化后: 20% (基于IC+成本分析)")
    print(f"  3. 评估: 需要更多数据验证实际效果")
    
    # 建议
    print(f"\n💡 建议:")
    print(f"  1. 继续积累实际交易数据")
    print(f"  2. 监控F2权重调整后的表现")
    print(f"  3. 定期重新校准成本模型")
    print(f"  4. 基于实际数据优化其他参数")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 基于实际交易数据分析盈利能力")
    print("=" * 60)
    
    # 分析实际交易
    trade_analysis = analyze_actual_trades()
    
    # 分析成本影响
    cost_analysis = analyze_cost_impact()
    
    # 生成报告
    generate_profitability_report(trade_analysis, cost_analysis)
    
    print("\n✅ 分析完成")
    print("=" * 60)
    
    print("\n💡 总结:")
    print("基于实际交易数据的分析比模拟回测更可靠")
    print("优化已基于真实数据实施，现在需要监控实际效果")

if __name__ == "__main__":
    main()