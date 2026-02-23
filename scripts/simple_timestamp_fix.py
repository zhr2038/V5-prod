#!/usr/bin/env python3
"""
简单时间戳对齐修复
"""

import sqlite3
from datetime import datetime


def main():
    db_path = "reports/alpha_history.db"
    
    print("🕒 时间戳对齐修复")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. 检查当前状态
    print("📊 当前状态:")
    
    # Alpha 时间戳
    cursor.execute("SELECT ts FROM alpha_snapshots LIMIT 3")
    alpha_timestamps = [row[0] for row in cursor.fetchall()]
    
    print("Alpha 时间戳示例:")
    for ts in alpha_timestamps:
        dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        hour_ts = (ts // 3600) * 3600
        hour_dt = datetime.fromtimestamp(hour_ts).strftime('%Y-%m-%d %H:%M:%S')
        offset = ts % 3600
        print(f"  {ts} -> {dt} (偏移: {offset}秒) -> 整点: {hour_ts} ({hour_dt})")
    
    # Market 时间戳
    cursor.execute("SELECT timestamp FROM market_data_1h WHERE symbol='BTC/USDT' ORDER BY timestamp DESC LIMIT 3")
    market_timestamps = [row[0] for row in cursor.fetchall()]
    
    print("\nMarket 时间戳示例:")
    for ts in market_timestamps:
        dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        print(f"  {ts} -> {dt}")
    
    # 2. 创建对齐后的视图
    print("\n🔧 创建对齐视图...")
    
    # 删除已存在的视图
    cursor.execute("DROP VIEW IF EXISTS alpha_snapshots_aligned_view")
    
    # 创建视图：将 alpha 时间戳对齐到整点
    cursor.execute("""
    CREATE VIEW alpha_snapshots_aligned_view AS
    SELECT 
        id,
        run_id,
        (ts / 3600) * 3600 as ts_aligned,  -- 对齐到整点
        symbol,
        f1_mom_5d,
        f2_mom_20d,
        f3_vol_adj_ret_20d,
        f4_volume_expansion,
        f5_rsi_trend_confirm,
        z1_mom_5d,
        z2_mom_20d,
        z3_vol_adj_ret_20d,
        z4_volume_expansion,
        z5_rsi_trend_confirm,
        score,
        score_rank,
        fwd_ret_1h,
        fwd_ret_4h,
        fwd_ret_12h,
        fwd_ret_24h,
        fwd_ret_72h,
        regime,
        regime_multiplier,
        selected,
        traded,
        pnl,
        ts as original_ts
    FROM alpha_snapshots
    """)
    
    print("✅ 创建对齐视图完成")
    
    # 3. 测试匹配
    print("\n🧪 测试匹配:")
    
    # 测试一个时间点
    test_ts = alpha_timestamps[0]
    aligned_ts = (test_ts // 3600) * 3600
    
    # 检查是否有 market data
    cursor.execute("SELECT COUNT(*) FROM market_data_1h WHERE timestamp = ?", (aligned_ts,))
    market_count = cursor.fetchone()[0]
    
    print(f"测试 Alpha 时间戳: {test_ts} -> 对齐: {aligned_ts}")
    print(f"匹配的 Market 数据: {market_count} 条")
    
    if market_count > 0:
        print("✅ 匹配成功！")
    else:
        # 找最近的市场数据
        cursor.execute("""
        SELECT timestamp, ABS(timestamp - ?) as diff_seconds
        FROM market_data_1h 
        ORDER BY diff_seconds LIMIT 1
        """, (aligned_ts,))
        nearest = cursor.fetchone()
        if nearest:
            nearest_ts, diff = nearest
            nearest_dt = datetime.fromtimestamp(nearest_ts).strftime('%Y-%m-%d %H:%M:%S')
            print(f"最近的市场数据: {nearest_ts} ({nearest_dt})")
            print(f"时间差: {diff} 秒 ({diff/3600:.2f} 小时)")
    
    # 4. 测试 IC 计算
    print("\n📈 测试 IC 计算:")
    
    query = """
    SELECT 
        a.ts_aligned as ts,
        a.symbol,
        a.score,
        f.return_1h,
        f.return_6h,
        f.return_24h
    FROM alpha_snapshots_aligned_view a
    JOIN forward_returns f ON a.symbol = f.symbol AND a.ts_aligned = f.timestamp
    WHERE a.score IS NOT NULL 
      AND f.return_1h IS NOT NULL
    ORDER BY a.ts_aligned
    LIMIT 10
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if rows:
            print(f"✅ 找到 {len(rows)} 个匹配的数据点")
            print("前3个匹配:")
            for i, row in enumerate(rows[:3]):
                ts, symbol, score, ret_1h, ret_6h, ret_24h = row
                dt = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
                print(f"  {dt} | {symbol:10s} | score: {score:.3f} | ret_1h: {ret_1h:.4f}")
            
            # 计算简单 IC
            import pandas as pd
            df = pd.read_sql_query(query.replace("LIMIT 10", ""), conn)
            
            if not df.empty:
                print(f"\n📊 IC 计算结果:")
                print(f"  总数据点: {len(df)}")
                print(f"  币种数量: {df['symbol'].nunique()}")
                print(f"  时间范围: {datetime.fromtimestamp(df['ts'].min()).strftime('%Y-%m-%d')} 到 {datetime.fromtimestamp(df['ts'].max()).strftime('%Y-%m-%d')}")
                
                # 计算各时间段的 IC
                for horizon in ['1h', '6h', '24h']:
                    col = f'return_{horizon}'
                    if col in df.columns:
                        valid = df[df[col].notna()]
                        if len(valid) > 5:
                            ic = valid['score'].corr(valid[col])
                            print(f"  IC({horizon}): {ic:.4f} (n={len(valid)})")
        else:
            print("❌ 没有找到匹配的数据")
            
    except Exception as e:
        print(f"❌ IC 计算错误: {e}")
    
    # 5. 创建更方便的查询视图
    print("\n🔧 创建 IC 计算视图...")
    
    cursor.execute("DROP VIEW IF EXISTS ic_calculation_view")
    
    cursor.execute("""
    CREATE VIEW ic_calculation_view AS
    SELECT 
        a.ts_aligned as timestamp,
        a.symbol,
        a.score,
        a.regime,
        a.regime_multiplier,
        f.return_1h,
        f.return_6h,
        f.return_24h,
        m.close as price,
        m.volume
    FROM alpha_snapshots_aligned_view a
    LEFT JOIN forward_returns f ON a.symbol = f.symbol AND a.ts_aligned = f.timestamp
    LEFT JOIN market_data_1h m ON a.symbol = m.symbol AND a.ts_aligned = m.timestamp
    WHERE a.score IS NOT NULL
    """)
    
    print("✅ 创建 IC 计算视图")
    
    # 验证视图
    cursor.execute("SELECT COUNT(*) FROM ic_calculation_view WHERE return_1h IS NOT NULL")
    valid_count = cursor.fetchone()[0]
    print(f"  有效数据点: {valid_count}")
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 50)
    print("🎯 时间对齐修复完成！")
    print("=" * 50)
    print("\n📋 可用视图:")
    print("  1. alpha_snapshots_aligned_view - 对齐后的 alpha 数据")
    print("  2. ic_calculation_view - 用于 IC 计算的完整视图")
    print("\n📊 下一步:")
    print("  运行: python3 scripts/evaluate_alpha_historical.py")
    print("  或直接查询数据库进行深入分析")


if __name__ == "__main__":
    main()