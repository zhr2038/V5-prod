#!/usr/bin/env python3
"""
修复时间对齐问题
将 alpha snapshot 的时间戳对齐到最近的整点
"""

import sqlite3
import pandas as pd
from datetime import datetime


def align_to_hour(timestamp: int) -> int:
    """将时间戳对齐到最近的整点"""
    # 转换为小时级时间戳（去掉分钟和秒）
    return (timestamp // 3600) * 3600


def check_alignment(db_path: str):
    """检查当前的时间对齐情况"""
    print("🔍 检查时间对齐情况")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    
    # 检查 alpha 时间戳分布
    query = """
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT ts) as unique_ts,
        MIN(ts) as min_ts,
        MAX(ts) as max_ts,
        AVG(ts % 3600) as avg_offset  -- 平均偏移秒数
    FROM alpha_snapshots
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    row = cursor.fetchone()
    
    if row:
        total, unique_ts, min_ts, max_ts, avg_offset = row
        min_dt = datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S')
        max_dt = datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Alpha snapshots:")
        print(f"  总记录数: {total}")
        print(f"  唯一时间戳: {unique_ts}")
        print(f"  时间范围: {min_dt} 到 {max_dt}")
        print(f"  平均偏移: {avg_offset:.0f} 秒")
        
        # 检查有多少是整点
        cursor.execute("SELECT COUNT(*) FROM alpha_snapshots WHERE ts % 3600 = 0")
        exact_hour = cursor.fetchone()[0]
        print(f"  整点时间戳: {exact_hour} ({exact_hour/total*100:.1f}%)")
    
    # 检查 market data 时间戳
    query = """
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT timestamp) as unique_ts,
        MIN(timestamp) as min_ts,
        MAX(timestamp) as max_ts,
        AVG(timestamp % 3600) as avg_offset
    FROM market_data_1h
    """
    
    cursor.execute(query)
    row = cursor.fetchone()
    
    if row:
        total, unique_ts, min_ts, max_ts, avg_offset = row
        min_dt = datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S')
        max_dt = datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\nMarket data:")
        print(f"  总记录数: {total}")
        print(f"  唯一时间戳: {unique_ts}")
        print(f"  时间范围: {min_dt} 到 {max_dt}")
        print(f"  平均偏移: {avg_offset:.0f} 秒")
        
        # 检查有多少是整点
        cursor.execute("SELECT COUNT(*) FROM market_data_1h WHERE timestamp % 3600 = 0")
        exact_hour = cursor.fetchone()[0]
        print(f"  整点时间戳: {exact_hour} ({exact_hour/total*100:.1f}%)")
    
    # 检查匹配情况
    print(f"\n📊 时间匹配测试:")
    
    # 取一个 alpha 时间戳示例
    cursor.execute("SELECT ts FROM alpha_snapshots LIMIT 1")
    alpha_ts = cursor.fetchone()[0]
    aligned_ts = align_to_hour(alpha_ts)
    
    print(f"  Alpha 时间戳: {alpha_ts} ({datetime.fromtimestamp(alpha_ts).strftime('%H:%M:%S')})")
    print(f"  对齐后: {aligned_ts} ({datetime.fromtimestamp(aligned_ts).strftime('%H:%M:%S')})")
    
    # 检查是否有对应的 market data
    cursor.execute("SELECT COUNT(*) FROM market_data_1h WHERE timestamp = ?", (aligned_ts,))
    market_count = cursor.fetchone()[0]
    
    if market_count > 0:
        print(f"  ✅ 找到 {market_count} 条匹配的市场数据")
    else:
        print(f"  ❌ 没有匹配的市场数据")
        
        # 找最近的市场数据
        cursor.execute("""
        SELECT timestamp, ABS(timestamp - ?) as diff 
        FROM market_data_1h 
        ORDER BY diff LIMIT 1
        """, (aligned_ts,))
        nearest = cursor.fetchone()
        if nearest:
            nearest_ts, diff = nearest
            print(f"  最近的市场数据: {nearest_ts} (差 {diff/3600:.1f} 小时)")
    
    conn.close()


def fix_alignment(db_path: str):
    """修复时间对齐"""
    print("\n🔧 修复时间对齐")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. 创建临时表存储对齐后的数据
    print("创建临时表...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS alpha_snapshots_aligned (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        f1_mom_5d REAL,
        f2_mom_20d REAL,
        f3_vol_adj_ret_20d REAL,
        f4_volume_expansion REAL,
        f5_rsi_trend_confirm REAL,
        z1_mom_5d REAL,
        z2_mom_20d REAL,
        z3_vol_adj_ret_20d REAL,
        z4_volume_expansion REAL,
        z5_rsi_trend_confirm REAL,
        score REAL,
        score_rank INTEGER,
        fwd_ret_1h REAL,
        fwd_ret_4h REAL,
        fwd_ret_12h REAL,
        fwd_ret_24h REAL,
        fwd_ret_72h REAL,
        regime TEXT,
        regime_multiplier REAL,
        selected INTEGER DEFAULT 0,
        traded INTEGER DEFAULT 0,
        pnl REAL DEFAULT 0.0,
        original_ts INTEGER,  -- 保存原始时间戳
        UNIQUE(run_id, ts, symbol)
    )
    """)
    
    # 2. 复制并对齐数据
    print("对齐时间戳...")
    
    # 先获取所有数据
    cursor.execute("SELECT * FROM alpha_snapshots")
    columns = [desc[0] for desc in cursor.description]
    
    # 准备插入语句
    placeholders = ', '.join(['?' for _ in range(len(columns) + 1)])  # +1 for original_ts
    insert_sql = f"INSERT INTO alpha_snapshots_aligned VALUES ({placeholders})"
    
    # 处理每条记录
    total = 0
    aligned = 0
    for row in cursor.fetchall():
        total += 1
        original_ts = row[columns.index('ts')]  # ts 是第2列（0-based）
        
        # 对齐到整点
        aligned_ts = align_to_hour(original_ts)
        
        # 创建新行（用对齐后的 ts）
        new_row = list(row)
        new_row[columns.index('ts')] = aligned_ts
        
        # 添加 original_ts
        new_row.append(original_ts)
        
        # 插入到临时表
        cursor.execute(insert_sql, new_row)
        aligned += 1
        
        if total % 50 == 0:
            print(f"  已处理 {total} 条记录...")
    
    conn.commit()
    print(f"✅ 对齐完成: {aligned}/{total} 条记录")
    
    # 3. 验证对齐结果
    print("\n📊 验证对齐结果:")
    
    # 检查对齐后的时间戳分布
    cursor.execute("SELECT COUNT(*) FROM alpha_snapshots_aligned WHERE ts % 3600 = 0")
    exact_hour = cursor.fetchone()[0]
    print(f"  整点时间戳: {exact_hour}/{aligned} ({exact_hour/aligned*100:.1f}%)")
    
    # 检查与 market data 的匹配
    cursor.execute("""
    SELECT COUNT(DISTINCT a.ts)
    FROM alpha_snapshots_aligned a
    JOIN market_data_1h m ON a.ts = m.timestamp
    """)
    matched_hours = cursor.fetchone()[0]
    print(f"  匹配的小时数: {matched_hours}")
    
    # 4. 替换原表（可选）
    print("\n🔄 替换原表...")
    response = input("是否替换原表？(y/n): ").strip().lower()
    
    if response == 'y':
        # 备份原表
        cursor.execute("ALTER TABLE alpha_snapshots RENAME TO alpha_snapshots_backup")
        
        # 重命名新表
        cursor.execute("ALTER TABLE alpha_snapshots_aligned RENAME TO alpha_snapshots")
        
        # 重新创建索引
        cursor.execute("CREATE INDEX idx_snapshots_ts ON alpha_snapshots(ts)")
        cursor.execute("CREATE INDEX idx_snapshots_symbol ON alpha_snapshots(symbol)")
        cursor.execute("CREATE INDEX idx_snapshots_run_id ON alpha_snapshots(run_id)")
        
        conn.commit()
        print("✅ 原表已替换")
    else:
        print("⚠️  保留临时表 alpha_snapshots_aligned")
    
    conn.close()


def test_ic_calculation(db_path: str):
    """测试 IC 计算"""
    print("\n🧪 测试 IC 计算")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    
    # 使用对齐后的表（如果存在）
    table_name = "alpha_snapshots_aligned"
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    
    if count == 0:
        table_name = "alpha_snapshots"
    
    # 尝试计算 IC
    query = f"""
    SELECT 
        a.ts,
        a.symbol,
        a.score,
        f.return_1h,
        f.return_6h,
        f.return_24h
    FROM {table_name} a
    JOIN forward_returns f ON a.symbol = f.symbol AND a.ts = f.timestamp
    WHERE a.score IS NOT NULL 
      AND f.return_1h IS NOT NULL
    ORDER BY a.ts
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("❌ 没有匹配的数据")
            return
        
        print(f"✅ 找到 {len(df)} 个匹配的数据点")
        print(f"  时间范围: {datetime.fromtimestamp(df['ts'].min()).strftime('%Y-%m-%d %H:%M')} 到 {datetime.fromtimestamp(df['ts'].max()).strftime('%Y-%m-%d %H:%M')}")
        print(f"  币种数量: {df['symbol'].nunique()}")
        
        # 计算 IC
        horizons = ['1h', '6h', '24h']
        for horizon in horizons:
            return_col = f'return_{horizon}'
            if return_col in df.columns:
                valid = df[df[return_col].notna()]
                if len(valid) > 10:
                    ic = valid['score'].corr(valid[return_col])
                    print(f"  IC({horizon}): {ic:.4f} (n={len(valid)})")
        
    except Exception as e:
        print(f"❌ IC 计算失败: {e}")
    
    conn.close()


def main():
    db_path = "reports/alpha_history.db"
    
    print("🕒 时间对齐修复工具")
    print("=" * 50)
    
    # 1. 检查当前状态
    check_alignment(db_path)
    
    # 2. 修复对齐
    fix_alignment(db_path)
    
    # 3. 测试 IC 计算
    test_ic_calculation(db_path)
    
    print("\n" + "=" * 50)
    print("🎯 完成！")
    print("=" * 50)


if __name__ == "__main__":
    main()