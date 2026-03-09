#!/usr/bin/env python3
"""
收集市场数据用于 alpha 评估
从 OKX 获取历史 K 线数据，计算 forward returns
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
import numpy as np

from configs.loader import load_config
from src.data.okx_ccxt_provider import OKXCCXTProvider


def create_market_data_tables(db_path: str):
    """创建市场数据表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建 1h K 线数据表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_data_1h (
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (symbol, timestamp)
    )
    """)
    
    # 创建 forward returns 表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forward_returns (
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        return_1h REAL,
        return_6h REAL,
        return_24h REAL,
        PRIMARY KEY (symbol, timestamp)
    )
    """)
    
    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON market_data_1h(symbol, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fr_symbol_timestamp ON forward_returns(symbol, timestamp)")
    
    conn.commit()
    conn.close()
    print(f"Created market data tables in {db_path}")


def fetch_historical_data(provider, symbol: str, timeframe: str, since: int, limit: int = 100) -> List[Dict]:
    """从 OKX 获取历史数据"""
    try:
        # 使用 CCXT 获取 OHLCV 数据
        ohlcv = provider.ccxt.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        
        data = []
        for row in ohlcv:
            data.append({
                'timestamp': row[0] // 1000,  # 毫秒转秒
                'open': row[1],
                'high': row[2],
                'low': row[3],
                'close': row[4],
                'volume': row[5]
            })
        
        print(f"Fetched {len(data)} bars for {symbol} ({timeframe})")
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return []


def calculate_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """计算 forward returns"""
    if df.empty:
        return df
    
    # 确保按时间排序
    df = df.sort_values('timestamp')
    
    # 计算对数收益率
    df['log_close'] = np.log(df['close'])
    
    # 1小时 forward return
    df['return_1h'] = df['log_close'].shift(-1) - df['log_close']
    
    # 6小时 forward return (需要足够的数据点)
    if len(df) >= 6:
        df['return_6h'] = df['log_close'].shift(-6) - df['log_close']
    
    # 24小时 forward return
    if len(df) >= 24:
        df['return_24h'] = df['log_close'].shift(-24) - df['log_close']
    
    # 转换回百分比
    for col in ['return_1h', 'return_6h', 'return_24h']:
        if col in df.columns:
            df[col] = np.exp(df[col]) - 1
    
    return df


def main():
    print("📊 市场数据收集")
    print("=" * 50)
    
    # 安全检查
    if os.getenv("V5_LIVE_ARM") != "YES":
        print("❌ Set V5_LIVE_ARM=YES to proceed")
        return
    
    # 加载配置
    cfg = load_config("configs/live_small.yaml", env_path=".env")
    
    # 初始化数据提供者
    # 注意：cfg.data 可能不存在，使用默认配置
    from src.data.okx_ccxt_provider import OKXCCXTProvider
    provider = OKXCCXTProvider({
        'exchange': 'okx',
        'timeframe_main': '1h',
        'timeframe_aux': '4h'
    })
    
    # 数据库路径
    db_path = "reports/alpha_history.db"
    
    # 创建表
    create_market_data_tables(db_path)
    
    # 获取关注的币种（从最近的运行中提取）
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取最近运行的 selected symbols
    cursor.execute("""
    SELECT snapshot_json FROM alpha_snapshots 
    ORDER BY timestamp DESC LIMIT 1
    """)
    row = cursor.fetchone()
    
    symbols = []
    if row:
        import json
        snapshot = json.loads(row[0])
        symbols = list(snapshot.get('scores', {}).keys())[:10]  # 取前10个
    
    if not symbols:
        # 默认币种
        symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT"]
    
    print(f"收集 {len(symbols)} 个币种的数据: {symbols[:5]}...")
    
    # 时间范围：最近7天
    end_time = int(time.time())
    start_time = end_time - 7 * 24 * 3600  # 7天前
    
    # 收集每个币种的数据
    total_bars = 0
    for symbol in symbols:
        print(f"\n收集 {symbol} 数据...")
        
        # 获取历史数据
        data = fetch_historical_data(provider, symbol, '1h', start_time * 1000, limit=168)  # 7天 * 24小时
        
        if not data:
            continue
        
        # 转换为 DataFrame
        df = pd.DataFrame(data)
        
        # 计算 forward returns
        df = calculate_forward_returns(df)
        
        # 保存到数据库
        conn = sqlite3.connect(db_path)
        
        # 保存 K 线数据
        for _, row in df.iterrows():
            cursor = conn.cursor()
            cursor.execute("""
            INSERT OR REPLACE INTO market_data_1h 
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                int(row['timestamp']),
                float(row.get('open', 0)),
                float(row.get('high', 0)),
                float(row.get('low', 0)),
                float(row.get('close', 0)),
                float(row.get('volume', 0))
            ))
        
        # 保存 forward returns
        for _, row in df.iterrows():
            if 'return_1h' in row and not pd.isna(row['return_1h']):
                cursor = conn.cursor()
                cursor.execute("""
                INSERT OR REPLACE INTO forward_returns 
                (symbol, timestamp, return_1h, return_6h, return_24h)
                VALUES (?, ?, ?, ?, ?)
                """, (
                    symbol,
                    int(row['timestamp']),
                    float(row.get('return_1h', 0)),
                    float(row.get('return_6h', 0)) if 'return_6h' in row and not pd.isna(row['return_6h']) else None,
                    float(row.get('return_24h', 0)) if 'return_24h' in row and not pd.isna(row['return_24h']) else None
                ))
        
        conn.commit()
        total_bars += len(data)
        print(f"  保存 {len(data)} 条K线数据")
    
    conn.close()
    
    print(f"\n✅ 数据收集完成")
    print(f"   币种数量: {len(symbols)}")
    print(f"   K线数据条数: {total_bars}")
    print(f"   数据库: {db_path}")
    
    # 验证数据
    print(f"\n🔍 数据验证:")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM market_data_1h")
    market_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM forward_returns")
    returns_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT symbol) FROM market_data_1h")
    symbol_count = cursor.fetchone()[0]
    
    print(f"   market_data_1h 记录数: {market_count}")
    print(f"   forward_returns 记录数: {returns_count}")
    print(f"   唯一币种数: {symbol_count}")
    
    conn.close()
    
    print("\n" + "=" * 50)
    print("下一步: 运行 evaluate_alpha_historical.py 计算 IC")
    print("=" * 50)


if __name__ == "__main__":
    main()