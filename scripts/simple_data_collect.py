#!/usr/bin/env python3
"""
简化版数据收集 - 先建立基础市场数据
"""

import os
import sqlite3
import time
import pandas as pd
import numpy as np
import requests


def create_tables(db_path: str):
    """创建必要的表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 市场数据表
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
    
    # forward returns 表
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
    
    conn.commit()
    conn.close()
    print(f"Tables created in {db_path}")


def fetch_okx_ohlcv(symbol: str, timeframe: str = '1h', limit: int = 168) -> pd.DataFrame:
    """从 OKX 公共 API 获取 OHLCV 数据"""
    # 转换 symbol: BTC/USDT -> BTC-USDT
    inst_id = symbol.replace('/', '-')
    
    url = f"https://www.okx.com/api/v5/market/candles"
    # OKX API 需要大写的时间框架
    tf_map = {'1h': '1H', '4h': '4H', '1d': '1D'}
    okx_timeframe = tf_map.get(timeframe, timeframe.upper())
    
    params = {
        'instId': inst_id,
        'bar': okx_timeframe,
        'limit': limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0':
                candles = data['data']
                
                records = []
                for candle in candles:
                    records.append({
                        'timestamp': int(candle[0]) // 1000,  # 毫秒转秒
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5])
                    })
                
                df = pd.DataFrame(records)
                df['symbol'] = symbol
                return df.sort_values('timestamp')
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    
    return pd.DataFrame()


def calculate_returns(df: pd.DataFrame) -> pd.DataFrame:
    """计算 forward returns"""
    if df.empty:
        return df
    
    df = df.sort_values('timestamp')
    
    # 简单收益率
    df['return_1h'] = df['close'].pct_change(periods=-1)  # 未来1小时
    
    # 需要足够数据点
    if len(df) >= 6:
        df['return_6h'] = df['close'].pct_change(periods=-6)
    
    if len(df) >= 24:
        df['return_24h'] = df['close'].pct_change(periods=-24)
    
    return df


def main():
    print("📊 简化数据收集")
    print("=" * 50)
    
    db_path = "reports/alpha_history.db"
    
    # 创建表
    create_tables(db_path)
    
    # 主要币种
    symbols = [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
        "ADA/USDT", "XRP/USDT", "DOGE/USDT", "DOT/USDT"
    ]
    
    print(f"收集 {len(symbols)} 个币种数据")
    
    total_rows = 0
    for symbol in symbols:
        print(f"\n{symbol}...")
        
        # 获取数据
        df = fetch_okx_ohlcv(symbol, '1h', 168)  # 最近7天
        
        if df.empty:
            print(f"  ⚠️  无数据")
            continue
        
        # 计算 returns
        df = calculate_returns(df)
        
        # 保存到数据库
        conn = sqlite3.connect(db_path)
        
        # 保存市场数据
        for _, row in df.iterrows():
            cursor = conn.cursor()
            cursor.execute("""
            INSERT OR REPLACE INTO market_data_1h 
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                int(row['timestamp']),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                float(row['volume'])
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
                    float(row['return_1h']),
                    float(row.get('return_6h', 0)) if 'return_6h' in row and not pd.isna(row['return_6h']) else None,
                    float(row.get('return_24h', 0)) if 'return_24h' in row and not pd.isna(row['return_24h']) else None
                ))
        
        conn.commit()
        conn.close()
        
        total_rows += len(df)
        print(f"  ✅ {len(df)} 条记录")
    
    # 验证
    print(f"\n✅ 收集完成")
    print(f"   总记录数: {total_rows}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM market_data_1h")
    market_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM forward_returns")
    returns_count = cursor.fetchone()[0]
    
    print(f"   market_data_1h: {market_count} 行")
    print(f"   forward_returns: {returns_count} 行")
    
    conn.close()
    
    print("\n" + "=" * 50)
    print("现在可以运行 evaluate_alpha_historical.py")
    print("=" * 50)


if __name__ == "__main__":
    main()