#!/usr/bin/env python3
"""
自动化数据采集器
1. 收集市场数据
2. 更新 forward returns
3. 监控数据质量
"""

import os
import sys
import time
import sqlite3
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reports/data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCollector:
    """数据采集器"""
    
    def __init__(self, db_path: str = "reports/alpha_history.db"):
        self.db_path = db_path
        self.setup_database()
        
        # 关注的币种（动态更新）
        self.symbols = self.load_tracked_symbols()
        
        logger.info(f"DataCollector initialized. Tracking {len(self.symbols)} symbols")
    
    def setup_database(self):
        """设置数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 检查并更新现有表结构
        self._update_table_structure(conn, cursor)
        
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
            updated_at INTEGER DEFAULT (strftime('%s', 'now')),
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
            updated_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (symbol, timestamp)
        )
        """)
        
        # 数据采集状态表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_collection_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT NOT NULL,  -- 'market_data', 'returns_update', 'quality_check'
            timestamp INTEGER NOT NULL,
            symbols_count INTEGER,
            records_updated INTEGER,
            success BOOLEAN,
            error_message TEXT,
            duration_seconds REAL,
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_md_symbol_ts ON market_data_1h(symbol, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fr_symbol_ts ON forward_returns(symbol, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dcs_timestamp ON data_collection_status(timestamp)")
        
        conn.commit()
        conn.close()
        logger.info("Database tables setup complete")
    
    def _update_table_structure(self, conn, cursor):
        """更新现有表结构"""
        try:
            # 检查 market_data_1h 是否有 updated_at 列
            cursor.execute("PRAGMA table_info(market_data_1h)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'updated_at' not in columns:
                logger.info("Adding updated_at column to market_data_1h")
                cursor.execute("ALTER TABLE market_data_1h ADD COLUMN updated_at INTEGER DEFAULT (strftime('%s', 'now'))")
            
            # 检查 forward_returns 是否有 updated_at 列
            cursor.execute("PRAGMA table_info(forward_returns)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'updated_at' not in columns:
                logger.info("Adding updated_at column to forward_returns")
                cursor.execute("ALTER TABLE forward_returns ADD COLUMN updated_at INTEGER DEFAULT (strftime('%s', 'now'))")
                
        except Exception as e:
            logger.warning(f"Table structure update failed (may not exist yet): {e}")
    
    def load_tracked_symbols(self) -> List[str]:
        """加载需要跟踪的币种"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        symbols = []
        
        # 方法1: 从 alpha_snapshots 获取
        try:
            cursor.execute("SELECT DISTINCT symbol FROM alpha_snapshots LIMIT 30")
            symbols = [row[0] for row in cursor.fetchall()]
        except:
            pass
        
        # 方法2: 如果 alpha 数据为空，使用默认币种
        if not symbols:
            symbols = [
                "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
                "ADA/USDT", "XRP/USDT", "DOGE/USDT", "DOT/USDT",
                "LINK/USDT", "MATIC/USDT"
            ]
        
        conn.close()
        return symbols
    
    def fetch_market_data(self, symbol: str, hours: int = 24) -> pd.DataFrame:
        """获取市场数据"""
        # 转换 symbol: BTC/USDT -> BTC-USDT
        inst_id = symbol.replace('/', '-')
        
        url = "https://www.okx.com/api/v5/market/candles"
        params = {
            'instId': inst_id,
            'bar': '1H',  # 1小时K线
            'limit': min(hours, 300)  # OKX API 限制
        }
        
        try:
            response = requests.get(url, params=params, timeout=15)
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
            logger.error(f"Error fetching {symbol}: {e}")
        
        return pd.DataFrame()
    
    def update_market_data(self, hours: int = 24) -> Dict:
        """更新市场数据"""
        logger.info(f"Updating market data for {len(self.symbols)} symbols (last {hours}h)")
        
        start_time = time.time()
        total_records = 0
        success_count = 0
        
        for symbol in self.symbols:
            try:
                df = self.fetch_market_data(symbol, hours)
                
                if df.empty:
                    logger.warning(f"No data for {symbol}")
                    continue
                
                # 保存到数据库
                conn = sqlite3.connect(self.db_path)
                for _, row in df.iterrows():
                    cursor = conn.cursor()
                    cursor.execute("""
                    INSERT OR REPLACE INTO market_data_1h 
                    (symbol, timestamp, open, high, low, close, volume, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        int(row['timestamp']),
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume']),
                        int(time.time())
                    ))
                
                conn.commit()
                conn.close()
                
                total_records += len(df)
                success_count += 1
                logger.info(f"  {symbol}: {len(df)} records")
                
                # 避免 API 限制
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Failed to update {symbol}: {e}")
        
        duration = time.time() - start_time
        
        # 记录状态
        self.log_collection_status(
            task_type='market_data',
            symbols_count=len(self.symbols),
            records_updated=total_records,
            success=success_count > 0,
            duration_seconds=duration
        )
        
        return {
            'symbols_processed': len(self.symbols),
            'symbols_success': success_count,
            'records_updated': total_records,
            'duration_seconds': duration
        }
    
    def calculate_forward_returns(self) -> Dict:
        """计算 forward returns"""
        logger.info("Calculating forward returns")
        
        start_time = time.time()
        
        conn = sqlite3.connect(self.db_path)
        
        # 获取所有市场数据
        cursor = conn.cursor()
        cursor.execute("""
        SELECT symbol, timestamp, close 
        FROM market_data_1h 
        ORDER BY symbol, timestamp
        """)
        
        data = cursor.fetchall()
        
        if not data:
            logger.warning("No market data available")
            return {'records_updated': 0}
        
        # 转换为 DataFrame
        df = pd.DataFrame(data, columns=['symbol', 'timestamp', 'close'])
        
        # 按币种分组计算 returns
        returns_data = []
        
        for symbol, group in df.groupby('symbol'):
            group = group.sort_values('timestamp')
            
            # 计算对数收益率
            group['log_close'] = np.log(group['close'])
            
            # 1小时 forward return
            group['return_1h'] = group['log_close'].shift(-1) - group['log_close']
            
            # 6小时 forward return
            if len(group) >= 6:
                group['return_6h'] = group['log_close'].shift(-6) - group['log_close']
            
            # 24小时 forward return
            if len(group) >= 24:
                group['return_24h'] = group['log_close'].shift(-24) - group['log_close']
            
            # 转换回百分比
            for col in ['return_1h', 'return_6h', 'return_24h']:
                if col in group.columns:
                    group[col] = np.exp(group[col]) - 1
            
            # 收集数据
            for _, row in group.iterrows():
                if pd.notna(row.get('return_1h')):
                    returns_data.append((
                        symbol,
                        int(row['timestamp']),
                        float(row.get('return_1h', 0)),
                        float(row.get('return_6h', 0)) if pd.notna(row.get('return_6h')) else None,
                        float(row.get('return_24h', 0)) if pd.notna(row.get('return_24h')) else None,
                        int(time.time())
                    ))
        
        # 保存到数据库
        if returns_data:
            cursor.executemany("""
            INSERT OR REPLACE INTO forward_returns 
            (symbol, timestamp, return_1h, return_6h, return_24h, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """, returns_data)
        
        conn.commit()
        
        records_updated = len(returns_data)
        duration = time.time() - start_time
        
        # 记录状态
        self.log_collection_status(
            task_type='returns_update',
            symbols_count=df['symbol'].nunique(),
            records_updated=records_updated,
            success=records_updated > 0,
            duration_seconds=duration
        )
        
        conn.close()
        
        logger.info(f"Updated {records_updated} forward returns")
        
        return {
            'records_updated': records_updated,
            'duration_seconds': duration
        }
    
    def check_data_quality(self) -> Dict:
        """检查数据质量"""
        logger.info("Checking data quality")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        quality_report = {}
        
        # 1. 检查市场数据
        cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT symbol) as symbols,
            MIN(timestamp) as min_ts,
            MAX(timestamp) as max_ts,
            SUM(CASE WHEN close <= 0 THEN 1 ELSE 0 END) as invalid_prices
        FROM market_data_1h
        """)
        
        market_stats = cursor.fetchone()
        if market_stats:
            total, symbols, min_ts, max_ts, invalid = market_stats
            quality_report['market_data'] = {
                'total_records': total,
                'symbols_count': symbols,
                'time_range_hours': (max_ts - min_ts) / 3600 if min_ts and max_ts else 0,
                'invalid_prices': invalid,
                'coverage_pct': (total / (symbols * 168 * 7)) * 100 if symbols > 0 else 0  # 假设7天*24h*7币种
            }
        
        # 2. 检查 forward returns
        cursor.execute("""
        SELECT 
            COUNT(*) as total,
            AVG(CASE WHEN return_1h IS NOT NULL THEN 1 ELSE 0 END) as pct_1h,
            AVG(CASE WHEN return_6h IS NOT NULL THEN 1 ELSE 0 END) as pct_6h,
            AVG(CASE WHEN return_24h IS NOT NULL THEN 1 ELSE 0 END) as pct_24h
        FROM forward_returns
        """)
        
        returns_stats = cursor.fetchone()
        if returns_stats:
            total, pct_1h, pct_6h, pct_24h = returns_stats
            quality_report['forward_returns'] = {
                'total_records': total,
                'completeness_1h': pct_1h * 100,
                'completeness_6h': pct_6h * 100,
                'completeness_24h': pct_24h * 100
            }
        
        # 3. 检查 alpha 数据
        try:
            cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
            alpha_count = cursor.fetchone()[0]
            quality_report['alpha_data'] = {
                'total_records': alpha_count
            }
        except:
            pass
        
        conn.close()
        
        # 记录质量检查
        self.log_collection_status(
            task_type='quality_check',
            symbols_count=quality_report.get('market_data', {}).get('symbols_count', 0),
            records_updated=0,
            success=True,
            duration_seconds=0
        )
        
        logger.info(f"Data quality check complete: {quality_report}")
        
        return quality_report
    
    def log_collection_status(self, task_type: str, symbols_count: int, 
                             records_updated: int, success: bool, 
                             duration_seconds: float, error_message: str = None):
        """记录采集状态"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO data_collection_status 
        (task_type, timestamp, symbols_count, records_updated, success, error_message, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            task_type,
            int(time.time()),
            symbols_count,
            records_updated,
            success,
            error_message,
            duration_seconds
        ))
        
        conn.commit()
        conn.close()
    
    def run_full_collection(self):
        """运行完整的数据采集"""
        logger.info("=" * 60)
        logger.info("Starting full data collection")
        logger.info("=" * 60)
        
        # 1. 更新市场数据（最近24小时）
        market_result = self.update_market_data(hours=24)
        
        # 2. 计算 forward returns
        returns_result = self.calculate_forward_returns()
        
        # 3. 检查数据质量
        quality_report = self.check_data_quality()
        
        logger.info("=" * 60)
        logger.info("Data collection complete")
        logger.info(f"Market data: {market_result}")
        logger.info(f"Forward returns: {returns_result}")
        logger.info(f"Quality: {quality_report}")
        logger.info("=" * 60)
        
        return {
            'market_data': market_result,
            'forward_returns': returns_result,
            'quality_report': quality_report
        }


def main():
    """主函数"""
    print("🚀 自动化数据采集系统")
    print("=" * 60)
    
    # 创建采集器
    collector = DataCollector()
    
    # 运行完整采集
    result = collector.run_full_collection()
    
    # 打印摘要
    print("\n📊 采集结果摘要:")
    print("-" * 40)
    
    market = result['market_data']
    returns = result['forward_returns']
    quality = result['quality_report']
    
    print(f"市场数据: {market.get('records_updated', 0)} 条记录")
    print(f"Forward returns: {returns.get('records_updated', 0)} 条记录")
    
    if 'market_data' in quality:
        md = quality['market_data']
        print(f"数据覆盖: {md.get('coverage_pct', 0):.1f}%")
        print(f"无效价格: {md.get('invalid_prices', 0)} 条")
    
    if 'forward_returns' in quality:
        fr = quality['forward_returns']
        print(f"Returns 完整度: 1h={fr.get('completeness_1h', 0):.1f}%, "
              f"6h={fr.get('completeness_6h', 0):.1f}%, "
              f"24h={fr.get('completeness_24h', 0):.1f}%")
    
    print("\n📋 自动化设置:")
    print("1. 每小时运行: python3 scripts/auto_data_collector.py")
    print("2. 添加到 crontab:")
    print("   */30 * * * * cd /home/admin/clawd/v5-trading-bot && python3 scripts/auto_data_collector.py")
    print("3. 日志文件: reports/data_collector.log")
    print("=" * 60)


if __name__ == "__main__":
    main()