#!/usr/bin/env python3
"""
历史数据回填脚本 - 一次性补充30天数据
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
        logging.FileHandler('reports/historical_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HistoricalDataBackfill:
    """历史数据回填器"""
    
    def __init__(self, db_path: str = "reports/alpha_history.db"):
        self.db_path = db_path
        self.okx_base_url = "https://www.okx.com"
        
    def get_historical_klines(self, symbol: str, start_time: int, end_time: int, 
                             interval: str = "1H", limit: int = 100) -> List[Dict]:
        """获取历史K线数据"""
        # 转换符号格式：BTC/USDT -> BTC-USDT
        inst_id = symbol.replace('/', '-')
        
        url = f"{self.okx_base_url}/api/v5/market/history-candles"
        params = {
            'instId': inst_id,
            'bar': interval,
            'after': str(end_time * 1000),  # OKX使用毫秒
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == '0':
                candles = data.get('data', [])
                # 转换数据格式
                result = []
                for candle in candles:
                    result.append({
                        'timestamp': int(candle[0]) // 1000,  # 毫秒转秒
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                        'volume_ccy': float(candle[6]),
                        'symbol': symbol
                    })
                return result
            else:
                logger.error(f"OKX API error for {symbol}: {data.get('msg', 'Unknown error')}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return []
    
    def backfill_symbol(self, symbol: str, days: int = 30) -> int:
        """回填单个币种的历史数据"""
        logger.info(f"开始回填 {symbol} 的 {days} 天历史数据...")
        
        # 计算时间范围
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 3600)
        
        # 获取当前已有的最新数据时间
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(timestamp) FROM market_data_1h WHERE symbol = ?",
            (symbol,)
        )
        latest_existing = cursor.fetchone()[0]
        
        # 如果已有数据，从最新数据时间开始回填
        if latest_existing:
            start_time = latest_existing + 3600  # 从下一小时开始
            logger.info(f"{symbol} 已有数据到 {datetime.fromtimestamp(latest_existing)}，从 {datetime.fromtimestamp(start_time)} 开始回填")
        
        total_records = 0
        current_end = end_time
        
        # 分批获取数据（每次100条，约4天数据）
        while current_end > start_time:
            batch_start = current_end - (100 * 3600)  # 100小时
            if batch_start < start_time:
                batch_start = start_time
            
            logger.info(f"获取 {symbol} 数据: {datetime.fromtimestamp(batch_start)} 到 {datetime.fromtimestamp(current_end)}")
            
            klines = self.get_historical_klines(symbol, batch_start, current_end)
            
            if klines:
                # 保存到数据库
                df = pd.DataFrame(klines)
                df.to_sql('market_data_1h', conn, if_exists='append', index=False)
                total_records += len(klines)
                logger.info(f"已保存 {len(klines)} 条记录")
            
            # 更新当前结束时间
            if klines:
                # 获取最早的时间戳继续向前获取
                earliest_in_batch = min(k['timestamp'] for k in klines)
                current_end = earliest_in_batch - 3600
            else:
                # 如果没有数据，向前移动100小时
                current_end = batch_start - 1
            
            # 避免请求过于频繁
            time.sleep(0.5)
        
        conn.close()
        logger.info(f"完成 {symbol} 回填，共添加 {total_records} 条记录")
        return total_records
    
    def calculate_forward_returns(self, symbol: str):
        """计算前向收益"""
        logger.info(f"计算 {symbol} 的前向收益...")
        
        conn = sqlite3.connect(self.db_path)
        
        # 读取数据
        query = """
        SELECT timestamp, close 
        FROM market_data_1h 
        WHERE symbol = ? 
        ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol,))
        
        if len(df) < 2:
            logger.warning(f"{symbol} 数据不足，跳过前向收益计算")
            conn.close()
            return
        
        # 计算前向收益
        df['return_1h'] = df['close'].pct_change(periods=1).shift(-1)
        df['return_6h'] = df['close'].pct_change(periods=6).shift(-6)
        df['return_24h'] = df['close'].pct_change(periods=24).shift(-24)
        
        # 保存到数据库
        returns_df = df[['timestamp', 'symbol', 'return_1h', 'return_6h', 'return_24h']].copy()
        returns_df = returns_df.dropna()
        
        # 删除旧的forward returns
        cursor = conn.cursor()
        cursor.execute("DELETE FROM forward_returns WHERE symbol = ?", (symbol,))
        
        # 插入新的forward returns
        returns_df.to_sql('forward_returns', conn, if_exists='append', index=False)
        
        conn.close()
        logger.info(f"完成 {symbol} 前向收益计算，共 {len(returns_df)} 条记录")
    
    def run_backfill(self, symbols: List[str], days: int = 30):
        """运行完整的历史数据回填"""
        logger.info(f"开始历史数据回填，币种数量: {len(symbols)}，天数: {days}")
        logger.info(f"币种列表: {symbols}")
        
        total_records = 0
        successful_symbols = []
        
        for symbol in symbols:
            try:
                records = self.backfill_symbol(symbol, days)
                if records > 0:
                    self.calculate_forward_returns(symbol)
                    total_records += records
                    successful_symbols.append(symbol)
                else:
                    logger.warning(f"{symbol} 没有新数据可回填")
            except Exception as e:
                logger.error(f"回填 {symbol} 时出错: {e}")
            
            # 币种间延迟，避免请求过于频繁
            time.sleep(2)
        
        # 更新数据收集状态
        self.update_collection_status(successful_symbols, total_records, days)
        
        logger.info(f"历史数据回填完成!")
        logger.info(f"成功回填币种: {len(successful_symbols)}/{len(symbols)}")
        logger.info(f"总记录数: {total_records}")
        
        return successful_symbols, total_records
    
    def update_collection_status(self, symbols: List[str], total_records: int, days: int):
        """更新数据收集状态"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO data_collection_status 
        (task_type, timestamp, symbols_count, records_updated, success, duration_seconds, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            'historical_backfill',
            int(time.time()),
            len(symbols),
            total_records,
            True,
            0,  # 持续时间在脚本外部计算
            f"Backfilled {days} days of historical data"
        ))
        
        conn.commit()
        conn.close()
    
    def get_tracked_symbols(self) -> List[str]:
        """获取当前跟踪的币种列表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT symbol FROM market_data_1h")
        symbols = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return symbols

def main():
    """主函数"""
    print("🚀 历史数据回填脚本 - 一次性补充30天数据")
    print("=" * 60)
    
    # 初始化回填器
    backfiller = HistoricalDataBackfill()
    
    # 获取当前跟踪的币种
    symbols = backfiller.get_tracked_symbols()
    
    if not symbols:
        print("❌ 未找到跟踪的币种，请先运行数据收集")
        return
    
    print(f"📊 当前跟踪币种: {len(symbols)} 个")
    print(f"📅 计划回填: 30天历史数据")
    print("")
    
    # 确认操作
    confirm = input("⚠️  确认开始回填30天历史数据？(yes/no): ")
    if confirm.lower() != 'yes':
        print("❌ 操作已取消")
        return
    
    print("")
    print("🔄 开始历史数据回填...")
    print("=" * 60)
    
    # 运行回填
    start_time = time.time()
    successful_symbols, total_records = backfiller.run_backfill(symbols, days=30)
    duration = time.time() - start_time
    
    print("")
    print("=" * 60)
    print("✅ 历史数据回填完成!")
    print(f"⏱️  总耗时: {duration:.2f} 秒")
    print(f"✅ 成功币种: {len(successful_symbols)}/{len(symbols)}")
    print(f"📈 总记录数: {total_records}")
    print(f"📊 平均每个币种: {total_records/len(successful_symbols):.0f} 条记录" if successful_symbols else "N/A")
    print("")
    print("📋 下一步:")
    print("1. 检查数据质量: 运行数据质量检查脚本")
    print("2. 验证覆盖率: 检查数据覆盖率是否提升")
    print("3. 继续常规收集: 每小时自动收集保持数据更新")
    print("=" * 60)

if __name__ == "__main__":
    main()