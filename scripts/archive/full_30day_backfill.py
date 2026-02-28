#!/usr/bin/env python3
"""
完整30天历史数据回填 - 所有币种
"""

import os
import sys
import time
import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reports/full_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FullBackfill:
    """完整历史数据回填"""
    
    def __init__(self, db_path="reports/alpha_history.db"):
        self.db_path = db_path
        self.okx_base_url = "https://www.okx.com"
        
    def get_all_symbols(self):
        """获取所有币种列表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
        symbols = [row[0] for row in cursor.fetchall()]
        conn.close()
        return symbols
    
    def get_historical_data(self, symbol, end_time, limit=100):
        """获取历史数据"""
        inst_id = symbol.replace('/', '-')
        url = f"{self.okx_base_url}/api/v5/market/history-candles"
        
        params = {
            'instId': inst_id,
            'bar': '1H',
            'after': str(int(end_time * 1000)),  # 毫秒
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params, timeout=15)
            data = response.json()
            
            if data.get('code') == '0':
                candles = data.get('data', [])
                records = []
                for candle in candles:
                    ts = int(candle[0]) // 1000  # 毫秒转秒
                    records.append({
                        'timestamp': ts,
                        'symbol': symbol,
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                        'volume_ccy': float(candle[6])
                    })
                return records
            else:
                logger.warning(f"{symbol}: API错误 - {data.get('msg', 'Unknown')}")
                return []
                
        except Exception as e:
            logger.error(f"{symbol}: 请求失败 - {e}")
            return []
    
    def backfill_symbol(self, symbol, days=30):
        """回填单个币种"""
        logger.info(f"开始回填 {symbol}...")
        
        conn = sqlite3.connect(self.db_path)
        
        # 获取当前最新数据时间
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(timestamp) FROM market_data_1h WHERE symbol = ?",
            (symbol,)
        )
        latest_ts = cursor.fetchone()[0]
        
        # 计算时间范围
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 3600)
        
        if latest_ts:
            # 如果已有数据，从最新数据后开始
            actual_start = latest_ts + 3600
            if actual_start > start_time:
                start_time = actual_start
        
        # 如果已经达到30天数据，跳过
        if latest_ts and (end_time - latest_ts) >= (days * 24 * 3600):
            logger.info(f"{symbol}: 已有足够数据，跳过")
            conn.close()
            return 0
        
        logger.info(f"{symbol}: 时间范围 {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
        
        total_added = 0
        current_end = end_time
        
        # 分批获取数据
        while current_end > start_time:
            batch_start = current_end - (100 * 3600)  # 每次100小时
            if batch_start < start_time:
                batch_start = start_time
            
            candles = self.get_historical_data(symbol, current_end)
            
            if candles:
                # 过滤重复数据
                df_new = pd.DataFrame(candles)
                
                # 检查数据库中是否已存在
                existing_query = f"""
                SELECT timestamp FROM market_data_1h 
                WHERE symbol = '{symbol}' 
                AND timestamp IN ({','.join([str(ts) for ts in df_new['timestamp'].tolist()])})
                """
                existing_df = pd.read_sql_query(existing_query, conn) if len(df_new) > 0 else pd.DataFrame()
                
                if not existing_df.empty:
                    existing_timestamps = set(existing_df['timestamp'].tolist())
                    df_new = df_new[~df_new['timestamp'].isin(existing_timestamps)]
                
                if len(df_new) > 0:
                    df_new.to_sql('market_data_1h', conn, if_exists='append', index=False)
                    total_added += len(df_new)
                    logger.info(f"{symbol}: 添加 {len(df_new)} 条记录")
                
                # 更新结束时间为这批数据的最早时间
                earliest = min(c['timestamp'] for c in candles)
                current_end = earliest - 3600
            else:
                # 没有数据，向前移动
                current_end = batch_start - 3600
            
            # 避免请求过于频繁
            time.sleep(0.3)
        
        conn.close()
        logger.info(f"{symbol}: 完成，共添加 {total_added} 条记录")
        return total_added
    
    def calculate_coverage_stats(self):
        """计算数据覆盖率统计"""
        conn = sqlite3.connect(self.db_path)
        
        # 获取所有币种
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM market_data_1h")
        symbols = [row[0] for row in cursor.fetchall()]
        
        # 计算每个币种的数据范围
        stats = []
        for symbol in symbols:
            cursor.execute("""
                SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest, COUNT(*) as count 
                FROM market_data_1h WHERE symbol = ?
            """, (symbol,))
            earliest, latest, count = cursor.fetchone()
            
            if earliest and latest:
                hours = (latest - earliest) / 3600
                days = hours / 24
                stats.append({
                    'symbol': symbol,
                    'count': count,
                    'hours': hours,
                    'days': days,
                    'earliest': datetime.fromtimestamp(earliest),
                    'latest': datetime.fromtimestamp(latest)
                })
        
        conn.close()
        return stats
    
    def run_full_backfill(self, days=30):
        """运行完整回填"""
        logger.info("=" * 60)
        logger.info(f"🚀 开始完整30天历史数据回填")
        logger.info("=" * 60)
        
        # 获取所有币种
        symbols = self.get_all_symbols()
        logger.info(f"📊 总币种数: {len(symbols)}")
        logger.info(f"📅 回填天数: {days}天")
        
        # 运行前统计
        pre_stats = self.calculate_coverage_stats()
        pre_total = sum(s['count'] for s in pre_stats)
        pre_avg_days = sum(s['days'] for s in pre_stats) / len(pre_stats) if pre_stats else 0
        
        logger.info(f"📈 回填前统计:")
        logger.info(f"  总记录数: {pre_total}")
        logger.info(f"  平均数据天数: {pre_avg_days:.1f}天")
        
        # 开始回填
        start_time = time.time()
        total_added = 0
        successful_symbols = []
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] 处理 {symbol}")
            try:
                added = self.backfill_symbol(symbol, days)
                if added > 0:
                    total_added += added
                    successful_symbols.append(symbol)
            except Exception as e:
                logger.error(f"{symbol}: 回填失败 - {e}")
            
            # 币种间延迟
            time.sleep(1)
        
        duration = time.time() - start_time
        
        # 运行后统计
        post_stats = self.calculate_coverage_stats()
        post_total = sum(s['count'] for s in post_stats)
        post_avg_days = sum(s['days'] for s in post_stats) / len(post_stats) if post_stats else 0
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ 完整回填完成!")
        logger.info("=" * 60)
        logger.info(f"⏱️  总耗时: {duration:.2f}秒")
        logger.info(f"✅ 成功币种: {len(successful_symbols)}/{len(symbols)}")
        logger.info(f"📈 添加记录: {total_added}条")
        logger.info(f"📊 回填后统计:")
        logger.info(f"  总记录数: {pre_total} → {post_total} (+{total_added})")
        logger.info(f"  平均数据天数: {pre_avg_days:.1f}天 → {post_avg_days:.1f}天")
        
        # 计算覆盖率
        symbol_count = len(symbols)
        theoretical_max_30d = symbol_count * 720  # 30天 × 24小时
        coverage = (post_total / theoretical_max_30d) * 100 if theoretical_max_30d > 0 else 0
        
        logger.info(f"📊 30天数据覆盖率: {coverage:.2f}%")
        
        # 保存报告
        self.save_report({
            'start_time': start_time,
            'end_time': time.time(),
            'duration': duration,
            'symbols_total': len(symbols),
            'symbols_successful': len(successful_symbols),
            'records_added': total_added,
            'pre_total': pre_total,
            'post_total': post_total,
            'pre_avg_days': pre_avg_days,
            'post_avg_days': post_avg_days,
            'coverage_30d': coverage,
            'successful_symbols': successful_symbols
        })
        
        return successful_symbols, total_added, coverage
    
    def save_report(self, data):
        """保存回填报告"""
        report_path = "reports/full_backfill_report.json"
        with open(report_path, 'w') as f:
            import json
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"📋 报告已保存: {report_path}")

def main():
    """主函数"""
    print("🚀 完整30天历史数据回填 - 所有币种")
    print("=" * 60)
    
    # 初始化回填器
    backfiller = FullBackfill()
    
    # 显示当前状态
    symbols = backfiller.get_all_symbols()
    print(f"📊 检测到币种: {len(symbols)} 个")
    print(f"📅 计划回填: 30天历史数据")
    print("")
    
    # 确认操作
    confirm = input("⚠️  确认开始回填所有币种的30天历史数据？(yes/no): ")
    if confirm.lower() != 'yes':
        print("❌ 操作已取消")
        return
    
    print("")
    print("🔄 开始历史数据回填...")
    print("日志输出到: reports/full_backfill.log")
    print("=" * 60)
    
    # 运行回填
    successful_symbols, total_added, coverage = backfiller.run_full_backfill(days=30)
    
    print("")
    print("=" * 60)
    print("✅ 历史数据回填完成!")
    print(f"⏱️  总耗时: 请查看日志")
    print(f"✅ 成功币种: {len(successful_symbols)}/{len(symbols)}")
    print(f"📈 添加记录: {total_added}条")
    print(f"📊 30天数据覆盖率: {coverage:.2f}%")
    print("")
    print("📋 详细报告: reports/full_backfill_report.json")
    print("📝 完整日志: reports/full_backfill.log")
    print("=" * 60)

if __name__ == "__main__":
    main()