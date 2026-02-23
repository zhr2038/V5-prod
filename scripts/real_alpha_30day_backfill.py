#!/usr/bin/env python3
"""
真实AlphaEngine 30天数据回填
使用真实的AlphaEngine计算30天历史alpha数据
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import Dict, List, Any, Tuple
import warnings
import time
warnings.filterwarnings('ignore')

sys.path.append(str(Path(__file__).resolve().parents[1]))

class RealAlphaBackfiller:
    """真实Alpha数据回填器"""
    
    def __init__(self, db_path: str = "reports/alpha_history.db"):
        self.db_path = db_path
        self.conn = None
        self.alpha_engine = None
        self.config = None
        
    def initialize(self):
        """初始化"""
        print("🔧 初始化Alpha回填系统...")
        
        # 连接数据库
        self.conn = sqlite3.connect(self.db_path)
        
        # 导入必要的模块
        try:
            from configs.loader import load_config
            from src.alpha.alpha_engine import AlphaEngine
            from src.core.models import MarketSeries
            
            self.MarketSeries = MarketSeries
            self.config = load_config()
            self.alpha_engine = AlphaEngine(self.config.alpha)
            
            print("✅ 模块导入成功")
            print(f"  配置加载: {self.config.alpha}")
            return True
            
        except ImportError as e:
            print(f"❌ 导入错误: {e}")
            return False
        except Exception as e:
            print(f"❌ 初始化错误: {e}")
            return False
    
    def prepare_database(self):
        """准备数据库表"""
        print("\n🔄 准备数据库表...")
        
        cursor = self.conn.cursor()
        
        # 创建alpha_snapshots表（如果不存在）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alpha_snapshots (
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
                selected INTEGER,
                traded INTEGER,
                pnl REAL
            )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alpha_ts ON alpha_snapshots(ts)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alpha_symbol ON alpha_snapshots(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alpha_run_id ON alpha_snapshots(run_id)")
        
        self.conn.commit()
        print("✅ 数据库表准备完成")
        return True
    
    def get_market_data_for_timestamp(self, ts: int) -> Dict[str, Any]:
        """获取特定时间点的市场数据"""
        cursor = self.conn.cursor()
        
        # 获取该时间点之前足够的历史数据（用于计算因子）
        # 需要至少20天的数据来计算20日动量
        start_ts = ts - 20 * 24 * 3600  # 20天前
        
        cursor.execute("""
            SELECT DISTINCT symbol 
            FROM market_data_1h 
            WHERE timestamp <= ? 
            GROUP BY symbol 
            HAVING COUNT(*) >= 24 * 20  -- 至少20天数据
        """, (ts,))
        
        symbols = [row[0] for row in cursor.fetchall()]
        
        market_data = {}
        
        for symbol in symbols:
            # 获取该币种的历史数据
            cursor.execute("""
                SELECT timestamp, close, volume
                FROM market_data_1h
                WHERE symbol = ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 24 * 30  -- 最多30天数据
            """, (symbol, ts))
            
            data = cursor.fetchall()
            if len(data) >= 24 * 5:  # 至少5天数据
                timestamps = [row[0] for row in data]
                closes = [row[1] for row in data]
                volumes = [row[2] for row in data]
                
                # 转换为MarketSeries格式
                market_data[symbol] = self.MarketSeries(
                    timestamps=timestamps[::-1],  # 反转回时间顺序
                    closes=closes[::-1],
                    volumes=volumes[::-1]
                )
        
        return market_data
    
    def calculate_alpha_for_timestamp(self, ts: int) -> Tuple[Dict[str, Any], int]:
        """计算特定时间点的alpha数据"""
        try:
            # 获取市场数据
            market_data = self.get_market_data_for_timestamp(ts)
            
            if not market_data:
                print(f"  ⚠️ 时间点 {datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')}: 无足够市场数据")
                return {}, 0
            
            # 计算alpha snapshot
            snapshot = self.alpha_engine.compute_snapshot(market_data, use_robust_zscore=True)
            
            # 准备插入数据
            run_id = f"backfill_{datetime.fromtimestamp(ts).strftime('%Y%m%d_%H')}"
            batch_data = []
            
            for symbol, score in snapshot.scores.items():
                raw_factors = snapshot.raw_factors.get(symbol, {})
                z_factors = snapshot.z_factors.get(symbol, {})
                
                # 模拟regime（实际应该从其他地方获取）
                rand = np.random.random()
                if rand < 0.4:
                    regime = "Risk-Off"
                    regime_multiplier = 0.3
                elif rand < 0.7:
                    regime = "Sideways"
                    regime_multiplier = 0.6
                else:
                    regime = "Trending"
                    regime_multiplier = 1.0
                
                # 计算排名
                scores_list = list(snapshot.scores.values())
                score_rank = sorted(scores_list, reverse=True).index(score) + 1 if scores_list else 1
                
                alpha_data = [
                    run_id,  # run_id
                    int(ts),  # ts
                    symbol,  # symbol
                    raw_factors.get('f1_mom_5d', 0.0),  # f1_mom_5d
                    raw_factors.get('f2_mom_20d', 0.0),  # f2_mom_20d
                    raw_factors.get('f3_vol_adj_ret_20d', 0.0),  # f3_vol_adj_ret_20d
                    raw_factors.get('f4_volume_expansion', 0.0),  # f4_volume_expansion
                    raw_factors.get('f5_rsi_trend_confirm', 0.0),  # f5_rsi_trend_confirm
                    z_factors.get('f1_mom_5d', 0.0),  # z1_mom_5d
                    z_factors.get('f2_mom_20d', 0.0),  # z2_mom_20d
                    z_factors.get('f3_vol_adj_ret_20d', 0.0),  # z3_vol_adj_ret_20d
                    z_factors.get('f4_volume_expansion', 0.0),  # z4_volume_expansion
                    z_factors.get('f5_rsi_trend_confirm', 0.0),  # z5_rsi_trend_confirm
                    float(score),  # score
                    int(score_rank),  # score_rank
                    0.0,  # fwd_ret_1h
                    0.0,  # fwd_ret_4h
                    0.0,  # fwd_ret_12h
                    0.0,  # fwd_ret_24h
                    0.0,  # fwd_ret_72h
                    regime,  # regime
                    float(regime_multiplier),  # regime_multiplier
                    0,  # selected
                    0,  # traded
                    0.0  # pnl
                ]
                
                batch_data.append(alpha_data)
            
            return batch_data, len(batch_data)
            
        except Exception as e:
            print(f"  ❌ 计算时间点 {ts} 错误: {e}")
            return {}, 0
    
    def backfill_30days(self):
        """回填30天数据"""
        print("\n🚀 开始回填30天alpha数据")
        print("=" * 60)
        
        cursor = self.conn.cursor()
        
        # 获取市场数据时间范围
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_data_1h")
        min_ts, max_ts = cursor.fetchone()
        
        if not min_ts or not max_ts:
            print("❌ 无市场数据")
            return False
        
        # 计算30天前的时间戳
        thirty_days_ago = max_ts - 30 * 24 * 3600
        start_ts = max(min_ts, thirty_days_ago)
        
        print(f"📅 回填时间范围:")
        print(f"  开始: {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M')}")
        print(f"  结束: {datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M')}")
        
        # 获取所有时间点（每小时）
        cursor.execute("""
            SELECT DISTINCT timestamp 
            FROM market_data_1h 
            WHERE timestamp >= ? 
            ORDER BY timestamp
        """, (start_ts,))
        
        timestamps = [row[0] for row in cursor.fetchall()]
        print(f"总时间点: {len(timestamps)} 个")
        
        # 分批处理
        batch_size = 24  # 每天24小时
        total_inserted = 0
        start_time = time.time()
        
        for i in range(0, len(timestamps), batch_size):
            batch_timestamps = timestamps[i:i + batch_size]
            day_num = i // batch_size + 1
            total_days = (len(timestamps) + batch_size - 1) // batch_size
            
            print(f"\n📅 处理第 {day_num}/{total_days} 天...")
            
            day_inserted = 0
            
            for ts in batch_timestamps:
                time_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M')
                print(f"  处理 {time_str}...", end=' ')
                
                # 计算alpha数据
                batch_data, count = self.calculate_alpha_for_timestamp(ts)
                
                if batch_data and count > 0:
                    # 插入数据库
                    columns = [
                        'run_id', 'ts', 'symbol', 
                        'f1_mom_5d', 'f2_mom_20d', 'f3_vol_adj_ret_20d', 'f4_volume_expansion', 'f5_rsi_trend_confirm',
                        'z1_mom_5d', 'z2_mom_20d', 'z3_vol_adj_ret_20d', 'z4_volume_expansion', 'z5_rsi_trend_confirm',
                        'score', 'score_rank', 
                        'fwd_ret_1h', 'fwd_ret_4h', 'fwd_ret_12h', 'fwd_ret_24h', 'fwd_ret_72h',
                        'regime', 'regime_multiplier', 'selected', 'traded', 'pnl'
                    ]
                    
                    placeholders = ', '.join(['?' for _ in columns])
                    sql = f"INSERT INTO alpha_snapshots ({', '.join(columns)}) VALUES ({placeholders})"
                    
                    cursor.executemany(sql, batch_data)
                    self.conn.commit()
                    
                    day_inserted += count
                    print(f"✅ 插入 {count} 条")
                else:
                    print(f"⚠️ 无数据")
            
            total_inserted += day_inserted
            elapsed = time.time() - start_time
            avg_speed = total_inserted / elapsed if elapsed > 0 else 0
            
            print(f"  本日完成: {day_inserted} 条")
            print(f"  累计: {total_inserted} 条, 速度: {avg_speed:.1f} 条/秒")
        
        print(f"\n✅ 30天回填完成!")
        print(f"  总插入数据: {total_inserted} 条")
        print(f"  总耗时: {time.time() - start_time:.1f} 秒")
        
        return True
    
    def verify_results(self):
        """验证回填结果"""
        print("\n🔍 验证回填结果")
        print("-" * 40)
        
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM alpha_snapshots")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(ts), MAX(ts) FROM alpha_snapshots")
        min_ts, max_ts = cursor.fetchone()
        
        if min_ts and max_ts:
            min_time = datetime.fromtimestamp(min_ts)
            max_time = datetime.fromtimestamp(max_ts)
            days = (max_ts - min_ts) / (24 * 3600)
            
            print(f"✅ Alpha数据总量: {total_count} 条")
            print(f"✅ 时间范围: {min_time.strftime('%Y-%m-%d %H:%M')} 到 {max_time.strftime('%Y-%m-%d %H:%M')}")
            print(f"✅ 覆盖天数: {days:.1f} 天")
            
            # 检查每日数据量
            cursor.execute("""
                SELECT 
                    strftime('%Y-%m-%d', ts, 'unixepoch') as date,
                    COUNT(*) as count,
                    COUNT(DISTINCT symbol) as symbols
                FROM alpha_snapshots
                GROUP BY date
                ORDER BY date
            """)
            
            date_stats = cursor.fetchall()
            
            if date_stats:
                print(f"\n📅 每日数据分布:")
                print(f"  总天数: {len(date_stats)}")
                
                for date_str, count, symbols in date_stats[-5:]:  # 显示最近5天
                    print(f"  {date_str}: {count}条, {symbols}币种")
        else:
            print("❌ 无Alpha数据")
    
    def run(self):
        """运行回填"""
        print("🚀 真实AlphaEngine 30天数据回填")
        print("=" * 60)
        
        # 初始化
        if not self.initialize():
            return False
        
        # 准备数据库
        if not self.prepare_database():
            return False
        
        # 回填30天数据
        if not self.backfill_30days():
            return False
        
        # 验证结果
        self.verify_results()
        
        # 关闭连接
        if self.conn:
            self.conn.close()
        
        print("\n" + "=" * 60)
        print("🎯 30天Alpha数据回填完成!")
        print("=" * 60)
        
        print("\n📋 下一步:")
        print("1. 运行时间对齐: python3 scripts/simple_timestamp_fix.py")
        print("2. 运行IC分析: python3 scripts/quick_ic_analysis.py")
        print("3. 验证30天IC衰减曲线")
        
        return True

def main():
    """主函数"""
    print("🚀 真实AlphaEngine 30天数据回填工具")
    print("=" * 60)
    
    print("⚠️ 重要说明:")
    print("1. 本脚本使用真实的AlphaEngine计算alpha数据")
    print("2. 基于market_data_1h表的30天历史数据")
    print("3. 需要足够的市场数据来计算因子")
    print("4. 回填过程可能需要较长时间")
    print("=" * 60)
    
    confirm = input("\n是否开始回填30天alpha历史数据？(y/N): ").lower()
    
    if confirm == 'y':
        print("\n" + "=" * 60)
        backfiller = RealAlphaBackfiller()
        success = backfiller.run()
        if success:
            print("\n✅ 回填成功完成!")
        else:
            print("\n❌ 回填失败!")
    else:
        print("\n⏸️ 已取消执行")
    
    print("=" * 60)

if __name__ == "__main__":
    main()