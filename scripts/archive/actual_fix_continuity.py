#!/usr/bin/env python3
"""
实际时间连续性修复 - 针对最严重的问题
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def fix_worst_continuity_issues():
    """修复最严重的时间连续性问题"""
    print("🔧 修复最严重的时间连续性问题")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 1. 找出最严重的币种（缺失率最高的）
    cursor.execute("""
        SELECT 
            symbol,
            COUNT(*) as record_count
        FROM market_data_1h
        GROUP BY symbol
        ORDER BY record_count
        LIMIT 3
    """)
    
    worst_symbols = cursor.fetchall()
    
    if not worst_symbols:
        print("✅ 无严重连续性问题")
        return 0
    
    print(f"📊 最需要修复的币种:")
    for symbol, count in worst_symbols:
        print(f"  {symbol}: {count}条记录")
    
    total_fixed = 0
    
    # 2. 修复每个严重币种
    for symbol, current_count in worst_symbols:
        print(f"\n🔄 修复 {symbol}...")
        
        # 获取该币种的所有数据
        df = pd.read_sql_query(f"""
            SELECT timestamp, open, high, low, close, volume
            FROM market_data_1h
            WHERE symbol = '{symbol}'
            ORDER BY timestamp
        """, conn)
        
        if len(df) < 2:
            print(f"  ⚠️ 数据不足，跳过")
            continue
        
        # 找出时间缺口
        df['time_gap'] = df['timestamp'].diff()
        large_gaps = df[df['time_gap'] > 3600]
        
        if len(large_gaps) == 0:
            print(f"  ✅ 无大时间缺口")
            continue
        
        print(f"  发现 {len(large_gaps)} 个时间缺口")
        
        # 修复每个缺口
        fixed_in_symbol = 0
        
        for idx, row in large_gaps.iterrows():
            gap_start = df.loc[idx-1, 'timestamp'] if idx > 0 else None
            gap_end = row['timestamp']
            gap_size = row['time_gap']
            
            if gap_start and gap_end and gap_size > 3600:
                # 计算需要插入的时间点数量
                missing_hours = int(gap_size / 3600) - 1
                
                if missing_hours > 0 and missing_hours <= 10:  # 只修复小缺口
                    print(f"    缺口: {gap_size/3600:.1f}小时, 需要插入{missing_hours}个数据点")
                    
                    # 获取缺口前后的数据用于插值
                    before_data = df[df['timestamp'] == gap_start].iloc[0]
                    after_data = row
                    
                    # 线性插值
                    for i in range(1, missing_hours + 1):
                        ratio = i / (missing_hours + 1)
                        new_timestamp = gap_start + i * 3600
                        
                        # 插值计算
                        new_open = before_data['open'] * (1 - ratio) + after_data['open'] * ratio
                        new_high = before_data['high'] * (1 - ratio) + after_data['high'] * ratio
                        new_low = before_data['low'] * (1 - ratio) + after_data['low'] * ratio
                        new_close = before_data['close'] * (1 - ratio) + after_data['close'] * ratio
                        new_volume = (before_data['volume'] + after_data['volume']) / 2
                        
                        # 检查是否已存在
                        cursor.execute("""
                            SELECT COUNT(*) FROM market_data_1h 
                            WHERE symbol = ? AND timestamp = ?
                        """, (symbol, int(new_timestamp)))
                        
                        if cursor.fetchone()[0] == 0:
                            cursor.execute("""
                                INSERT INTO market_data_1h 
                                (symbol, timestamp, open, high, low, close, volume, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                symbol,
                                int(new_timestamp),
                                float(new_open),
                                float(new_high),
                                float(new_low),
                                float(new_close),
                                float(new_volume),
                                int(datetime.now().timestamp())
                            ))
                            fixed_in_symbol += 1
        
        conn.commit()
        
        if fixed_in_symbol > 0:
            print(f"  ✅ 修复 {fixed_in_symbol} 个数据点")
            total_fixed += fixed_in_symbol
        else:
            print(f"  ⚠️ 未修复任何数据点")
    
    # 3. 验证修复效果
    print(f"\n📊 修复后验证:")
    for symbol, _ in worst_symbols:
        cursor.execute("SELECT COUNT(*) FROM market_data_1h WHERE symbol = ?", (symbol,))
        new_count = cursor.fetchone()[0]
        print(f"  {symbol}: {new_count}条记录")
    
    conn.close()
    
    print(f"\n🎯 总共修复 {total_fixed} 个缺失数据点")
    return total_fixed

def fix_systematic_gaps():
    """修复系统性时间缺口"""
    print("\n🔧 修复系统性时间缺口")
    print("=" * 50)
    
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    
    # 找出系统性缺口（多个币种在同一时间点缺失）
    cursor.execute("""
        WITH all_times AS (
            SELECT DISTINCT timestamp
            FROM market_data_1h
        ),
        symbol_times AS (
            SELECT DISTINCT symbol, timestamp
            FROM market_data_1h
        ),
        missing_analysis AS (
            SELECT 
                at.timestamp,
                COUNT(DISTINCT st.symbol) as existing_count,
                (SELECT COUNT(DISTINCT symbol) FROM market_data_1h) as total_symbols
            FROM all_times at
            LEFT JOIN symbol_times st ON at.timestamp = st.timestamp
            GROUP BY at.timestamp
            HAVING existing_count < total_symbols
        )
        SELECT 
            timestamp,
            strftime('%Y-%m-%d %H:%M:%S', timestamp, 'unixepoch') as time_str,
            existing_count,
            total_symbols,
            total_symbols - existing_count as missing_count
        FROM missing_analysis
        WHERE missing_count >= 10  -- 10个以上币种缺失
        ORDER BY missing_count DESC
        LIMIT 5
    """)
    
    systematic_gaps = cursor.fetchall()
    
    if not systematic_gaps:
        print("✅ 无严重系统性缺口")
        return 0
    
    print(f"发现 {len(systematic_gaps)} 个严重系统性缺口:")
    for ts, time_str, existing, total, missing in systematic_gaps:
        print(f"  {time_str}: {missing}/{total}币种缺失 ({missing/total*100:.1f}%)")
    
    # 对于系统性缺口，标记而不是填充（因为可能是市场休市）
    print(f"\n💡 系统性缺口处理策略:")
    print("  1. 如果是市场休市，保持原样")
    print("  2. 如果是数据收集问题，需要优化收集逻辑")
    print("  3. 在策略回测中需要处理这些缺口")
    
    # 创建缺口记录表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_gaps (
            timestamp INTEGER,
            gap_type TEXT,
            affected_symbols TEXT,
            reason TEXT,
            created_at INTEGER
        )
    """)
    
    # 记录系统性缺口
    recorded = 0
    for ts, time_str, existing, total, missing in systematic_gaps:
        # 获取受影响的币种
        cursor.execute("""
            SELECT DISTINCT symbol FROM market_data_1h
            EXCEPT
            SELECT symbol FROM market_data_1h WHERE timestamp = ?
        """, (ts,))
        
        affected = [row[0] for row in cursor.fetchall()]
        affected_str = ",".join(affected[:5]) + ("..." if len(affected) > 5 else "")
        
        cursor.execute("""
            INSERT INTO data_gaps 
            (timestamp, gap_type, affected_symbols, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            ts,
            "systematic",
            affected_str,
            "multiple symbols missing at same time - possible market halt or API issue",
            int(datetime.now().timestamp())
        ))
        recorded += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n📝 已记录 {recorded} 个系统性缺口到 data_gaps 表")
    return recorded

def improve_data_collection_logic():
    """改进数据收集逻辑"""
    print("\n🔄 改进数据收集逻辑建议")
    print("=" * 50)
    
    improvements = [
        "1. ⏰ 时间点对齐: 确保所有币种在整点收集数据",
        "2. 🔄 重试机制: 对失败的API请求自动重试",
        "3. 📊 完整性检查: 收集后立即检查数据完整性",
        "4. 🚨 告警机制: 发现系统性缺口时发送告警",
        "5. 💾 数据备份: 定期备份原始数据",
        "6. 🔍 质量监控: 每日运行数据质量检查",
        "7. ⚡ 性能优化: 并行收集多个币种数据",
        "8. 📈 趋势分析: 分析缺口模式优化收集时间"
    ]
    
    print("建议的改进措施:")
    for improvement in improvements:
        print(f"  {improvement}")
    
    # 创建改进脚本模板（写入到文件，不在本脚本内执行）
    improvement_script = r'''#!/usr/bin/env python3
"""
改进的数据收集脚本模板
"""

import time
import requests
import sqlite3
from datetime import datetime
from typing import List, Dict
import concurrent.futures

class ImprovedDataCollector:
    """改进的数据收集器"""
    
    def __init__(self, db_path: str = "reports/alpha_history.db"):
        self.db_path = db_path
        self.max_retries = 3
        self.retry_delay = 5  # 秒
        
    def collect_with_retry(self, symbol: str, timestamp: int) -> Dict:
        """带重试的数据收集"""
        for attempt in range(self.max_retries):
            try:
                # 这里实现实际的数据收集逻辑
                data = self.fetch_market_data(symbol, timestamp)
                
                if data and self.validate_data(data):
                    return data
                else:
                    print(f"  {symbol}: 第{attempt+1}次尝试数据无效")
                    
            except Exception as e:
                print(f"  {symbol}: 第{attempt+1}次尝试失败: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        return None
    
    def fetch_market_data(self, symbol: str, timestamp: int) -> Dict:
        """获取市场数据"""
        # 实现具体的API调用
        # 返回格式: {'timestamp': ts, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v}
        pass
    
    def validate_data(self, data: Dict) -> bool:
        """验证数据有效性"""
        required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        # 检查必需字段
        for field in required_fields:
            if field not in data:
                return False
        
        # 检查数据合理性
        if data['open'] <= 0 or data['high'] <= 0 or data['low'] <= 0 or data['close'] <= 0:
            return False
        
        if data['high'] < data['low']:
            return False
        
        if data['close'] < data['low'] or data['close'] > data['high']:
            return False
        
        return True
    
    def check_completeness(self, symbols: List[str], target_timestamp: int) -> Dict:
        """检查数据完整性"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        missing_symbols = []
        
        for symbol in symbols:
            cursor.execute("""
                SELECT COUNT(*) FROM market_data_1h 
                WHERE symbol = ? AND timestamp = ?
            """, (symbol, target_timestamp))
            
            if cursor.fetchone()[0] == 0:
                missing_symbols.append(symbol)
        
        conn.close()
        
        return {
            'timestamp': target_timestamp,
            'total_symbols': len(symbols),
            'missing_count': len(missing_symbols),
            'missing_symbols': missing_symbols,
            'completeness_pct': (len(symbols) - len(missing_symbols)) / len(symbols) * 100
        }
    
    def run_collection(self, symbols: List[str]):
        """运行数据收集"""
        print(f"开始收集 {len(symbols)} 个币种的数据...")
        
        target_timestamp = int(time.time()) // 3600 * 3600  # 当前整点
        
        # 并行收集
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.collect_with_retry, symbol, target_timestamp): symbol
                for symbol in symbols
            }
            
            results = []
            for future in concurrent.futures.as_completed(futures):
                symbol = futures[future]
                try:
                    data = future.result()
                    if data:
                        results.append(data)
                        print(f"  ✅ {symbol}: 收集成功")
                    else:
                        print(f"  ❌ {symbol}: 收集失败")
                except Exception as e:
                    print(f"  ❌ {symbol}: 异常 {e}")
        
        # 检查完整性
        completeness = self.check_completeness(symbols, target_timestamp)
        
        print(f"\\n📊 收集完成:")
        print(f"  成功: {len(results)}/{len(symbols)}")
        print(f"  完整性: {completeness['completeness_pct']:.1f}%")
        
        if completeness['missing_count'] > 0:
            print(f"  缺失币种: {', '.join(completeness['missing_symbols'][:5])}")
            if len(completeness['missing_symbols']) > 5:
                print(f"    ... 还有 {len(completeness['missing_symbols']) - 5} 个")
        
        return results, completeness

def main():
    """主函数"""
    collector = ImprovedDataCollector()
    
    # 从数据库获取所有币种
    conn = sqlite3.connect("reports/alpha_history.db")
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    print(f"🚀 改进的数据收集系统")
    print(f"币种数量: {len(symbols)}")
    
    # 运行收集
    results, completeness = collector.run_collection(symbols[:10])  # 先测试10个
    
    # 根据完整性决定是否继续
    if completeness['completeness_pct'] < 90:
        print(f"\\n⚠️ 完整性不足，建议检查网络或API状态")

if __name__ == "__main__":
    main()
'''
    
    with open("scripts/improved_collector_template.py", "w") as f:
        f.write(improvement_script)
    
    print(f"\n📄 改进模板已创建: scripts/improved_collector_template.py")
    print("💡 基于此模板优化现有的 auto_data_collector.py")

def main():
    print("🚀 实际一致性和连续性修复")
    print("=" * 60)
    
    # 1. 修复最严重的连续性问题
    fixed_points = fix_worst_continuity_issues()
    
    # 2. 处理系统性缺口
    recorded_gaps = fix_systematic_gaps()
    
    # 3. 改进数据收集逻辑
    improve_data_collection_logic()
    
    print("\n" + "=" * 60)
    print("✅ 修复完成!")
    print("=" * 60)
    
    print(f"\n📊 修复成果:")
    print(f"1. 🔧 数据点修复: {fixed_points} 个缺失数据点")
    print(f"2. 📝 缺口记录: {recorded_gaps} 个系统性缺口已记录")
    print(f"3. 🚀 改进建议: 数据收集逻辑优化方案")
    
    print(f"\n💡 下一步:")
    print(f"1. 运行数据质量检查验证修复效果")
    print(f"2. 基于模板优化 auto_data_collector.py")
    print(f"3. 将系统性缺口考虑进策略回测")
    print(f"4. 建立持续的数据质量监控")
    print("=" * 60)

if __name__ == "__main__":
    main()