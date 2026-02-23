"""
ML训练数据收集系统
用于收集特征数据和标签数据，训练MLFactorModel
"""

import sqlite3
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

@dataclass
class FeatureRecord:
    """特征记录"""
    timestamp: int
    symbol: str
    
    # 价格特征
    returns_1h: float
    returns_6h: float
    returns_24h: float
    
    # 动量特征
    momentum_5d: float
    momentum_20d: float
    
    # 波动率特征
    volatility_6h: float
    volatility_24h: float
    volatility_ratio: float
    
    # 成交量特征
    volume_ratio: float
    obv: float
    
    # 技术指标
    rsi: float
    macd: float
    macd_signal: float
    bb_position: float
    price_position: float
    
    # 市场状态
    regime: str
    
    # 标签（未来收益率）
    future_return_6h: Optional[float] = None
    
    def to_dict(self) -> dict:
        return asdict(self)

class MLDataCollector:
    """
    ML训练数据收集器
    
    收集策略：
    1. 每小时记录一次特征快照（所有币种的当前状态）
    2. 6小时后回填标签（未来6小时收益率）
    3. 存储到SQLite数据库
    4. 定期导出为CSV供模型训练
    """
    
    def __init__(self, db_path: str = "reports/ml_training_data.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                symbol TEXT,
                returns_1h REAL,
                returns_6h REAL,
                returns_24h REAL,
                momentum_5d REAL,
                momentum_20d REAL,
                volatility_6h REAL,
                volatility_24h REAL,
                volatility_ratio REAL,
                volume_ratio REAL,
                obv REAL,
                rsi REAL,
                macd REAL,
                macd_signal REAL,
                bb_position REAL,
                price_position REAL,
                regime TEXT,
                future_return_6h REAL,
                label_filled INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp ON feature_snapshots(timestamp)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_symbol ON feature_snapshots(symbol)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_label_filled ON feature_snapshots(label_filled)
        ''')
        
        conn.commit()
        conn.close()
    
    def collect_features(
        self,
        timestamp: int,
        symbol: str,
        market_data: Dict,
        regime: str
    ) -> bool:
        """
        收集特征快照
        
        Args:
            timestamp: 当前时间戳（毫秒）
            symbol: 币种符号
            market_data: 市场数据 {close, high, low, volume}
            regime: 当前市场状态
        
        Returns:
            是否成功
        """
        try:
            # 计算特征
            features = self._calculate_features(market_data)
            
            record = FeatureRecord(
                timestamp=timestamp,
                symbol=symbol,
                regime=regime,
                future_return_6h=None,  # 稍后回填
                **features
            )
            
            # 保存到数据库
            self._save_record(record)
            return True
            
        except Exception as e:
            print(f"Error collecting features for {symbol}: {e}")
            return False
    
    def _calculate_features(self, data: Dict) -> Dict:
        """计算特征"""
        close = pd.Series(data['close'])
        volume = pd.Series(data.get('volume', [0] * len(close)))
        high = pd.Series(data.get('high', close))
        low = pd.Series(data.get('low', close))
        
        # 收益率
        returns_1h = close.pct_change(1).iloc[-1] if len(close) > 1 else 0
        returns_6h = close.pct_change(6).iloc[-1] if len(close) > 6 else 0
        returns_24h = close.pct_change(24).iloc[-1] if len(close) > 24 else 0
        
        # 动量
        momentum_5d = (close.iloc[-1] - close.shift(5*24).iloc[-1]) / close.shift(5*24).iloc[-1] if len(close) > 5*24 else 0
        momentum_20d = (close.iloc[-1] - close.shift(20*24).iloc[-1]) / close.shift(20*24).iloc[-1] if len(close) > 20*24 else 0
        
        # 波动率
        returns_series = close.pct_change()
        volatility_6h = returns_series.rolling(6).std().iloc[-1] if len(close) > 6 else 0
        volatility_24h = returns_series.rolling(24).std().iloc[-1] if len(close) > 24 else 0
        volatility_ratio = volatility_6h / volatility_24h if volatility_24h > 0 else 1
        
        # 成交量
        volume_sma = volume.rolling(24).mean()
        volume_ratio = volume.iloc[-1] / volume_sma.iloc[-1] if volume_sma.iloc[-1] > 0 else 1
        obv = (np.sign(returns_series) * volume).cumsum().iloc[-1]
        
        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1] if not rs.iloc[-1] != rs.iloc[-1] else 50
        
        # MACD
        exp1 = close.ewm(span=12).mean()
        exp2 = close.ewm(span=26).mean()
        macd = (exp1 - exp2).iloc[-1]
        macd_signal = (exp1 - exp2).ewm(span=9).mean().iloc[-1]
        
        # 布林带位置
        bb_middle = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_position = ((close.iloc[-1] - bb_middle.iloc[-1]) / (2 * bb_std.iloc[-1])) if bb_std.iloc[-1] > 0 else 0
        
        # 价格位置
        high_20d = high.rolling(20*24).max()
        low_20d = low.rolling(20*24).min()
        price_position = ((close.iloc[-1] - low_20d.iloc[-1]) / (high_20d.iloc[-1] - low_20d.iloc[-1])) if (high_20d.iloc[-1] - low_20d.iloc[-1]) > 0 else 0.5
        
        return {
            'returns_1h': returns_1h,
            'returns_6h': returns_6h,
            'returns_24h': returns_24h,
            'momentum_5d': momentum_5d,
            'momentum_20d': momentum_20d,
            'volatility_6h': volatility_6h,
            'volatility_24h': volatility_24h,
            'volatility_ratio': volatility_ratio,
            'volume_ratio': volume_ratio,
            'obv': obv,
            'rsi': rsi,
            'macd': macd,
            'macd_signal': macd_signal,
            'bb_position': bb_position,
            'price_position': price_position,
        }
    
    def _save_record(self, record: FeatureRecord):
        """保存记录到数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO feature_snapshots (
                timestamp, symbol, returns_1h, returns_6h, returns_24h,
                momentum_5d, momentum_20d, volatility_6h, volatility_24h,
                volatility_ratio, volume_ratio, obv, rsi, macd, macd_signal,
                bb_position, price_position, regime, future_return_6h, label_filled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        ''', (
            record.timestamp, record.symbol, record.returns_1h, record.returns_6h,
            record.returns_24h, record.momentum_5d, record.momentum_20d,
            record.volatility_6h, record.volatility_24h, record.volatility_ratio,
            record.volume_ratio, record.obv, record.rsi, record.macd,
            record.macd_signal, record.bb_position, record.price_position,
            record.regime, record.future_return_6h
        ))
        
        conn.commit()
        conn.close()
    
    def fill_labels(self, current_timestamp: int):
        """
        回填6小时后的标签（未来收益率）
        
        在每个交易周期调用，回填6小时前的记录
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 找到6小时前(±30分钟)未回填标签的记录
        six_hours_ago = current_timestamp - 6 * 3600 * 1000  # 6小时前的毫秒时间戳
        tolerance = 30 * 60 * 1000  # 30分钟容差
        
        cursor.execute('''
            SELECT id, timestamp, symbol FROM feature_snapshots
            WHERE label_filled = 0
            AND timestamp <= ?
            AND timestamp >= ?
        ''', (six_hours_ago + tolerance, six_hours_ago - tolerance))
        
        records_to_fill = cursor.fetchall()
        
        filled_count = 0
        for record_id, record_ts, symbol in records_to_fill:
            # 计算未来6小时收益率
            future_return = self._calculate_future_return(symbol, record_ts, 6)
            
            if future_return is not None:
                cursor.execute('''
                    UPDATE feature_snapshots
                    SET future_return_6h = ?, label_filled = 1
                    WHERE id = ?
                ''', (future_return, record_id))
                filled_count += 1
        
        conn.commit()
        conn.close()
        
        return filled_count
    
    def _calculate_future_return(
        self,
        symbol: str,
        start_timestamp: int,
        hours: int
    ) -> Optional[float]:
        """
        计算未来收益率
        从orders表或价格数据中获取
        """
        try:
            # 尝试从已有订单数据计算
            conn = sqlite3.connect('/home/admin/clawd/v5-trading-bot/reports/orders.sqlite')
            
            # 查找start_timestamp之后hours小时内的成交记录
            end_timestamp = start_timestamp + hours * 3600 * 1000
            
            query = '''
                SELECT AVG(px) as avg_price
                FROM orders
                WHERE inst_id = ?
                AND state = 'FILLED'
                AND created_ts BETWEEN ? AND ?
            '''
            
            df = pd.read_sql_query(
                query, conn,
                params=(symbol.replace('/', '-'), start_timestamp, end_timestamp)
            )
            conn.close()
            
            if not df.empty and df['avg_price'].iloc[0] is not None:
                # 简化的未来收益计算
                # 实际应该获取start_timestamp时的价格
                # 这里返回一个占位值，实际实现需要价格数据
                return 0.0  # 占位
            
            return None
            
        except Exception as e:
            print(f"Error calculating future return: {e}")
            return None
    
    def export_training_data(
        self,
        output_path: str = "reports/ml_training_data.csv",
        min_samples: int = 100
    ) -> bool:
        """
        导出训练数据到CSV
        """
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT * FROM feature_snapshots
            WHERE label_filled = 1
            AND future_return_6h IS NOT NULL
            ORDER BY timestamp
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if len(df) < min_samples:
            print(f"Insufficient samples: {len(df)} < {min_samples}")
            return False
        
        # 保存到CSV
        df.to_csv(output_path, index=False)
        print(f"Exported {len(df)} samples to {output_path}")
        
        # 打印统计
        print(f"\nTraining Data Statistics:")
        print(f"  Total samples: {len(df)}")
        print(f"  Symbols: {df['symbol'].nunique()}")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"  Avg future return: {df['future_return_6h'].mean():.4f}")
        print(f"  Return std: {df['future_return_6h'].std():.4f}")
        
        return True
    
    def get_statistics(self) -> Dict:
        """获取数据收集统计"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 总记录数
        cursor.execute('SELECT COUNT(*) FROM feature_snapshots')
        total_records = cursor.fetchone()[0]
        
        # 已回填标签的记录数
        cursor.execute('SELECT COUNT(*) FROM feature_snapshots WHERE label_filled = 1')
        labeled_records = cursor.fetchone()[0]
        
        # 币种数量
        cursor.execute('SELECT COUNT(DISTINCT symbol) FROM feature_snapshots')
        num_symbols = cursor.fetchone()[0]
        
        # 时间范围
        cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM feature_snapshots')
        min_ts, max_ts = cursor.fetchone()
        
        conn.close()
        
        return {
            'total_records': total_records,
            'labeled_records': labeled_records,
            'unlabeled_records': total_records - labeled_records,
            'num_symbols': num_symbols,
            'time_range': (min_ts, max_ts) if min_ts else None,
            'ready_for_training': labeled_records >= 100
        }

# 集成到V5 Pipeline的用法
"""
# 在 main.py 或 pipeline.py 中使用

from src.execution.ml_data_collector import MLDataCollector

# 初始化收集器
data_collector = MLDataCollector()

# 在每个交易周期（如05:00）
def on_trading_cycle(timestamp, market_data, regime):
    # 1. 收集当前特征
    for symbol, data in market_data.items():
        data_collector.collect_features(
            timestamp=timestamp,
            symbol=symbol,
            market_data=data,
            regime=str(regime.state)
        )
    
    # 2. 回填6小时前的标签
    filled_count = data_collector.fill_labels(timestamp)
    print(f"Filled {filled_count} labels")

# 定期导出训练数据（每天一次）
def daily_export():
    stats = data_collector.get_statistics()
    if stats['ready_for_training']:
        data_collector.export_training_data()
        
        # 训练ML模型
        from src.execution.ml_factor_model import MLFactorModel
        ml_model = MLFactorModel()
        # ... 加载数据并训练 ...
"""
