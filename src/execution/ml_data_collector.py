"""
ML训练数据收集系统
用于收集特征数据和标签数据，训练MLFactorModel
"""

import sqlite3
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

# 设置日志
logger = logging.getLogger(__name__)


class MLDataCollectorError(Exception):
    """ML数据收集器自定义异常"""
    pass


class FeatureCalculationError(MLDataCollectorError):
    """特征计算错误"""
    pass


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

    # 类级别的连接池
    _connection_pool = {}

    def __init__(self, db_path: str = "reports/ml_training_data.db"):
        self.db_path = db_path
        self._init_database()
        self._conn = None  # 实例连接缓存

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接（使用连接池）"""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _close_connection(self) -> None:
        """关闭数据库连接"""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as e:
                logger.warning(f"[ML] Warning: error closing connection: {e}")
            finally:
                self._conn = None

    def __enter__(self) -> 'MLDataCollector':
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[Any]) -> bool:
        """上下文管理器出口，确保关闭连接"""
        self._close_connection()
        return False

    def _init_database(self) -> None:
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
            # 验证输入数据
            if not market_data or 'close' not in market_data:
                raise FeatureCalculationError(f"Invalid market_data for {symbol}: missing 'close'")

            if len(market_data['close']) < 2:
                raise FeatureCalculationError(f"Insufficient data for {symbol}: need at least 2 bars")

            # 计算特征
            features = self._calculate_features(market_data)

            # 验证特征有效性
            for key, value in features.items():
                if pd.isna(value) or np.isinf(value):
                    raise FeatureCalculationError(f"Invalid feature {key}={value} for {symbol}")

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

        except FeatureCalculationError as e:
            # 可恢复错误，记录警告
            logger.warning(f"[ML Warning] Feature calculation failed for {symbol}: {e}")
            return False
        except MLDataCollectorError as e:
            # 数据库错误，可能需要重试
            logger.error(f"[ML Error] Database error for {symbol}: {e}")
            return False
        except Exception as e:
            # 意外错误，记录详细堆栈
            logger.exception(f"[ML Critical] Unexpected error collecting features for {symbol}: {e}")
            return False

    def _calculate_features(self, data: Dict) -> Dict[str, float]:
        """
        计算特征
        
        使用 src/utils/features.py 中的公共函数避免代码重复
        """
        from src.utils.features import calculate_all_features
        
        close = pd.Series(data['close'])
        volume = pd.Series(data.get('volume', [0] * len(close)))
        high = pd.Series(data.get('high', close))
        low = pd.Series(data.get('low', close))
        
        return calculate_all_features(close, volume, high, low)

    def _save_record(self, record: FeatureRecord) -> None:
        """保存记录到数据库（使用连接池）"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
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
        except sqlite3.Error as e:
            conn.rollback()
            raise MLDataCollectorError(f"Database error saving record: {e}") from e

    def fill_labels(self, current_timestamp: int) -> int:
        """
        回填6小时后的标签（未来收益率）

        回填所有在current_timestamp之前6小时以上的未标记记录
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 找到所有6小时前以上且未回填标签的记录
            six_hours_ago = current_timestamp - 6 * 3600 * 1000  # 6小时前的毫秒时间戳

            cursor.execute('''
                SELECT id, timestamp, symbol FROM feature_snapshots
                WHERE label_filled = 0
                AND timestamp <= ?
                ORDER BY timestamp
                LIMIT 1000  -- 每次最多处理1000条
            ''', (six_hours_ago,))

            records_to_fill = cursor.fetchall()

            filled_count = 0
            failed_count = 0
            for record_id, record_ts, symbol in records_to_fill:
                try:
                    # 计算未来6小时收益率
                    future_return = self._calculate_future_return(symbol, record_ts, 6)

                    if future_return is not None:
                        cursor.execute('''
                            UPDATE feature_snapshots
                            SET future_return_6h = ?, label_filled = 1
                            WHERE id = ?
                        ''', (future_return, record_id))
                        filled_count += 1
                    else:
                        # 如果无法计算收益，标记为-1表示失败
                        cursor.execute('''
                            UPDATE feature_snapshots
                            SET label_filled = -1
                            WHERE id = ?
                        ''', (record_id,))
                        failed_count += 1
                except Exception as e:
                    logger.error(f"[ML] Error processing record {record_id}: {e}")
                    failed_count += 1

            conn.commit()

            if filled_count > 0 or failed_count > 0:
                logger.info(f"[ML] 回填完成: {filled_count}条成功, {failed_count}条失败")

            return filled_count

        except sqlite3.Error as e:
            conn.rollback()
            raise MLDataCollectorError(f"Database error filling labels: {e}") from e

    def _calculate_future_return(
        self,
        symbol: str,
        start_timestamp: int,
        hours: int
    ) -> Optional[float]:
        """
        计算未来收益率
        从K线缓存文件中获取价格数据

        重要：确保不使用未来数据！只使用start_timestamp时刻已经存在的数据
        """
        try:
            # 转换symbol格式: BTC/USDT -> BTC_USDT
            symbol_file = symbol.replace('/', '_').replace('-', '_')

            # 查找K线缓存文件
            cache_dir = Path(self.db_path).parent.parent / 'data' / 'cache'
            pattern = f"{symbol_file}_1H_*.csv"
            cache_files = list(cache_dir.glob(pattern))

            if not cache_files:
                return None

            # 关键修复：筛选缓存文件，只使用不包含未来数据的文件
            # 文件命名格式: BTC_USDT_1H_YYYYMMDDHHMMSS.csv
            # 只使用数据截止时间 <= start_timestamp + hours 的文件
            end_timestamp = start_timestamp + hours * 3600 * 1000

            valid_files = []
            for f in cache_files:
                # 从文件名解析数据截止时间
                try:
                    ts_str = f.stem.split('_')[-1]  # 获取YYYYMMDDHHMMSS部分
                    file_end_ts = pd.to_datetime(ts_str, format='%Y%m%d%H%M%S').timestamp() * 1000
                    # 只使用数据截止时间 >= 需要的结束时间的文件
                    if file_end_ts >= end_timestamp:
                        valid_files.append(f)
                except:
                    # 如果解析失败，使用文件修改时间作为fallback
                    file_mtime = f.stat().st_mtime * 1000
                    if file_mtime >= end_timestamp:
                        valid_files.append(f)

            if not valid_files:
                # 如果没有包含足够数据的文件，使用最新的（但有泄露风险警告）
                latest_file = max(cache_files, key=lambda x: x.stat().st_mtime)
                logger.warning(f"[ML Warning] 可能使用不完整数据计算 {symbol} 的未来收益")
            else:
                # 使用包含足够数据的最早文件（最接近start_timestamp的文件）
                latest_file = min(valid_files, key=lambda x: x.stat().st_mtime)

            df = pd.read_csv(latest_file)

            if df.empty or 'close' not in df.columns:
                return None

            # 转换timestamp到datetime
            start_dt = pd.to_datetime(start_timestamp, unit='ms')
            end_dt = start_dt + timedelta(hours=hours)

            # 查找最接近的K线
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # 关键验证：确保数据不包含未来时间
            max_data_time = df['timestamp'].max()
            if max_data_time < end_dt:
                logger.warning(f"[ML Warning] 缓存数据不足: 需要{end_dt}, 实际{max_data_time}")
                return None

            # 获取起始价格（最接近start_timestamp的K线收盘价）
            start_mask = df['timestamp'] <= start_dt
            if not start_mask.any():
                return None
            start_price = df.loc[start_mask, 'close'].iloc[-1]

            # 获取结束价格（最接近end_timestamp的K线收盘价）
            end_mask = df['timestamp'] <= end_dt
            if not end_mask.any():
                return None
            end_price = df.loc[end_mask, 'close'].iloc[-1]

            # 计算收益率
            future_return = (end_price - start_price) / start_price

            return float(future_return)

        except Exception as e:
            print(f"[ML] Error calculating future return for {symbol}: {e}")
            return None

    def export_training_data(
        self,
        output_path: str = "reports/ml_training_data.csv",
        min_samples: int = 100
    ) -> bool:
        """
        导出训练数据到CSV
        """
        conn = self._get_connection()

        try:
            # 明确指定需要的列，排除id（会导致泄露）和label_filled/created_at（非特征）
            query = '''
                SELECT
                    timestamp, symbol,
                    returns_1h, returns_6h, returns_24h,
                    momentum_5d, momentum_20d,
                    volatility_6h, volatility_24h, volatility_ratio,
                    volume_ratio, obv,
                    rsi, macd, macd_signal,
                    bb_position, price_position,
                    regime,
                    future_return_6h
                FROM feature_snapshots
                WHERE label_filled = 1
                AND future_return_6h IS NOT NULL
                ORDER BY timestamp
            '''

            df = pd.read_sql_query(query, conn)

            if len(df) < min_samples:
                logger.warning(f"Insufficient samples: {len(df)} < {min_samples}")
                return False

            # 保存到CSV
            df.to_csv(output_path, index=False)
            logger.info(f"Exported {len(df)} samples to {output_path}")

            # 打印统计
            logger.info(f"\nTraining Data Statistics:")
            logger.info(f"  Total samples: {len(df)}")
            logger.info(f"  Symbols: {df['symbol'].nunique()}")
            logger.info(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            logger.info(f"  Avg future return: {df['future_return_6h'].mean():.4f}")
            logger.info(f"  Return std: {df['future_return_6h'].std():.4f}")

            return True

        except sqlite3.Error as e:
            raise MLDataCollectorError(f"Database error exporting data: {e}") from e

    def get_statistics(self) -> Dict:
        """获取数据收集统计"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
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

            return {
                'total_records': total_records,
                'labeled_records': labeled_records,
                'unlabeled_records': total_records - labeled_records,
                'num_symbols': num_symbols,
                'time_range': (min_ts, max_ts) if min_ts else None,
                'ready_for_training': labeled_records >= 100
            }

        except sqlite3.Error as e:
            raise MLDataCollectorError(f"Database error getting statistics: {e}") from e

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
    logger.info(f"Filled {filled_count} labels")

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
