#!/usr/bin/env python3
"""
V5 HMM市场状态检测器

使用隐马尔可夫模型自动识别市场状态：
- 0: TrendingUp (上涨趋势)
- 1: TrendingDown (下跌趋势)  
- 2: Ranging/Sideways (震荡)

特征: 收益率、波动率、动量、RSI
"""

import sys
import numpy as np
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')
from src.regime.hmm_model import SimpleGaussianHMM


class HMMRegimeDetector:
    """基于HMM的市场状态检测器"""
    
    def __init__(self, n_components: int = 3, model_path: Path = None):
        self.n_components = n_components
        self.model = SimpleGaussianHMM(n_components=n_components)
        self.model_path = model_path or Path('/home/admin/clawd/v5-trading-bot/models/hmm_regime.pkl')
        self.state_names = {0: 'TrendingUp', 1: 'TrendingDown', 2: 'Sideways'}
        
    def load_training_data(self, db_path: Path = None, symbol: str = 'BTC/USDT', 
                           lookback_days: int = 60) -> np.ndarray:
        """从数据库加载训练数据（使用alpha_snapshots因子数据）"""
        db_path = db_path or Path('/home/admin/clawd/v5-trading-bot/reports/alpha_history.db')
        
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        start_ts = int((datetime.now() - timedelta(days=lookback_days)).timestamp())
        
        # 从alpha_snapshots获取因子数据
        cursor.execute("""
            SELECT ts, f1_mom_5d, f2_mom_20d, f3_vol_adj_ret_20d, score
            FROM alpha_snapshots
            WHERE symbol = ? AND ts > ?
            ORDER BY ts
        """, (symbol, start_ts))
        
        rows = cursor.fetchall()
        conn.close()
        
        if len(rows) < 50:
            print(f"[HMM] 数据不足: 只有 {len(rows)} 条记录")
            return None
        
        # 用动量因子构造特征
        features = []
        for i, row in enumerate(rows):
            if i < 14:  # 跳过前14个，确保RSI等能计算
                continue
            
            f1_mom = row[1] or 0  # 5天动量
            f2_mom = row[2] or 0  # 20天动量
            vol_adj_ret = row[3] or 0  # 波动率调整收益
            
            # 用历史数据构造近似的RSI
            window = rows[max(0, i-14):i+1]
            moms = [r[1] for r in window if r[1]]
            
            gains = [m for m in moms if m > 0]
            losses = [abs(m) for m in moms if m < 0]
            
            avg_gain = np.mean(gains) if gains else 0
            avg_loss = np.mean(losses) if losses else 0.001
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            # 特征: [短期动量, 中期动量, 波动率, RSI]
            features.append([
                f1_mom * 0.01,  # 归一化
                f2_mom * 0.01,
                abs(vol_adj_ret) * 0.01,
                rsi
            ])
        
        return np.array(features)
    
    def train(self, X: np.ndarray = None):
        """训练HMM模型"""
        if X is None:
            X = self.load_training_data()
        
        if X is None or len(X) < 100:
            print("[HMM] 训练数据不足，使用默认参数")
            return False
        
        print(f"[HMM] 开始训练，样本数: {len(X)}, 特征数: {X.shape[1]}")
        
        self.model.fit(X)
        
        # 保存模型
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save(self.model_path)
        
        print(f"[HMM] 训练完成，收敛: {self.model.converged}")
        print(f"[HMM] 状态转移矩阵:\n{self.model.transmat_}")
        
        # 打印各状态特征
        for i in range(self.n_components):
            print(f"[HMM] 状态{i} ({self.state_names.get(i, 'Unknown')}): "
                  f"均值={self.model.means_[i]}")
        
        return True
    
    def predict(self, features: np.ndarray) -> dict:
        """预测当前市场状态"""
        if self.model.means_ is None:
            if self.model_path.exists():
                self.model.load(self.model_path)
            else:
                return {'state': 'Unknown', 'state_id': -1, 'probability': 0, 'probs': [0.33, 0.33, 0.34]}
        
        states = self.model.predict(features)
        current_state = states[-1]
        
        probs = self.model.predict_proba(features)
        current_probs = probs[-1]
        
        return {
            'state': self.state_names.get(current_state, f'State{current_state}'),
            'state_id': int(current_state),
            'probability': float(current_probs[current_state]),
            'probs': current_probs.tolist(),
            'all_states': {self.state_names.get(i, f'State{i}'): float(p) 
                          for i, p in enumerate(current_probs)}
        }
    
    def detect_regime(self, features_list: list) -> dict:
        """端到端市场状态检测"""
        features_arr = np.array(features_list)
        result = self.predict(features_arr)
        
        result['timestamp'] = datetime.now().isoformat()
        result['features'] = {
            'mom_5d': float(features_arr[-1][0]),
            'mom_20d': float(features_arr[-1][1]),
            'volatility': float(features_arr[-1][2]),
            'rsi': float(features_arr[-1][3])
        }
        
        return result


if __name__ == '__main__':
    # 测试
    detector = HMMRegimeDetector()
    X = detector.load_training_data()
    if X is not None:
        detector.train(X)
