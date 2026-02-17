#!/usr/bin/env python3
"""
收集 alpha 历史数据用于评估
在每次 V5 运行时调用，记录：
- 当前 alpha snapshot
- 未来收益（延迟计算）
- 交易结果
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

import numpy as np

from configs.loader import load_config
from src.alpha.alpha_engine import AlphaEngine, AlphaSnapshot
from src.core.models import MarketSeries


class AlphaHistoryCollector:
    """Alpha 历史数据收集器"""
    
    def __init__(self, db_path: str = "reports/alpha_history.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self) -> None:
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 主表：alpha snapshot
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alpha_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts INTEGER NOT NULL,  -- 时间戳（秒）
                symbol TEXT NOT NULL,
                -- 原始因子值
                f1_mom_5d REAL,
                f2_mom_20d REAL,
                f3_vol_adj_ret_20d REAL,
                f4_volume_expansion REAL,
                f5_rsi_trend_confirm REAL,
                -- z-score 值
                z1_mom_5d REAL,
                z2_mom_20d REAL,
                z3_vol_adj_ret_20d REAL,
                z4_volume_expansion REAL,
                z5_rsi_trend_confirm REAL,
                -- 总分和排名
                score REAL,
                score_rank INTEGER,
                -- 未来收益（延迟填充）
                fwd_ret_1h REAL,
                fwd_ret_4h REAL,
                fwd_ret_12h REAL,
                fwd_ret_24h REAL,
                fwd_ret_72h REAL,
                -- 元数据
                regime TEXT,
                regime_multiplier REAL,
                selected INTEGER DEFAULT 0,  -- 是否被选中
                traded INTEGER DEFAULT 0,    -- 是否交易
                pnl REAL DEFAULT 0.0,       -- 交易盈亏
                UNIQUE(run_id, ts, symbol)
            )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON alpha_snapshots(ts)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_symbol ON alpha_snapshots(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_run_id ON alpha_snapshots(run_id)")
        
        # 运行元数据表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS run_metadata (
                run_id TEXT PRIMARY KEY,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                window_start_ts INTEGER,
                window_end_ts INTEGER,
                regime TEXT,
                regime_multiplier REAL,
                num_symbols INTEGER,
                selected_symbols TEXT,  -- JSON数组
                traded_symbols TEXT,    -- JSON数组
                summary_path TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def save_snapshot(
        self,
        run_id: str,
        ts: int,
        snapshot: AlphaSnapshot,
        regime: str = "",
        regime_multiplier: float = 1.0,
        selected_symbols: List[str] = None,
        traded_symbols: List[str] = None
    ) -> None:
        """保存 alpha snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 计算排名
        scores = list(snapshot.scores.values())
        symbols = list(snapshot.scores.keys())
        if scores:
            # 按分数降序排名（分数越高越好）
            sorted_indices = np.argsort(scores)[::-1]  # 降序
            rank_dict = {}
            for rank, idx in enumerate(sorted_indices, 1):
                rank_dict[symbols[idx]] = rank
        else:
            rank_dict = {}
        
        for symbol in snapshot.scores.keys():
            raw = snapshot.raw_factors.get(symbol, {})
            z = snapshot.z_factors.get(symbol, {})
            score = snapshot.scores.get(symbol, 0.0)
            
            cursor.execute("""
                INSERT OR REPLACE INTO alpha_snapshots (
                    run_id, ts, symbol,
                    f1_mom_5d, f2_mom_20d, f3_vol_adj_ret_20d, f4_volume_expansion, f5_rsi_trend_confirm,
                    z1_mom_5d, z2_mom_20d, z3_vol_adj_ret_20d, z4_volume_expansion, z5_rsi_trend_confirm,
                    score, score_rank,
                    regime, regime_multiplier,
                    selected, traded
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id, ts, symbol,
                raw.get("f1_mom_5d", 0.0),
                raw.get("f2_mom_20d", 0.0),
                raw.get("f3_vol_adj_ret_20d", 0.0),
                raw.get("f4_volume_expansion", 0.0),
                raw.get("f5_rsi_trend_confirm", 0.0),
                z.get("f1_mom_5d", 0.0),
                z.get("f2_mom_20d", 0.0),
                z.get("f3_vol_adj_ret_20d", 0.0),
                z.get("f4_volume_expansion", 0.0),
                z.get("f5_rsi_trend_confirm", 0.0),
                score,
                rank_dict.get(symbol, 0),
                regime,
                regime_multiplier,
                1 if selected_symbols and symbol in selected_symbols else 0,
                1 if traded_symbols and symbol in traded_symbols else 0
            ))
        
        # 保存运行元数据
        if selected_symbols is not None:
            cursor.execute("""
                INSERT OR REPLACE INTO run_metadata (
                    run_id, start_ts, end_ts, regime, regime_multiplier,
                    num_symbols, selected_symbols, traded_symbols
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                ts,
                ts,
                regime,
                regime_multiplier,
                len(snapshot.scores),
                json.dumps(selected_symbols or []),
                json.dumps(traded_symbols or [])
            ))
        
        conn.commit()
        conn.close()
    
    def update_forward_returns(
        self,
        symbol: str,
        ts: int,
        fwd_ret_1h: Optional[float] = None,
        fwd_ret_4h: Optional[float] = None,
        fwd_ret_12h: Optional[float] = None,
        fwd_ret_24h: Optional[float] = None,
        fwd_ret_72h: Optional[float] = None
    ) -> None:
        """更新未来收益数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updates = []
        params = []
        
        if fwd_ret_1h is not None:
            updates.append("fwd_ret_1h = ?")
            params.append(fwd_ret_1h)
        if fwd_ret_4h is not None:
            updates.append("fwd_ret_4h = ?")
            params.append(fwd_ret_4h)
        if fwd_ret_12h is not None:
            updates.append("fwd_ret_12h = ?")
            params.append(fwd_ret_12h)
        if fwd_ret_24h is not None:
            updates.append("fwd_ret_24h = ?")
            params.append(fwd_ret_24h)
        if fwd_ret_72h is not None:
            updates.append("fwd_ret_72h = ?")
            params.append(fwd_ret_72h)
        
        if updates:
            params.extend([symbol, ts])
            cursor.execute(f"""
                UPDATE alpha_snapshots 
                SET {', '.join(updates)}
                WHERE symbol = ? AND ts = ?
            """, params)
        
        conn.commit()
        conn.close()
    
    def update_trade_pnl(
        self,
        symbol: str,
        ts: int,
        pnl: float
    ) -> None:
        """更新交易盈亏"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE alpha_snapshots 
            SET pnl = ?, traded = 1
            WHERE symbol = ? AND ts = ?
        """, (pnl, symbol, ts))
        
        conn.commit()
        conn.close()
    
    def get_snapshots_for_evaluation(
        self,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        min_symbols: int = 5
    ) -> List[Dict[str, Any]]:
        """获取用于评估的 snapshot 数据"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
            SELECT ts, symbol, score, 
                   fwd_ret_1h, fwd_ret_4h, fwd_ret_12h, fwd_ret_24h, fwd_ret_72h,
                   selected, traded, pnl
            FROM alpha_snapshots
            WHERE 1=1
        """
        params = []
        
        if start_ts:
            query += " AND ts >= ?"
            params.append(start_ts)
        if end_ts:
            query += " AND ts <= ?"
            params.append(end_ts)
        
        query += " ORDER BY ts, symbol"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # 按时间点分组
        snapshots_by_ts = {}
        for row in rows:
            ts = row["ts"]
            if ts not in snapshots_by_ts:
                snapshots_by_ts[ts] = {
                    "ts": ts,
                    "alpha_scores": {},
                    "fwd_ret_1h": {},
                    "fwd_ret_4h": {},
                    "fwd_ret_12h": {},
                    "fwd_ret_24h": {},
                    "fwd_ret_72h": {}
                }
            
            symbol = row["symbol"]
            snapshots_by_ts[ts]["alpha_scores"][symbol] = row["score"]
            
            if row["fwd_ret_1h"] is not None:
                snapshots_by_ts[ts]["fwd_ret_1h"][symbol] = row["fwd_ret_1h"]
            if row["fwd_ret_4h"] is not None:
                snapshots_by_ts[ts]["fwd_ret_4h"][symbol] = row["fwd_ret_4h"]
            if row["fwd_ret_12h"] is not None:
                snapshots_by_ts[ts]["fwd_ret_12h"][symbol] = row["fwd_ret_12h"]
            if row["fwd_ret_24h"] is not None:
                snapshots_by_ts[ts]["fwd_ret_24h"][symbol] = row["fwd_ret_24h"]
            if row["fwd_ret_72h"] is not None:
                snapshots_by_ts[ts]["fwd_ret_72h"][symbol] = row["fwd_ret_72h"]
        
        # 过滤掉数据不足的时间点
        result = []
        for ts, data in snapshots_by_ts.items():
            if len(data["alpha_scores"]) >= min_symbols:
                result.append(data)
        
        conn.close()
        return result


def collect_current_alpha_history(
    config_path: str = "configs/live_small.yaml",
    env_path: str = ".env",
    run_id: Optional[str] = None
) -> None:
    """收集当前运行的 alpha 历史数据"""
    if run_id is None:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    cfg = load_config(config_path, env_path=env_path)
    
    # 这里需要 market_data，简化实现
    # 实际应该从数据库或API获取
    print(f"Collecting alpha history for run {run_id}...")
    print("Note: This is a placeholder. Need market data source.")
    
    # 初始化收集器
    collector = AlphaHistoryCollector()
    
    # 示例：保存一个空的snapshot（实际需要真实数据）
    # snapshot = AlphaSnapshot(raw_factors={}, z_factors={}, scores={})
    # collector.save_snapshot(
    #     run_id=run_id,
    #     ts=int(time.time()),
    #     snapshot=snapshot,
    #     regime="Unknown",
    #     regime_multiplier=1.0
    # )
    
    print(f"Alpha history collector initialized at: reports/alpha_history.db")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/live_small.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--run-id")
    args = ap.parse_args()
    
    collect_current_alpha_history(args.config, args.env, args.run_id)