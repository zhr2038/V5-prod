#!/usr/bin/env python3
"""
更新未来收益数据
定期运行，为历史 alpha snapshot 计算未来收益
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

from configs.loader import load_config
from src.data.mock_provider import MockProvider
from src.data.okx_ccxt_provider import OKXCCXTProvider


def get_price_at_time(
    symbol: str,
    target_ts: int,
    provider,
    hours_after: int
) -> Optional[float]:
    """获取指定时间后的价格"""
    try:
        # 获取目标时间附近的数据
        # 简化实现：实际需要根据数据源调整
        end_ts = target_ts + (hours_after * 3600)
        
        # 使用 provider 获取数据
        # 这里需要根据实际 provider 接口调整
        return None  # 占位
    except Exception as e:
        logging.warning(f"Failed to get price for {symbol} at ts={target_ts}: {e}")
        return None


def calculate_forward_return(
    entry_price: float,
    exit_price: float
) -> float:
    """计算收益"""
    if entry_price and exit_price and entry_price > 0:
        return (exit_price - entry_price) / entry_price
    return 0.0


def update_forward_returns_for_snapshot(
    db_path: str,
    snapshot_id: int,
    symbol: str,
    snapshot_ts: int,
    provider,
    holding_periods: List[int] = [1, 4, 12, 24, 72]
) -> Dict[int, float]:
    """为单个 snapshot 更新未来收益"""
    returns = {}
    
    # 获取 snapshot 时的价格（作为 entry price）
    entry_price = get_price_at_time(symbol, snapshot_ts, provider, 0)
    if not entry_price:
        return returns
    
    for hours in holding_periods:
        exit_price = get_price_at_time(symbol, snapshot_ts, provider, hours)
        if exit_price:
            ret = calculate_forward_return(entry_price, exit_price)
            returns[hours] = ret
    
    return returns


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/live_small.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--db-path", default="reports/alpha_history.db")
    ap.add_argument("--days-back", type=int, default=7, help="处理多少天前的数据")
    ap.add_argument("--holding-periods", type=str, default="1,4,12,24,72", help="持有期（小时）")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    
    # 加载配置
    cfg = load_config(args.config, env_path=args.env)
    
    # 初始化数据 provider
    provider_type = os.getenv("V5_DATA_PROVIDER", "mock").lower()
    if provider_type == "okx":
        provider = OKXCCXTProvider(cfg.data)
    else:
        provider = MockProvider(cfg.data)
    
    # 解析持有期
    holding_periods = [int(h) for h in args.holding_periods.split(",")]
    
    # 连接数据库
    conn = sqlite3.connect(args.db_path)
    cursor = conn.cursor()
    
    # 查找需要更新未来收益的 snapshot
    # 只处理足够时间前的数据（确保未来收益已实现）
    cutoff_ts = int(time.time()) - (max(holding_periods) * 3600)
    
    cursor.execute("""
        SELECT id, ts, symbol 
        FROM alpha_snapshots 
        WHERE ts <= ? 
        AND (fwd_ret_1h IS NULL OR fwd_ret_4h IS NULL OR fwd_ret_12h IS NULL)
        ORDER BY ts DESC
        LIMIT 100
    """, (cutoff_ts,))
    
    snapshots = cursor.fetchall()
    logging.info(f"Found {len(snapshots)} snapshots needing forward return updates")
    
    updated_count = 0
    for snapshot_id, snapshot_ts, symbol in snapshots:
        try:
            # 计算未来收益
            returns = update_forward_returns_for_snapshot(
                db_path=args.db_path,
                snapshot_id=snapshot_id,
                symbol=symbol,
                snapshot_ts=snapshot_ts,
                provider=provider,
                holding_periods=holding_periods
            )
            
            if returns:
                # 更新数据库
                update_sql = """
                    UPDATE alpha_snapshots 
                    SET fwd_ret_1h = ?, fwd_ret_4h = ?, fwd_ret_12h = ?, fwd_ret_24h = ?, fwd_ret_72h = ?
                    WHERE id = ?
                """
                cursor.execute(update_sql, (
                    returns.get(1),
                    returns.get(4),
                    returns.get(12),
                    returns.get(24),
                    returns.get(72),
                    snapshot_id
                ))
                updated_count += 1
                
                if updated_count % 10 == 0:
                    logging.info(f"Updated {updated_count} snapshots...")
        
        except Exception as e:
            logging.error(f"Failed to update snapshot {snapshot_id} ({symbol}): {e}")
    
    conn.commit()
    conn.close()
    
    logging.info(f"Completed. Updated {updated_count} snapshots with forward returns.")


if __name__ == "__main__":
    import os
    main()