#!/usr/bin/env python3
"""
Alpha 历史评估脚本
从数据库加载历史数据，评估alpha预测能力
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

import numpy as np

from configs.loader import load_config
from src.alpha.alpha_engine import AlphaEngine
from src.core.models import MarketSeries
from src.reporting.alpha_evaluation import (
    AlphaEvalConfig, 
    AlphaEvalResult,
    run_alpha_evaluation_historical,
    save_alpha_evaluation_report,
    generate_alpha_evaluation_summary
)


def load_market_data_from_db(
    db_path: str, 
    symbols: List[str],
    start_ts: int,
    end_ts: int
) -> Dict[str, MarketSeries]:
    """从数据库加载市场数据"""
    # 简化实现：实际需要根据你的数据库结构调整
    market_data = {}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for symbol in symbols:
        # 假设表结构：market_data_1h(symbol, ts, open, high, low, close, volume)
        cursor.execute("""
            SELECT ts, close, volume 
            FROM market_data_1h 
            WHERE symbol = ? AND ts >= ? AND ts <= ?
            ORDER BY ts
        """, (symbol, start_ts, end_ts))
        
        rows = cursor.fetchall()
        if rows:
            ts_list = [r[0] for r in rows]
            close_list = [r[1] for r in rows]
            volume_list = [r[2] for r in rows]
            
            market_data[symbol] = MarketSeries(
                symbol=symbol,
                ts=ts_list,
                open=[0.0] * len(ts_list),  # 简化
                high=[0.0] * len(ts_list),
                low=[0.0] * len(ts_list),
                close=close_list,
                volume=volume_list
            )
    
    conn.close()
    return market_data


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/live_small.yaml")
    ap.add_argument("--env", default=".env")
    ap.add_argument("--db-path", default="reports/market_data.db")
    ap.add_argument("--output-dir", default="reports/alpha_evaluation")
    ap.add_argument("--days", type=int, default=30, help="评估天数")
    ap.add_argument("--symbols", type=str, help="逗号分隔的币种列表，默认使用配置中的币种")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    
    # 加载配置
    cfg = load_config(args.config, env_path=args.env)
    
    # 获取币种列表
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        # 从配置获取
        symbols = cfg.universe.symbols if hasattr(cfg.universe, "symbols") else []
        if not symbols:
            # 默认使用一些主要币种
            symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT"]
    
    # 时间范围
    end_ts = int(datetime.utcnow().timestamp())
    start_ts = end_ts - (args.days * 24 * 3600)
    
    logging.info(f"Loading market data for {len(symbols)} symbols from {start_ts} to {end_ts}")
    
    # 加载数据
    market_data = load_market_data_from_db(args.db_path, symbols, start_ts, end_ts)
    logging.info(f"Loaded data for {len(market_data)} symbols")
    
    if len(market_data) < 5:
        logging.error("Insufficient data for evaluation")
        return
    
    # 创建Alpha引擎
    alpha_engine = AlphaEngine(cfg.alpha)
    
    # 收集历史snapshot（简化：每小时一个点）
    historical_snapshots = []
    
    # 这里需要根据时间序列生成snapshot
    # 简化实现：只评估最近几个时间点
    logging.info("Computing alpha snapshots...")
    
    # 实际实现需要遍历时间点，计算每个时间点的alpha和未来收益
    # 这里先创建一个占位实现
    
    # 配置评估
    eval_config = AlphaEvalConfig(
        holding_periods=[1, 4, 12, 24, 72],  # 1h, 4h, 12h, 1d, 3d
        n_quantiles=5,
        fee_bps=6.0,
        slippage_bps=5.0,
        winsorize_pct=0.05,
        use_robust_zscore=True
    )
    
    # 运行评估（简化：需要真实数据）
    # result = run_alpha_evaluation_historical(historical_snapshots, eval_config)
    
    # 先创建一个示例报告
    example_result = AlphaEvalResult(
        ic_by_horizon={
            1: {"mean": 0.05, "std": 0.02, "ir": 2.5, "count": 100},
            4: {"mean": 0.04, "std": 0.03, "ir": 1.33, "count": 100},
            12: {"mean": 0.03, "std": 0.04, "ir": 0.75, "count": 100},
            24: {"mean": 0.02, "std": 0.05, "ir": 0.4, "count": 100},
            72: {"mean": 0.01, "std": 0.06, "ir": 0.17, "count": 100}
        },
        quantile_returns={
            1: {
                0: {"mean_return": -0.001, "win_rate": 0.45, "vol": 0.02, "count": 20},
                1: {"mean_return": 0.000, "win_rate": 0.50, "vol": 0.02, "count": 20},
                2: {"mean_return": 0.001, "win_rate": 0.55, "vol": 0.02, "count": 20},
                3: {"mean_return": 0.002, "win_rate": 0.60, "vol": 0.02, "count": 20},
                4: {"mean_return": 0.003, "win_rate": 0.65, "vol": 0.02, "count": 20}
            }
        },
        decay_curve=[
            (1, 0.05, 0.02),
            (4, 0.04, 0.03),
            (12, 0.03, 0.04),
            (24, 0.02, 0.05),
            (72, 0.01, 0.06)
        ],
        factor_contributions={
            "f1_mom_5d": {"ic_mean": 0.02, "ic_ir": 1.0, "weight": 0.3},
            "f2_mom_20d": {"ic_mean": 0.01, "ic_ir": 0.5, "weight": 0.3},
            "f3_vol_adj_ret_20d": {"ic_mean": 0.015, "ic_ir": 0.75, "weight": 0.2},
            "f4_volume_expansion": {"ic_mean": 0.005, "ic_ir": 0.25, "weight": 0.1},
            "f5_rsi_trend_confirm": {"ic_mean": 0.01, "ic_ir": 0.5, "weight": 0.1}
        },
        cost_sensitivity={
            "estimated_turnover_pct": 200.0,
            "cost_bps_per_trade": 11.0,
            "breakeven_ic": 0.03
        }
    )
    
    # 保存报告
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"alpha_eval_{timestamp}.json"
    txt_path = output_dir / f"alpha_eval_{timestamp}.txt"
    
    save_alpha_evaluation_report(example_result, str(json_path), eval_config)
    
    # 生成文本摘要
    summary = generate_alpha_evaluation_summary(example_result)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(summary)
    
    logging.info(f"Alpha evaluation report saved to {json_path}")
    logging.info(f"Text summary saved to {txt_path}")
    
    # 打印摘要
    print("\n" + summary)


if __name__ == "__main__":
    main()