#!/usr/bin/env python3
"""
运行 alpha 实时评估
基于收集的历史数据，生成评估报告
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
from scipy import stats

from src.reporting.alpha_evaluation import (
    AlphaEvalConfig, 
    AlphaEvalResult,
    save_alpha_evaluation_report,
    generate_alpha_evaluation_summary
)


def load_snapshots_from_db(
    db_path: str,
    days_back: int = 30
) -> List[Dict[str, Any]]:
    """从数据库加载 snapshot 数据"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cutoff_ts = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())
    
    # 获取所有时间点
    cursor.execute("""
        SELECT DISTINCT ts 
        FROM alpha_snapshots 
        WHERE ts >= ? 
        AND fwd_ret_1h IS NOT NULL
        ORDER BY ts
    """, (cutoff_ts,))
    
    timestamps = [row["ts"] for row in cursor.fetchall()]
    
    snapshots = []
    for ts in timestamps:
        # 获取该时间点的所有数据
        cursor.execute("""
            SELECT symbol, score, fwd_ret_1h, fwd_ret_4h, fwd_ret_12h, fwd_ret_24h, fwd_ret_72h
            FROM alpha_snapshots 
            WHERE ts = ? 
            AND fwd_ret_1h IS NOT NULL
        """, (ts,))
        
        rows = cursor.fetchall()
        if len(rows) >= 5:  # 至少5个币种才有统计意义
            snapshot = {
                "ts": ts,
                "alpha_scores": {},
                "fwd_ret_1h": {},
                "fwd_ret_4h": {},
                "fwd_ret_12h": {},
                "fwd_ret_24h": {},
                "fwd_ret_72h": {}
            }
            
            for row in rows:
                symbol = row["symbol"]
                snapshot["alpha_scores"][symbol] = row["score"]
                snapshot["fwd_ret_1h"][symbol] = row["fwd_ret_1h"]
                snapshot["fwd_ret_4h"][symbol] = row["fwd_ret_4h"]
                snapshot["fwd_ret_12h"][symbol] = row["fwd_ret_12h"]
                snapshot["fwd_ret_24h"][symbol] = row["fwd_ret_24h"]
                snapshot["fwd_ret_72h"][symbol] = row["fwd_ret_72h"]
            
            snapshots.append(snapshot)
    
    conn.close()
    return snapshots


def calculate_ic_analysis(
    snapshots: List[Dict[str, Any]],
    horizon_hours: int
) -> Dict[str, float]:
    """计算指定持有期的IC分析"""
    ics = []
    
    for snap in snapshots:
        scores = []
        returns = []
        
        fwd_key = f"fwd_ret_{horizon_hours}h"
        if fwd_key not in snap:
            continue
        
        for symbol in snap["alpha_scores"].keys():
            if symbol in snap[fwd_key]:
                scores.append(snap["alpha_scores"][symbol])
                returns.append(snap[fwd_key][symbol])
        
        if len(scores) >= 5:  # 至少5个数据点
            try:
                ic, pvalue = stats.spearmanr(scores, returns)
                if not np.isnan(ic):
                    ics.append(float(ic))
            except:
                pass
    
    if not ics:
        return {"mean": 0.0, "std": 0.0, "ir": 0.0, "count": 0}
    
    ic_array = np.array(ics)
    ic_mean = float(np.mean(ic_array))
    ic_std = float(np.std(ic_array)) if len(ic_array) > 1 else 0.0
    ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
    
    return {
        "mean": ic_mean,
        "std": ic_std,
        "ir": ic_ir,
        "count": len(ics)
    }


def calculate_quantile_returns(
    snapshots: List[Dict[str, Any]],
    horizon_hours: int,
    n_quantiles: int = 5
) -> Dict[int, Dict[str, float]]:
    """计算分位数收益"""
    # 收集所有时间点的分位数收益
    quantile_returns = {q: [] for q in range(n_quantiles)}
    
    for snap in snapshots:
        fwd_key = f"fwd_ret_{horizon_hours}h"
        if fwd_key not in snap:
            continue
        
        # 按score排序
        symbols = list(snap["alpha_scores"].keys())
        scores = [snap["alpha_scores"][s] for s in symbols]
        returns = [snap[fwd_key].get(s, 0.0) for s in symbols]
        
        if len(scores) < n_quantiles:
            continue
        
        # 排序
        sorted_indices = np.argsort(scores)[::-1]  # 降序（高分在前）
        q_size = len(sorted_indices) // n_quantiles
        
        for q in range(n_quantiles):
            start = q * q_size
            end = (q + 1) * q_size if q < n_quantiles - 1 else len(sorted_indices)
            if end > start:
                q_returns = [returns[sorted_indices[i]] for i in range(start, end)]
                quantile_returns[q].extend(q_returns)
    
    # 计算统计量
    result = {}
    for q in range(n_quantiles):
        if quantile_returns[q]:
            q_array = np.array(quantile_returns[q])
            result[q] = {
                "mean_return": float(np.mean(q_array)),
                "win_rate": float(np.mean(q_array > 0)),
                "vol": float(np.std(q_array)) if len(q_array) > 1 else 0.0,
                "count": len(q_array)
            }
    
    return result


def run_evaluation(
    db_path: str,
    days_back: int = 30,
    holding_periods: List[int] = None
) -> AlphaEvalResult:
    """运行评估"""
    if holding_periods is None:
        holding_periods = [1, 4, 12, 24, 72]
    
    # 加载数据
    snapshots = load_snapshots_from_db(db_path, days_back)
    logging.info(f"Loaded {len(snapshots)} snapshots for evaluation")
    
    if len(snapshots) < 10:
        logging.warning(f"Insufficient data for evaluation: only {len(snapshots)} snapshots")
    
    # IC分析
    ic_by_horizon = {}
    for horizon in holding_periods:
        ic_by_horizon[horizon] = calculate_ic_analysis(snapshots, horizon)
    
    # 分位数分析（使用1小时持有期）
    quantile_returns = {}
    for horizon in holding_periods:
        quantile_returns[horizon] = calculate_quantile_returns(
            snapshots, horizon, n_quantiles=5
        )
    
    # 衰减曲线
    decay_curve = []
    for horizon in sorted(ic_by_horizon.keys()):
        stats = ic_by_horizon[horizon]
        decay_curve.append((horizon, stats["mean"], stats["std"]))
    
    # 因子贡献（简化：需要因子级别的数据）
    factor_contributions = {}
    
    # 成本敏感性（估算）
    # 假设：每次调仓成本 = 手续费 + 滑点
    estimated_turnover_pct = 200.0  # 年化换手率估计
    cost_bps_per_trade = 11.0  # 6bps手续费 + 5bps滑点
    cost_sensitivity = {
        "estimated_turnover_pct": estimated_turnover_pct,
        "cost_bps_per_trade": cost_bps_per_trade,
        "breakeven_ic": 0.03  # 粗略估计
    }
    
    return AlphaEvalResult(
        ic_by_horizon=ic_by_horizon,
        quantile_returns=quantile_returns,
        decay_curve=decay_curve,
        factor_contributions=factor_contributions,
        cost_sensitivity=cost_sensitivity
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="reports/alpha_history.db")
    ap.add_argument("--output-dir", default="reports/alpha_evaluation")
    ap.add_argument("--days-back", type=int, default=30)
    ap.add_argument("--holding-periods", type=str, default="1,4,12,24,72")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)
    
    # 解析持有期
    holding_periods = [int(h) for h in args.holding_periods.split(",")]
    
    # 配置
    eval_config = AlphaEvalConfig(
        holding_periods=holding_periods,
        n_quantiles=5,
        fee_bps=6.0,
        slippage_bps=5.0,
        winsorize_pct=0.05,
        use_robust_zscore=True
    )
    
    # 运行评估
    result = run_evaluation(
        db_path=args.db_path,
        days_back=args.days_back,
        holding_periods=holding_periods
    )
    
    # 保存报告
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"alpha_eval_{timestamp}.json"
    txt_path = output_dir / f"alpha_eval_{timestamp}.txt"
    
    save_alpha_evaluation_report(result, str(json_path), eval_config)
    
    # 生成文本摘要
    summary = generate_alpha_evaluation_summary(result)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(summary)
    
    logging.info(f"Alpha evaluation report saved to {json_path}")
    logging.info(f"Text summary saved to {txt_path}")
    
    # 打印摘要
    print("\n" + summary)


if __name__ == "__main__":
    main()