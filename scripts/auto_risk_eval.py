#!/usr/bin/env python3
"""
Auto Risk Evaluator - 自动风险评估与档位切换

每小时运行一次，评估交易表现并自动切换风险档位
"""

from __future__ import annotations

import sys
import json
import glob
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 自动检测项目根目录
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.risk.auto_risk_guard import get_auto_risk_guard

REPORTS_DIR = PROJECT_ROOT / 'reports'
RUNS_DIR = REPORTS_DIR / 'runs'


def load_recent_runs(hours: int = 24) -> List[Dict]:
    """加载最近N小时的运行数据"""
    runs = []
    cutoff = datetime.now() - timedelta(hours=hours)
    
    if not RUNS_DIR.exists():
        return runs
    
    for run_dir in sorted(RUNS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not run_dir.is_dir():
            continue
        mtime = datetime.fromtimestamp(run_dir.stat().st_mtime)
        if mtime < cutoff:
            continue
        
        audit_file = run_dir / 'decision_audit.json'
        if not audit_file.exists():
            continue
        
        try:
            with open(audit_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            data['_run_id'] = run_dir.name
            data['_mtime'] = mtime.isoformat()
            runs.append(data)
        except Exception:
            continue
    
    return runs


def calculate_metrics(runs: List[Dict]) -> Dict:
    """计算关键指标"""
    if not runs:
        return {
            'dd_pct': 0.0,
            'conversion_rate': 0.0,
            'dust_reject_rate': 0.0,
            'pnl_trend': 'flat',
            'consecutive_losses': 0,
            'sample_size': 0,
        }
    
    total_selected = 0
    total_rebalance = 0
    total_rejected = 0
    total_dust = 0
    
    pnl_values = []
    
    for run in runs:
        counts = run.get('counts', {})
        total_selected += int(counts.get('selected', 0) or 0)
        total_rebalance += int(counts.get('orders_rebalance', 0) or 0)
        total_rejected += int(counts.get('orders_exit', 0) or 0)
        
        # Count dust skips from router decisions
        for rd in run.get('router_decisions', []):
            if rd.get('reason') == 'min_notional':
                total_dust += 1
        
        # Track PnL if available
        pnl = run.get('realized_pnl')
        if pnl is not None:
            pnl_values.append(float(pnl))
    
    # Calculate conversion rate
    conversion_rate = total_rebalance / total_selected if total_selected > 0 else 0.0
    
    # Calculate dust reject rate (as percentage of total orders)
    total_orders = total_selected + total_rejected
    dust_rate = total_dust / total_orders if total_orders > 0 else 0.0
    
    # Determine PnL trend
    pnl_trend = 'flat'
    if len(pnl_values) >= 3:
        recent = sum(pnl_values[-3:])
        previous = sum(pnl_values[-6:-3]) if len(pnl_values) >= 6 else recent
        if recent > previous * 1.05:
            pnl_trend = 'up'
        elif recent < previous * 0.95:
            pnl_trend = 'down'
    
    # Count consecutive loss rounds (simplified)
    consecutive_losses = 0
    for run in reversed(runs):
        pnl = run.get('realized_pnl')
        if pnl is not None and float(pnl) < 0:
            consecutive_losses += 1
        elif pnl is not None:
            break
    
    # Estimate drawdown from notes
    dd_pct = 0.0
    for run in runs:
        for note in run.get('notes', []):
            if 'drawdown' in str(note).lower():
                try:
                    import re
                    m = re.search(r'drawdown[:\s]+([\d.]+)%', str(note), re.IGNORECASE)
                    if m:
                        dd_pct = max(dd_pct, float(m.group(1)) / 100)
                except Exception:
                    pass
    
    return {
        'dd_pct': dd_pct,
        'conversion_rate': conversion_rate,
        'dust_reject_rate': dust_rate,
        'pnl_trend': pnl_trend,
        'consecutive_losses': consecutive_losses,
        'sample_size': len(runs),
        'total_selected': total_selected,
        'total_rebalance': total_rebalance,
    }


def evaluate_and_switch():
    """评估并执行档位切换"""
    guard = get_auto_risk_guard()
    
    # Load recent runs (last 12 hours for evaluation)
    runs = load_recent_runs(hours=12)
    
    if len(runs) < 3:
        print(f"[AutoRiskEval] 样本不足 ({len(runs)}轮)，维持当前档位: {guard.current_level}")
        return
    
    # Calculate metrics
    metrics = calculate_metrics(runs)
    
    print(f"[AutoRiskEval] 样本: {metrics['sample_size']}轮 | "
          f"转化率: {metrics['conversion_rate']:.1%} | "
          f"回撤: {metrics['dd_pct']:.1%} | "
          f"趋势: {metrics['pnl_trend']}")
    
    # Evaluate and potentially switch
    level, config, reason = guard.evaluate(
        dd_pct=metrics['dd_pct'],
        conversion_rate=metrics['conversion_rate'],
        dust_reject_rate=metrics['dust_reject_rate'],
        recent_pnl_trend=metrics['pnl_trend'],
        consecutive_losses=metrics['consecutive_losses']
    )
    
    print(f"[AutoRiskEval] 结果: {guard.current_level} | 原因: {reason}")
    
    # Save evaluation result
    eval_file = REPORTS_DIR / 'auto_risk_eval.json'
    eval_data = {
        'ts': datetime.now().isoformat(),
        'current_level': guard.current_level,
        'metrics': metrics,
        'reason': reason,
    }
    try:
        with open(eval_file, 'w', encoding='utf-8') as f:
            json.dump(eval_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[AutoRiskEval] 保存评估结果失败: {e}")


def main():
    """主函数 - 可被定时任务调用"""
    print("="*60)
    print("V5 自动风险评估")
    print("="*60)
    evaluate_and_switch()
    print("="*60)


if __name__ == '__main__':
    main()
