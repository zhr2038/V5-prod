#!/usr/bin/env python3
"""
使用真实数据运行walk-forward回测
"""

import json
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

def load_real_market_data():
    """从alpha_history.db加载真实市场数据"""
    
    print("📊 加载真实市场数据...")
    
    import sqlite3
    from src.core.models import MarketSeries
    
    db_path = "reports/alpha_history.db"
    conn = sqlite3.connect(db_path)
    
    # 获取所有币种
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data_1h ORDER BY symbol")
    symbols = [row[0] for row in cursor.fetchall()]
    
    print(f"✅ 找到 {len(symbols)} 个币种")
    
    # 加载每个币种的数据
    market_data = {}
    
    for symbol in symbols[:10]:  # 先加载前10个币种以加快速度
        query = f"""
        SELECT timestamp, open, high, low, close, volume 
        FROM market_data_1h 
        WHERE symbol = ? 
        ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol,))
        
        if len(df) >= 100:  # 至少100根K线
            market_data[symbol] = MarketSeries(
                symbol=symbol,
                timeframe="1h",
                ts=df['timestamp'].tolist(),
                open=df['open'].tolist(),
                high=df['high'].tolist(),
                low=df['low'].tolist(),
                close=df['close'].tolist(),
                volume=df['volume'].tolist()
            )
            
            print(f"  {symbol}: {len(df)}根K线 ({datetime.fromtimestamp(df['timestamp'].min())} 到 {datetime.fromtimestamp(df['timestamp'].max())})")
    
    conn.close()
    
    print(f"✅ 成功加载 {len(market_data)} 个币种的实时数据")
    return market_data

def run_walk_forward_with_real_data():
    """使用真实数据运行walk-forward"""
    
    print("🚀 使用真实数据运行Walk-Forward回测")
    print("=" * 60)
    
    from configs.loader import load_config
    from src.backtest.walk_forward import run_walk_forward, build_walk_forward_report
    
    # 加载配置
    cfg_path = "configs/config.yaml"
    cfg = load_config(cfg_path, env_path=".env")
    
    print(f"📋 配置信息:")
    print(f"  策略: F2权重{cfg.alpha.weights.f2_mom_20d*100:.0f}%")
    print(f"  成本模型: {cfg.backtest.cost_model}")
    print(f"  成本数据: {cfg.backtest.cost_stats_dir}")
    
    # 加载真实市场数据
    market_data = load_real_market_data()
    
    if not market_data:
        print("❌ 无法加载市场数据")
        return None
    
    # 运行walk-forward
    print(f"\n⚙️ 运行Walk-Forward回测...")
    folds = run_walk_forward(market_data, folds=int(cfg.backtest.walk_forward_folds), cfg=cfg)
    
    # 构建报告
    report = build_walk_forward_report(folds, cost_meta={
        "mode": str(cfg.backtest.cost_model),
        "fee_quantile": str(cfg.backtest.fee_quantile),
        "slippage_quantile": str(cfg.backtest.slippage_quantile),
        "min_fills_global": int(cfg.backtest.min_fills_global),
        "min_fills_bucket": int(cfg.backtest.min_fills_bucket),
        "max_stats_age_days": int(cfg.backtest.max_stats_age_days),
        "cost_stats_dir": str(cfg.backtest.cost_stats_dir),
    })
    
    # 保存报告
    Path("reports").mkdir(exist_ok=True)
    output_path = Path("reports/walk_forward_real.json")
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    
    print(f"\n✅ Walk-Forward回测完成!")
    print(f"   保存到: {output_path}")
    print(f"   Folds数量: {len(report.get('folds') or [])}")
    
    return report

def analyze_real_walk_forward_results(report):
    """分析真实数据的walk-forward结果"""
    
    print("\n" + "=" * 60)
    print("📊 Walk-Forward结果分析 (真实数据)")
    print("=" * 60)
    
    folds = report.get('folds', [])
    
    if not folds:
        print("❌ 无folds数据")
        return
    
    # 提取绩效指标
    metrics = []
    for i, fold in enumerate(folds):
        result = fold['result']
        metrics.append({
            "fold": i + 1,
            "sharpe": result['sharpe'],
            "cagr": result['cagr'],
            "max_dd": result['max_dd'],
            "profit_factor": result['profit_factor'],
            "turnover": result['turnover'],
        })
    
    df = pd.DataFrame(metrics)
    
    print("📈 Fold绩效表现:")
    print(df.to_string(index=False))
    
    # 检查是否有交易
    all_zero = df['sharpe'].sum() == 0 and df['cagr'].sum() == 0
    
    if all_zero:
        print(f"\n⚠️ 所有folds绩效为0")
        print("可能原因:")
        print("  1. 策略参数在真实数据中也未触发交易")
        print("  2. 市场条件不适合当前策略")
        print("  3. 成本数据要求可能影响交易决策")
        
        # 检查成本数据
        cost_meta = report['cost_assumption_meta']
        print(f"\n💰 成本数据状态:")
        print(f"  模式: {cost_meta['mode']}")
        print(f"  数据目录: {cost_meta['cost_stats_dir']}")
        
        # 检查第一个fold的成本假设
        first_fold = folds[0]
        cost_assumption = first_fold.get('cost_assumption', {})
        print(f"  使用的成本数据: {cost_assumption.get('source_day', 'N/A')}")
        print(f"  全局fills: {cost_assumption.get('global_fills', 0)}")
        
    else:
        # 计算统计量
        avg_sharpe = df['sharpe'].mean()
        avg_cagr = df['cagr'].mean()
        sharpe_std = df['sharpe'].std()
        
        print(f"\n📊 统计摘要:")
        print(f"  平均夏普: {avg_sharpe:.3f}")
        print(f"  平均年化收益: {avg_cagr*100:.2f}%")
        print(f"  夏普标准差: {sharpe_std:.4f}")
        
        # 稳定性评估
        if sharpe_std < 0.1:
            print(f"  ✅ 策略表现稳定")
        elif sharpe_std < 0.2:
            print(f"  ⚠️ 策略表现一般稳定")
        else:
            print(f"  ❌ 策略表现不稳定")
        
        # 检查趋势
        if len(df) > 1:
            sharpe_trend = (df['sharpe'].iloc[-1] - df['sharpe'].iloc[0]) / len(df)
            if sharpe_trend > 0.01:
                print(f"  📈 夏普呈上升趋势")
            elif sharpe_trend < -0.01:
                print(f"  📉 夏普呈下降趋势")
            else:
                print(f"  ↔️ 夏普趋势平稳")
    
    # 优化效果评估
    print(f"\n🎯 优化效果评估:")
    print(f"  F2权重: 20% (优化后)")
    print(f"  成本模型: 校准模型")
    print(f"  数据基础: 30天真实市场数据")
    
    if not all_zero:
        # 如果有交易，评估优化效果
        best_fold = df.loc[df['sharpe'].idxmax()]
        print(f"  最佳fold夏普: {best_fold['sharpe']:.3f}")
        print(f"  最佳fold收益: {best_fold['cagr']*100:.2f}%")
    
    print("=" * 60)

def main():
    """主函数"""
    
    print("🚀 Walk-Forward回测 (使用30天真实数据)")
    print("=" * 60)
    print("验证优化后策略在真实市场中的稳定性")
    print("=" * 60)
    
    # 运行walk-forward
    report = run_walk_forward_with_real_data()
    
    if report:
        # 分析结果
        analyze_real_walk_forward_results(report)
    
    print("\n✅ 真实数据Walk-Forward完成")
    print("=" * 60)

if __name__ == "__main__":
    main()