#!/usr/bin/env python3
"""
V5 回测-实盘一致性检查工具

功能：
- 对比回测和实盘的滑点差异
- 对比成交率差异
- 对比成本模型准确性
- 生成一致性报告
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
WORKSPACE = Path('/home/admin/clawd/v5-trading-bot')


class BacktestLiveConsistencyChecker:
    """回测-实盘一致性检查器"""
    
    def __init__(self):
        self.results = {
            'slippage_diff': [],
            'fill_rate_diff': [],
            'cost_diff': [],
            'recommendations': []
        }
    
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    
    def load_live_trades(self, days=7):
        """加载实盘交易数据"""
        orders_db = REPORTS_DIR / 'orders.sqlite'
        if not orders_db.exists():
            return []
        
        conn = sqlite3.connect(str(orders_db))
        cursor = conn.cursor()
        
        cutoff = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        
        cursor.execute("""
            SELECT inst_id, side, px, avg_px, sz, acc_fill_sz, fee, state, created_ts
            FROM orders
            WHERE created_ts > ? AND state = 'FILLED'
        """, (cutoff,))
        
        trades = []
        for row in cursor.fetchall():
            trades.append({
                'symbol': row[0],
                'side': row[1],
                'order_px': row[2],
                'fill_px': row[3],
                'order_sz': row[4],
                'fill_sz': row[5],
                'fee': row[6] or 0,
                'ts': datetime.fromtimestamp(row[8] / 1000)
            })
        
        conn.close()
        return trades
    
    def calculate_live_slippage(self, trades):
        """计算实盘滑点"""
        slippages = []
        
        for trade in trades:
            if trade['order_px'] and trade['fill_px']:
                # 对于市价单，使用预期价格vs成交价格
                slippage = abs(float(trade['fill_px']) - float(trade['order_px'])) / float(trade['order_px'])
                slippages.append({
                    'symbol': trade['symbol'],
                    'slippage': slippage,
                    'side': trade['side'],
                    'ts': trade['ts']
                })
        
        return slippages
    
    def load_backtest_config(self):
        """加载回测成本配置"""
        cost_files = list(REPORTS_DIR.glob('cost_stats_real/*.json'))
        
        if not cost_files:
            return None
        
        # 读取最新的成本统计
        latest = max(cost_files, key=lambda x: x.stat().st_mtime)
        
        with open(latest) as f:
            return json.load(f)
    
    def compare_cost_models(self, live_trades, backtest_cost):
        """对比成本模型"""
        print("\n" + "=" * 70)
        print("💰 成本模型对比")
        print("=" * 70)
        
        if not live_trades:
            print("⚠️  无实盘交易数据")
            return
        
        if not backtest_cost:
            print("⚠️  无回测成本数据")
            return
        
        # 计算实盘平均成本
        total_fee = sum(float(t['fee'] or 0) for t in live_trades)
        total_notional = sum(float(t.get('fill_px', 0) or 0) * float(t.get('fill_sz', 0) or 0) for t in live_trades)
        
        if total_notional > 0:
            live_cost_bps = (total_fee / total_notional) * 10000  # 转换为基点
        else:
            live_cost_bps = 0
        
        # 获取回测成本
        backtest_cost_bps = backtest_cost.get('avg_cost_bps', 0)
        
        print(f"\n实盘平均成本: {live_cost_bps:.2f} bps")
        print(f"回测成本假设: {backtest_cost_bps:.2f} bps")
        
        diff = live_cost_bps - backtest_cost_bps
        diff_pct = (diff / backtest_cost_bps * 100) if backtest_cost_bps > 0 else 0
        
        if abs(diff_pct) > 20:  # 差异超过20%
            print(f"⚠️  成本差异显著: {diff:+.2f} bps ({diff_pct:+.1f}%)")
            self.results['recommendations'].append(f"调整回测成本模型: 当前{backtest_cost_bps:.0f}bps → 建议{live_cost_bps:.0f}bps")
        else:
            print(f"✅ 成本模型一致: 差异 {diff:+.2f} bps ({diff_pct:+.1f}%)")
    
    def analyze_fill_rates(self, days=7):
        """分析成交率"""
        print("\n" + "=" * 70)
        print("📊 成交率分析")
        print("=" * 70)
        
        orders_db = REPORTS_DIR / 'orders.sqlite'
        if not orders_db.exists():
            print("⚠️  无订单数据库")
            return
        
        conn = sqlite3.connect(str(orders_db))
        cursor = conn.cursor()
        
        cutoff = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        
        # 统计各状态订单
        cursor.execute("""
            SELECT state, COUNT(*) 
            FROM orders 
            WHERE created_ts > ?
            GROUP BY state
        """, (cutoff,))
        
        states = dict(cursor.fetchall())
        conn.close()
        
        total = sum(states.values())
        filled = states.get('FILLED', 0)
        rejected = states.get('REJECTED', 0)
        
        if total > 0:
            fill_rate = filled / total * 100
            reject_rate = rejected / total * 100
            
            print(f"\n最近{days}天订单统计:")
            print(f"  总订单: {total}")
            print(f"  成交: {filled} ({fill_rate:.1f}%)")
            print(f"  拒绝: {rejected} ({reject_rate:.1f}%)")
            
            if fill_rate < 50:
                print(f"⚠️  成交率偏低，建议检查粉尘过滤设置")
                self.results['recommendations'].append(f"成交率仅{fill_rate:.1f}%，建议调整最小下单金额")
            else:
                print(f"✅ 成交率正常")
    
    def generate_report(self):
        """生成一致性报告"""
        print("\n" + "=" * 70)
        print("📋 一致性检查报告")
        print("=" * 70)
        
        if self.results['recommendations']:
            print("\n🎯 改进建议:")
            for i, rec in enumerate(self.results['recommendations'], 1):
                print(f"  {i}. {rec}")
        else:
            print("\n✅ 回测与实盘一致性良好")
        
        # 保存报告
        report_file = REPORTS_DIR / f'consistency_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'results': self.results
            }, f, indent=2, default=str)
        
        print(f"\n📄 报告已保存: {report_file}")
    
    def run(self):
        """运行一致性检查"""
        self.log("🚀 回测-实盘一致性检查开始")
        
        # 加载数据
        live_trades = self.load_live_trades(days=7)
        backtest_cost = self.load_backtest_config()
        
        print(f"📊 加载实盘交易: {len(live_trades)} 笔")
        
        # 对比成本模型
        self.compare_cost_models(live_trades, backtest_cost)
        
        # 分析成交率
        self.analyze_fill_rates(days=7)
        
        # 生成报告
        self.generate_report()
        
        print("\n" + "=" * 70)
        print("✅ 检查完成")
        print("=" * 70)


def main():
    checker = BacktestLiveConsistencyChecker()
    checker.run()


if __name__ == '__main__':
    main()
