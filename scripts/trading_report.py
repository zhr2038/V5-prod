#!/usr/bin/env python3
"""
V5 交易报告自动化

功能：
- 生成标准化日报/周报
- 权益曲线分析
- 策略表现归因
- 市场状态分布统计
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
ORDERS_DB = REPORTS_DIR / 'orders.sqlite'


class TradingReportGenerator:
    """交易报告生成器"""
    
    def __init__(self):
        self.data = {}
    
    def log(self, msg):
        print(msg)
    
    def load_equity_data(self, days=7):
        """加载权益数据"""
        points = []
        cutoff = datetime.now() - timedelta(days=days)
        
        # 从所有runs目录收集equity数据
        runs_dir = REPORTS_DIR / 'runs'
        if runs_dir.exists():
            for run_dir in runs_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                equity_file = run_dir / 'equity.jsonl'
                if equity_file.exists():
                    try:
                        with open(equity_file) as f:
                            for line in f:
                                try:
                                    data = json.loads(line)
                                    ts = datetime.fromisoformat(data.get('ts', '').replace('Z', '+00:00').replace('+00:00', ''))
                                    if ts > cutoff:
                                        points.append({
                                            'ts': ts,
                                            'equity': data.get('equity', 0),
                                            'cash': data.get('cash', 0),
                                            'positions_value': data.get('positions_value', 0)
                                        })
                                except:
                                    continue
                    except:
                        continue
        
        # 排序并去重
        points.sort(key=lambda x: x['ts'])
        seen = set()
        unique = []
        for p in points:
            key = p['ts'].strftime('%Y-%m-%d %H:%M')
            if key not in seen:
                seen.add(key)
                unique.append(p)
        
        return unique
    
    def load_trade_data(self, days=7):
        """加载交易数据"""
        if not ORDERS_DB.exists():
            return []
        
        conn = sqlite3.connect(str(ORDERS_DB))
        cursor = conn.cursor()
        
        cutoff_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        
        cursor.execute("""
            SELECT inst_id, side, state, notional_usdt, fee, created_ts
            FROM orders
            WHERE created_ts > ? AND state = 'FILLED'
            ORDER BY created_ts DESC
        """, (cutoff_ts,))
        
        trades = []
        for row in cursor.fetchall():
            trades.append({
                'symbol': row[0].replace('-USDT', ''),
                'side': row[1],
                'state': row[2],
                'notional': row[3] or 0,
                'fee': row[4] or 0,
                'ts': datetime.fromtimestamp(row[5] / 1000)
            })
        
        conn.close()
        return trades
    
    def load_regime_history(self, days=7):
        """加载市场状态历史"""
        regimes = []
        cutoff = datetime.now() - timedelta(days=days)
        
        runs_dir = REPORTS_DIR / 'runs'
        if runs_dir.exists():
            for run_dir in runs_dir.iterdir():
                try:
                    audit_file = run_dir / 'decision_audit.json'
                    if audit_file.exists():
                        mtime = datetime.fromtimestamp(audit_file.stat().st_mtime)
                        if mtime > cutoff:
                            with open(audit_file) as f:
                                data = json.load(f)
                                regimes.append({
                                    'ts': mtime,
                                    'regime': data.get('regime', 'Unknown'),
                                    'multiplier': data.get('regime_multiplier', 0.6)
                                })
                except:
                    continue
        
        regimes.sort(key=lambda x: x['ts'])
        return regimes
    
    def generate_daily_report(self):
        """生成日报"""
        self.log("=" * 60)
        self.log("📊 V5 交易日报")
        self.log("=" * 60)
        self.log(f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        self.log(f"报告周期: 最近24小时")
        self.log()
        
        # 权益曲线
        equity_data = self.load_equity_data(days=1)
        if equity_data:
            start_eq = equity_data[0]['equity']
            end_eq = equity_data[-1]['equity']
            change = end_eq - start_eq
            change_pct = (change / start_eq * 100) if start_eq > 0 else 0
            
            self.log(f"💰 权益变化")
            self.log(f"  起始: ${start_eq:.2f}")
            self.log(f"  结束: ${end_eq:.2f}")
            self.log(f"  变化: ${change:+.2f} ({change_pct:+.2f}%)")
            self.log()
        
        # 交易统计
        trades = self.load_trade_data(days=1)
        if trades:
            buy_count = sum(1 for t in trades if t['side'] == 'buy')
            sell_count = sum(1 for t in trades if t['side'] == 'sell')
            buy_value = sum(t['notional'] for t in trades if t['side'] == 'buy')
            sell_value = sum(t['notional'] for t in trades if t['side'] == 'sell')
            total_fee = sum(t['fee'] for t in trades)
            
            self.log(f"🔄 交易统计")
            self.log(f"  买入: {buy_count} 笔, ${buy_value:.2f}")
            self.log(f"  卖出: {sell_count} 笔, ${sell_value:.2f}")
            self.log(f"  手续费: ${total_fee:.4f}")
            self.log()
            
            # 交易明细
            self.log("  最近5笔交易:")
            for t in trades[:5]:
                self.log(f"    {t['ts'].strftime('%H:%M')} {t['side']:4} {t['symbol']:8} ${t['notional']:.2f}")
            self.log()
        else:
            self.log("🔄 今日无交易")
            self.log()
        
        # 市场状态
        regimes = self.load_regime_history(days=1)
        if regimes:
            current = regimes[-1]
            self.log(f"📈 市场状态")
            self.log(f"  当前: {current['regime']}")
            self.log(f"  乘数: {current['multiplier']:.2f}x")
            self.log()
        
        self.log("=" * 60)
    
    def generate_weekly_report(self):
        """生成周报"""
        self.log("=" * 60)
        self.log("📊 V5 交易周报")
        self.log("=" * 60)
        self.log(f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        self.log(f"报告周期: 最近7天")
        self.log()
        
        # 权益曲线
        equity_data = self.load_equity_data(days=7)
        if equity_data:
            start_eq = equity_data[0]['equity']
            end_eq = equity_data[-1]['equity']
            peak = max(p['equity'] for p in equity_data)
            trough = min(p['equity'] for p in equity_data)
            
            change = end_eq - start_eq
            change_pct = (change / start_eq * 100) if start_eq > 0 else 0
            max_dd = (trough - peak) / peak if peak > 0 else 0
            
            self.log(f"💰 权益表现")
            self.log(f"  周初: ${start_eq:.2f}")
            self.log(f"  周末: ${end_eq:.2f}")
            self.log(f"  变化: ${change:+.2f} ({change_pct:+.2f}%)")
            self.log(f"  最高: ${peak:.2f}")
            self.log(f"  最低: ${trough:.2f}")
            self.log(f"  最大回撤: {max_dd:.1%}")
            self.log()
        
        # 交易统计
        trades = self.load_trade_data(days=7)
        if trades:
            buy_count = sum(1 for t in trades if t['side'] == 'buy')
            sell_count = sum(1 for t in trades if t['side'] == 'sell')
            buy_value = sum(t['notional'] for t in trades if t['side'] == 'buy')
            sell_value = sum(t['notional'] for t in trades if t['side'] == 'sell')
            total_fee = sum(t['fee'] for t in trades)
            
            # 按币种统计
            symbol_stats = defaultdict(lambda: {'buy': 0, 'sell': 0})
            for t in trades:
                symbol_stats[t['symbol']][t['side']] += t['notional']
            
            self.log(f"🔄 交易统计")
            self.log(f"  总买入: {buy_count} 笔, ${buy_value:.2f}")
            self.log(f"  总卖出: {sell_count} 笔, ${sell_value:.2f}")
            self.log(f"  总手续费: ${total_fee:.4f}")
            self.log()
            
            self.log("  活跃币种（按交易额）:")
            for sym, stats in sorted(symbol_stats.items(), key=lambda x: x[1]['buy'] + x[1]['sell'], reverse=True)[:5]:
                self.log(f"    {sym:8} 买:${stats['buy']:8.2f} 卖:${stats['sell']:8.2f}")
            self.log()
        
        # 市场状态分布
        regimes = self.load_regime_history(days=7)
        if regimes:
            regime_counts = defaultdict(int)
            for r in regimes:
                regime_counts[r['regime']] += 1
            
            total = len(regimes)
            self.log(f"📈 市场状态分布（共{total}次检测）")
            for regime, count in sorted(regime_counts.items(), key=lambda x: x[1], reverse=True):
                pct = count / total * 100
                self.log(f"  {regime:12} {count:3}次 ({pct:5.1f}%)")
            self.log()
        
        self.log("=" * 60)
    
    def run(self, report_type='daily'):
        """运行报告生成"""
        if report_type == 'daily':
            self.generate_daily_report()
        elif report_type == 'weekly':
            self.generate_weekly_report()
        else:
            self.generate_daily_report()
            self.generate_weekly_report()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 交易报告生成')
    parser.add_argument('--type', choices=['daily', 'weekly', 'all'], default='daily', help='报告类型')
    args = parser.parse_args()
    
    generator = TradingReportGenerator()
    generator.run(report_type=args.type)


if __name__ == '__main__':
    main()
