#!/usr/bin/env python3
"""
V5 交易审计脚本 V2 - 智能版

改进:
1. 精确时间窗口过滤
2. 逻辑一致性检查
3. 异常模式识别
4. 报告与实际记录交叉验证
"""

import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
ORDERS_DB = Path('/home/admin/clawd/v5-trading-bot/reports/orders.sqlite')
LOG_FILE = Path('/home/admin/clawd/v5-trading-bot/logs/trade_audit_v2.log')
ALERT_FILE = Path('/home/admin/clawd/v5-trading-bot/logs/trade_alert_v2.json')

class SmartTradeAuditor:
    """智能交易审计器"""
    
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.insights = []
    
    def log(self, msg, level='INFO'):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] [{level}] {msg}"
        print(line)
        LOG_FILE.parent.mkdir(exist_ok=True)
        with open(LOG_FILE, 'a') as f:
            f.write(line + '\n')
    
    def get_orders_in_window(self, minutes=65):
        """
        获取精确时间窗口内的订单
        默认65分钟（覆盖整点前的5分钟到整点后60分钟）
        """
        if not ORDERS_DB.exists():
            return []
        
        conn = sqlite3.connect(str(ORDERS_DB))
        cursor = conn.cursor()
        
        # 计算时间窗口（毫秒时间戳）
        now = datetime.now()
        end_ts = int(now.timestamp() * 1000)
        start_ts = int((now - timedelta(minutes=minutes)).timestamp() * 1000)
        
        cursor.execute("""
            SELECT cl_ord_id, inst_id, side, state, intent, ord_id, 
                   last_error_code, last_error_msg, created_ts
            FROM orders 
            WHERE created_ts BETWEEN ? AND ?
            ORDER BY created_ts DESC
        """, (start_ts, end_ts))
        
        orders = cursor.fetchall()
        conn.close()
        
        return orders
    
    def analyze_orders(self, orders):
        """深度分析订单"""
        buy_filled = []
        sell_filled = []
        buy_rejected = []
        sell_rejected = []
        
        for order in orders:
            cl_ord_id, inst_id, side, state, intent, ord_id, err_code, err_msg, created_ts = order
            
            # 精确分类
            if side == 'buy':
                if state == 'FILLED':
                    buy_filled.append(order)
                elif state == 'REJECTED':
                    buy_rejected.append(order)
            elif side == 'sell':
                if state == 'FILLED':
                    sell_filled.append(order)
                elif state == 'REJECTED':
                    sell_rejected.append(order)
        
        return {
            'buy_filled': buy_filled,
            'sell_filled': sell_filled,
            'buy_rejected': buy_rejected,
            'sell_rejected': sell_rejected
        }
    
    def check_market_regime(self):
        """检查当前市场状态（优先最新 decision_audit，回退旧文件）"""
        # 1) 优先最新 run 的 decision_audit（与实盘执行口径一致）
        try:
            runs_dir = REPORTS_DIR / 'runs'
            if runs_dir.exists():
                run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
                run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                if run_dirs:
                    with open(run_dirs[0] / 'decision_audit.json', 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    details = data.get('regime_details') or {}
                    regime = details.get('final_state') or data.get('regime')
                    if regime:
                        return regime
        except Exception:
            pass

        # 2) 回退旧路径
        possible_paths = [
            REPORTS_DIR / 'regime_state.json',
            REPORTS_DIR / 'regime.json',
            Path('/home/admin/clawd/v5-trading-bot/reports/regime.json'),
        ]

        for regime_file in possible_paths:
            if regime_file.exists():
                try:
                    with open(regime_file) as f:
                        data = json.load(f)
                        regime = data.get('regime') or data.get('state') or data.get('current_regime')
                        if regime:
                            return regime
                except Exception:
                    continue

        return 'Unknown'
    
    def validate_logic(self, analysis, regime):
        """
        逻辑一致性验证
        这是智能审计的核心！
        """
        buy_filled = analysis['buy_filled']
        sell_filled = analysis['sell_filled']

        regime_norm = str(regime or '').upper().replace('-', '_')

        # 检查1: Risk-Off状态下的买入
        if regime_norm == 'RISK_OFF' and len(buy_filled) > 0:
            self.warnings.append({
                'level': 'HIGH',
                'type': 'regime_conflict',
                'message': f'⚠️ Risk-Off状态下出现{len(buy_filled)}笔买入！',
                'details': [f"{o[1]} ({o[4]})" for o in buy_filled],
                'suggestion': '检查是否为REBALANCE（已有持仓调整）或配置错误'
            })
        
        # 检查2: 全是卖出没有买入（可能的大清仓）
        if len(sell_filled) > 5 and len(buy_filled) == 0:
            self.insights.append({
                'level': 'INFO',
                'type': 'mass_liquidation',
                'message': f'📉 纯卖出模式：{len(sell_filled)}笔卖出，0笔买入',
                'interpretation': '可能触发止损或Risk-Off减仓'
            })
        
        # 检查3: 买入卖出都有（活跃交易）
        if len(buy_filled) > 0 and len(sell_filled) > 0:
            self.insights.append({
                'level': 'INFO',
                'type': 'active_trading',
                'message': f'🔄 双向交易：{len(buy_filled)}笔买入，{len(sell_filled)}笔卖出',
                'interpretation': '市场震荡，策略在调仓'
            })
        
        # 检查4: 大量REJECTED
        total_rejected = len(analysis['buy_rejected']) + len(analysis['sell_rejected'])
        if total_rejected >= 15:
            all_rejected = analysis['buy_rejected'] + analysis['sell_rejected']
            dust_skip_count = sum(1 for o in all_rejected
                                  if 'dust' in str(o[7]).lower() or '51020' in str(o[6]))
            if dust_skip_count >= 10:
                self.insights.append({
                    'level': 'INFO',
                    'type': 'dust_cleanup',
                    'message': f'🧹 灰尘/最小下单限制：{dust_skip_count}笔被系统拦截',
                    'interpretation': '多数是交易所最小下单或微量残留导致，属于保护性拒单'
                })
    
    def check_risk_controls(self):
        """检查风控状态"""
        issues = []
        
        # 检查 kill_switch
        kill_switch = REPORTS_DIR / 'kill_switch.json'
        if kill_switch.exists():
            with open(kill_switch) as f:
                ks = json.load(f)
                if ks.get('enabled'):
                    issues.append({
                        'level': 'CRITICAL',
                        'message': f"🚨 Kill Switch 已启用: {ks.get('reason', 'unknown')}"
                    })
        
        # 检查 reconcile 状态
        reconcile = REPORTS_DIR / 'reconcile_status.json'
        if reconcile.exists():
            with open(reconcile) as f:
                rc = json.load(f)
                if not rc.get('ok'):
                    issues.append({
                        'level': 'WARNING',
                        'message': f"⚠️ 对账异常: {rc.get('reason', 'unknown')}"
                    })
        
        return issues
    
    def generate_report(self, analysis, regime):
        """生成审计报告"""
        buy_filled = analysis['buy_filled']
        sell_filled = analysis['sell_filled']
        buy_rejected = analysis['buy_rejected']
        sell_rejected = analysis['sell_rejected']
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'market_regime': regime,
            'summary': {
                'buy_filled': len(buy_filled),
                'sell_filled': len(sell_filled),
                'buy_rejected': len(buy_rejected),
                'sell_rejected': len(sell_rejected),
                'total': len(buy_filled) + len(sell_filled) + len(buy_rejected) + len(sell_rejected)
            },
            'issues': self.issues,
            'warnings': self.warnings,
            'insights': self.insights
        }
        
        return report
    
    def print_report(self, report):
        """打印报告"""
        self.log("=" * 70)
        self.log("🤖 V5 智能交易审计报告 V2")
        self.log("=" * 70)
        
        # 市场状态
        regime = report['market_regime']
        self.log(f"\n📊 市场状态: {regime}")
        
        # 交易统计
        s = report['summary']
        self.log(f"\n📈 交易统计:")
        self.log(f"  ✅ 买入成交: {s['buy_filled']} 笔")
        self.log(f"  ✅ 卖出成交: {s['sell_filled']} 笔")
        self.log(f"  ❌ 买入拒绝: {s['buy_rejected']} 笔")
        self.log(f"  ❌ 卖出拒绝: {s['sell_rejected']} 笔")
        
        # 重要发现
        if self.insights:
            self.log(f"\n💡 智能分析:")
            for insight in self.insights:
                self.log(f"  {insight['message']}")
                if 'interpretation' in insight:
                    self.log(f"     → {insight['interpretation']}")
        
        # 警告
        if self.warnings:
            self.log(f"\n⚠️  警告 ({len(self.warnings)}):")
            for warning in self.warnings:
                self.log(f"  [{warning['level']}] {warning['message']}")
                if 'suggestion' in warning:
                    self.log(f"     💡 建议: {warning['suggestion']}")
        
        # 问题
        if self.issues:
            self.log(f"\n🚨 问题 ({len(self.issues)}):")
            for issue in self.issues:
                self.log(f"  [{issue['level']}] {issue['message']}")
        
        # 结论
        if not self.warnings and not self.issues:
            self.log(f"\n✅ 审计通过，无异常")
        elif not self.issues:
            self.log(f"\n⚠️  审计完成，有警告但无严重问题")
        else:
            self.log(f"\n🚨 审计发现严重问题，需要人工介入")
        
        self.log("=" * 70)
    
    def run(self):
        """运行审计"""
        self.log("🔍 智能交易审计启动...")
        
        # 1. 获取订单（精确时间窗口）
        orders = self.get_orders_in_window(minutes=65)
        if not orders:
            self.log("ℹ️ 时间窗口内无交易记录")
            return
        
        self.log(f"分析最近65分钟内 {len(orders)} 笔订单")
        
        # 2. 分析订单
        analysis = self.analyze_orders(orders)
        
        # 3. 检查市场状态
        regime = self.check_market_regime()
        
        # 4. 逻辑验证（核心！）
        self.validate_logic(analysis, regime)
        
        # 5. 检查风控
        risk_issues = self.check_risk_controls()
        self.issues.extend(risk_issues)
        
        # 6. 生成报告
        report = self.generate_report(analysis, regime)
        
        # 7. 打印报告
        self.print_report(report)
        
        # 8. 保存详细报告
        ALERT_FILE.parent.mkdir(exist_ok=True)
        with open(ALERT_FILE, 'w') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        return report


def main():
    auditor = SmartTradeAuditor()
    auditor.run()


if __name__ == '__main__':
    main()
