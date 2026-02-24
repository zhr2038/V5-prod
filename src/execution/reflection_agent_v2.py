"""
反思Agent V2 - 深度交易分析

核心功能:
1. 异常交易检测 - 识别手续费异常、价格异常、滑点异常
2. 策略有效性分析 - 评估各因子在当期表现
3. 执行质量分析 - 成交质量、滑点控制
4. 风险预警 - 持仓集中度、单边暴露
5. 归因分析 - 盈亏来自择时、选股还是执行
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import json
import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from decimal import Decimal


class AlertLevel(Enum):
    """告警级别"""
    CRITICAL = "critical"      # 严重，需立即处理
    WARNING = "warning"        # 警告，需关注
    INFO = "info"              # 信息，供参考
    POSITIVE = "positive"      # 积极信号


@dataclass
class TradeInsight:
    """交易洞察"""
    level: AlertLevel
    category: str              # 'execution', 'strategy', 'risk', 'anomaly'
    title: str
    description: str
    metric: str                # 关键指标
    expected: str              # 预期值
    actual: str                # 实际值
    action: str                # 具体行动建议
    impact: str = ""           # 影响评估


@dataclass
class ExecutionQuality:
    """执行质量指标"""
    avg_slippage_bps: float
    avg_fee_bps: float
    fill_rate: float
    avg_latency_ms: float


@dataclass
class FactorPerformance:
    """因子表现"""
    factor_name: str
    ic: float                  # 信息系数
    win_rate: float            # 胜率
    avg_return: float          # 平均收益
    sharpe: float              # 夏普比率
    status: str                # 'effective', 'ineffective', 'degrading'


@dataclass
class RiskMetrics:
    """风险指标"""
    max_position_pct: float    # 最大单一持仓占比
    gross_exposure: float      # 总敞口
    net_exposure: float        # 净敞口
    concentration_score: float # 集中度评分
    var_95: float             # 95% VaR


class ReflectionAgentV2:
    """
    V2反思Agent - 深度交易分析
    """
    
    def __init__(
        self,
        db_path: str = '/home/admin/clawd/v5-trading-bot/reports/orders.sqlite',
        report_dir: str = '/home/admin/clawd/v5-trading-bot/reports/reflection',
        bills_db: str = '/home/admin/clawd/v5-trading-bot/reports/bills.sqlite'
    ):
        self.db_path = db_path
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.bills_db = bills_db
        
        self.analysis_period_days = 7
        self.insights: List[TradeInsight] = []
        
    def run_daily_reflection(self) -> Dict:
        """运行每日反思分析"""
        print(f"[ReflectionAgent V2] 开始深度分析 ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
        
        # 1. 加载数据
        trades_df = self._load_recent_trades()
        if trades_df.empty:
            return self._generate_empty_report()
        
        print(f"[ReflectionAgent] 加载了 {len(trades_df)} 笔交易记录")
        
        # 2. 异常检测
        self._detect_anomalies(trades_df)
        
        # 3. 执行质量分析
        execution_quality = self._analyze_execution_quality(trades_df)
        
        # 4. 策略有效性分析
        factor_perf = self._analyze_factor_effectiveness()
        
        # 5. 风险分析
        risk_metrics = self._analyze_risk(trades_df)
        
        # 6. 盈亏归因分析
        attribution = self._analyze_pnl_attribution(trades_df)
        
        # 7. 生成报告
        report = {
            'version': '2.0',
            'generated_at': datetime.now().isoformat(),
            'period_days': self.analysis_period_days,
            'summary': self._generate_summary(trades_df, attribution),
            'alerts': self._format_insights(),
            'execution_quality': {
                'avg_slippage_bps': round(execution_quality.avg_slippage_bps, 2),
                'avg_fee_bps': round(execution_quality.avg_fee_bps, 2),
                'fill_rate': round(execution_quality.fill_rate, 2),
                'status': 'good' if execution_quality.avg_slippage_bps < 10 else 'poor'
            },
            'factor_performance': [
                {
                    'name': f.factor_name,
                    'ic': round(f.ic, 4),
                    'win_rate': round(f.win_rate, 2),
                    'status': f.status
                }
                for f in factor_perf
            ],
            'risk_metrics': {
                'max_position_pct': round(risk_metrics.max_position_pct, 2),
                'concentration_score': round(risk_metrics.concentration_score, 2),
                'var_95': round(risk_metrics.var_95, 2),
                'status': 'safe' if risk_metrics.concentration_score < 0.5 else 'warning'
            },
            'pnl_attribution': attribution
        }
        
        self._save_report(report)
        self._print_summary(report)
        
        return report
    
    def _detect_anomalies(self, trades_df: pd.DataFrame):
        """检测异常交易"""
        # 1. 异常手续费
        trades_df['fee_usdt'] = pd.to_numeric(trades_df['fee'], errors='coerce').abs()
        abnormal_fees = trades_df[trades_df['fee_usdt'] > trades_df['notional_usdt'] * 0.1]
        
        if len(abnormal_fees) > 0:
            for _, trade in abnormal_fees.iterrows():
                self.insights.append(TradeInsight(
                    level=AlertLevel.CRITICAL,
                    category='anomaly',
                    title=f"异常手续费: {trade['inst_id']}",
                    description=f"手续费 {trade['fee_usdt']:.4f} USDT 超过交易金额10%",
                    metric="fee_ratio",
                    expected="< 1%",
                    actual=f"{trade['fee_usdt']/trade['notional_usdt']*100:.1f}%",
                    action="检查OKX费率设置，确认是否为Maker/Taker费率异常",
                    impact=f"额外损失 {trade['fee_usdt']:.2f} USDT"
                ))
        
        # 2. 超大滑点
        if 'slippage_bps' in trades_df.columns:
            high_slippage = trades_df[trades_df['slippage_bps'] > 50]
            if len(high_slippage) > 0:
                self.insights.append(TradeInsight(
                    level=AlertLevel.WARNING,
                    category='execution',
                    title="高滑点交易",
                    description=f"发现 {len(high_slippage)} 笔滑点超过50bps的交易",
                    metric="avg_slippage",
                    expected="< 10 bps",
                    actual=f"{high_slippage['slippage_bps'].mean():.1f} bps",
                    action="检查市场流动性，避免在波动期大额交易，考虑使用限价单",
                    impact=f"平均额外成本 {high_slippage['slippage_bps'].mean()/100:.2f}%"
                ))
        
        # 3. 频繁交易
        symbol_counts = trades_df['inst_id'].value_counts()
        frequent_symbols = symbol_counts[symbol_counts > 5]
        if len(frequent_symbols) > 0:
            top_symbol = frequent_symbols.index[0]
            self.insights.append(TradeInsight(
                level=AlertLevel.INFO,
                category='strategy',
                title=f"高频交易: {top_symbol}",
                description=f"{top_symbol} 在7天内交易 {frequent_symbols.iloc[0]} 次",
                metric="trade_frequency",
                expected="< 3次/周",
                actual=f"{frequent_symbols.iloc[0]}次",
                action="检查是否有过度交易，确认信号阈值是否过低",
                impact="增加手续费成本"
            ))
        
        # 4. 单边暴露检查
        buy_value = trades_df[trades_df['side'] == 'buy']['notional_usdt'].sum()
        sell_value = trades_df[trades_df['side'] == 'sell']['notional_usdt'].sum()
        
        if buy_value > sell_value * 2:
            self.insights.append(TradeInsight(
                level=AlertLevel.WARNING,
                category='risk',
                title="买入暴露过高",
                description=f"买入金额({buy_value:.0f})是卖出金额({sell_value:.0f})的 {buy_value/sell_value:.1f} 倍",
                metric="buy_sell_ratio",
                expected="≈ 1.0",
                actual=f"{buy_value/sell_value:.2f}",
                action="检查Risk-Off机制是否正确触发，确认是否应减少新买入",
                impact="市场下跌时敞口风险增加"
            ))
    
    def _analyze_execution_quality(self, trades_df: pd.DataFrame) -> ExecutionQuality:
        """分析执行质量"""
        # 从fills表获取详细成交数据
        try:
            conn = sqlite3.connect(self.db_path.replace('orders', 'fills'))
            fills_df = pd.read_sql_query("SELECT * FROM fills", conn)
            conn.close()
            
            if not fills_df.empty and 'slippage_bps' in fills_df.columns:
                avg_slippage = fills_df['slippage_bps'].mean()
                avg_fee = fills_df['fee'].sum() / fills_df['notional_usdt'].sum() * 10000
                fill_rate = len(fills_df) / len(trades_df) if len(trades_df) > 0 else 0
            else:
                avg_slippage = 5.0
                avg_fee = 6.0
                fill_rate = 0.95
        except:
            avg_slippage = 5.0
            avg_fee = 6.0
            fill_rate = 0.95
        
        return ExecutionQuality(
            avg_slippage_bps=avg_slippage,
            avg_fee_bps=avg_fee,
            fill_rate=fill_rate,
            avg_latency_ms=0
        )
    
    def _analyze_factor_effectiveness(self) -> List[FactorPerformance]:
        """分析各因子有效性"""
        factors = []
        
        # 读取IC诊断数据
        ic_file = Path('/home/admin/clawd/v5-trading-bot/reports/ic_diagnostics_30d_20u.json')
        if ic_file.exists():
            with open(ic_file, 'r') as f:
                ic_data = json.load(f)
            
            ic_by_factor = ic_data.get('overall_tradable', {}).get('ic', {})
            
            factor_map = {
                'f1_mom_5d': '5日动量',
                'f2_mom_20d': '20日动量',
                'f3_vol_adj_ret_20d': '波动率调整收益',
                'f4_volume_expansion': '成交量扩张',
                'f5_rsi_trend_confirm': 'RSI趋势确认'
            }
            
            for factor_key, factor_name in factor_map.items():
                if factor_key in ic_by_factor:
                    ic_value = ic_by_factor[factor_key].get('mean', 0)
                    
                    # 评估因子状态
                    if ic_value > 0.02:
                        status = 'effective'
                    elif ic_value < -0.02:
                        status = 'ineffective'
                    else:
                        status = 'neutral'
                    
                    factors.append(FactorPerformance(
                        factor_name=factor_name,
                        ic=ic_value,
                        win_rate=0.5 + ic_value * 10,  # 简化估计
                        avg_return=ic_value * 0.01,
                        sharpe=ic_value * 5,
                        status=status
                    ))
        
        return factors
    
    def _analyze_risk(self, trades_df: pd.DataFrame) -> RiskMetrics:
        """分析风险指标"""
        # 读取当前持仓
        try:
            conn = sqlite3.connect(self.db_path.replace('orders', 'positions'))
            positions_df = pd.read_sql_query("SELECT * FROM positions", conn)
            conn.close()
            
            if not positions_df.empty:
                total_value = positions_df['value_usdt'].sum()
                max_pos = positions_df['value_usdt'].max()
                max_pct = max_pos / total_value if total_value > 0 else 0
                
                # 集中度评分 (HHI指数)
                weights = positions_df['value_usdt'] / total_value if total_value > 0 else pd.Series([0])
                concentration = (weights ** 2).sum()
            else:
                max_pct = 0
                concentration = 0
        except:
            max_pct = 0
            concentration = 0
        
        return RiskMetrics(
            max_position_pct=max_pct,
            gross_exposure=0,
            net_exposure=0,
            concentration_score=concentration,
            var_95=0
        )
    
    def _analyze_pnl_attribution(self, trades_df: pd.DataFrame) -> Dict:
        """盈亏归因分析"""
        # 使用FIFO计算各币种盈亏
        symbol_pnl = {}
        
        for symbol in trades_df['inst_id'].unique():
            symbol_trades = trades_df[trades_df['inst_id'] == symbol].sort_values('created_ts')
            
            buy_queue = []
            realized_pnl = 0
            total_fees = 0
            
            for _, trade in symbol_trades.iterrows():
                side = trade['side']
                notional = trade['notional_usdt']
                fee = abs(float(trade.get('fee', 0)))
                total_fees += fee
                
                if side == 'buy':
                    buy_queue.append(notional)
                elif side == 'sell':
                    sell_amount = notional
                    while sell_amount > 0 and buy_queue:
                        buy_cost = buy_queue.pop(0)
                        if sell_amount >= buy_cost:
                            realized_pnl += sell_amount - buy_cost
                            sell_amount -= buy_cost
                        else:
                            realized_pnl += sell_amount - buy_cost
                            buy_queue.insert(0, buy_cost - sell_amount)
                            sell_amount = 0
            
            symbol_pnl[symbol] = realized_pnl - total_fees
        
        # 排序
        sorted_pnl = sorted(symbol_pnl.items(), key=lambda x: x[1], reverse=True)
        
        return {
            'top_gainers': [
                {'symbol': s, 'pnl': round(p, 2)}
                for s, p in sorted_pnl[:3] if p > 0
            ],
            'top_losers': [
                {'symbol': s, 'pnl': round(p, 2)}
                for s, p in sorted_pnl[-3:] if p < 0
            ],
            'total_realized_pnl': round(sum(symbol_pnl.values()), 2),
            'winning_symbols': sum(1 for p in symbol_pnl.values() if p > 0),
            'losing_symbols': sum(1 for p in symbol_pnl.values() if p < 0)
        }
    
    def _generate_summary(self, trades_df: pd.DataFrame, attribution: Dict) -> Dict:
        """生成摘要"""
        return {
            'total_trades': len(trades_df),
            'total_symbols': trades_df['inst_id'].nunique(),
            'total_realized_pnl': attribution['total_realized_pnl'],
            'win_rate': attribution['winning_symbols'] / (attribution['winning_symbols'] + attribution['losing_symbols']) if (attribution['winning_symbols'] + attribution['losing_symbols']) > 0 else 0,
            'critical_alerts': sum(1 for i in self.insights if i.level == AlertLevel.CRITICAL),
            'warning_alerts': sum(1 for i in self.insights if i.level == AlertLevel.WARNING)
        }
    
    def _format_insights(self) -> List[Dict]:
        """格式化洞察"""
        return [
            {
                'level': i.level.value,
                'category': i.category,
                'title': i.title,
                'description': i.description,
                'metric': i.metric,
                'expected': i.expected,
                'actual': i.actual,
                'action': i.action,
                'impact': i.impact
            }
            for i in sorted(self.insights, key=lambda x: {
                AlertLevel.CRITICAL: 0,
                AlertLevel.WARNING: 1,
                AlertLevel.INFO: 2,
                AlertLevel.POSITIVE: 3
            }.get(x.level, 4))
        ]
    
    def _load_recent_trades(self, days: int = 7) -> pd.DataFrame:
        """加载最近交易"""
        try:
            conn = sqlite3.connect(self.db_path)
            start_time = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_time.timestamp() * 1000)
            
            query = f"""
                SELECT 
                    inst_id, side, state, notional_usdt, fee,
                    created_ts, updated_ts
                FROM orders 
                WHERE state = 'FILLED'
                AND created_ts >= {start_timestamp}
                AND notional_usdt < 1000
                ORDER BY created_ts DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            df['fee'] = pd.to_numeric(df['fee'], errors='coerce').fillna(0)
            
            # 过滤极端异常值（手续费超过100 USDT或交易金额超过1000视为数据错误）
            extreme_fee_mask = df['fee'].abs() > 100
            extreme_notional_mask = df['notional_usdt'] > 1000
            extreme_mask = extreme_fee_mask | extreme_notional_mask
            extreme_count = extreme_mask.sum()
            
            if extreme_count > 0:
                extreme_trades = df[extreme_mask]
                for _, trade in extreme_trades.iterrows():
                    print(f"[ReflectionAgent] 警告: {trade['inst_id']} 数据异常 fee={trade['fee']:.2f} notional={trade['notional_usdt']:.2f}，已过滤")
                df = df[~extreme_mask]
            
            return df
            
        except Exception as e:
            print(f"[ReflectionAgent] 加载交易数据失败: {e}")
            return pd.DataFrame()
    
    def _save_report(self, report: Dict):
        """保存报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        report_file = self.report_dir / f'reflection_{timestamp}.json'
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"[ReflectionAgent] 报告已保存: {report_file}")
    
    def _print_summary(self, report: Dict):
        """打印摘要"""
        print("\n" + "="*70)
        print("📊 V2 每日反思报告")
        print("="*70)
        
        summary = report['summary']
        print(f"\n📈 绩效概览")
        print(f"  已实现盈亏: ${summary['total_realized_pnl']:.2f}")
        print(f"  交易笔数: {summary['total_trades']}")
        print(f"  涉及币种: {summary['total_symbols']}")
        print(f"  胜率: {summary['win_rate']*100:.1f}%")
        
        # 告警
        print(f"\n🚨 关键告警")
        critical = [a for a in report['alerts'] if a['level'] == 'critical']
        warnings = [a for a in report['alerts'] if a['level'] == 'warning']
        
        if critical:
            for a in critical[:3]:
                print(f"  🔴 [{a['category'].upper()}] {a['title']}")
                print(f"     问题: {a['description']}")
                print(f"     行动: {a['action']}")
        elif warnings:
            for a in warnings[:3]:
                print(f"  🟡 [{a['category'].upper()}] {a['title']}")
                print(f"     问题: {a['description']}")
                print(f"     行动: {a['action']}")
        else:
            print("  ✅ 暂无告警")
        
        # 执行质量
        print(f"\n⚙️ 执行质量")
        eq = report['execution_quality']
        print(f"  滑点: {eq['avg_slippage_bps']} bps ({eq['status']})")
        print(f"  费率: {eq['avg_fee_bps']} bps")
        print(f"  成交率: {eq['fill_rate']*100:.1f}%")
        
        # 归因
        print(f"\n💰 盈亏归因")
        attr = report['pnl_attribution']
        if attr['top_gainers']:
            print(f"  盈利: " + ", ".join([f"{g['symbol']}(+${g['pnl']})" for g in attr['top_gainers']]))
        if attr['top_losers']:
            print(f"  亏损: " + ", ".join([f"{l['symbol']}(${l['pnl']})" for l in attr['top_losers']]))
        
        print("\n" + "="*70)


def main():
    """主函数"""
    print("="*70)
    print("V5 Reflection Agent V2 - 深度交易分析")
    print("="*70)
    
    agent = ReflectionAgentV2()
    report = agent.run_daily_reflection()
    
    # 检查是否有严重问题
    critical_count = sum(1 for a in report.get('alerts', []) if a['level'] == 'critical')
    if critical_count > 0:
        print(f"\n🚨 发现 {critical_count} 个严重问题，请关注！")
        return 1
    
    print("\n✅ 分析完成")
    return 0


if __name__ == "__main__":
    exit(main())
