"""
反思Agent - Reflection Agent

职责:
1. 定期分析交易记录，识别盈亏归因
2. 评估策略/因子有效性
3. 生成优化建议
4. 支持自动参数调优

运行频率: 每日/每周 (通过systemd timer)
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


class InsightType(Enum):
    """洞察类型"""
    STRONG_PERFORMER = "strong_performer"      # 表现优秀
    UNDER_PERFORMER = "under_performer"        # 表现不佳
    FACTOR_DECAY = "factor_decay"              # 因子失效
    RISK_CONCENTRATION = "risk_concentration"  # 风险集中
    OPPORTUNITY = "opportunity"                # 潜在机会
    ANOMALY = "anomaly"                        # 异常检测


@dataclass
class TradingInsight:
    """交易洞察"""
    insight_type: InsightType
    title: str
    description: str
    severity: str  # 'high', 'medium', 'low'
    metric_value: float
    benchmark: float
    recommendation: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict = field(default_factory=dict)


@dataclass
class StrategyDiagnosis:
    """策略诊断报告"""
    strategy_name: str
    period_days: int
    total_trades: int
    win_rate: float
    avg_profit: float
    avg_loss: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    insights: List[TradingInsight] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class CoinPerformance:
    """币种绩效"""
    symbol: str
    total_pnl: float
    trade_count: int
    win_rate: float
    avg_hold_time: timedelta
    best_strategy: str
    worst_strategy: str


class ReflectionAgent:
    """
    反思Agent - 交易后分析与优化建议
    """
    
    def __init__(
        self,
        db_path: str = '/home/admin/clawd/v5-trading-bot/reports/orders.sqlite',
        report_dir: str = '/home/admin/clawd/v5-trading-bot/reports/reflection'
    ):
        self.db_path = db_path
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # 分析参数
        self.analysis_period_days = 7  # 默认分析最近7天
        self.win_rate_threshold = 0.4  # 胜率低于40%认为有问题
        self.profit_factor_threshold = 1.0  # 盈亏比低于1认为有问题
        self.max_drawdown_threshold = 0.15  # 最大回撤15%警戒线
        
    def run_daily_reflection(self) -> Dict:
        """运行每日反思分析"""
        print(f"[ReflectionAgent] 开始每日反思分析 ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
        
        # 1. 加载交易数据
        trades_df = self._load_recent_trades(days=self.analysis_period_days)
        if trades_df.empty:
            print("[ReflectionAgent] 没有找到交易记录")
            return self._generate_empty_report()
        
        print(f"[ReflectionAgent] 加载了 {len(trades_df)} 笔交易记录")
        
        # 2. 分析整体绩效
        overall_metrics = self._calculate_overall_metrics(trades_df)
        
        # 3. 分析各币种绩效
        coin_performance = self._analyze_coin_performance(trades_df)
        
        # 4. 分析策略绩效
        strategy_diagnosis = self._diagnose_strategies(trades_df)
        
        # 5. 生成洞察
        insights = self._generate_insights(
            trades_df, overall_metrics, coin_performance, strategy_diagnosis
        )
        
        # 6. 生成建议
        recommendations = self._generate_recommendations(insights, strategy_diagnosis)
        
        # 7. 生成报告
        report = self._compile_report(
            overall_metrics, coin_performance, strategy_diagnosis, 
            insights, recommendations
        )
        
        # 8. 保存报告
        self._save_report(report)
        
        # 9. 输出摘要
        self._print_summary(report)
        
        return report
    
    def _load_recent_trades(self, days: int = 7) -> pd.DataFrame:
        """从数据库加载最近交易"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 计算起始时间
            start_time = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_time.timestamp() * 1000)
            
            query = f"""
                SELECT 
                    inst_id, side, state, notional_usdt, fee, 
                    created_ts, updated_ts
                FROM orders 
                WHERE state = 'FILLED'
                AND created_ts >= {start_timestamp}
                AND notional_usdt < 1000  -- 排除异常数据
                ORDER BY created_ts DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if not df.empty:
                df['created_dt'] = pd.to_datetime(df['created_ts'], unit='ms')
                df['updated_dt'] = pd.to_datetime(df['updated_ts'], unit='ms')
            
            return df
            
        except Exception as e:
            print(f"[ReflectionAgent] 加载交易数据失败: {e}")
            return pd.DataFrame()
    
    def _calculate_overall_metrics(self, trades_df: pd.DataFrame) -> Dict:
        """计算整体绩效指标"""
        if trades_df.empty:
            return {}
        
        # 转换数据类型
        trades_df['fee'] = pd.to_numeric(trades_df['fee'], errors='coerce').fillna(0)
        
        # 计算每笔交易的盈亏（简化计算）
        trades_df['net_flow'] = trades_df.apply(
            lambda row: row['notional_usdt'] if row['side'] == 'sell' else -row['notional_usdt'],
            axis=1
        )
        trades_df['net_flow'] = trades_df['net_flow'] - trades_df['fee']
        
        # 按币种聚合计算实际盈亏
        symbol_pnl = {}
        for symbol in trades_df['inst_id'].unique():
            symbol_trades = trades_df[trades_df['inst_id'] == symbol].sort_values('created_ts')
            if len(symbol_trades) >= 2:
                # 简化为：卖出总额 - 买入总额
                buys = symbol_trades[symbol_trades['side'] == 'buy']['notional_usdt'].sum()
                sells = symbol_trades[symbol_trades['side'] == 'sell']['notional_usdt'].sum()
                fees = symbol_trades['fee'].sum()
                pnl = sells - buys - fees
                symbol_pnl[symbol] = pnl
        
        total_pnl = sum(symbol_pnl.values())
        winning_symbols = sum(1 for pnl in symbol_pnl.values() if pnl > 0)
        total_symbols = len(symbol_pnl)
        
        metrics = {
            'total_trades': len(trades_df),
            'unique_symbols': total_symbols,
            'total_pnl': total_pnl,
            'winning_symbols': winning_symbols,
            'win_rate_symbols': winning_symbols / total_symbols if total_symbols > 0 else 0,
            'symbol_pnl': symbol_pnl,
            'avg_trade_size': trades_df['notional_usdt'].mean(),
            'total_fees': trades_df['fee'].sum(),
            'period_days': self.analysis_period_days
        }
        
        return metrics
    
    def _analyze_coin_performance(self, trades_df: pd.DataFrame) -> List[CoinPerformance]:
        """分析各币种绩效"""
        if trades_df.empty:
            return []
        
        performances = []
        
        for symbol in trades_df['inst_id'].unique():
            symbol_trades = trades_df[trades_df['inst_id'] == symbol]
            
            if len(symbol_trades) < 2:
                continue
            
            # 计算盈亏
            buys = symbol_trades[symbol_trades['side'] == 'buy']
            sells = symbol_trades[symbol_trades['side'] == 'sell']
            
            if buys.empty or sells.empty:
                continue
            
            total_buy = buys['notional_usdt'].sum()
            total_sell = sells['notional_usdt'].sum()
            total_fee = symbol_trades['fee'].sum()
            pnl = total_sell - total_buy - total_fee
            
            # 计算胜率（按卖出次数）
            # 简化：假设每次卖出对应一次买入
            sell_count = len(sells)
            # 这里简化处理，实际需要更复杂的匹配逻辑
            
            performance = CoinPerformance(
                symbol=symbol,
                total_pnl=pnl,
                trade_count=len(symbol_trades),
                win_rate=0.5,  # 简化
                avg_hold_time=timedelta(hours=12),  # 简化
                best_strategy="unknown",
                worst_strategy="unknown"
            )
            performances.append(performance)
        
        # 按盈亏排序
        performances.sort(key=lambda x: x.total_pnl, reverse=True)
        return performances
    
    def _diagnose_strategies(self, trades_df: pd.DataFrame) -> List[StrategyDiagnosis]:
        """诊断各策略表现"""
        # 简化：目前只有一个主策略，将来可以按tag区分
        
        diagnosis = StrategyDiagnosis(
            strategy_name="V5_Multi_Factor",
            period_days=self.analysis_period_days,
            total_trades=len(trades_df),
            win_rate=0.0,
            avg_profit=0.0,
            avg_loss=0.0,
            profit_factor=0.0,
            max_drawdown=0.0,
            sharpe_ratio=0.0,
            insights=[],
            recommendations=[]
        )
        
        return [diagnosis]
    
    def _generate_insights(
        self,
        trades_df: pd.DataFrame,
        overall_metrics: Dict,
        coin_performance: List[CoinPerformance],
        strategy_diagnosis: List[StrategyDiagnosis]
    ) -> List[TradingInsight]:
        """生成交易洞察"""
        insights = []
        
        # 1. 整体盈亏洞察
        if overall_metrics.get('total_pnl', 0) > 0:
            insights.append(TradingInsight(
                insight_type=InsightType.STRONG_PERFORMER,
                title="整体盈利",
                description=f"最近{self.analysis_period_days}天总盈利 ${overall_metrics['total_pnl']:.2f}",
                severity="low",
                metric_value=overall_metrics['total_pnl'],
                benchmark=0,
                recommendation="保持当前策略，关注盈利最大回撤"
            ))
        else:
            insights.append(TradingInsight(
                insight_type=InsightType.UNDER_PERFORMER,
                title="整体亏损",
                description=f"最近{self.analysis_period_days}天总亏损 ${abs(overall_metrics['total_pnl']):.2f}",
                severity="high",
                metric_value=overall_metrics['total_pnl'],
                benchmark=0,
                recommendation="检查Risk-Off触发频率，考虑降低仓位或暂停交易"
            ))
        
        # 2. 币种表现洞察
        if coin_performance:
            best = coin_performance[0]
            worst = coin_performance[-1]
            
            insights.append(TradingInsight(
                insight_type=InsightType.STRONG_PERFORMER,
                title=f"最佳表现: {best.symbol}",
                description=f"盈利 ${best.total_pnl:.2f}, {best.trade_count}笔交易",
                severity="low",
                metric_value=best.total_pnl,
                benchmark=0,
                recommendation=f"考虑增加{best.symbol}的权重或关注类似币种"
            ))
            
            if worst.total_pnl < 0:
                insights.append(TradingInsight(
                    insight_type=InsightType.UNDER_PERFORMER,
                    title=f"最差表现: {worst.symbol}",
                    description=f"亏损 ${abs(worst.total_pnl):.2f}, {worst.trade_count}笔交易",
                    severity="medium" if worst.total_pnl > -5 else "high",
                    metric_value=worst.total_pnl,
                    benchmark=0,
                    recommendation=f"考虑将{worst.symbol}加入黑名单或降低权重"
                ))
        
        # 3. 手续费洞察
        fee_ratio = overall_metrics.get('total_fees', 0) / abs(overall_metrics.get('total_pnl', 1))
        if fee_ratio > 0.1:  # 手续费占盈亏10%以上
            insights.append(TradingInsight(
                insight_type=InsightType.ANOMALY,
                title="手续费占比过高",
                description=f"手续费 ${overall_metrics['total_fees']:.2f} 占盈亏的 {fee_ratio*100:.1f}%",
                severity="medium",
                metric_value=fee_ratio,
                benchmark=0.05,
                recommendation="减少交易频率，或使用Maker订单降低手续费"
            ))
        
        # 4. 风险集中洞察
        if coin_performance and len(coin_performance) >= 3:
            total_pnl = sum(c.total_pnl for c in coin_performance)
            top3_pnl = sum(c.total_pnl for c in coin_performance[:3])
            concentration = abs(top3_pnl / total_pnl) if total_pnl != 0 else 0
            
            if concentration > 0.8:
                insights.append(TradingInsight(
                    insight_type=InsightType.RISK_CONCENTRATION,
                    title="盈亏过度集中",
                    description=f"前3个币种贡献了{concentration*100:.1f}%的盈亏",
                    severity="medium",
                    metric_value=concentration,
                    benchmark=0.6,
                    recommendation="考虑分散持仓，降低单一币种风险敞口"
                ))
        
        return insights
    
    def _generate_recommendations(
        self,
        insights: List[TradingInsight],
        strategy_diagnosis: List[StrategyDiagnosis]
    ) -> List[str]:
        """生成优化建议"""
        recommendations = []
        
        # 按严重程度排序
        high_severity = [i for i in insights if i.severity == 'high']
        medium_severity = [i for i in insights if i.severity == 'medium']
        
        # 高优先级建议
        for insight in high_severity:
            recommendations.append(f"【紧急】{insight.recommendation}")
        
        # 中优先级建议
        for insight in medium_severity[:3]:  # 最多3个中优先级
            recommendations.append(f"【建议】{insight.recommendation}")
        
        # 通用建议
        if not any(i.insight_type == InsightType.UNDER_PERFORMER for i in insights):
            recommendations.append("【维持】当前策略运行良好，继续监控")
        
        return recommendations
    
    def _compile_report(
        self,
        overall_metrics: Dict,
        coin_performance: List[CoinPerformance],
        strategy_diagnosis: List[StrategyDiagnosis],
        insights: List[TradingInsight],
        recommendations: List[str]
    ) -> Dict:
        """编译完整报告"""
        return {
            'report_type': 'daily_reflection',
            'generated_at': datetime.now().isoformat(),
            'analysis_period_days': self.analysis_period_days,
            'overall_metrics': overall_metrics,
            'top_performers': [
                {
                    'symbol': c.symbol,
                    'pnl': c.total_pnl,
                    'trades': c.trade_count
                }
                for c in coin_performance[:5]
            ],
            'worst_performers': [
                {
                    'symbol': c.symbol,
                    'pnl': c.total_pnl,
                    'trades': c.trade_count
                }
                for c in coin_performance[-5:]
            ],
            'insights': [
                {
                    'type': i.insight_type.value,
                    'title': i.title,
                    'description': i.description,
                    'severity': i.severity,
                    'recommendation': i.recommendation
                }
                for i in insights
            ],
            'recommendations': recommendations
        }
    
    def _save_report(self, report: Dict):
        """保存报告到文件"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        report_file = self.report_dir / f'reflection_{timestamp}.json'
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"[ReflectionAgent] 报告已保存: {report_file}")
    
    def _print_summary(self, report: Dict):
        """打印报告摘要"""
        print("\n" + "=" * 60)
        print("📊 每日反思报告摘要")
        print("=" * 60)
        
        metrics = report.get('overall_metrics', {})
        print(f"\n📈 整体绩效 (最近{metrics.get('period_days', 7)}天)")
        print(f"  总盈亏: ${metrics.get('total_pnl', 0):.2f}")
        print(f"  交易笔数: {metrics.get('total_trades', 0)}")
        print(f"  涉及币种: {metrics.get('unique_symbols', 0)}")
        print(f"  手续费: ${metrics.get('total_fees', 0):.4f}")
        
        print(f"\n🏆 最佳表现")
        for item in report.get('top_performers', [])[:3]:
            print(f"  {item['symbol']}: +${item['pnl']:.2f} ({item['trades']}笔)")
        
        print(f"\n⚠️ 需要关注")
        for item in report.get('worst_performers', [])[-3:]:
            if item['pnl'] < 0:
                print(f"  {item['symbol']}: ${item['pnl']:.2f} ({item['trades']}笔)")
        
        print(f"\n💡 关键洞察")
        for insight in report.get('insights', [])[:3]:
            icon = "🔴" if insight['severity'] == 'high' else "🟡" if insight['severity'] == 'medium' else "🟢"
            print(f"  {icon} {insight['title']}: {insight['description']}")
        
        print(f"\n📋 行动建议")
        for rec in report.get('recommendations', []):
            print(f"  • {rec}")
        
        print("=" * 60)
    
    def _generate_empty_report(self) -> Dict:
        """生成空报告"""
        return {
            'report_type': 'daily_reflection',
            'generated_at': datetime.now().isoformat(),
            'overall_metrics': {},
            'message': '没有足够的数据进行分析'
        }


# ============ 定时任务入口 ============

def main():
    """主函数 - 供systemd调用"""
    print("=" * 60)
    print("V5 Reflection Agent - 每日交易反思")
    print("=" * 60)
    
    agent = ReflectionAgent()
    report = agent.run_daily_reflection()
    
    # 检查是否有严重问题
    insights = report.get('insights', [])
    high_severity = [i for i in insights if i.get('severity') == 'high']
    
    if high_severity:
        print(f"\n🚨 发现 {len(high_severity)} 个高优先级问题，请关注！")
        return 1  # 非零退出码，可用于systemd告警
    
    print("\n✅ 反思分析完成")
    return 0


if __name__ == "__main__":
    exit(main())
