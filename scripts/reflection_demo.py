#!/usr/bin/env python3
"""
反思Agent演示

展示如何使用ReflectionAgent分析交易记录
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot/src')

from execution.reflection_agent import ReflectionAgent, TradingInsight, InsightType
from datetime import datetime, timedelta


def demo_with_mock_data():
    """使用模拟数据演示"""
    print("=" * 70)
    print("🧠 V5 反思Agent 演示")
    print("=" * 70)
    
    # 创建Agent
    agent = ReflectionAgent()
    
    # 模拟一些洞察数据
    print("\n模拟生成交易洞察...\n")
    
    mock_insights = [
        TradingInsight(
            insight_type=InsightType.STRONG_PERFORMER,
            title="BTC表现优秀",
            description="最近7天盈利 $12.50，胜率60%",
            severity="low",
            metric_value=12.5,
            benchmark=5.0,
            recommendation="保持BTC配置，考虑类似强势币种"
        ),
        TradingInsight(
            insight_type=InsightType.UNDER_PERFORMER,
            title="ETH持续亏损",
            description="最近7天亏损 $8.30，多笔止损",
            severity="medium",
            metric_value=-8.3,
            benchmark=0,
            recommendation="降低ETH权重，或检查入场时机"
        ),
        TradingInsight(
            insight_type=InsightType.FACTOR_DECAY,
            title="RSI因子效果下降",
            description="均值回归策略胜率从55%降至35%",
            severity="high",
            metric_value=0.35,
            benchmark=0.50,
            recommendation="考虑调整RSI阈值或暂时停用均值回归策略"
        ),
        TradingInsight(
            insight_type=InsightType.RISK_CONCENTRATION,
            title="持仓过度集中",
            description="前3个币种贡献90%盈亏",
            severity="medium",
            metric_value=0.90,
            benchmark=0.60,
            recommendation="增加币种数量，降低单一币种风险敞口"
        )
    ]
    
    # 打印洞察
    print("📊 生成的洞察:")
    print("-" * 70)
    
    for insight in mock_insights:
        icon = "🔴" if insight.severity == 'high' else "🟡" if insight.severity == 'medium' else "🟢"
        print(f"\n{icon} [{insight.insight_type.value.upper()}] {insight.title}")
        print(f"   描述: {insight.description}")
        print(f"   建议: {insight.recommendation}")
    
    # 生成建议
    print("\n" + "=" * 70)
    print("💡 系统建议")
    print("=" * 70)
    
    recommendations = [
        "【紧急】考虑调整RSI阈值或暂时停用均值回归策略",
        "【建议】降低ETH权重，或检查入场时机", 
        "【建议】增加币种数量，降低单一币种风险敞口",
        "【维持】BTC表现良好，保持当前配置"
    ]
    
    for rec in recommendations:
        print(f"  • {rec}")
    
    print("\n" + "=" * 70)
    print("✅ 演示完成")
    print("=" * 70)
    print("\n实际使用时，反思Agent会:")
    print("  1. 从SQLite数据库读取真实交易记录")
    print("  2. 计算各币种/策略的实际盈亏")
    print("  3. 识别失效因子和异常模式")
    print("  4. 生成可执行的优化建议")
    print("  5. 保存报告到 reports/reflection/ 目录")
    print("\n定时任务: 每天21:00自动运行")
    print("systemd: --user start v5-reflection-agent.timer")


def demo_real_analysis():
    """使用真实数据分析（如果有数据的话）"""
    print("=" * 70)
    print("🧠 V5 反思Agent - 真实数据分析")
    print("=" * 70)
    
    agent = ReflectionAgent()
    
    # 尝试运行真实分析
    try:
        report = agent.run_daily_reflection()
        
        if report.get('overall_metrics'):
            print("\n✅ 分析完成！")
        else:
            print("\n⚠️ 没有找到足够的交易数据")
            print("提示: 系统需要至少2天的实盘交易记录才能生成有意义的分析")
            
    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        print("提示: 请确保数据文件存在且有写入权限")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--real':
        demo_real_analysis()
    else:
        demo_with_mock_data()
        print("\n" + "-" * 70)
        print("提示: 使用 --real 参数可以运行真实数据分析")
        print("      python scripts/reflection_demo.py --real")
