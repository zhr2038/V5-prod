"""
V5多策略集成示例

展示如何将多策略系统接入现有的execution_pipeline
"""

import pandas as pd
from decimal import Decimal
from typing import List, Dict
import yaml

# 导入多策略系统
import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot/src')

from strategy.multi_strategy_system import (
    StrategyOrchestrator, 
    TrendFollowingStrategy, 
    MeanReversionStrategy,
    MultiStrategyAdapter
)


def load_market_data_from_cache(cache_dir: str = '/home/admin/clawd/v5-trading-bot/data/cache') -> pd.DataFrame:
    """从V5缓存加载市场数据"""
    import glob
    import os
    
    all_data = []
    
    # 读取所有1小时K线数据
    for file in glob.glob(f"{cache_dir}/*_1H_*.csv"):
        try:
            df = pd.read_csv(file)
            # 提取币种名称
            symbol = os.path.basename(file).split('_')[0] + '-USDT'
            df['symbol'] = symbol
            all_data.append(df)
        except Exception as e:
            print(f"读取 {file} 失败: {e}")
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def integrate_with_v5_pipeline(
    total_capital: float = 20.0,
    config_path: str = '/home/admin/clawd/v5-trading-bot/configs/multi_strategy.yaml'
) -> List[Dict]:
    """
    与V5 pipeline集成的主函数
    
    返回: 目标持仓列表，格式与V5兼容
    """
    
    # 加载配置
    config = {}
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"配置加载失败，使用默认配置: {e}")
    
    # 创建策略编排器
    orchestrator = StrategyOrchestrator(total_capital=Decimal(str(total_capital)))
    
    # 注册趋势跟踪策略
    if config.get('trend_following', {}).get('enabled', True):
        trend_config = config.get('trend_following', {})
        trend_strategy = TrendFollowingStrategy(config=trend_config)
        
        allocation = Decimal(str(
            config.get('strategy_allocations', {}).get('TrendFollowing', 0.5)
        ))
        orchestrator.register_strategy(trend_strategy, allocation=allocation)
    
    # 注册均值回归策略
    if config.get('mean_reversion', {}).get('enabled', True):
        mr_config = config.get('mean_reversion', {})
        mr_strategy = MeanReversionStrategy(config=mr_config)
        
        allocation = Decimal(str(
            config.get('strategy_allocations', {}).get('MeanReversion', 0.3)
        ))
        orchestrator.register_strategy(mr_strategy, allocation=allocation)
    
    # 加载市场数据
    market_data = load_market_data_from_cache()
    
    if market_data.empty:
        print("警告: 没有加载到市场数据")
        return []
    
    print(f"加载了 {len(market_data)} 条K线数据，{market_data['symbol'].nunique()} 个币种")
    
    # 运行策略
    adapter = MultiStrategyAdapter(orchestrator)
    targets = adapter.run_strategy_cycle(market_data)
    
    return targets


def print_targets(targets: List[Dict]):
    """打印目标持仓"""
    print("\n" + "=" * 80)
    print(f"多策略目标持仓 ({len(targets)} 个)")
    print("=" * 80)
    
    # 按策略分组
    by_strategy = {}
    for t in targets:
        strategy = t['source_strategy']
        if strategy not in by_strategy:
            by_strategy[strategy] = []
        by_strategy[strategy].append(t)
    
    for strategy, items in by_strategy.items():
        print(f"\n【{strategy}】")
        total = sum(t['target_position_usdt'] for t in items)
        print(f"  预计总投入: ${total:.2f}")
        
        for t in items:
            print(f"  {t['symbol']}: {t['side'].upper():4} ${t['target_position_usdt']:7.2f} "
                  f"(评分: {t['signal_score']:.2f}, 置信度: {t['confidence']:.2f})")
    
    print("\n" + "=" * 80)
    total_all = sum(t['target_position_usdt'] for t in targets)
    print(f"总投入: ${total_all:.2f}")
    print("=" * 80)


def main():
    """主函数"""
    print("V5 多策略集成演示")
    print("-" * 80)
    
    # 运行集成
    targets = integrate_with_v5_pipeline(total_capital=20.0)
    
    # 打印结果
    if targets:
        print_targets(targets)
    else:
        print("没有生成目标持仓")
    
    return targets


if __name__ == "__main__":
    main()
