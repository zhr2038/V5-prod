#!/usr/bin/env python3
"""
调试和修复成本模型工厂问题
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_cost_factory_issue():
    """调试成本模型工厂问题"""
    
    print("🔍 调试成本模型工厂问题")
    print("=" * 60)
    
    try:
        from configs.loader import load_config
        from src.backtest.cost_factory import make_cost_model_from_cfg
        
        # 加载配置
        cfg = load_config("configs/config.yaml", env_path=".env")
        
        print("📋 配置信息:")
        print(f"  成本模型: {cfg.backtest.cost_model}")
        print(f"  成本数据目录: {cfg.backtest.cost_stats_dir}")
        print(f"  最小全局fills: {cfg.backtest.min_fills_global}")
        print(f"  最小bucket fills: {cfg.backtest.min_fills_bucket}")
        
        # 调用成本模型工厂
        print(f"\n🎯 调用make_cost_model_from_cfg...")
        result = make_cost_model_from_cfg(cfg)
        
        print(f"  返回类型: {type(result)}")
        print(f"  长度: {len(result) if isinstance(result, tuple) else 'N/A'}")
        
        if isinstance(result, tuple):
            print(f"  ⚠️ 问题: 返回tuple而不是单个CostModel对象")
            print(f"    元素0类型: {type(result[0])}")
            print(f"    元素1类型: {type(result[1])}")
            
            # 检查第一个元素是否是CostModel
            model = result[0]
            meta = result[1]
            
            print(f"\n  🔧 模型信息:")
            print(f"    模型类型: {type(model).__name__}")
            print(f"    元数据模式: {meta.mode}")
            print(f"    来源日期: {meta.source_day}")
            print(f"    全局fills: {meta.global_fills}")
            print(f"    原因: {meta.reason}")
            
            # 尝试调用模型方法
            print(f"\n  🎯 测试模型方法:")
            try:
                test_fee = model.estimate_fee("BTC/USDT", 1000.0)
                print(f"    ✅ estimate_fee成功: {test_fee*10000:.2f}bps")
            except AttributeError as e:
                print(f"    ❌ estimate_fee失败: {e}")
                
            try:
                test_slippage = model.estimate_slippage("BTC/USDT", 1000.0)
                print(f"    ✅ estimate_slippage成功: {test_slippage*10000:.2f}bps")
            except AttributeError as e:
                print(f"    ❌ estimate_slippage失败: {e}")
        
        # 检查成本数据
        print(f"\n📊 检查成本数据...")
        
        from src.backtest.cost_calibration import load_latest_cost_stats
        
        stats, stats_path = load_latest_cost_stats(
            str(cfg.backtest.cost_stats_dir), 
            max_age_days=int(cfg.backtest.max_stats_age_days)
        )
        
        print(f"  成本数据路径: {stats_path}")
        print(f"  是否有数据: {'是' if stats else '否'}")
        
        if stats:
            print(f"  数据日期: {stats.get('day', 'N/A')}")
            print(f"  全局fills: {(stats.get('coverage') or {}).get('fills', 0)}")
            print(f"  币种数量: {len(stats.get('fee_stats', {}))}")
        
        print(f"\n💡 问题分析:")
        print(f"  1. make_cost_model_from_cfg返回tuple(model, meta)")
        print(f"  2. 调用者可能错误地使用了整个tuple")
        print(f"  3. 需要检查调用代码如何处理返回值")
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def find_cost_model_usage():
    """查找成本模型使用位置"""
    
    print("\n" + "=" * 60)
    print("🔍 查找成本模型使用位置")
    print("=" * 60)
    
    import os
    
    # 搜索使用make_cost_model_from_cfg的地方
    print("搜索make_cost_model_from_cfg调用...")
    
    src_dir = Path("/home/admin/clawd/v5-trading-bot/src")
    
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r') as f:
                        content = f.read()
                        if 'make_cost_model_from_cfg' in content:
                            print(f"\n📄 文件: {filepath}")
                            # 显示相关行
                            lines = content.split('\n')
                            for i, line in enumerate(lines):
                                if 'make_cost_model_from_cfg' in line:
                                    print(f"  行 {i+1}: {line.strip()}")
                except:
                    pass
    
    print(f"\n💡 使用位置分析:")
    print(f"  需要检查调用代码是否正确解包tuple")
    print(f"  可能的问题: 直接使用tuple而不是model对象")

def create_fixed_cost_model():
    """创建修复的成本模型"""
    
    print("\n" + "=" * 60)
    print("🚀 创建修复的成本模型")
    print("=" * 60)
    
    fixed_code = """from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from configs.schema import AppConfig

from .cost_calibration import CalibratedCostModel, FixedCostModel, load_latest_cost_stats


@dataclass
class CostModelMeta:
    mode: str  # calibrated|default
    source_day: Optional[str]
    fee_quantile: str
    slippage_quantile: str
    min_fills_global: int
    min_fills_bucket: int
    max_stats_age_days: int
    stats_path: Optional[str]
    global_fills: Optional[int]
    reason: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "source_day": self.source_day,
            "fee_quantile": self.fee_quantile,
            "slippage_quantile": self.slippage_quantile,
            "min_fills_global": self.min_fills_global,
            "min_fills_bucket": self.min_fills_bucket,
            "max_stats_age_days": self.max_stats_age_days,
            "stats_path": self.stats_path,
            "global_fills": self.global_fills,
            "reason": self.reason,
        }


def make_cost_model_from_cfg(cfg: AppConfig):
    '''创建成本模型，返回(model, meta)元组'''
    bt = cfg.backtest
    default_model = FixedCostModel(fee_bps=float(bt.fee_bps), slippage_bps=float(bt.slippage_bps))

    if str(bt.cost_model).lower() != "calibrated":
        return default_model, CostModelMeta(
            mode="default",
            source_day=None,
            fee_quantile=str(bt.fee_quantile),
            slippage_quantile=str(bt.slippage_quantile),
            min_fills_global=int(bt.min_fills_global),
            min_fills_bucket=int(bt.min_fills_bucket),
            max_stats_age_days=int(bt.max_stats_age_days),
            stats_path=None,
            global_fills=None,
            reason="cost_model_disabled",
        )

    stats, stats_path = load_latest_cost_stats(str(bt.cost_stats_dir), max_age_days=int(bt.max_stats_age_days))
    if not stats:
        return default_model, CostModelMeta(
            mode="default",
            source_day=None,
            fee_quantile=str(bt.fee_quantile),
            slippage_quantile=str(bt.slippage_quantile),
            min_fills_global=int(bt.min_fills_global),
            min_fills_bucket=int(bt.min_fills_bucket),
            max_stats_age_days=int(bt.max_stats_age_days),
            stats_path=stats_path,
            global_fills=None,
            reason="no_stats_found_or_too_old",
        )

    global_fills = None
    try:
        global_fills = int((stats.get("coverage") or {}).get("fills") or 0)
    except Exception:
        global_fills = None

    model = CalibratedCostModel(
        stats=stats,
        fee_quantile=str(bt.fee_quantile),
        slippage_quantile=str(bt.slippage_quantile),
        min_fills_global=int(bt.min_fills_global),
        min_fills_bucket=int(bt.min_fills_bucket),
        default_fee_bps=float(bt.fee_bps),
        default_slippage_bps=float(bt.slippage_bps),
    )

    # even if global fills insufficient, resolve() will fallback; meta explains source
    mode = "calibrated" if (global_fills is not None and global_fills >= int(bt.min_fills_global)) else "default"
    reason = None if mode == "calibrated" else "global_fills_insufficient"

    return model, CostModelMeta(
        mode=mode,
        source_day=stats.get("day"),
        fee_quantile=str(bt.fee_quantile),
        slippage_quantile=str(bt.slippage_quantile),
        min_fills_global=int(bt.min_fills_global),
        min_fills_bucket=int(bt.min_fills_bucket),
        max_stats_age_days=int(bt.max_stats_age_days),
        stats_path=stats_path,
        global_fills=global_fills,
        reason=reason,
    )


def make_cost_model_simple(cfg: AppConfig):
    '''简化版本：只返回模型，不返回元数据'''
    model, _ = make_cost_model_from_cfg(cfg)
    return model
"""
    
    print("📋 修复方案:")
    print("  1. 当前函数设计返回tuple(model, meta)")
    print("  2. 问题：调用者可能没有正确解包")
    print("  3. 解决方案：")
    print("     a) 检查调用代码是否正确解包")
    print("     b) 或创建简化版本只返回模型")
    
    # 保存修复代码
    fix_path = Path("/home/admin/clawd/v5-trading-bot/src/backtest/cost_factory_fixed.py")
    fix_path.write_text(fixed_code, encoding="utf-8")
    
    print(f"\n✅ 修复代码已保存到: {fix_path}")
    print(f"   添加了简化函数: make_cost_model_simple()")

def main():
    """主函数"""
    
    print("🚀 成本模型工厂问题调试")
    print("=" * 60)
    
    # 调试成本模型工厂问题
    debug_cost_factory_issue()
    
    # 查找使用位置
    find_cost_model_usage()
    
    # 创建修复版本
    create_fixed_cost_model()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 下一步建议:")
    print("1. 检查调用make_cost_model_from_cfg的代码")
    print("2. 确保正确解包tuple: model, meta = make_cost_model_from_cfg(cfg)")
    print("3. 或使用新的简化函数: model = make_cost_model_simple(cfg)")
    print("4. 重新测试walk-forward")

if __name__ == "__main__":
    main()