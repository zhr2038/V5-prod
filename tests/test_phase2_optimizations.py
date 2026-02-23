"""
测试 PositionBuilder 和 MultiLevelStopLoss
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

from src.execution.position_builder import PositionBuilder
from src.execution.multi_level_stop_loss import MultiLevelStopLoss, StopLossConfig

def test_position_builder():
    """测试分批建仓"""
    print("=" * 60)
    print("测试 PositionBuilder - 分批建仓")
    print("=" * 60)
    
    builder = PositionBuilder(
        stages=[0.3, 0.3, 0.4],
        price_drop_threshold=0.02,
        trend_confirmation_bars=2
    )
    
    # 清理之前的状态
    import os
    if os.path.exists("reports/position_builder_state.json"):
        os.remove("reports/position_builder_state.json")
    builder.position_states = {}
    
    symbol = "BTC/USDT"
    target_notional = 100.0
    
    # 第一批：应该立即买入30%
    notional_1 = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=50000,
        price_history=[48000, 49000, 50000]
    )
    print(f"第一批建仓: ${notional_1:.2f} (预期: $30.00)")
    assert abs(notional_1 - 30.0) < 0.01, "第一批建仓金额错误"
    
    # 第二批：价格未下跌，不应买入
    notional_2 = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=51000,  # 价格上涨
        price_history=[50000, 50500, 51000]
    )
    print(f"第二批建仓 (价格↑): ${notional_2:.2f} (预期: $0.00)")
    assert notional_2 == 0.0, "价格未下跌不应建仓"
    
    # 第二批：价格下跌2%，应该买入
    notional_2b = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=48500,  # 下跌2.9%
        price_history=[50000, 49000, 48500]
    )
    print(f"第二批建仓 (价格↓2%): ${notional_2b:.2f} (预期: $30.00)")
    assert abs(notional_2b - 30.0) < 0.01, "第二批建仓金额错误"
    
    # 第三批：趋势未确认（价格下跌），不应买入
    notional_3 = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=48000,  # 价格下跌
        price_history=[48500, 48200, 48000]  # 连续下跌
    )
    print(f"第三批建仓 (趋势未确认): ${notional_3:.2f} (预期: $0.00)")
    assert notional_3 == 0.0, "趋势未确认不应建仓"
    
    # 第三批：趋势确认（连续上涨），应该买入
    notional_3b = builder.get_build_notional(
        symbol=symbol,
        target_notional=target_notional,
        current_price=49500,
        price_history=[48500, 49000, 49500]  # 连续上涨
    )
    print(f"第三批建仓 (趋势确认): ${notional_3b:.2f} (预期: $40.00)")
    assert abs(notional_3b - 40.0) < 0.01, "第三批建仓金额错误"
    
    # 检查完成后的状态
    summary = builder.get_position_summary(symbol)
    print(f"建仓状态: {summary}")
    assert summary['status'] == 'completed', "建仓应已完成"
    
    print("\n✅ PositionBuilder 测试通过!")
    return True

def test_multi_level_stop_loss():
    """测试多级止损"""
    print("\n" + "=" * 60)
    print("测试 MultiLevelStopLoss - 多级动态止损")
    print("=" * 60)
    
    # 清理之前的状态
    import os
    if os.path.exists("reports/stop_loss_state.json"):
        os.remove("reports/stop_loss_state.json")
    
    config = StopLossConfig(
        tight_pct=0.03,
        normal_pct=0.05,
        loose_pct=0.08
    )
    
    stop_loss = MultiLevelStopLoss(config)
    stop_loss.positions = {}
    
    symbol = "ETH/USDT"
    entry_price = 2000.0
    
    # 初始化持仓（Sideways状态，5%止损）
    stop_price = stop_loss.initialize_position(symbol, entry_price, "Sideways")
    expected_stop = entry_price * 0.95  # 5%止损
    print(f"初始止损价格: ${stop_price:.2f} (预期: ${expected_stop:.2f})")
    assert abs(stop_price - expected_stop) < 0.01, "初始止损价格错误"
    
    # 价格跌至1900（亏损5%），触发止损
    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 1900)
    print(f"价格$1900时: 止损=${new_stop:.2f}, 类型={stop_type}, 触发={triggered}")
    assert triggered == True, "应触发止损"
    
    # 重置，测试盈利保护
    stop_loss.remove_position(symbol)
    stop_loss.initialize_position(symbol, entry_price, "Trending")
    
    # 价格上涨到2200（盈利10%），应保本+5%
    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 2200)
    expected_breakeven = entry_price * 1.05  # 保本+5%
    print(f"价格$2200(盈利10%): 止损=${new_stop:.2f} (预期: ${expected_breakeven:.2f}), 类型={stop_type}")
    assert abs(new_stop - expected_breakeven) < 0.01, "保本止损价格错误"
    assert triggered == False, "不应触发止损"
    
    # 价格上涨到2400（盈利20%），应追踪止损
    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 2400)
    print(f"价格$2400(盈利20%): 止损=${new_stop:.2f}, 类型={stop_type}")
    assert "trailing" in stop_type, "应为追踪止损"
    
    # 价格从2400回撤到2200，检查追踪止损
    new_stop, stop_type, triggered = stop_loss.update_stop_price(symbol, 2200)
    print(f"价格从$2400回撤到$2200: 触发={triggered}")
    # 追踪止损价 = 2400 - (2400-2000)*0.2 = 2320
    assert triggered == True, "应触发追踪止损"
    
    # 测试Risk-Off状态收紧止损
    stop_loss.remove_position(symbol)
    stop_price = stop_loss.initialize_position(symbol, entry_price, "Risk-Off")
    expected_tight_stop = entry_price * 0.97  # 3%止损
    print(f"Risk-Off状态初始止损: ${stop_price:.2f} (预期: ${expected_tight_stop:.2f})")
    assert abs(stop_price - expected_tight_stop) < 0.01, "Risk-Off止损价格错误"
    
    print("\n✅ MultiLevelStopLoss 测试通过!")
    return True

def main():
    print("\n🧪 V5 Phase 2 优化模块测试")
    print("=" * 60)
    
    all_passed = True
    
    try:
        all_passed &= test_position_builder()
    except Exception as e:
        print(f"\n❌ PositionBuilder 测试失败: {e}")
        all_passed = False
    
    try:
        all_passed &= test_multi_level_stop_loss()
    except Exception as e:
        print(f"\n❌ MultiLevelStopLoss 测试失败: {e}")
        all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✅ 所有测试通过!")
        print("\nPhase 2优化模块已就绪:")
        print("  1. PositionBuilder - 分批建仓系统")
        print("  2. MultiLevelStopLoss - 多级动态止损")
    else:
        print("❌ 部分测试失败，请检查代码")
    print("=" * 60)
    
    return all_passed

if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
