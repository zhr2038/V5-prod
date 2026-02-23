# V5 Phase 2 优化集成指南

## 概述

Phase 2 优化已完成，包含两个核心模块：

1. **PositionBuilder** - 分批建仓系统（DCA策略）
2. **MultiLevelStopLoss** - 多级动态止损系统

## 模块功能

### PositionBuilder - 分批建仓

**策略**：
- **第一批 30%**：信号触发立即买入，抢占先机
- **第二批 30%**：价格下跌 2% 时买入，降低平均成本
- **第三批 40%**：趋势确认（连续2根K线上涨）后加仓

**优势**：
- 降低择时风险
- 摊薄成本
- 避免一次性满仓的风险

### MultiLevelStopLoss - 多级止损

**策略**：
- **未盈利**：根据市场状态设置 3%/5%/8% 止损
- **盈利 5%+**：保本止损（成本价+1%）
- **盈利 10%+**：保本+5%
- **盈利 15%+**：追踪止损（保护80%利润）

**优势**：
- 保护利润
- 让利润奔跑
- 限制亏损

## 集成步骤

### Step 1: 在 Pipeline 中初始化

修改 `src/core/pipeline.py`：

```python
from src.execution.position_builder import PositionBuilder
from src.execution.multi_level_stop_loss import MultiLevelStopLoss, StopLossConfig

class V5Pipeline:
    def __init__(self, cfg, clock=None):
        # ... 现有代码 ...
        
        # 新增：分批建仓管理器
        self.position_builder = PositionBuilder(
            stages=[0.3, 0.3, 0.4],
            price_drop_threshold=0.02,
            trend_confirmation_bars=2
        )
        
        # 新增：动态止损管理器
        self.stop_loss_manager = MultiLevelStopLoss(
            config=StopLossConfig(
                tight_pct=0.03,    # Risk-Off: 3%
                normal_pct=0.05,   # Sideways: 5%
                loose_pct=0.08     # Trending: 8%
            )
        )
```

### Step 2: 在仓位分配后使用分批建仓

修改 `run` 方法中的 rebalance 逻辑：

```python
def run(self, market_data_1h, positions, cash_usdt, ...):
    # ... 现有代码：获取 target_weights ...
    
    # 新增：分批建仓
    for sym in target_symbols:
        tw = target_weights[sym]
        target_notional = tw * equity
        
        # 获取价格历史（用于趋势确认）
        price_history = market_data_1h[sym].close if sym in market_data_1h else []
        current_price = prices.get(sym, 0)
        
        # 使用 PositionBuilder 计算本次建仓金额
        build_notional = self.position_builder.get_build_notional(
            symbol=sym,
            target_notional=target_notional,
            current_price=current_price,
            price_history=price_history
        )
        
        if build_notional > 0:
            # 生成买入订单
            rebalance_orders.append(Order(
                symbol=sym,
                side='buy',
                intent='OPEN_LONG' if is_new_position else 'REBALANCE',
                notional_usdt=build_notional,
                signal_price=current_price,
                meta={'stage': self.position_builder.get_current_stage(sym)}
            ))
            
            # 初始化止损
            if is_new_position:
                stop_price = self.stop_loss_manager.initialize_position(
                    sym, current_price, str(regime.state)
                )
```

### Step 3: 在 Exit Policy 中使用动态止损

修改 `src/risk/exit_policy.py`：

```python
class ExitPolicy:
    def __init__(self, config, clock=None):
        # ... 现有代码 ...
        from src.execution.multi_level_stop_loss import MultiLevelStopLoss
        self.stop_loss_manager = MultiLevelStopLoss()
    
    def evaluate(self, positions, market_data_1h, regime_state):
        exit_orders = []
        
        # 现有逻辑：regime_exit
        if regime_state == "Risk-Off":
            for pos in positions:
                exit_orders.append(self._create_exit_order(pos, "regime_exit"))
                # 清仓后移除止损状态
                self.stop_loss_manager.remove_position(pos.symbol)
        
        # 新增：动态止损检查
        for pos in positions:
            current_price = market_data_1h.get(pos.symbol, {}).get('close', [0])[-1]
            
            should_exit, reason, exit_price = self.stop_loss_manager.should_exit(
                pos.symbol, current_price
            )
            
            if should_exit:
                exit_orders.append(self._create_exit_order(
                    pos, reason, price=exit_price
                ))
                self.stop_loss_manager.remove_position(pos.symbol)
        
        return exit_orders
```

### Step 4: 在成交确认后更新止损

在订单成交后，更新最高价：

```python
def on_fill(self, symbol, fill_price, fill_qty):
    """
    订单成交回调
    """
    # 更新持仓
    # ...
    
    # 更新止损最高价
    if symbol in self.stop_loss_manager.positions:
        state = self.stop_loss_manager.positions[symbol]
        if fill_price > state.highest_price:
            state.highest_price = fill_price
            self.stop_loss_manager._save_state()
```

## 配置建议

### PositionBuilder 配置

```python
# 保守型（低风险）
PositionBuilder(
    stages=[0.5, 0.5],  # 分两批，每批50%
    price_drop_threshold=0.03,  # 下跌3%买第二批
    trend_confirmation_bars=3   # 3根K线确认
)

# 激进型（高风险高收益）
PositionBuilder(
    stages=[0.5, 0.3, 0.2],  # 第一批重仓
    price_drop_threshold=0.01,  # 下跌1%就抄底
    trend_confirmation_bars=1   # 1根K线确认
)

# V5推荐配置（平衡型）
PositionBuilder(
    stages=[0.3, 0.3, 0.4],
    price_drop_threshold=0.02,
    trend_confirmation_bars=2
)
```

### MultiLevelStopLoss 配置

```python
# 保守型
StopLossConfig(
    tight_pct=0.02,   # 2%止损
    normal_pct=0.04,  # 4%止损
    loose_pct=0.06,   # 6%止损
    profit_threshold_1=0.03,  # 3%盈利保本
    profit_threshold_2=0.06,  # 6%盈利保护
    profit_threshold_3=0.10   # 10%追踪止损
)

# V5推荐配置（小资金适用）
StopLossConfig(
    tight_pct=0.03,
    normal_pct=0.05,
    loose_pct=0.08,
    profit_threshold_1=0.05,
    profit_threshold_2=0.10,
    profit_threshold_3=0.15
)
```

## 测试验证

运行测试：

```bash
cd /home/admin/clawd/v5-trading-bot
source .venv/bin/activate
python tests/test_phase2_optimizations.py
```

预期输出：
```
✅ PositionBuilder 测试通过!
✅ MultiLevelStopLoss 测试通过!
```

## 预期效果

### 分批建仓效果

| 场景 | 旧策略（一次性建仓） | 新策略（分批建仓） | 改进 |
|------|---------------------|-------------------|------|
| 买入后下跌 | 满仓亏损 | 第二批抄底，摊薄成本 | 降低成本3-5% |
| 买入后上涨 | 满仓盈利 | 第三批加仓，扩大盈利 | 提高收益5-10% |
| 震荡市 | 频繁止损 | 分批建仓，减少噪音 | 降低交易次数30% |

### 动态止损效果

| 市场情况 | 旧策略（固定止损） | 新策略（动态止损） | 改进 |
|---------|-------------------|-------------------|------|
| 盈利15%后回撤 | 止损在-5%，亏损离场 | 追踪止损在+8%，保护利润 | 多赚13% |
| 假突破 | 止损触发，错失后续涨幅 | 保本止损，继续持有 | 提高胜率20% |
| 强趋势 | 过早止损，错过后续涨幅 | 宽松止损，让利润奔跑 | 提高收益15% |

## 注意事项

1. **状态持久化**：两个模块都有状态文件，部署时注意备份
   - `reports/position_builder_state.json`
   - `reports/stop_loss_state.json`

2. **与现有逻辑兼容性**：
   - PositionBuilder 与现有 deadband 逻辑并存
   - MultiLevelStopLoss 与 regime_exit 并存

3. **回测支持**：
   - 需要将这两个模块也纳入回测框架
   - 注意状态在回测中的重置

## 下一步

1. 在模拟盘测试1-2周
2. 监控建仓和止损的实际表现
3. 根据实盘数据调整参数
4. 考虑加入更多高级功能：
   - 根据波动率动态调整批次大小
   - 机器学习预测最优建仓时机
   - 跨币种对冲止损

---

**状态**：Phase 2 优化代码已完成并测试通过  
**下一步**：集成到 Pipeline 中进行实盘测试
