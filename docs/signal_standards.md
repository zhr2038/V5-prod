# 事件驱动触发标准 (Signal Standards)

## 触发条件总览

```
P0 (立即执行) > P1 (无视冷却) > P2 (检查冷却) > P3 (兜底)
```

---

## P0 风控信号 (立即执行，无视冷却)

### 1. 固定止损 (Fixed Stop Loss)
```python
触发条件: 当前价格 <= 买入价 * (1 - 5%)

示例:
  买入 XRP @ 1.3954
  止损位 = 1.3954 * 0.95 = 1.3256
  当前价格 1.32 -> 触发止损
```

### 2. 追踪止损 (ATR Trailing Stop)
```python
触发条件: 当前价格 <= 最高价 - (ATR14 * 2.2)

示例:
  ETH 最高价 2083.62
  ATR14 = 30.94
  止损位 = 2083.62 - (30.94 * 2.2) = 2015.55
  当前价格 2010.96 -> 触发追踪止损
```

### 3. 多级动态止盈 (Multi-Level Take Profit)
```python
盈利5%:  保本止损 (stop = entry_price)
盈利10%: 保本+5% (stop = entry_price * 1.05)  
盈利15%: 追踪止盈 (保护80%利润)

触发条件: 当前价格 <= 对应止损位
```

### 4. 排名退出 (Rank Exit)
```python
触发条件: 持仓币种排名 > 配置的最大排名

配置:
  max_position_rank: 3  # 只持有前3名

示例:
  持有 SOL (原排名2)
  新排名计算: SOL -> 排名4
  4 > 3 -> 触发退出
```

### 5. RiskOff 状态 (市场状态风控)
```python
触发条件: HMM/GMM 状态 == RISK_OFF

动作: 清仓所有持仓
```

---

## P1 市场状态信号 (无视冷却，立即响应)

### 1. HMM/GMM 状态切换
```python
关键切换:
  SIDEWAYS -> TRENDING_UP    (趋势启动，建仓)
  TRENDING_UP -> SIDEWAYS    (趋势结束，减仓)
  TRENDING_DOWN -> SIDEWAYS  (止跌，准备)
  ANY -> RISK_OFF            (风险，清仓)

检测方法:
  新状态 != 上周期状态
```

### 2. 资金费率极端信号
```python
多头拥挤: 资金费率 > 0.01%  (1小时)
空头拥挤: 资金费率 < -0.01%

触发: 费率方向变化 (正->负 或 负->正)
```

---

## P2 策略信号变化 (检查冷却，30-60分钟)

### 1. 方向反转 (Direction Flip)
```python
触发条件: 信号方向变化

示例:
  上周期: SOL = SELL (score=0.2)
  本周期: SOL = BUY (score=0.8)
  
  变化: SELL -> BUY -> 触发买入事件
```

### 2. 得分剧烈变化 (Score Jump)
```python
阈值: |新得分 - 旧得分| > 0.30 (30%)

示例:
  ETH 旧得分 = 0.3
  ETH 新得分 = 0.7
  变化 = 0.4 > 0.3 -> 触发
```

### 3. 排名剧烈变化 (Rank Jump)
```python
阈值: |新排名 - 旧排名| >= 3位

示例:
  BTC 旧排名 = 5
  BTC 新排名 = 1
  变化 = 4 >= 3 -> 触发
```

### 4. 选币组合变化 (Selection Change)
```python
触发条件: 选中币种集合发生变化

示例:
  上次选中: ['ETH', 'SOL', 'XRP']
  本次选中: ['ETH', 'BTC', 'SOL']
  
  变化: XRP退出, BTC进入 -> 触发调仓
```

### 5. 新币入选 (New Entry)
```python
触发条件: 币种首次进入选中列表

示例:
  历史从未选中 ADA
  本次 ADA 进入前3 -> 触发买入
```

---

## P2 价格突破信号 (检查冷却)

### 1. 突破前高 (Breakout Up)
```python
触发条件: 当前价格 >= 24h最高价 * 0.995

示例:
  BTC 24h最高 = 65000
  触发线 = 65000 * 0.995 = 64675
  当前价格 64700 -> 触发突破买入
```

### 2. 跌破前低 (Breakdown)
```python
触发条件: 当前价格 <= 24h最低价 * 1.005

示例:
  ETH 24h最低 = 3000
  触发线 = 3000 * 1.005 = 3015
  当前价格 3010 -> 触发跌破卖出
```

### 3. 布林带突破 (Bollinger Break)
```python
计算:
  中轨 = SMA20
  上轨 = 中轨 + (STD20 * 2)
  下轨 = 中轨 - (STD20 * 2)

触发:
  价格上穿上轨 -> 突破买入
  价格跌穿下轨 -> 跌破卖出
```

---

## P3 兜底信号 (最低优先级)

### 1. 时间兜底 (Heartbeat)
```python
触发条件: 距离上次交易 > 4小时

作用: 防止长期无事件导致持仓老化
```

### 2. 数据更新兜底
```python
触发条件: 
  - 资金费率更新 (每8小时)
  - 情绪指数更新 (每小时)
  - ML模型重训练完成 (每天)

动作: 重新评估但不强制交易
```

---

## 信号合并规则

### 同向信号叠加
```python
场景: SOL 同时满足
  - 方向反转: SELL -> BUY
  - 突破前高
  - HMM状态: TRENDING_UP

处理: 合并为一个交易事件，优先级取最高(P1)
```

### 反向信号冲突
```python
场景: 
  - Alpha6Factor: BUY (趋势)
  - MeanReversion: SELL (均值回归)

处理:
  - 如果 HMM=TRENDING -> 趋势优先
  - 如果 HMM=SIDEWAYS -> 均值回归优先
  - 否则 -> 得分高者优先
```

### 多币种同时触发
```python
场景: 同时满足条件的币种 > 3个

处理:
  - 按得分排序
  - 只取前3名
  - 其余进入等待队列
```

---

## 冷却规则详解

### 全局冷却 (Global Cooldown)
```yaml
P0 风控: 无冷却 (立即执行)
P1 状态: 无冷却 (立即执行)
P2 信号: 30分钟冷却
P3 兜底: 60分钟冷却
```

### 单币种冷却 (Symbol Cooldown)
```yaml
同一币种交易后: 60分钟内不再交易

例外:
  - P0 风控可打破冷却
  - 价格突破 +/-10% 可打破冷却
```

### 信号确认 (Signal Confirmation)
```python
# 防止假信号
新信号出现 -> 记录但不交易
连续2个周期相同信号 -> 确认后交易

示例:
  T+0: SOL 信号 BUY (新出现) -> 观察
  T+15min: SOL 信号 BUY (确认) -> 执行买入
```

---

## 配置参数汇总

```yaml
# event_driven_signals.yaml

signals:
  # P0 风控 (立即执行)
  risk:
    fixed_stop_loss_pct: 5.0          # 固定止损 5%
    atr_multiplier: 2.2               # ATR追踪倍数
    take_profit_levels: [5, 10, 15]   # 止盈档位(%)
    max_position_rank: 3              # 最大持仓排名
    
  # P1 状态 (无视冷却)
  regime:
    hmm_state_change: true            # HMM状态切换
    funding_rate_threshold: 0.0001    # 资金费率阈值(0.01%)
    
  # P2 信号 (检查冷却)
  strategy:
    direction_flip: true              # 方向反转
    score_change_threshold: 0.30      # 得分变化30%
    rank_jump_threshold: 3            # 排名跳变3位
    selection_change: true            # 选币组合变化
    
  # P2 突破 (检查冷却)
  breakout:
    enabled: true
    lookback_hours: 24                # 回看24小时
    threshold_pct: 0.5                # 突破0.5%触发
    bollinger_enabled: true           # 布林带突破
    
  # P3 兜底
  heartbeat:
    max_idle_hours: 4                 # 4小时无交易兜底
    
  # 冷却设置
cooldown:
  global_p2_minutes: 30               # P2全局冷却
  global_p3_minutes: 60               # P3全局冷却
  symbol_minutes: 60                  # 单币种冷却
  confirmation_periods: 2             # 信号确认周期
```

---

## 示例场景

### 场景1: 趋势启动
```
T+0: HMM=SIDEWAYS, 持仓无
T+15: HMM=TRENDING_UP (P1触发)
     -> 无视冷却
     -> 买入 SOL, ETH (前2名)
     
T+30: 突破前高 (P2触发，但冷却中)
     -> 跳过
     
T+45: 突破前高 + 冷却结束
     -> 不买入（已在持仓中）
```

### 场景2: 止损触发
```
T+0: 买入 XRP @ 1.40
T+30: XRP 跌到 1.32 (触发止损P0)
     -> 立即卖出
     -> 无视任何冷却
     
T+45: 信号又变 BUY
     -> 可以买入（风控后重置冷却）
```

### 场景3: 假信号过滤
```
T+0: SOL 信号 BUY (新出现)
     -> 记录，不交易
     
T+15: SOL 信号 BUY (确认)
     -> 执行买入
     
T+30: SOL 信号 SELL (方向反转)
     -> 卖出（但可能亏损）
     
优化后:
T+0: SOL 信号 BUY
     -> 记录
     
T+15: SOL 信号 BUY (确认)
     -> 买入
     
T+30: SOL 信号 SELL
     -> 记录，不交易
     
T+45: SOL 信号 SELL (确认)
     -> 卖出
```
