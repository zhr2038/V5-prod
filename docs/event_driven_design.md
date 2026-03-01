# 事件驱动交易系统设计 (Event-Driven V5)

## 核心问题
当前每小时固定交易，无论信号是否变化，导致：
- 无效交易（信号没变也调仓）
- 手续费浪费
- 错过关键点位（非整点突破）

## 目标
- **只在必要时交易**（信号变化/风控触发/机会出现）
- **不错过关键点位**（突破立即响应）
- **不过度交易**（冷却期保护）

---

## 事件类型设计

### 1. 信号变化事件 (Signal Change)
**触发条件:**
```python
# 选币结果变化
selected_changed = set(new_selected) != set(last_selected)

# 因子得分剧烈变化
score_changed = any(
    abs(new_scores[sym] - last_scores[sym]) > 0.3 
    for sym in universe
)

# 方向反转
direction_flip = any(
    new_signals[sym] != last_signals[sym]  # buy -> sell 或反之
    for sym in universe
)
```

**阈值:**
- 选币变化: 任何变化立即触发
- 得分变化: >30% 变化触发
- 方向反转: 立即触发

### 2. 市场状态变化 (Regime Change)
**触发条件:**
```python
# HMM/GMM 状态切换
hmm_state_changed = new_hmm_state != last_hmm_state

# 例如: SIDEWAYS -> TRENDING_UP (趋势开始，立即入场)
# 例如: TRENDING -> RISK_OFF (风险出现，立即清仓)
```

**优先级: 最高** (状态切换必须立即响应)

### 3. 风控事件 (Risk Event)
**触发条件:**
```python
# 止损触发
stop_loss_hit = any(
    current_price <= stop_price 
    for pos in positions
)

# 止盈触发
take_profit_hit = any(
    current_price >= target_price
    for pos in positions
)

# 排名退出
rank_exit = any(
    current_rank[sym] > max_allowed_rank
    for sym in positions
)
```

**优先级: 最高** (风控无条件执行)

### 4. 价格突破事件 (Price Breakout)
**触发条件:**
```python
# 突破前高/前低
breakout_up = current_price > highest_24h * 0.995  # 接近前高
breakout_down = current_price < lowest_24h * 1.005  # 接近前低

# 或布林带突破
bollinger_break = current_price > upper_band or current_price < lower_band
```

**用途:** 捕捉突发趋势

### 5. 定时兜底 (Heartbeat)
**频率:** 每15分钟检查一次
**作用:** 
- 防止事件遗漏
- 长时间无事件时重新评估
- 数据更新（资金费率等）

---

## 冷却机制 (Anti-Overtrading)

### 1. 全局冷却
```python
MIN_TRADE_INTERVAL_MINUTES = 30  # 最少30分钟一次

if now - last_trade_time < 30min:
    logger.info("冷却期内，跳过交易")
    return
```

### 2. 单币种冷却
```python
SYMBOL_COOLDOWN_MINUTES = 60  # 同币种1小时内不重复交易

if now - last_trade_time[sym] < 60min:
    logger.info(f"{sym} 冷却期内")
    continue
```

### 3. 信号稳定性检查
```python
# 新信号必须持续2个周期才确认
if new_signal != prev_signal:  # 第一次变化
    signal_pending[sym] = new_signal
    return  # 不交易，等待确认
    
if new_signal == signal_pending[sym]:  # 第二次相同
    execute_trade()  # 确认后交易
```

---

## 实现架构

### 文件结构
```
src/execution/
├── event_driven_engine.py      # 事件引擎主类
├── event_monitor.py            # 事件监控（价格/状态）
├── signal_change_detector.py   # 信号变化检测
├── trade_cooldown.py           # 冷却管理
└── event_types.py              # 事件类型定义
```

### 主循环
```python
# event_driven_engine.py
class EventDrivenEngine:
    def __init__(self):
        self.cooldown = TradeCooldown()
        self.signal_detector = SignalChangeDetector()
        self.event_monitor = EventMonitor()
        
    def check_and_trade(self):
        """每次调用检查事件并决策"""
        
        # 1. 风控事件（最高优先级）
        risk_events = self.event_monitor.check_risk()
        if risk_events:
            self.execute_risk_trades(risk_events)  # 无视冷却
            return
        
        # 2. 检查冷却
        if self.cooldown.is_global_cooldown():
            return
            
        # 3. 收集事件
        events = []
        
        # 3.1 市场状态变化
        if self.event_monitor.regime_changed():
            events.append(Event.REGIME_CHANGE)
            
        # 3.2 信号变化
        signal_changes = self.signal_detector.detect_changes()
        if signal_changes.significant:
            events.append(Event.SIGNAL_CHANGE)
            
        # 3.3 价格突破
        if self.event_monitor.price_breakout():
            events.append(Event.BREAKOUT)
        
        # 4. 决策
        if not events:
            logger.info("无显著事件，跳过交易")
            return
            
        # 5. 执行（考虑冷却）
        tradable_symbols = self.cooldown.filter_cooldown(
            signal_changes.symbols
        )
        
        if tradable_symbols:
            self.execute_trades(tradable_symbols)
            self.cooldown.record_trade()
```

---

## Timer 配置

### 从每小时改为每15分钟
```ini
# v5-event-driven.timer
[Unit]
Description=V5 Event-Driven Trading (check every 15 min)

[Timer]
# 每15分钟检查一次
OnCalendar=*:00:00,*:15:00,*:30:00,*:45:00

# 或者更细粒度
OnCalendar=*:0/15:00

[Install]
WantedBy=timers.target
```

### 监控模式（实时）
```python
# 可选: WebSocket 实时推送
# 当前用轮询即可，OKX API 限制不多
```

---

## 配置参数

```yaml
# configs/live_20u_real.yaml
event_driven:
  enabled: true
  check_interval_minutes: 15  # 检查频率
  
  # 冷却设置
  global_cooldown_minutes: 30      # 全局最少30分钟
  symbol_cooldown_minutes: 60      # 单币种1小时
  signal_confirm_periods: 2        # 信号确认周期数
  
  # 信号变化阈值
  score_change_threshold: 0.3      # 得分变化>30%触发
  direction_change_triggers: true  # 方向反转触发
  
  # 市场状态
  regime_change_triggers: true     # 状态切换触发
  
  # 风控
  stop_loss_immediate: true        # 止损立即执行
  take_profit_immediate: true      # 止盈立即执行
  rank_exit_immediate: true        # 排名退出立即执行
  
  # 价格突破
  breakout_detection: true
  breakout_lookback_hours: 24      # 24小时前高/前低
  breakout_threshold_pct: 0.5      # 突破0.5%触发
```

---

## 风控保留

**必须立即执行的事件**（无视冷却）:
1. 止损触发
2. 止盈触发
3. RiskOff 状态（清仓）
4. Kill Switch 启用

**可以跳过的事件**（尊重冷却）:
1. 轻微信号变化
2. 价格波动（非突破）
3. 定时兜底检查

---

## 预期效果

| 指标 | 当前(1h固定) | 事件驱动 | 改善 |
|------|-------------|---------|------|
| 交易次数/天 | 24次检查 | 5-10次实际 | -60% |
| 无效调仓 | 高 | 低 | -80% |
| 手续费 | 高 | 低 | -60% |
| 响应速度 | 整点延迟 | 实时响应 | +100% |
| 滑点 | 大(整点拥挤) | 小(分散) | -30% |

---

## 实施步骤

1. **Phase 1**: 添加信号变化检测（不动timer）
2. **Phase 2**: 改为15分钟timer + 冷却机制
3. **Phase 3**: 添加价格突破检测
4. **Phase 4**: WebSocket实时（可选）

**先从 Phase 1 开始？**
