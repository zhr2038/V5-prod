# 方案B：激进改革 - 完全事件驱动

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                     Event-Driven V5                          │
├─────────────────────────────────────────────────────────────┤
│  Timer (15min)                                               │
│     │                                                        │
│     ▼                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │ Event Monitor│───▶│ Signal Engine│───▶│ Cooldown Mgr │   │
│  └──────────────┘    └──────────────┘    └──────────────┘   │
│         │                   │                   │            │
│         │                   │                   │            │
│         ▼                   ▼                   ▼            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Trade Decision Engine                   │    │
│  │  (风控P0 > 状态P1 > 信号P2 > 兜底P3)                 │    │
│  └─────────────────────────────────────────────────────┘    │
│                            │                                 │
│                            ▼                                 │
│                    ┌──────────────┐                         │
│                    │ OKX Execution│                         │
│                    └──────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. Event Monitor (事件监控器)

```python
# src/execution/event_monitor.py

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List
import time

class EventType(Enum):
    RISK_STOP_LOSS = auto()      # P0: 止损
    RISK_TAKE_PROFIT = auto()    # P0: 止盈  
    RISK_RANK_EXIT = auto()      # P0: 排名退出
    REGIME_CHANGE = auto()       # P1: 状态切换
    SIGNAL_CHANGE = auto()       # P2: 信号变化
    BREAKOUT = auto()            # P2: 价格突破
    HEARTBEAT = auto()           # P3: 定时兜底

@dataclass
class TradingEvent:
    type: EventType
    priority: int  # 0=P0, 1=P1, 2=P2, 3=P3
    symbol: Optional[str]  # None = 全局事件
    data: dict
    timestamp_ms: int
    
class EventMonitor:
    """
    监控所有交易相关事件
    15分钟轮询一次，但实时检查价格/风控
    """
    
    def __init__(self, cfg):
        self.cfg = cfg
        self.last_regime = None
        self.last_prices = {}
        self.price_high_24h = {}
        self.price_low_24h = {}
        
    def collect_events(self, current_state) -> List[TradingEvent]:
        """
        收集所有触发的事件，按优先级排序
        """
        events = []
        
        # P0: 风控事件 (立即检查)
        events.extend(self._check_risk_events(current_state))
        
        # P1: 市场状态变化
        events.extend(self._check_regime_events(current_state))
        
        # P2: 信号变化
        events.extend(self._check_signal_events(current_state))
        
        # P2: 价格突破
        events.extend(self._check_breakout_events(current_state))
        
        # P3: 兜底检查 (长时间无交易)
        if self._should_heartbeat():
            events.append(TradingEvent(
                type=EventType.HEARTBEAT,
                priority=3,
                symbol=None,
                data={},
                timestamp_ms=int(time.time() * 1000)
            ))
        
        # 按优先级排序
        return sorted(events, key=lambda e: e.priority)
    
    def _check_risk_events(self, state) -> List[TradingEvent]:
        """检查风控事件"""
        events = []
        
        for sym, pos in state.positions.items():
            current_px = state.prices.get(sym)
            if not current_px:
                continue
                
            # 止损检查
            if pos.stop_price and current_px <= pos.stop_price:
                events.append(TradingEvent(
                    type=EventType.RISK_STOP_LOSS,
                    priority=0,
                    symbol=sym,
                    data={'price': current_px, 'stop': pos.stop_price},
                    timestamp_ms=int(time.time() * 1000)
                ))
            
            # 止盈检查
            if pos.take_profit_price and current_px >= pos.take_profit_price:
                events.append(TradingEvent(
                    type=EventType.RISK_TAKE_PROFIT,
                    priority=0,
                    symbol=sym,
                    data={'price': current_px, 'tp': pos.take_profit_price},
                    timestamp_ms=int(time.time() * 1000)
                ))
        
        return events
    
    def _check_regime_events(self, state) -> List[TradingEvent]:
        """检查市场状态变化"""
        events = []
        
        current_regime = state.regime
        if self.last_regime is None:
            self.last_regime = current_regime
            return events
        
        if current_regime != self.last_regime:
            events.append(TradingEvent(
                type=EventType.REGIME_CHANGE,
                priority=1,
                symbol=None,  # 全局事件
                data={
                    'from': self.last_regime,
                    'to': current_regime
                },
                timestamp_ms=int(time.time() * 1000)
            ))
            self.last_regime = current_regime
        
        return events
    
    def _check_signal_events(self, state) -> List[TradingEvent]:
        """检查因子信号变化"""
        events = []
        
        # 从缓存读取上周期信号
        last_signals = self._load_last_signals()
        current_signals = state.signals
        
        for sym in current_signals:
            if sym not in last_signals:
                continue
            
            last = last_signals[sym]
            curr = current_signals[sym]
            
            # 方向反转
            if last['direction'] != curr['direction']:
                events.append(TradingEvent(
                    type=EventType.SIGNAL_CHANGE,
                    priority=2,
                    symbol=sym,
                    data={
                        'change_type': 'direction_flip',
                        'from': last['direction'],
                        'to': curr['direction']
                    },
                    timestamp_ms=int(time.time() * 1000)
                ))
            
            # 得分剧烈变化 (>30%)
            elif abs(curr['score'] - last['score']) > 0.3:
                events.append(TradingEvent(
                    type=EventType.SIGNAL_CHANGE,
                    priority=2,
                    symbol=sym,
                    data={
                        'change_type': 'score_jump',
                        'from_score': last['score'],
                        'to_score': curr['score']
                    },
                    timestamp_ms=int(time.time() * 1000)
                ))
        
        # 保存当前信号
        self._save_current_signals(current_signals)
        
        return events
    
    def _check_breakout_events(self, state) -> List[TradingEvent]:
        """检查价格突破"""
        events = []
        
        for sym, px in state.prices.items():
            if sym not in self.price_high_24h:
                continue
            
            high = self.price_high_24h[sym]
            low = self.price_low_24h[sym]
            
            # 突破前高 (0.5%阈值)
            if px >= high * 0.995:
                events.append(TradingEvent(
                    type=EventType.BREAKOUT,
                    priority=2,
                    symbol=sym,
                    data={
                        'direction': 'up',
                        'price': px,
                        'resistance': high
                    },
                    timestamp_ms=int(time.time() * 1000)
                ))
            
            # 跌破前低
            elif px <= low * 1.005:
                events.append(TradingEvent(
                    type=EventType.BREAKOUT,
                    priority=2,
                    symbol=sym,
                    data={
                        'direction': 'down',
                        'price': px,
                        'support': low
                    },
                    timestamp_ms=int(time.time() * 1000)
                ))
        
        return events
    
    def _should_heartbeat(self) -> bool:
        """检查是否需要兜底"""
        # 4小时无交易则兜底检查
        last_trade = self._load_last_trade_time()
        return (time.time() - last_trade) > 4 * 3600
```

### 2. Cooldown Manager (冷却管理器)

```python
# src/execution/cooldown_manager.py

import time
import json
from pathlib import Path
from typing import Set, Optional

class CooldownManager:
    """
    管理交易冷却期
    """
    
    def __init__(self, cfg):
        self.cfg = cfg
        self.global_cooldown_sec = cfg.get('global_cooldown_minutes', 30) * 60
        self.symbol_cooldown_sec = cfg.get('symbol_cooldown_minutes', 60) * 60
        self.state_path = cfg.get('cooldown_state_path', 'reports/cooldown_state.json')
        
        # 加载状态
        self._load_state()
    
    def can_trade(self, symbol: Optional[str] = None, event_priority: int = 2) -> bool:
        """
        检查是否可以交易
        
        Args:
            symbol: 交易对，None 表示检查全局冷却
            event_priority: 0=P0(无视冷却), 1=P1(30min), 2=P2(60min)
        """
        now = time.time()
        
        # P0 事件（风控）无视冷却
        if event_priority == 0:
            return True
        
        # 检查全局冷却
        if now - self.last_global_trade < self.global_cooldown_sec:
            return False
        
        # 检查单币种冷却
        if symbol and symbol in self.last_symbol_trade:
            if now - self.last_symbol_trade[symbol] < self.symbol_cooldown_sec:
                return False
        
        return True
    
    def record_trade(self, symbol: Optional[str] = None):
        """记录交易时间"""
        now = time.time()
        self.last_global_trade = now
        if symbol:
            self.last_symbol_trade[symbol] = now
        self._save_state()
    
    def filter_symbols(self, symbols: Set[str], event_priority: int = 2) -> Set[str]:
        """过滤掉冷却期内的币种"""
        return {s for s in symbols if self.can_trade(s, event_priority)}
    
    def _load_state(self):
        """加载冷却状态"""
        try:
            path = Path(self.state_path)
            if path.exists():
                data = json.loads(path.read_text())
                self.last_global_trade = data.get('last_global', 0)
                self.last_symbol_trade = data.get('symbols', {})
            else:
                self.last_global_trade = 0
                self.last_symbol_trade = {}
        except:
            self.last_global_trade = 0
            self.last_symbol_trade = {}
    
    def _save_state(self):
        """保存冷却状态"""
        path = Path(self.state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            'last_global': self.last_global_trade,
            'symbols': self.last_symbol_trade
        }
        path.write_text(json.dumps(data, indent=2))
```

### 3. Decision Engine (决策引擎)

```python
# src/execution/event_decision_engine.py

from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class EventDecisionEngine:
    """
    基于事件的决策引擎
    """
    
    def __init__(self, cfg, event_monitor, cooldown_mgr, executor):
        self.cfg = cfg
        self.monitor = event_monitor
        self.cooldown = cooldown_mgr
        self.executor = executor
    
    def run(self, current_state):
        """
        主运行循环
        """
        # 1. 收集所有事件
        events = self.monitor.collect_events(current_state)
        
        if not events:
            logger.info("无触发事件，跳过本次检查")
            return
        
        logger.info(f"检测到 {len(events)} 个事件")
        for e in events:
            logger.info(f"  - {e.type.name} (P{e.priority}): {e.symbol}")
        
        # 2. 处理最高优先级事件
        highest_priority = events[0].priority
        urgent_events = [e for e in events if e.priority == highest_priority]
        
        # P0: 风控事件 - 立即执行
        if highest_priority == 0:
            self._handle_risk_events(urgent_events, current_state)
            return
        
        # P1: 状态变化 - 无视冷却立即执行
        if highest_priority == 1:
            self._handle_regime_change(urgent_events[0], current_state)
            return
        
        # P2/P3: 检查冷却
        tradable = self._filter_with_cooldown(urgent_events)
        if tradable:
            self._handle_signal_events(tradable, current_state)
        else:
            logger.info("所有事件均在冷却期内，跳过交易")
    
    def _handle_risk_events(self, events: List[TradingEvent], state):
        """处理风控事件 - 立即执行"""
        for event in events:
            logger.warning(f"风控触发: {event.type.name} - {event.symbol}")
            
            if event.type.name.startswith('RISK'):
                # 立即平仓
                self.executor.close_position(
                    symbol=event.symbol,
                    reason=event.type.name,
                    urgency='immediate'
                )
                # 记录但不进入冷却（风控必须能连续执行）
    
    def _handle_regime_change(self, event: TradingEvent, state):
        """处理市场状态变化"""
        logger.info(f"市场状态变化: {event.data['from']} -> {event.data['to']}")
        
        from_state = event.data['from']
        to_state = event.data['to']
        
        if to_state == 'RISK_OFF':
            # 清仓
            logger.warning("进入RiskOff状态，执行清仓")
            for sym in state.positions:
                self.executor.close_position(sym, reason='regime_risk_off')
        
        elif to_state == 'TRENDING_UP' and from_state == 'SIDEWAYS':
            # 趋势启动，立即建仓
            logger.info("趋势启动，建仓")
            selected = state.selected_symbols[:3]  # 前3名
            for sym in selected:
                if sym not in state.positions:
                    self.executor.open_position(sym, reason='trend_start')
            self.cooldown.record_trade()
        
        elif to_state == 'SIDEWAYS' and from_state == 'TRENDING_UP':
            # 趋势结束，减仓
            logger.info("趋势结束，减仓")
            # 可以减仓部分...
    
    def _handle_signal_events(self, events: List[TradingEvent], state):
        """处理信号变化事件"""
        symbols_to_trade = set()
        
        for event in events:
            if event.symbol:
                symbols_to_trade.add(event.symbol)
        
        if not symbols_to_trade:
            return
        
        logger.info(f"信号变化交易: {symbols_to_trade}")
        
        # 执行交易
        for sym in symbols_to_trade:
            signal = state.signals.get(sym, {})
            
            if signal.get('direction') == 'buy' and sym not in state.positions:
                self.executor.open_position(sym, reason='signal_change')
                self.cooldown.record_trade(sym)
            
            elif signal.get('direction') == 'sell' and sym in state.positions:
                self.executor.close_position(sym, reason='signal_change')
                self.cooldown.record_trade(sym)
    
    def _filter_with_cooldown(self, events: List[TradingEvent]) -> List[TradingEvent]:
        """过滤冷却期内的币种"""
        result = []
        for event in events:
            if self.cooldown.can_trade(event.symbol, event.priority):
                result.append(event)
            else:
                logger.info(f"{event.symbol} 在冷却期内，跳过")
        return result
```

## 配置示例

```yaml
# configs/event_driven.yaml
event_driven:
  enabled: true
  
  # 检查频率
  check_interval_minutes: 15
  
  # 冷却设置
  global_cooldown_minutes: 30      # P1/P2事件最少间隔
  symbol_cooldown_minutes: 60      # 同币种1小时内不重复
  
  # 信号变化阈值
  score_change_threshold: 0.30     # 得分变化>30%
  direction_change_triggers: true  # 方向反转触发
  
  # 突破检测
  breakout_enabled: true
  breakout_lookback_hours: 24      # 24h前高/前低
  breakout_threshold_pct: 0.5      # 0.5%突破即触发
  
  # 兜底
  heartbeat_interval_hours: 4      # 4小时无事件强制检查
  
  # 风控（立即执行，无视冷却）
  risk_events:
    stop_loss: immediate
    take_profit: immediate
    rank_exit: immediate
    regime_risk_off: immediate
```

## Timer 配置

```ini
# ~/.config/systemd/user/v5-event-driven.timer
[Unit]
Description=V5 Event-Driven Trading Check

[Timer]
# 每15分钟检查一次
OnCalendar=*:0/15:00

# 如果错过时间立即补执行
Persistent=true

[Install]
WantedBy=timers.target
```

## 启动流程修改

```python
# main.py 修改

def main():
    cfg = load_config()
    
    if cfg.event_driven.enabled:
        # 事件驱动模式
        monitor = EventMonitor(cfg)
        cooldown = CooldownManager(cfg)
        executor = ExecutionEngine(cfg)
        engine = EventDecisionEngine(cfg, monitor, cooldown, executor)
        
        # 加载当前状态
        state = load_current_state()
        
        # 执行决策
        engine.run(state)
    else:
        # 原有模式
        ...
```

## 风险与回滚

### 潜在风险

1. **事件风暴**: 连续触发导致过度交易
   - 解决: 冷却机制 + 信号确认(2周期)

2. **错过机会**: 冷却期内出现更好信号
   - 解决: P1状态变化无视冷却

3. **延迟**: 15分钟轮询 vs 实时
   - 解决: 后续可换WebSocket

### 回滚方案

```bash
# 如果出问题，立即切回1小时固定
sudo systemctl --user stop v5-event-driven.timer
sudo systemctl --user start v5-live-20u.user.timer
```

### A/B测试

```python
# 可以先并行运行，对比结果
if random.random() < 0.5:
    use_event_driven()  # 50%流量
else:
    use_fixed_hourly()   # 50%流量
```

## 预期效果

| 指标 | 当前(1h固定) | 事件驱动 | 变化 |
|------|-------------|---------|------|
| 检查次数/天 | 24 | 96 | +300% |
| 实际交易次数 | 10-15 | 5-8 | -50% |
| 无效调仓 | 50% | 10% | -80% |
| 手续费 | 高 | 低 | -60% |
| 响应延迟 | 0-60min | 0-15min | -75% |
| 滑点 | 大(整点拥挤) | 小(分散) | -30% |

**结论**: 检查更多，交易更少，效果更好
