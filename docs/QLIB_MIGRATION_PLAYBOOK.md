# Qlib 迁移实战笔记（V5）

> 更新时间：2026-03-06
> 目标：把 Qlib 的核心经验（因子、IC 监控、换手约束、成本纪律）落到 V5 实盘框架里。

---

## 1) 这次到底迁移了什么

### A. Alpha158 风格因子（已接入）
新增文件：`src/alpha/qlib_factors.py`

已实现因子：
- `f6_corr_pv_10`：价格与成交量相关（CORR）
- `f7_cord_10`：价变与量变相关（CORD）
- `f8_rsqr_10`：趋势拟合度（RSQR）
- `f9_rank_20`：窗口分位（RANK）
- `f10_imax_14` / `f11_imin_14` / `f12_imxd_14`：Aroon 风格时间位置因子

### B. IC / RankIC 监控（已接入）
新增文件：`src/alpha/ic_monitor.py`

运行产物：
- `reports/alpha_ic_history.jsonl`
- `reports/alpha_ic_timeseries.jsonl`
- `reports/alpha_ic_monitor.json`

用途：
- 看信号是否有效（score IC / rank IC）
- 看因子是否失效（factor IC）
- 看衰减（short vs long）

### C. TopkDropout（限量换仓，已接入）
接入位置：`src/portfolio/portfolio_engine.py`

机制：
- 每轮最多替换 `n_drop_per_cycle` 个标的
- 旧标的未达到最短持有轮次 `hold_cycles` 不允许被替换
- 状态持久化：`reports/topk_dropout_state.json`

### D. 负期望标的冷却（已接入）
新增文件：`src/risk/negative_expectancy_cooldown.py`
接入位置：`src/core/pipeline.py`

机制：
- 扫描 `reports/orders.sqlite` 近 `lookback_hours` 的闭环交易（FIFO 近似）
- 若某标的平均期望 `expectancy < threshold` 且样本数达标，进入冷却
- 冷却期间禁止买入
- 状态持久化：`reports/negative_expectancy_cooldown.json`

---

## 2) 配置总开关（生产可直接调）

### `alpha` 下
- `alpha158_overlay.enabled`
- `alpha158_overlay.blend_weight`（建议 0.25~0.45）
- `topk_dropout.enabled`
- `topk_dropout.n_drop_per_cycle`
- `topk_dropout.hold_cycles`
- `dynamic_ic_weighting.enabled`

### `execution` 下
- `negative_expectancy_cooldown_enabled`
- `negative_expectancy_lookback_hours`
- `negative_expectancy_min_closed_cycles`
- `negative_expectancy_threshold_usdt`
- `negative_expectancy_cooldown_hours`

---

## 3) 推荐参数（当前口径）

### 正式盘（`configs/live_prod.yaml`）
- `alpha158_overlay.blend_weight: 0.35`
- `topk_dropout.n_drop_per_cycle: 2`
- `topk_dropout.hold_cycles: 2`
- 负期望冷却：`24h / min 4 cycles / expectancy < 0 / cool 24h`

### 20U 小盘（`configs/live_20u_real.yaml`）
- `alpha158_overlay.blend_weight: 0.40`
- `topk_dropout.n_drop_per_cycle: 1`（更保守）
- `topk_dropout.hold_cycles: 2`
- 负期望冷却：`24h / min 3 cycles / expectancy < 0 / cool 24h`

---

## 4) 排障手册（高频问题）

### 情况1：有信号但很少下单
重点检查：
1. `cost_aware_min_score_floor` 是否太高
2. `topk_dropout.n_drop_per_cycle` 是否过小
3. `negative_expectancy_cooldown.json` 是否屏蔽了主力币
4. `min_notional` 与 `exchange_min_notional` 是否双重卡死

### 情况2：换手仍偏高
重点检查：
1. `topk_dropout.hold_cycles` 提高到 3
2. `max_rebalance_turnover_per_cycle` 下调（如 0.30 -> 0.20）
3. `rank_exit_confirm_rounds` 提高（如 2 -> 3）

### 情况3：策略“全卖无买”
重点检查：
1. `alpha.min_score_threshold` 是否过高
2. `cost_aware_entry` 是否过严
3. IC monitor 里因子 `rank_ic_short.mean` 是否长期为负
4. 是否被 `negative_expectancy` 冷却全面拦截

---

## 5) 运行链路（简化版）

1. `AlphaEngine` 计算 base 因子 + Alpha158 overlay
2. `ic_monitor` 更新滚动 IC
3. `PortfolioEngine` 做 TopK 选择 + TopkDropout 限量替换
4. `Pipeline` 走风控门控（成本、换手、冷却等）
5. `Execution` 执行并回写订单

---

## 6) 后续可继续做（下一阶段）

1. **组合优化器**（Qlib EnhancedIndexing 思想）
   - 真正引入 `sum(|w_new-w_old|)<=delta` 求解器级约束
2. **DoubleEnsemble**
   - 子模型集成 + 样本重加权 + 特征重采样
3. **PIT/防泄漏训练流程**
   - 统一滚动切片，避免未来信息污染

---

## 7) 关键文件索引

- 因子：`src/alpha/qlib_factors.py`
- IC监控：`src/alpha/ic_monitor.py`
- Alpha主引擎：`src/alpha/alpha_engine.py`
- 多策略：`src/strategy/multi_strategy_system.py`
- 组合层（TopkDropout）：`src/portfolio/portfolio_engine.py`
- 负期望冷却：`src/risk/negative_expectancy_cooldown.py`
- 主流程接线：`src/core/pipeline.py` / `main.py`
- 配置：`configs/schema.py` / `configs/live_prod.yaml` / `configs/live_20u_real.yaml`
