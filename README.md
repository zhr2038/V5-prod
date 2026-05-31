# V5-prod 量化交易系统

V5-prod 是一个面向 OKX 现货市场的实盘量化交易系统。当前生产版本的核心原则是：信号质量优先、风险控制优先、执行可追踪优先、诊断可复盘优先。

本仓库包含实盘主链路、风控与执行、状态清理、诊断打包、Web Dashboard、shadow/paper 研究闭环以及配套测试。V5 是唯一会连接交易所并提交真实订单的组件；研究、shadow、paper 与打包逻辑只能提供诊断证据，不应绕过 V5 的实盘风控。

---

## 目录

- [当前生产定位](#当前生产定位)
- [核心链路](#核心链路)
- [生产配置基线](#生产配置基线)
- [策略与信号](#策略与信号)
- [PROTECT 风控体系](#protect-风控体系)
- [Probe 小仓试探机制](#probe-小仓试探机制)
- [普通仓退出与浮盈保护](#普通仓退出与浮盈保护)
- [Swing 持仓保护](#swing-持仓保护)
- [Dust 与状态清理](#dust-与状态清理)
- [Negative Expectancy](#negative-expectancy)
- [Same-symbol Re-entry Guard](#same-symbol-re-entry-guard)
- [Candidate Snapshot](#candidate-snapshot)
- [SOL Paper Strategy Tracking](#sol-paper-strategy-tracking)
- [Alt Impulse Regime Shadow](#alt-impulse-regime-shadow)
- [Order Lifecycle](#order-lifecycle)
- [Quant-lab 接入边界](#quant-lab-接入边界)
- [ML 生产状态](#ml-生产状态)
- [诊断与打包](#诊断与打包)
- [Web Dashboard](#web-dashboard)
- [目录结构](#目录结构)
- [依赖与环境](#依赖与环境)
- [常用命令](#常用命令)
- [测试](#测试)
- [生产部署注意事项](#生产部署注意事项)
- [回滚与排障](#回滚与排障)
- [安全说明](#安全说明)

---

## 当前生产定位

当前 V5-prod 是小账户、白名单、低频实盘系统，不是高频系统，也不是无限扩 universe 的实验框架。生产默认只关注明确白名单中的主流现货交易对：

```text
BTC/USDT
ETH/USDT
SOL/USDT
BNB/USDT
```

生产目标不是“尽可能多交易”，而是回答以下问题：

- 当前 signal 是否足够强；
- 交易成本、滑点、手续费是否会吞掉 edge；
- 在 PROTECT 风险档位下，普通 entry 是否应该更严格；
- probe 小仓是否能在可控风险下捕捉突破初段；
- 已有利润是否能被及时保护；
- dust 残仓是否会污染持仓判断；
- negative expectancy 是否会阻止同一 symbol 连续试错；
- 每次买、卖、不买、不卖是否能在 bundle 中复盘；
- shadow/paper 策略是否有足够样本和成本质量，避免过早进入 live。

---

## 核心链路

V5 主链路可以概括为：

```text
market data
  -> alpha / trend / factor signals
  -> regime / risk level
  -> portfolio target
  -> entry gate / risk guard / cost guard
  -> router decision
  -> live execution engine
  -> order / fill / ledger / reconcile
  -> reports / bundle / dashboard
```

实盘订单必须经过 router decision 和 execution safety。任何 shadow、paper、diagnostic、bundle 输出都不能直接生成真实订单。

关键产物包括：

- `reports/runs/**/decision_audit.json`
- `reports/runs/**/trades.csv`
- `reports/runs/**/summary.json`
- `reports/runs/**/candidate_snapshot.csv`
- `reports/runs/**/order_lifecycle.csv`
- `reports/candidate_snapshot.csv`
- `reports/order_lifecycle.csv`
- `reports/alt_impulse_shadow_labels.jsonl`
- `reports/sol_paper_strategy_labels.jsonl`
- `reports/skipped_candidate_labels.jsonl`
- `reports/negative_expectancy_cooldown.json`

---

## 生产配置基线

主要生产配置在：

```text
configs/live_prod.yaml
```

重要基线：

- 显式白名单 universe；
- PROTECT 下普通 entry 更严格；
- `fee_bps` 和 `slippage_bps` 使用生产成本；
- backtest 默认成本不低于 live 成本；
- ML live overlay 关闭；
- split order runtime inactive；
- quant-lab 可以作为 shadow/cost/permission 诊断来源，但 V5 仍是唯一执行方。

当前 split order 状态：

```text
split_order_runtime_active=false
```

`split_orders` / `split_interval_sec` 不应被理解为生产已启用分单。当前账户订单较小，生产暂不实现 split order，避免引入 min-notional、dust 和执行复杂度。

---

## 策略与信号

V5 生产主链路使用多种信号，但每类信号的权限不同。

### Alpha6

Alpha6 是当前生产最重要的确认信号之一。PROTECT 下普通开多通常要求 Alpha6 同向 buy，并且 score、f4、f5 等确认项达到门槛。

常见字段：

- `alpha6_score`
- `alpha6_side`
- `f4_volume_expansion`
- `f5_rsi_trend_confirm`

### TrendFollowing

TrendFollowing 可以参与排序、候选解释和 shadow 研究，但在 PROTECT 下不能单独放松普通 entry gate。Trend-only 被拦截的样本会进入 high-score blocked / alt impulse shadow 等诊断路径。

### MeanReversion

MeanReversion 属于辅助信号，不应绕过 PROTECT entry gate。

### Cost-aware score

OPEN_LONG / REBALANCE buy order 会带上：

- `final_score`
- `alpha6_score`
- `trend_score`
- `expected_edge_bps`
- `expected_edge_source`

如果没有直接的 `expected_net_bps`，系统会用 score proxy 估算 `expected_edge_bps`，用于 quant-lab cost shadow 和本地诊断。

---

## PROTECT 风控体系

PROTECT 是当前小账户实盘最重要的保护档位。在 PROTECT 下，系统更保守：

- 普通 OPEN_LONG 必须经过 Alpha6 / f4 / f5 / cost-aware / negative expectancy 等 gate；
- Trend-only 不直接触发普通买入；
- short-cycle negative expectancy 可以在样本数较少但亏损明显时阻断普通开仓；
- re-entry guard 会阻止刚止盈或止损后的同 symbol 过早追高；
- profit-lock 会在普通仓浮盈后抬高止损或触发 trailing exit；
- probe 仍可以存在，但必须走专用 probe policy，不得放松普通 gate。

典型保护原因会写入：

```text
router_decisions
target_execution_explain
counts
skipped_candidate_labels.jsonl
candidate_snapshot.csv
```

`target_execution_explain` 中的 `passed_protect_entry_gate` 只表示 PROTECT entry gate 本身完成评估后的结果。若订单在更早的 hard guard 被拦截，例如 `protect_alt_short_cycle_negative_expectancy`、`negative_expectancy_cooldown`、`same_symbol_reentry_cooldown` 或 `cost_aware_edge`，会写入 `protect_entry_gate_evaluation_status=skipped_due_to_prior_guard` 和 `prior_guard_reason`，不得解读为“已通过 PROTECT gate 后被拦”。

---

## Probe 小仓试探机制

Probe 是小仓试探，不是普通趋势仓。probe 的目标是在风险可控前提下捕捉突破初段，如果没有兑现，应快速退出。

当前支持：

```text
market_impulse_probe
btc_leadership_probe
```

### market_impulse_probe

用于 broad market impulse：多个白名单 symbol 同时出现趋势买入，BTC 背景也偏正时，小仓试探。

### btc_leadership_probe

用于 BTC 率先突破场景。典型条件：

- 仅 BTC/USDT；
- 通常仅 PROTECT；
- 当前必须 flat 或只有 dust；
- regime 不得 Risk-Off；
- BTC 突破 lookback high 加 buffer；
- Alpha6 buy 存在；
- `alpha6_score`、`f4_volume_expansion`、`f5_rsi_trend_confirm` 达标；
- 可在严格限制下 bypass 单次 negative expectancy；
- 不能绕过 active cooldown。

### probe exit policy

当 `probe_exit_enabled=true` 时，probe 仓位统一走专用 exit policy，旧 `market_impulse_probe_time_stop` 只能作为 fallback。

优先级：

```text
probe_stop_loss
probe_take_profit
probe_trailing_stop
probe_time_stop
```

所有 probe exit 必须 bypass turnover cap，并写入 audit/counts。CLOSE_LONG 成交后会触发 position lifecycle cleanup。

### active probe 免疫普通 zero_target_close

active probe 由 probe exit policy 管理。普通 target rebalance、target_w=0 或 replacement target 被 gate 拦住，不应直接通过 `zero_target_close` 平掉 active probe。

如果跳过普通 zero-target close，会写入：

```text
active_probe_ignore_zero_target_close
```

---

## 普通仓退出与浮盈保护

普通非-probe 仓不走 probe exit policy。PROTECT 下新增 profit-lock trailing，用于保护已形成的净浮盈。

典型逻辑：

- `net_bps` 达到 `protect_profit_lock_min_net_bps` 后，有效 stop 至少抬到 breakeven plus；
- `highest_net_bps` 达到 trailing start 后，若回撤超过 gap，触发 `protect_profit_lock_trailing`；
- 更高浮盈可使用 strong trailing gap；
- profit-lock exit bypass turnover cap。

audit 字段包括：

- `protect_profit_lock_active`
- `entry_px`
- `current_px`
- `net_bps`
- `highest_net_bps`
- `effective_stop_px`
- `exit_reason`

---

## Swing 持仓保护

Swing 持仓用于避免优质中短周期信号被普通软退出过早打掉。

### min-hold exit guard

当 `swing_hold_position=true` 且未达到 `swing_min_hold_hours` 时：

允许 min-hold 前退出的硬原因：

- hard stop loss；
- kill switch；
- reconcile failure；
- exchange/account anomaly；
- Risk-Off 强制退出；
- `emergency_close` / `max_loss_hard_stop`；
- 明确 `zero_target_close`；
- 明确 hard risk close。

min-hold 前不应直接退出的软原因：

- `atr_trailing`
- `protect_profit_lock_trailing`
- `rank_exit`
- `normal_zero_target_close`
- `weak_signal_exit`
- `soft_stop`

soft exit 被拦截时会写：

- `hold_hours`
- `hold_hours_at_exit_check`
- `min_hold_hours`
- `swing_min_hold_hours`
- `exit_priority`
- `exit_allowed_before_min_hold`
- `exit_blocked_by_min_hold`
- `swing_min_hold_guard_checked`
- `swing_min_hold_guard_blocked`
- `soft_exit_blocked_by_min_hold`
- `hard_exit_exception_reason`
- `min_hold_block_reason`
- `would_exit_shadow`
- `blocked_exit_reason=swing_min_hold_soft_exit_blocked`
- `blocked_source_reason`

### swing ATR trailing early-exit soft guard

对于 min-hold 前的 `atr_trailing`，一律按 soft exit 处理。只要不是明确 hard stop / emergency / exchange risk / risk-off force exit，就先拦截，继续持有到 min-hold 或等待硬 exit。

典型 blocked reason：

```text
swing_atr_early_exit_guard
```

### f3-dominant swing qualification guard

f3_vol_adj_ret 主导且 f4/f5 确认弱的候选，仍可作为普通 entry 继续评估，但不能被标记为 `swing_hold_position=true`。

这不是禁止交易，而是不让 f3-dominant 弱确认候选享受 swing min-hold 保护。

输出：

- `dominant_factor`
- `dominant_factor_contribution_pct`
- `swing_f3_dominant_blocked`
- `swing_hold_position`
- `f4_volume_expansion`
- `f5_rsi_trend_confirm`

---

## Dust 与状态清理

小账户平仓后经常留下极小 dust。V5 对 dust 做统一处理：

- dust 不参与 anti-chase add-size；
- dust 不被视为有效持仓；
- dust 不反复生成低于 min-notional 的 close order；
- CLOSE_LONG 成交后，如果剩余价值低于 dust threshold，清理 position/profit/stop/highest/probe active state；
- 保留 dust 余额本身，不强卖 dust。

关键 audit：

- `dust_position_ignored_for_add_size=true`
- `raw_held_value_usdt`
- `effective_held_value_usdt=0`
- `dust_threshold_usdt`
- `dust_residual_no_close_order`
- `position_state_cleared_after_close`

---

## Negative Expectancy

Negative expectancy 用于根据近期真实 closed roundtrip 表现对 symbol 做 penalty、cooldown 或 open block。

当前口径：

- closed cycle 过滤按 `close_ts`，不是按 entry_ts；
- 如果 close leg 在 lookback/release window 内，即使 entry leg 在窗口前，也要回溯纳入；
- 如果找不到 entry leg，标记 degraded，不把该 close cycle 当负样本；
- `release_start_ts` 必须对应当前 `config_fingerprint`；
- fingerprint 改变时重置 release-scoped 统计起点。

输出字段包括：

- `closed_cycles`
- `net_pnl_sum_usdt`
- `net_expectancy_bps`
- `fast_fail_net_expectancy_bps`
- `last_close_ts`
- `closed_cycles_included_by_close_ts`
- `closed_cycles_with_entry_before_window`
- `missing_entry_leg_count`
- `lookback_filter_mode=close_ts`

PROTECT 下还包含 short-cycle guard：如果 closed cycles 少但亏损明显，例如 2 个 closed cycles 后净期望低于阈值，可以阻断普通非-probe OPEN_LONG。

---

## Same-symbol Re-entry Guard

同一 symbol 刚刚通过 profit-lock、probe stop、probe take-profit 或 trailing stop 退出后，不应马上在相近价格重新追高。

每次 CLOSE_LONG 成交后记录：

- `symbol`
- `exit_ts`
- `exit_px`
- `exit_reason`
- `highest_px_before_exit`
- `net_bps`

冷却期内准备 OPEN_LONG 时检查：

- normal entry；
- market impulse probe；
- btc leadership probe。

冷却期内允许 breakout exception，但必须满足明显突破上一高点或 exit price 的条件，并且原 entry/probe 条件本身仍然通过。

blocked reason：

```text
same_symbol_reentry_cooldown
```

---

## Candidate Snapshot

`candidate_snapshot.csv` 是 V5 给诊断、bundle 和后续研究使用的候选快照。当前要求每个 live run 都输出 candidate snapshot，每个 universe symbol 至少一行，即使没有订单也要记录 `final_decision=no_order` 和具体原因。

关键字段：

- `run_id`
- `ts_utc`
- `symbol`
- `strategy_candidate`
- `final_score`
- `rank`
- `alpha6_score`
- `alpha6_side`
- `f1` 到 `f5`
- `regime_state`
- `risk_level`
- `final_decision`
- `block_reason`

成本字段也必须覆盖 blocked/no_order candidate：

- `cost_source`
- `cost_bps`
- `selected_total_cost_bps`
- `cost_model_version`
- `expected_edge_bps`
- `required_edge_bps`
- `cost_gate_verified`
- `would_block_by_cost`
- `cost_source_quality`
- `expected_edge_source`
- `candidate_cost_trusted`
- `degraded_cost_model`
- `cost_resolution_reason`

成本优先级：

1. 请求级 Quant Lab cost cache；
2. latest symbol cost table；
3. public spread proxy / mixed actual proxy；
4. local estimate；
5. 只有 symbol missing 或 service unavailable 时才允许 global default，并必须 degraded。

---

## SOL Paper Strategy Tracking

V5 内置 SOL paper tracking，用于跟踪 quant-style 研究候选，但不生成真实订单。

当前跟踪策略：

```text
SOL_PROTECT_ALPHA6_LOW_EXCEPTION_PAPER_V1
SOL_F4_VOLUME_EXPANSION_PAPER_V1
ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1
```

每轮都会输出 heartbeat，即使没有 qualifying candidate。heartbeat 的目的不是交易，而是解释为什么没有入场。

heartbeat 诊断字段：

- `sol_candidate_present`
- `risk_level`
- `risk_off`
- `cooldown_active`
- `alpha6_score`
- `alpha6_side`
- `f4_volume_expansion`
- `f4_threshold`
- `f5_rsi_trend_confirm`
- `original_block_reason`
- `cost_source`
- `advisory_decision`
- `advisory_match_key`
- `no_sample_reason`

`no_sample_reason` 使用标准枚举：

```text
no_sol_candidate
f4_below_threshold
alpha6_not_buy
risk_not_protect
cooldown_active
risk_off
quant_lab_advisory_kill
```

如果 `would_enter=true`，还会输出：

- `would_size_usdt`
- `expected_exit_horizon`
- `arrival_bid`
- `arrival_ask`
- `arrival_mid`
- `estimated_spread_bps`
- `expected_order_type`
- `estimated_fill_px`

汇总输出：

- `reports/sol_paper_strategy_labels.jsonl`
- `summaries/paper_strategy_runs.csv`
- `summaries/paper_strategy_daily.csv`
- `summaries/paper_slippage_coverage.csv`
- `summaries/strategy_opportunity_advisory_reader.csv`

Live small ready 前必须满足 paper days、entry day count、arrival mid coverage、spread observation coverage 和成本质量要求。public spread proxy 本身不能直接让策略晋级 live。

V5 也会只读 quant-lab `strategy_opportunity_advisory.csv`。生产优先从 `/var/lib/v5-prod/strategy_opportunity_advisory.csv` 或 `/var/lib/v5-prod/quant_lab_latest_bundle.zip` 这类运行时同步文件读取，避免把中台包写进 Git 工作树；输入也可以是仓库内本地同步 CSV，或同步过来的 quant-lab expert pack `zip/tar/tar.gz`，reader 会从包内提取 `reports/strategy_opportunity_advisory.csv`；本地文件缺失时才尝试 quant-lab API JSON fallback。支持字段包括 `strategy_candidate`、`symbol`、`decision`、`recommended_mode`、`max_paper_notional_usdt`、`max_live_notional_usdt` 和 `live_block_reasons`。当前只响应 `recommended_mode=paper` 或 `recommended_mode=shadow` 的 advisory：SOL `PAPER_READY` 继续进入 paper tracking；`KILL` 会被记录为 negative advisory，不会生成 live order。`max_live_notional_usdt` 默认忽略，除非本地显式设置 `enable_live_small_from_quant_lab=true` 且 advisory 为 `LIVE_SMALL_READY`。生产默认值为 `false`。

V5 还会只读 quant-lab `paper_strategy_proposals.csv`。当前仅把 `ETH_USDT_F3_DOMINANT_ENTRY_PAPER_V1` 转为 paper-only tracker：要求 `symbol=ETH/USDT`、`strategy_candidate=f3_dominant_entry`、`horizon=48h`，并且当前上下文不能是 Risk-Off。该策略不产生真实订单，live 阻断原因固定包含 `cost_source_not_actual_or_mixed`、`f3_global_evidence_negative` 和 `no_paper_pnl_observations`。

---

## Alt Impulse Regime Shadow

ALT impulse 历史表现明显 regime-dependent，因此当前只能进入 regime shadow，不得进入 PAPER_READY 或 LIVE。

V5 输出以下上下文字段：

- `regime_state`
- `risk_level`
- `btc_trend_state`
- `broad_market_positive_count`
- `volatility_bucket`
- `funding_state` 如可用

状态字段：

- `shadow_decision=REGIME_SHADOW` 或 `KEEP_SHADOW`
- `alpha_discovery_board_status=REGIME_SHADOW` 或 `KEEP_SHADOW`
- `paper_ready_allowed=false`
- `live_ready_allowed=false`
- `shadow_decision_reason=alt_impulse_regime_dependent_shadow_only`

输出文件：

- `reports/alt_impulse_shadow_labels.jsonl`
- `summaries/alt_impulse_shadow_outcomes.csv`
- `summaries/alt_impulse_shadow_outcomes_by_symbol.csv`
- `summaries/alt_impulse_shadow_outcomes_by_reason.csv`
- `summaries/alt_impulse_shadow_outcomes_by_horizon.csv`
- `summaries/alt_impulse_shadow_by_regime.csv`
- `summaries/alt_impulse_shadow_by_symbol_regime_horizon.csv`
- `summaries/alt_impulse_shadow_readiness.json`
- `summaries/alt_impulse_shadow_readiness_by_symbol.csv`

验收重点是：下一包能回答“ALT impulse 到底在哪种 regime、哪个 symbol、哪个 horizon 下有效”，而不是只给出整体平均表现。

ALT impulse readiness 是只读诊断门，不会改变实盘交易逻辑。默认结论应为 `ready_for_live_probe=false`，直到每个 symbol 自己满足足够样本与收益稳定性后才允许进入未来 live probe 评估。当前规则包括：

- 总样本数至少 30，最近 7 天样本数至少 10；
- 24h 平均 net bps > 80，24h win rate > 0.60；
- 48h 平均 net bps > 50；
- 按 symbol 单独判定，不能用 ETH/SOL 的强样本证明 BNB；
- BNB 还必须满足 high-score blocked 24h 平均 net bps > 0，且 negative expectancy >= 0。

---

## Order Lifecycle

`order_lifecycle.csv` 用于把订单从 decision 到 submit 到 fill 串起来，帮助成本模型从 proxy 升级到 actual fills。

订单生成时记录：

- `decision_ts`
- `signal_price`
- `arrival_bid`
- `arrival_ask`
- `arrival_mid`
- `spread_bps_at_decision`

提交订单时记录：

- `submit_ts`
- `cl_ord_id`
- `order_px`
- `order_type`

成交后记录：

- `first_fill_ts`
- `last_fill_ts`
- `avg_fill_px`
- `filled_qty`
- `fee_usdt`

如果 `trade_metrics` 有成交但 `order_lifecycle.csv` 为空，bundle 会标记 high issue。

---

## Quant-lab 接入边界

V5 可以读取 quant-lab 的 permission/cost 结果，也可以输出 bundle 供 quant-lab ingest。但边界必须清楚：

- V5 是唯一实盘执行方；
- quant-lab 不放置、撤销、修改真实订单；
- V5 不向 quant-lab 写 lake；
- quant-lab shadow 不应影响真实订单；
- enforce/cost_only/permission_only 只有在配置明确启用时才可影响订单；
- 如果 quant-lab 不可用，V5 必须按配置 fail-open/fail-closed，并写入 telemetry。

常见 telemetry：

- `quant_lab_permission_audit`
- `quant_lab_cost_usage.csv`
- `quant_lab_shadow_outcomes.csv`
- `quant_lab_shadow_outcomes_by_permission.csv`

---

## ML 生产状态

当前 `live_prod` 明确关闭 ML live overlay：

```yaml
alpha:
  ml_factor:
    enabled: false

execution:
  collect_ml_training_data: false
  ml_research_use_stable_universe: false
```

生产路径不应：

- 加载 ML 模型；
- 读取 active model pointer；
- 写 ML overlay score；
- 把 `promotion_not_passed` 当生产 health 红色事故。

保留的脚本仅用于离线研究：

- `scripts/daily_ml_training.py`
- `scripts/model_promotion_gate.py`
- `scripts/run_shadow_tuned_xgboost.py`

研究依赖放在：

```text
requirements-research.txt
```

生产依赖放在：

```text
requirements.txt
```

没有安装 xgboost / scikit-learn 时，生产 pipeline 仍应可启动。

---

## 诊断与打包

V5 的诊断打包用于导出最近 runs、交易、状态、候选、shadow、paper、issues 和 README 摘要。

常见输出：

```text
summaries/window_summary.json
summaries/issues_to_fix.json
summaries/trade_metrics.csv
summaries/fill_metrics.csv
summaries/trades_roundtrips.csv
summaries/probe_lifecycle_audit.csv
summaries/candidate_snapshot.csv
summaries/order_lifecycle.csv
summaries/paper_strategy_runs.csv
summaries/alt_impulse_shadow_by_regime.csv
raw/recent_runs/<run_id>/*
raw/reports/*
README.md
```

典型 high/medium issue 会覆盖：

- trades.csv 与 summary.json count mismatch；
- order lifecycle 缺失；
- candidate snapshot 缺失或成本降级；
- negative expectancy state 与 roundtrip summary 不一致；
- swing soft exit 违反 min-hold；
- alt impulse / skipped label 缺 entry_px 或 future_px；
- dirty worktree / provenance degraded。

---

## Web Dashboard

Dashboard 是观察入口，不是交易入口。它读取 V5 workspace 下的 reports、state、sqlite 和运行日志，提供：

- 健康状态；
- 当前风险档位；
- 持仓和 ledger；
- 最近成交；
- router decisions；
- candidate snapshot；
- skipped labels；
- probe / paper / shadow 诊断；
- bundle 和 issue 摘要。

ML overlay 已在 live_prod 关闭，因此 Dashboard 不应把 ML promotion failure 当作红色生产事故。

启动示例：

```bash
python scripts/web_dashboard.py
```

生产建议放在内网、VPN 或带鉴权的反向代理之后。

---

## 目录结构

```text
.
├── main.py
├── configs/
│   ├── live_prod.yaml
│   └── schema.py
├── src/
│   ├── alpha/
│   ├── core/
│   ├── execution/
│   ├── portfolio/
│   ├── regime/
│   ├── reporting/
│   │   ├── alt_impulse_shadow.py
│   │   ├── candidate_snapshot.py
│   │   ├── order_lifecycle.py
│   │   ├── sol_paper_strategy_tracker.py
│   │   └── skipped_candidate_tracker.py
│   ├── research/
│   └── risk/
├── scripts/
│   ├── generate_v5_bundle_remote.sh
│   ├── test_v5_bundle_export.py
│   └── web_dashboard.py
├── tests/
│   ├── test_alt_impulse_shadow.py
│   ├── test_sol_paper_strategy_tracker.py
│   ├── test_candidate_snapshot.py
│   ├── test_probe_exit_policy.py
│   └── ...
├── docs/
├── reports/
├── requirements.txt
└── requirements-research.txt
```

---

## 依赖与环境

生产依赖：

```bash
pip install -r requirements.txt
```

研究依赖：

```bash
pip install -r requirements-research.txt
```

生产环境不应强制依赖 xgboost / heavy ML research dependencies。

---

## 常用命令

运行主程序：

```bash
python main.py --config configs/live_prod.yaml
```

运行 Dashboard：

```bash
python scripts/web_dashboard.py
```

生成 V5 bundle：

```bash
bash scripts/generate_v5_bundle_remote.sh
```

检查 bundle exporter：

```bash
python scripts/test_v5_bundle_export.py
```

---

## 测试

常用最小回归：

```bash
python -m pytest tests/test_alt_impulse_shadow.py -q
python -m pytest tests/test_sol_paper_strategy_tracker.py -q
python -m pytest tests/test_candidate_snapshot.py -q
python -m pytest tests/test_probe_exit_policy.py -q
python scripts/test_v5_bundle_export.py
```

针对最近改动的重点测试：

- `tests/test_alt_impulse_shadow.py`：regime shadow、by-regime 聚合、REGIME_SHADOW 状态；
- `tests/test_sol_paper_strategy_tracker.py`：SOL paper heartbeat、no_sample_reason、成本和报价诊断；
- `tests/test_candidate_snapshot.py`：每 run 候选覆盖、成本字段、symbol-level fallback；
- `tests/test_probe_exit_policy.py`：probe exit 接管、time stop、stop loss、take profit、trailing；
- `scripts/test_v5_bundle_export.py`：bundle 输出、README 摘要、issues 口径。

---

## 生产部署注意事项

生产部署应保证：

- git worktree clean；
- `main` 与 GitHub 远端一致；
- `reports/` 和运行时 state 不混入源码提交；
- 配置改动必须显式；
- runtime consumption audit 不应出现未解释的 configured_not_consumed；
- dirty worktree 应进入 data quality warning；
- 修改后需要推送 GitHub，并同步 qyun 生产目录。

生产目录建议以 git repo 方式部署，使用 fast-forward 更新，避免手工覆盖文件造成 provenance degraded。

---

## 回滚与排障

常见排障顺序：

1. 看 `git status --short`，确认是否 dirty；
2. 看最近 commit；
3. 看 `reports/runs/**/decision_audit.json`；
4. 看 `reports/runs/**/trades.csv` 与 `summary.json` 是否一致；
5. 看 `summaries/issues_to_fix.json`；
6. 看 `negative_expectancy_cooldown.json` 与 `trades_roundtrips.csv`；
7. 看 `candidate_snapshot.csv` 是否每个 run / symbol 都覆盖；
8. 看 `order_lifecycle.csv` 是否在有成交时非空；
9. 看 probe / swing / profit-lock / re-entry guard 的 router reason；
10. 必要时回滚到上一个 clean commit。

回滚原则：

- 优先 git revert 或 fast-forward 到已知 good commit；
- 不要手工删除 state 来掩盖问题；
- 不要在未确认 dust / ledger / fills 前强制清仓；
- 不要启用 shadow/paper 策略为 live，除非已有明确配置和足够后验样本。

---

## 安全说明

本仓库用于真实交易系统。请遵守以下边界：

- 不在 README 或代码中提交 API key、密码、cookie、token；
- 不把生产 Dashboard 裸露到公网；
- 不在 dirty worktree 状态下依赖策略 evidence 做 live 决策；
- 不让 research-only ML 依赖进入生产必需路径；
- 不让 paper/shadow 输出直接生成实盘订单；
- 不绕过 kill switch、reconcile、ledger、dust 和 negative expectancy guard；
- 对生产配置相关改动，优先 fail-fast 或显式 warning，不要静默忽略。

---

## 免责声明

V5-prod 是真实交易系统代码，不构成投资建议。任何策略、配置、风控、执行或部署改动都可能造成真实资金损失。使用者需要自行理解风险、验证配置、控制仓位，并对最终交易结果负责。
