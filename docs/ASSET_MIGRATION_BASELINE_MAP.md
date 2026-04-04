# V5 资产拆解与迁移清单

日期：2026-03-21

## 0. 范围与约束

- 本轮只做资产拆解、模块映射和迁移风险说明，不改任何 live runtime 代码。
- 当前 worktree 很脏，本清单基于只读盘点形成，避免碰已有运行改动。
- 本清单里的“底座”只指可复用的 live execution substrate，不包含研究、ML、回测、多策略实验能力。
- 本清单里的 `baseline` 指“保留为规则/契约/公式参考，不接入 live runtime”；`evidence` 指“只保留为训练、验证、监控、实验或归因证据”。

## 1. 可复用到底座的资产清单

| 源模块 | 可复用资产 | 建议落点 | 迁移形态 | 证据 | 迁移说明 |
| --- | --- | --- | --- | --- | --- |
| `src/execution/live_preflight.py` | `ALLOW/SELL_ONLY/ABORT` 前置风控决策协议；“账单同步 -> ledger -> reconcile -> kill-switch -> borrow/account config” 的串行闸门顺序 | `PreTradeSafetyStage` | 抽象后下沉 | `main.py:760-805`，`src/execution/live_preflight.py:59-67`，`src/execution/live_preflight.py:120-132`，`src/execution/live_preflight.py:134-507` | 可复用的是决策协议和顺序，不是当前实现的 OKX/BillsStore/JSON 文件路径。先抽象 `ExchangeAccountGateway`、`BillFeed`、`StatusRepository`、`KillSwitchStore`、`BlacklistWriter`。 |
| `src/execution/ledger_engine.py` | baseline 账务校验算法；基线快照 + bills 增量累加 + 当前余额比对；dust 基线重置策略 | `LedgerChecker` | 抽象后下沉 | `src/execution/ledger_engine.py:42-50`，`src/execution/ledger_engine.py:74-108`，`src/execution/ledger_engine.py:110-216` | 这是底座里最值得复用的“轻量账务核对”资产。需去掉对 `OKXPrivateClient.get_balance()`、`BillsStore`、`ledger_state.json` 的硬编码。 |
| `src/execution/reconcile_engine.py` | 交易所余额 vs 本地持仓/现金的对账模型；`ReconcileThresholds`；dust 忽略和状态输出 schema | `InventoryReconcileService` | 抽象后下沉 | `src/execution/reconcile_engine.py:38-43`，`src/execution/reconcile_engine.py:45-66`，`src/execution/reconcile_engine.py:67-121`，`src/execution/reconcile_engine.py:155-281` | 可复用的是对账语义和状态结构。需先抽象 `ExchangeBalanceSnapshot`、`LocalInventorySnapshot`、`PriceResolver`，否则仍然绑死 OKX 和 `SpreadSnapshotStore`。 |
| `src/reporting/decision_audit.py` | 运行级审计壳；拒绝原因计数；备注流；JSON sidecar 持久化/回读模式 | `ExecutionAuditEnvelope` | 缩核后下沉 | `src/reporting/decision_audit.py:11-115`，`src/reporting/decision_audit.py:117-150` | 底座可以保留“审计容器”和 I/O 习惯，但必须拆掉 ML、多策略、预算等 V5 扩展字段，改成 core schema + extension bag。 |
| `src/execution/live_execution_engine.py` | live 下单适配层；基于 `clOrdId + decision_hash` 的幂等提交；ACK/POLL 状态机；dust 卖单本地清理；fill 驱动的本地持仓同步 | `BrokerExecutionAdapter` | 抽象后下沉 | `src/execution/live_execution_engine.py:155-175`，`src/execution/live_execution_engine.py:377-397`，`src/execution/live_execution_engine.py:736-977`，`src/execution/live_execution_engine.py:1018-1124` | 这是底座的核心资产之一，但不能按文件直接复制。当前实现同时依赖 OKX 规格、文件式 gate、本地 Store、pipeline 元数据。 |
| `src/execution/live_execution_engine.py` | `submit_gate_for_live()` 的 submit gate 语义：kill-switch/reconcile 状态与下单通道对齐 | `SubmitGatePolicy` | 直接保留语义，重写依赖 | `src/execution/live_execution_engine.py:155-175` | 语义可直接沿用，但状态读取不应继续依赖 `reports/kill_switch.json` 与 `reports/reconcile_status.json`。 |
| `src/core/pipeline.py` 输出的订单元数据 | `decision_hash`、`window_start_ts`、`window_end_ts`、`regime`、`deadband_pct` 等执行期元数据 | `ExecutionIntent.meta` 契约 | 缩核后下沉 | `src/core/pipeline.py:2376-2402`，`src/execution/live_execution_engine.py:377-397` | 底座需要保留的是“执行意图元数据契约”，不是整个 `V5Pipeline`。这部分是 live execution 与 strategy stage 的边界。 |

## 2. 必须降级为 baseline/evidence 的模块清单

| 模块/目录 | 降级形态 | 原因 | 保留内容 |
| --- | --- | --- | --- |
| `src/alpha/alpha_engine.py` | `baseline` | 该文件把多策略融合、ML overlay、promotion gate、策略审计文件回填一起塞进 alpha 打分链路，不适合作为底座 runtime 复制。证据：`src/alpha/alpha_engine.py:21-31`，`src/alpha/alpha_engine.py:351-489`，`src/alpha/alpha_engine.py:771-922`，`src/alpha/alpha_engine.py:1087-1142`。 | 保留经典因子/评分契约、快照 schema、权重映射规则，去掉多策略和 ML 在线叠加。 |
| `src/alpha/qlib_factors.py` | `baseline` | 因子公式本身可以作为参考，但它属于策略层，不属于 execution substrate。 | 保留公式和字段定义，不接入 live execution 底座。 |
| `src/alpha/ic_monitor.py` | `evidence` | `main.py` 在交易后更新 IC 监控，属于评估/归因，不应进入底座运行环。证据：`main.py:585-600`。 | 保留评估输出和监控指标。 |
| `src/alpha/` 其余内容 | `baseline/evidence` | 整个目录代表打分与评估层，而不是 broker / state / safety 底座。 | 只保留规则说明与历史证据。 |
| `src/backtest/backtest_engine.py` | `evidence` | 明确是回测引擎，而且直接复用 `V5Pipeline`。证据：`src/backtest/backtest_engine.py:37-47`，`src/backtest/backtest_engine.py:86-99`。 | 保留回测方法论和指标。 |
| `src/backtest/walk_forward.py` | `evidence` | 这是 walk-forward 验证 harness，不应进入 live 底座。证据：`src/backtest/walk_forward.py:47-71`，`src/backtest/walk_forward.py:119-179`。 | 保留折叠方法、统计报告。 |
| `src/backtest/cost_factory.py`、`src/backtest/cost_factory_fixed.py`、`src/backtest/cost_calibration.py` | `evidence` | 这些都是回测成本建模与校准组件，不是 live substrate。 | 保留成本假设和校准证据。 |
| `src/backtest/` 其余内容 | `evidence` | 全部属于验证层。 | 只保留报告与参数依据。 |
| `src/research/` 全目录 | `evidence` | 该目录承担数据集构建、训练任务、优化器、shadow/AB 监控、窗口诊断、实验脚手架。证据：`src/research/task_runner.py:13-22`，`src/research/task_runner.py:363-368`，`src/research/task_runner.py:772-782`。 | 保留训练记录、数据准备规则、实验报告，不进入底座 runtime。 |
| `src/execution/ml_data_collector.py` | `evidence` | 这是训练快照收集和标签回填数据库，不是 live execution 必需资产。证据：`src/execution/ml_data_collector.py:1-3`，`src/execution/ml_data_collector.py:54-63`，`src/core/pipeline.py:2528-2559`。 | 保留样本采集 schema 与训练数据证据。 |
| `src/execution/ml_factor_model.py` | `evidence` | 这是训练/加载/预测模型实现，且依赖 `src.research.dataset_builder`。证据：`src/execution/ml_factor_model.py:1-8`，`src/execution/ml_factor_model.py:19`，`src/execution/ml_factor_model.py:188-218`，`src/execution/ml_factor_model.py:236-559`。 | 保留模型定义、训练记录、产物格式。 |
| `src/execution/ml_feature_optimizer.py` | `evidence` | 这是训练特征筛选工具，不应进入底座 live 环。证据：`src/execution/ml_feature_optimizer.py:1-6`，`src/execution/ml_feature_optimizer.py:124-157`。 | 保留特征筛选规则。 |
| `src/execution/ml_time_series_cv.py` | `evidence` | 时间序列 CV 只服务训练验证。证据：`src/execution/ml_time_series_cv.py:1-3`，`src/execution/ml_time_series_cv.py:23-179`。 | 保留验证方法和分割规则。 |
| `src/execution/ml_*` 其余内容 | `evidence` | `ml_` 前缀模块整体都属于训练/验证/模型评价扩展，不属于底座最小 live 资产。 | 保留模型、特征、CV、训练记录。 |
| `src/strategy/multi_strategy_system.py` | `baseline` | 这是策略编排、信号融合、动态资金分配系统，而且会落 `reports/runs/<run_id>/strategy_signals.json`；它通过 `AlphaEngine` 被拉入 live 打分链路，但不属于 execution substrate。证据：`src/strategy/multi_strategy_system.py:845-907`，`src/strategy/multi_strategy_system.py:995-1005`，`src/strategy/multi_strategy_system.py:1213-1286`，`src/alpha/alpha_engine.py:351-489`。 | 只保留策略融合设计和信号结构说明。 |

## 3. 迁移风险表：与 `main.py` / `src/core/pipeline.py` 强耦合、不能直接复制的模块

| 模块 | 与 `main.py` / `src/core/pipeline.py` 的耦合点 | 风险等级 | 不能直拷的原因 | 必须先抽象的接口 |
| --- | --- | --- | --- | --- |
| `main.py` | 入口直接把预算状态写进 `audit`，调用 `pipe.run()`，然后串接 order arbitration、`LivePreflight`、`LiveExecutionEngine`、fills sync、summary/budget 回填。证据：`main.py:443-565`，`main.py:667-884`，`main.py:925-997`。 | 高 | 这是 V5 特化 orchestration，不是底座组件；直接复制会把预算、研究、汇总、OKX 同步逻辑整包带走。 | `RunContext`、`StrategyStage`、`SafetyStage`、`ExecutionStage`、`PostTradeStage`。 |
| `src/core/pipeline.py` | 初始化时直接引入 `AlphaEngine`、`PortfolioEngine`、`RiskEngine`、`DecisionAudit`、`NegativeExpectancyCooldown`、`MLDataCollector`。证据：`src/core/pipeline.py:57-83`，`src/core/pipeline.py:137-235`。同时它输出的订单 `meta` 被 live execution 消费：`src/core/pipeline.py:2376-2402`。 | 高 | 这是“策略 + 风控 + 预算 + ML + 审计”的聚合器，不是 execution substrate。任何直接复用都会把策略层污染带到底座。 | `SignalEngine`、`TargetAllocator`、`RiskRouter`、`ExecutionIntentBuilder`、`AuditExtensionSink`。 |
| `src/execution/live_preflight.py` | `main.py` 直接把 `cfg.execution`、executor 内的 `okx`、`PositionStore`、`AccountStore`、`reports/*.json/sqlite` 传进去。证据：`main.py:760-805`，`src/execution/live_preflight.py:75-98`。 | 高 | 当前实现既做编排又做 OKX/文件/Store 适配；底座如果直拷，会保留 V5 的文件布局和交易所语义。 | `ExchangeAccountGateway`、`BillFeed`、`LedgerChecker`、`ReconcileChecker`、`StatusRepository`、`KillSwitchStore`。 |
| `src/execution/live_execution_engine.py` | 依赖 pipeline 生成的 `decision_hash/window_start_ts/window_end_ts/regime/deadband_pct` 元数据。证据：`src/core/pipeline.py:2376-2402`，`src/execution/live_execution_engine.py:377-397`。同时 submit gate 读 `reconcile_status_path/kill_switch_path`：`src/execution/live_execution_engine.py:155-175`。 | 高 | 这个模块表面是 broker adapter，实际上绑定了 V5 的文件 gate、订单元数据、OrderStore/PositionStore、OKX fills/lot 规则。 | `SubmitGate`、`BrokerClient`、`OrderRepository`、`PositionProjection`、`InstrumentSpecProvider`、`RiskStateCleaner`。 |
| `src/reporting/decision_audit.py` | `main.py` 在 pre-run / post-run 写预算字段，`pipeline` 写 `budget_action`、`strategy_signals`、`ml_signal_overview`、reject 计数。证据：`main.py:443-550`，`main.py:991-996`，`src/core/pipeline.py:1244-1287`，`src/core/pipeline.py:1761-1775`，`src/core/pipeline.py:2528-2559`。 | 高 | 这个 schema 已被 V5 的预算、ML、多策略审计污染；直拷会把不属于底座的字段固定成核心协议。 | `CoreExecutionAudit`、`BudgetAuditExtension`、`StrategyAuditExtension`、`MLAuditExtension`。 |
| `src/execution/reconcile_engine.py` | 当前实现直接读取 OKX `cashBal/eqUsd`，本地快照来自 `PositionStore + AccountStore`，dust 估值依赖 `SpreadSnapshotStore`。证据：`src/execution/reconcile_engine.py:67-121`，`src/execution/reconcile_engine.py:155-205`。 | 中 | 算法可复用，但数据源和估值器被写死成 V5 当前实现。 | `ExchangeBalanceSnapshot`、`LocalInventorySnapshot`、`PriceResolver`。 |
| `src/execution/ledger_engine.py` | 当前实现直接读取 OKX `cashBal`，账单来自 `BillsStore`，状态落地到 JSON baseline/status。证据：`src/execution/ledger_engine.py:74-108`，`src/execution/ledger_engine.py:110-216`。 | 中 | 账务算法能留，但存储、账单源和余额 authority 都要抽象。 | `CashBalanceProvider`、`BillLedgerStore`、`BaselineRepository`。 |

## 4. 结论

- 能直接沉到底座的不是整文件，而是五类资产：前置风控决策协议、ledger/reconcile 校验算法、执行意图元数据契约、broker 幂等下单状态机、核心审计壳。
- `main.py` 和 `src/core/pipeline.py` 都不应直接复制。它们必须先被拆成“策略层”和“底座层”的明确接口边界。
- `src/alpha/`、`src/backtest/`、`src/research/`、`src/execution/ml_*`、`src/strategy/multi_strategy_system.py` 本轮都应视为 `baseline/evidence`，不能作为底座 live runtime 迁移对象。
- 本次交付仅新增这份文档，不包含任何 live runtime 行为修改。
