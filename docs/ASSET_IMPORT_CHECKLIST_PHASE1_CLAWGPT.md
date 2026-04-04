# V5 首批逐文件导入清单

日期：2026-03-21

## 0. 目标与约束

- 基准文档：`docs/ASSET_MIGRATION_BASELINE_MAP.md`
- 统一主仓目标根路径：`\\192.168.1.15\docker\ClawGPT-trading-bot`
- 本清单只列首批 5 个文件，不直接拷文件到主仓
- 本清单只产出导入映射，不改 V5 live runtime

## 1. 明确排除

以下内容本轮不得作为首批导入对象，也不得被顺手带入主仓：

- `main.py`
- `src/core/pipeline.py`
- `src/alpha/`
- `src/backtest/`
- `src/research/`
- `src/execution/ml_*`
- 运行数据、缓存、部署脚本、模型文件

## 2. 首批导入总表

| 顺序 | source_file | proposed_target_path | 导入方式 | 当前结论 |
| --- | --- | --- | --- | --- |
| 1 | `src/execution/ledger_engine.py` | `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\ledger_engine.py` | 抽算法，不直拷 | 先抽 `CashBalanceProvider` / `BillLedgerStore` / `BaselineRepository` |
| 2 | `src/execution/reconcile_engine.py` | `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\reconcile_engine.py` | 抽算法，不直拷 | 先抽 `ExchangeBalanceSnapshot` / `LocalInventorySnapshot` / `PriceResolver` |
| 3 | `src/reporting/decision_audit.py` | `\\192.168.1.15\docker\ClawGPT-trading-bot\src\reporting\decision_audit.py` | 缩核迁移 | 只保留 core audit 壳，去掉 V5 扩展字段依赖 |
| 4 | `src/execution/live_preflight.py` | `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\live_preflight.py` | 抽编排壳，不直拷 | 依赖 1/2 完成后再接 `PreTradeSafetyStage` |
| 5 | `src/execution/live_execution_engine.py` | `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\live_execution_engine.py` | 抽 broker adapter，不直拷 | 最后接入，单独抽 submit gate、broker client、stores |

## 3. 逐文件导入清单

### 3.1 `src/execution/ledger_engine.py`

- `source_file`: `\\192.168.1.15\docker\V5-trading-bot\src\execution\ledger_engine.py`
- `proposed_target_path`: `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\ledger_engine.py`
- `asset_role`: baseline 账务核验算法
- `保留内容`:
  - baseline 快照 schema
  - `expected_balance = baseline_balance + SUM(balChg)` 校验逻辑
  - USDT / base tolerance 分流
  - dust baseline reset 判定
- `直接导入时会碰到的依赖`:
  - `src.execution.bills_store.BillsStore`
  - `src.execution.okx_private_client.OKXPrivateClient`
- `隐藏耦合`:
  - 默认写 `reports/ledger_state.json`
  - 默认写 `reports/ledger_status.json`
  - 余额 authority 固定为 OKX `get_balance()`
- `必须先抽象的接口`:
  - `CashBalanceProvider`
  - `BillLedgerStore`
  - `BaselineRepository`
- `禁止跟入`:
  - `main.py`
  - `src/core/pipeline.py`
  - 任意 `src/alpha/`、`src/backtest/`、`src/research/`、`src/execution/ml_*`
  - 任意 `reports/*.json`、`reports/*.sqlite`
- `导入结论`:
  - 只迁算法和状态 schema
  - 不迁 V5 的文件路径、OKX client 实现、运行期 baseline 文件

### 3.2 `src/execution/reconcile_engine.py`

- `source_file`: `\\192.168.1.15\docker\V5-trading-bot\src\execution\reconcile_engine.py`
- `proposed_target_path`: `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\reconcile_engine.py`
- `asset_role`: 交易所余额 vs 本地库存对账算法
- `保留内容`:
  - `ReconcileThresholds`
  - exchange/local snapshot 比较逻辑
  - `USDT` 与 `base asset` 分桶校验
  - dust ignore 统计输出 schema
- `直接导入时会碰到的依赖`:
  - `src.execution.account_store.AccountStore`
  - `src.execution.position_store.PositionStore`
  - `src.execution.okx_private_client.OKXPrivateClient`
- `隐藏耦合`:
  - 函数体内依赖 `src.reporting.spread_snapshot_store.SpreadSnapshotStore`
  - 输出固定落 `reports/reconcile_status.json`
  - exchange snapshot 字段按 OKX `cashBal/eqUsd/ordFrozen` 命名
- `必须先抽象的接口`:
  - `ExchangeBalanceSnapshot`
  - `LocalInventorySnapshot`
  - `PriceResolver`
  - `ReconcileStatusRepository`
- `禁止跟入`:
  - `main.py`
  - `src/core/pipeline.py`
  - `src/reporting/spread_snapshot_store.py` 的 V5 文件落地实现
  - 任意研究、回测、ML 目录
- `导入结论`:
  - 可以先把算法骨架迁过去
  - 不能把 OKX 快照字段和 V5 的 `reports/` 状态文件当成主仓固定协议

### 3.3 `src/reporting/decision_audit.py`

- `source_file`: `\\192.168.1.15\docker\V5-trading-bot\src\reporting\decision_audit.py`
- `proposed_target_path`: `\\192.168.1.15\docker\ClawGPT-trading-bot\src\reporting\decision_audit.py`
- `asset_role`: run-level execution audit envelope
- `保留内容`:
  - `DecisionAudit` 壳
  - reject counter
  - note stream
  - `save()` / `load_decision_audit()` 的 sidecar I/O 模式
- `当前字段污染来源`:
  - `main.py` 写 `budget`
  - `src/core/pipeline.py` 写 `budget_action`
  - `src/core/pipeline.py` 写 `strategy_signals`
  - `src/core/pipeline.py` 写 `ml_signal_overview`
- `必须先缩核的字段`:
  - 保留 `run_id`、`window_*`、`counts`、`rejects`、`notes`
  - 将 `budget`、`budget_action`、`strategy_signals`、`ml_signal_overview` 改为 extension bag 或暂不落地
- `禁止跟入`:
  - 对 `main.py` 的预算写入约定
  - 对 `src/core/pipeline.py` 的策略/ML 审计扩展约定
  - 任意 `src/alpha/`、`src/backtest/`、`src/research/`、`src/execution/ml_*`
- `导入结论`:
  - 这是 5 个文件里最适合先落主仓的一个
  - 但必须用缩核 schema，不能把 V5 扩展字段原样定成主仓核心协议

### 3.4 `src/execution/live_preflight.py`

- `source_file`: `\\192.168.1.15\docker\V5-trading-bot\src\execution\live_preflight.py`
- `proposed_target_path`: `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\live_preflight.py`
- `asset_role`: live pre-trade safety orchestration shell
- `保留内容`:
  - `LivePreflightResult` 决策协议
  - `ALLOW / SELL_ONLY / ABORT` 决策面
  - `bills -> ledger -> reconcile -> kill-switch -> borrow/account config` 顺序
- `直接导入时会碰到的依赖`:
  - `configs.schema.ExecutionConfig`
  - `scripts.bills_sync.sync_once`
  - `src.execution.bills_store.BillsStore`
  - `src.execution.bootstrap_patch.controlled_patch_from_okx_balance`
  - `src.execution.borrow_guard.check_okx_borrows`
  - `src.execution.kill_switch_guard.GuardConfig`, `KillSwitchGuard`
  - `src.execution.ledger_engine.LedgerEngine`
  - `src.execution.okx_private_client.OKXPrivateClient`
  - `src.execution.reconcile_engine.ReconcileEngine`, `ReconcileThresholds`
  - `src.utils.auto_blacklist.add_symbol`
- `强耦合来源`:
  - 当前由 `main.py` 直接传入 `okx`、`position_store`、`account_store`
  - 当前默认使用 `reports/bills.sqlite`、`reports/ledger_state.json`、`reports/ledger_status.json`、`reports/reconcile_status.json`
- `必须先抽象的接口`:
  - `ExchangeAccountGateway`
  - `BillFeed`
  - `LedgerChecker`
  - `ReconcileChecker`
  - `StatusRepository`
  - `KillSwitchStore`
  - `BlacklistWriter`
- `禁止跟入`:
  - `main.py` 的调用方式
  - `src/core/pipeline.py`
  - V5 现有 `reports/*.json` / `reports/*.sqlite` 文件布局
  - 任意运行数据、缓存和部署脚本
- `导入结论`:
  - 先迁决策协议和执行顺序
  - 不迁 V5 的具体 OKX 适配、文件路径和自动 patch / blacklist 落地实现

### 3.5 `src/execution/live_execution_engine.py`

- `source_file`: `\\192.168.1.15\docker\V5-trading-bot\src\execution\live_execution_engine.py`
- `proposed_target_path`: `\\192.168.1.15\docker\ClawGPT-trading-bot\src\execution\live_execution_engine.py`
- `asset_role`: broker execution adapter
- `保留内容`:
  - `submit_gate_for_live()` 语义
  - `clOrdId + decision_hash` 幂等提交逻辑
  - ACK / POLL / QUERY 状态机
  - fill 后本地头寸同步的处理顺序
  - dust sell 本地清理策略
- `直接导入时会碰到的依赖`:
  - `configs.schema.ExecutionConfig`
  - `src.core.models.ExecutionReport`, `Order`
  - `src.execution.clordid.make_cl_ord_id`, `make_decision_hash`
  - `src.execution.okx_private_client.OKXPrivateClient`, `OKXPrivateClientError`, `OKXResponse`
  - `src.execution.order_store.OrderStore`
  - `src.execution.position_store.PositionStore`
  - `src.data.okx_instruments.OKXSpotInstrumentsCache`, `round_down_to_lot`
  - 运行时动态依赖 `FillStore`、`FillReconciler`
- `关键强耦合`:
  - submit gate 直接读 `kill_switch_path` / `reconcile_status_path`
  - `_decision_hash_for_order()` 依赖上游订单 `meta` 中的 `target_w`、`window_start_ts`、`window_end_ts`、`regime`、`deadband_pct`
  - 这些 `meta` 当前来自被排除项 `src/core/pipeline.py`
- `必须先抽象的接口`:
  - `SubmitGate`
  - `BrokerClient`
  - `OrderRepository`
  - `PositionProjection`
  - `InstrumentSpecProvider`
  - `FillProjection`
  - `RiskStateCleaner`
  - `ExecutionIntentMetaContract`
- `禁止跟入`:
  - `main.py`
  - `src/core/pipeline.py`
  - 任意 `src/alpha/`、`src/backtest/`、`src/research/`、`src/execution/ml_*`
  - 任意现成 `reports/*.json`、`reports/*.sqlite`
  - 运行数据、缓存、模型文件、部署脚本
- `导入结论`:
  - 只能迁 broker adapter 语义和幂等状态机
  - 不能把 V5 的 gate 文件、OKX 专属字段和 pipeline 订单元数据原样复制进主仓

## 4. 首批导入顺序建议

1. 先定 `decision_audit` 的 core schema，避免后续接口签名漂移。
2. 再拆 `ledger_engine` 和 `reconcile_engine` 的算法层，沉成主仓通用 service。
3. 在主仓定义 `PreTradeSafetyStage` 所需接口后，再接 `live_preflight`。
4. 最后再接 `live_execution_engine`，并在主仓单独定义 `ExecutionIntentMetaContract`，不要反向依赖 V5 的 `src/core/pipeline.py`。

## 5. 本轮结论

- 本轮 `proposed_target_path` 已全部统一指向 `\\192.168.1.15\docker\ClawGPT-trading-bot`。
- 首批只列 5 个文件，未把 `main.py`、`src/core/pipeline.py`、`src/alpha/`、`src/backtest/`、`src/research/`、`src/execution/ml_*`、运行数据、缓存、部署脚本、模型文件纳入导入对象。
- 本轮只新增清单文档，不向主仓拷文件，不改任何 live runtime。
