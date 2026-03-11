# V5 Trading Bot

V5 是当前在用的 OKX 现货实盘仓库。它已经不是单纯的研究代码集合，而是一套收口后的生产系统，包含：

- 1 小时主交易循环
- 15 分钟事件驱动补充检查
- 多策略融合选币与仓位控制
- 统一的预交易安全检查与 kill-switch
- Web Dashboard / 运行审计 / 对账 / 成本归因
- ML 数据采集、日训、门控与可选实盘叠加

当前生产工作目录是 `/home/admin/clawd/v5-prod`。

## 当前生产入口

核心入口：

- `main.py`
- `event_driven_check.py`
- `scripts/web_dashboard.py`

正式配置：

- `configs/live_prod.yaml`

当前生产 timer / service：

- `deploy/systemd/v5-prod.user.service`
- `deploy/systemd/v5-prod.user.timer`
- `deploy/systemd/v5-event-driven.service`
- `deploy/systemd/v5-event-driven.timer`
- `deploy/systemd/v5-auto-risk-eval.service`
- `deploy/systemd/v5-auto-risk-eval.timer`
- `deploy/systemd/v5-sentiment-collect.service`
- `deploy/systemd/v5-sentiment-collect.timer`
- `deploy/systemd/v5-daily-ml-training.service`
- `deploy/systemd/v5-daily-ml-training.timer`
- `deploy/systemd/v5-model-promotion-gate.service`
- `deploy/systemd/v5-model-promotion-gate.timer`

说明：

- `deploy/systemd/` 目录里仍保留一些历史 unit。
- 上面这几组才是当前生产链路需要关注的 unit。

## 系统现在怎么工作

### 1. 交易主链路

`main.py` 每小时跑一次，流程是：

1. 读取 `.env` 和 `configs/live_prod.yaml`
2. 拉取 OKX 市场数据和交易 universe
3. 计算 alpha / regime / 风控姿态
4. 生成目标组合
5. 执行 live preflight
6. 生成并执行订单
7. 写入 `reports/`、SQLite 和每轮审计

当前主策略不是单一因子，而是三路融合：

- `TrendFollowing`
- `MeanReversion`
- `Alpha6Factor + Alpha158 overlay`

### 2. Regime

当前 regime 不是只看均线，已经切到 ensemble：

- HMM
- funding sentiment
- RSS sentiment

Funding / RSS 会持续采集，Dashboard 展示的是和 HMM 同风格的 vote 结果，不再只是一行文字状态。

### 3. 风控与退出

当前生产逻辑里，已经实际生效的关键风险控制有：

- live preflight：`bills -> ledger -> reconcile -> kill-switch`
- `auto_sync_before_trade.py`：开盘前对齐本地状态，避免 cash / position 漂移导致整套停摆
- `auto_risk_eval.json`：动态限制最大持仓数
- 严格排名退出：连续跌出前 3 的持仓会强制退出
- 峰值回撤止盈：盈利后按回撤分层卖出
- `negative_expectancy`：对最近期望差的币压分或禁开新仓

## 因子、评分和 ML 的当前状态

### 因子与评分

当前仓库里的 alpha 相关重点是：

- 多策略模式下，`Alpha6` 的 `f1..f12` 现在会真正写入 `alpha_snapshot` 和 `alpha_ic_monitor`
- `dynamic_ic_weighting` 现在是“负 IC 先降权”，不再直接把因子翻成反向权重
- `dynamic_weights_by_regime` 会优先保：
  - `Trending`: `f4 / f5`
  - `Sideways`: `f2 / f3`

相关产物：

- `reports/alpha_snapshot.json`
- `reports/alpha_ic_monitor.json`
- `reports/alpha_dynamic_weights_by_regime.json`

### ML

ML 链路已经接通，但默认仍然受门控保护：

1. 采样写入 `reports/ml_training_data.db`
2. 日训生成模型
3. `model_promotion_gate.py` 决定是否通过
4. 只有通过门控的模型，才会被实盘叠加到 alpha 上

当前 ML 训练特征：

- 多 horizon 标签：`6h / 12h / 24h`
- 日训前会做 label backfill
- 训练集使用滚动窗口和最近样本加权
- 默认候选模型是保守版 `ridge`

运行状态看这里：

- `reports/ml_runtime_status.json`
- `reports/model_promotion_decision.json`

## Dashboard

Dashboard 在 `scripts/web_dashboard.py`，前端模板和静态资源在 `web/`。

当前页面特性：

- 移动端可用
- 首屏信息收紧，减少文字噪音
- `ML 链路` 用全宽阶段带展示
- 持仓、评分、regime、funding、RSS、风险档位都走当前接口
- 持仓浮盈亏按真实余额和 fills 重建，不再直接信本地脏仓位表

常看接口：

- `/health`
- `/api/account`
- `/api/dashboard`
- `/api/positions`
- `/api/market_state`
- `/api/ml_training`

## 目录说明

核心目录：

- `src/`: 策略、组合、执行、风控、因子、监控
- `configs/`: 正式配置
- `deploy/systemd/`: systemd unit
- `scripts/`: 运维、采集、训练、门控、Dashboard
- `web/`: Dashboard 模板与静态资源
- `reports/`: 运行输出、状态文件、SQLite、审计
- `tests/`: 回归测试

历史/研究内容还在仓库里，但不是当前生产主路径：

- `study_notes/`
- `v4_export/`
- 若干 legacy unit / script

## 本地启动

### 1. 安装依赖

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. 准备 `.env`

至少需要：

```env
EXCHANGE_API_KEY=...
EXCHANGE_API_SECRET=...
EXCHANGE_PASSPHRASE=...
```

### 3. 本地手动运行

```powershell
$env:V5_CONFIG='configs/live_prod.yaml'
$env:V5_DATA_PROVIDER='okx'
$env:V5_LIVE_ARM='YES'
python main.py
```

只做页面：

```powershell
python scripts/web_dashboard.py
```

## 生产部署

推荐 user-level systemd：

```bash
bash deploy/install_systemd.sh --user
```

如果希望用户不登录也持续跑：

```bash
sudo loginctl enable-linger admin
```

注意：

- `reports/*` 是运行态文件，不要提交到 GitHub
- 生产仓库可能带本地 sync commit，这是刻意保守处理，不要直接 `reset --hard`
- 真正控制实盘开闸的仍然是 `V5_LIVE_ARM=YES`

## 关键产物

每轮运行重点看：

- `reports/runs/<run_id>/decision_audit.json`
- `reports/runs/<run_id>/summary.json`
- `reports/runs/<run_id>/trades.csv`

全局状态重点看：

- `reports/reconcile_status.json`
- `reports/kill_switch.json`
- `reports/ledger_status.json`
- `reports/alpha_snapshot.json`
- `reports/alpha_ic_monitor.json`
- `reports/auto_risk_eval.json`
- `reports/ml_runtime_status.json`

## 当前边界

- 这是 OKX 现货系统，不做杠杆和做空
- ML 不是默认常开 alpha；没过门控就不会进实盘
- Dashboard 是运维界面，不是交易前端
- 历史研究脚本仍可能存在，但 README 只描述当前生产主链路
