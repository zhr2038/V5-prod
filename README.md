# V5 Trading Bot

当前仓库已经按正式上线路径收口。

核心目标：
- OKX 现货实盘执行
- 统一的预交易安全检查
- 运行状态、对账、账本、成本统计分离
- 保留本地研究历史，但不再让研究脚本污染 GitHub 主运行路径

## 当前正式入口

主入口：
- `main.py`
- `event_driven_check.py`

正式配置：
- `configs/live_prod.yaml`

环境文件：
- 根目录 `.env`

正式 systemd 名称：
- `v5-prod.user.service`
- `v5-prod.user.timer`
- `v5-event-driven.service`
- `v5-event-driven.timer`

## 当前业务流程

1. `main.py` 读取根目录 `.env` 和 `configs/live_prod.yaml`
2. 拉取 OKX 市场数据和交易 universe
3. 计算 alpha 与 regime
4. 做组合构建与风险约束
5. 执行 `live_preflight`
6. 生成订单并进入 live execution
7. 同步 fills、更新本地状态、写入 `reports/`

预交易安全顺序：
1. bills
2. ledger
3. reconcile
4. kill-switch
5. borrow 检查
6. account config 检查
7. 输出 `ALLOW / SELL_ONLY / ABORT`

## 正式运行依赖

必须保留：
- `src/`
- `configs/live_prod.yaml`
- `scripts/run_hourly_live_window.sh`
- `scripts/bills_sync.py`
- `scripts/ledger_once.py`
- `scripts/reconcile_guard_once.py`
- `scripts/rollup_last24h.py`
- `scripts/rollup_costs.py`
- `scripts/rollup_spreads.py`
- `scripts/health_check.py`
- `scripts/v5_status_report.py`
- `scripts/okx_private_selfcheck.py`
- `scripts/live_preflight_once.py`
- `scripts/orders_gc_once.py`
- `scripts/orders_repair_once.py`

详细清单见：
- `docs/PRODUCTION_MINIMAL_FILES.md`
- `docs/CURRENT_PRODUCTION_FLOW.md`
- `scripts/README.md`

## 脚本分层

`scripts/` 根目录现在只保留三类内容：
- 定时运行脚本
- 手工运维 / 恢复脚本
- 监控与状态脚本

非生产脚本：
- 已从 GitHub 主运行路径移除
- 本地如需保留，可放在 `scripts/archive/`
- `scripts/archive/` 按 `.gitignore` 规则不会上传 GitHub

## 快速启动

### 1. 安装依赖

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. 准备环境

根目录 `.env` 至少需要：

```env
EXCHANGE_API_KEY=...
EXCHANGE_API_SECRET=...
EXCHANGE_PASSPHRASE=...
```

### 3. 手工运行

Dry-run 或配置检查：

```powershell
python main.py
```

显式指定正式配置：

```powershell
$env:V5_CONFIG='configs/live_prod.yaml'
python main.py
```

实盘执行前必须显式 arm：

```powershell
$env:V5_LIVE_ARM='YES'
$env:V5_DATA_PROVIDER='okx'
python main.py
```

## 运维命令

健康检查：

```powershell
python scripts/health_check.py
```

状态报告：

```powershell
python scripts/v5_status_report.py
```

一次性 preflight：

```powershell
python scripts/live_preflight_once.py --config configs/live_prod.yaml --env .env
```

一次性对账：

```powershell
python scripts/reconcile_guard_once.py --config configs/live_prod.yaml --env .env
```

一次性账本刷新：

```powershell
python scripts/bills_sync.py --config configs/live_prod.yaml --env .env --db reports/bills.sqlite
python scripts/ledger_once.py --config configs/live_prod.yaml --env .env --bills-db reports/bills.sqlite --out reports/ledger_status.json
```

## systemd 说明

仓库内已经提供：
- `deploy/systemd/v5-prod.user.service`
- `deploy/systemd/v5-prod.user.timer`
- `deploy/systemd/v5-event-driven.service`
- `deploy/systemd/v5-event-driven.timer`

安装脚本：
- `deploy/install_systemd.sh`

注意：
- 安装脚本会复制 unit 文件
- 它不会自动开启正式实盘 timer
- `v5-prod.user.timer` 应该由运维显式启用

## 当前仓库状态

已经完成的收口方向：
- 修复缺失依赖声明
- 清理主路径中的硬编码绝对路径
- 恢复 `RegimeEngine` 的合理回退逻辑
- 修复 kill-switch 默认自动清除问题
- 修复 live preflight 的顺序问题
- 将非生产脚本从 GitHub 主运行目录移出
- 补齐正式生产 deploy unit 和最小必需文件文档

## 后续文档

- 正式主流程：`docs/CURRENT_PRODUCTION_FLOW.md`
- 最小部署面：`docs/PRODUCTION_MINIMAL_FILES.md`
- 脚本分类：`scripts/README.md`

## 不再作为首页主内容的部分

以下内容仍可能存在于本地工作区，但不是当前正式上线主路径：
- 回测和研究脚本
- 历史 20u 试运行内容
- study notes
- v4 导出内容
- 历史迁移文档
