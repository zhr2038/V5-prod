# V5 Trading Bot

[English](./README.md)

V5 是当前用于生产运行的 OKX 现货交易仓库。它已经不只是研究代码集合，而是一套完整的生产工作区，包含实盘主循环、事件驱动检查、风控与对账、Flask Dashboard，以及 ML 训练与门控链路。

## 项目概览

当前仓库覆盖的能力包括：

- `main.py` 驱动的小时级交易主循环
- `event_driven_check.py` 驱动的事件补充检查
- OKX 现货执行与显式实盘解锁
- 开仓前安全检查：bills、ledger、reconcile、kill-switch
- 监控与审计：健康检查、运行报告、Dashboard、run artifacts
- ML 数据采集、训练、门控与可选实盘叠加

当前生产运行目录：

- `/home/admin/clawd/v5-prod`

## 当前主入口

核心入口：

- `main.py`
- `event_driven_check.py`
- `scripts/web_dashboard.py`

生产配置：

- `configs/live_prod.yaml`

关键文档：

- [当前生产链路](./docs/CURRENT_PRODUCTION_FLOW.md)
- [生产同步部署](./docs/PRODUCTION_ONLY_DEPLOYMENT.md)
- [生产最小文件面](./docs/PRODUCTION_MINIMAL_FILES.md)

## 系统怎么工作

主链路大致是：

1. 读取 `.env` 与 `configs/live_prod.yaml`
2. 从 OKX 公共行情获取市场数据
3. 计算 alpha、regime、组合与风控决策
4. 执行 live preflight 安全检查
5. 生成并执行订单
6. 把订单、成交、持仓、汇总和审计信息写入 `reports/`

当前 regime 不是单一规则，而是 ensemble：

- HMM
- funding sentiment
- RSS sentiment

当前监控面主要来自：

- `/api/*` Dashboard 接口
- `/health`、`/ready`、`/liveness`
- `reports/runs/<run_id>/` 下的逐轮产物

## Dashboard

Dashboard 后端：

- `scripts/web_dashboard.py`

前端模板与静态资源：

- `web/templates/`
- `web/static/`

当前 Dashboard 特性：

- 单页运营总览
- 集中展示市场状态、风险档位、持仓、成交、信号、健康、ML 阶段
- 新增持仓聚焦区，支持按持仓币查看 K 线
- 适配桌面与移动端

常用入口：

- `/`
- `/monitor`
- `/api/dashboard`
- `/api/account`
- `/api/positions`
- `/api/market_state`
- `/api/position_kline`
- `/api/ml_training`

本地启动：

```bash
python scripts/web_dashboard.py
```

默认地址：

- `http://127.0.0.1:5000`

## 快速开始

### 1. 创建虚拟环境

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell：

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

### 3. 本地运行

本地默认运行：

```bash
python main.py
```

显式解锁实盘：

```bash
export V5_CONFIG=configs/live_prod.yaml
export V5_DATA_PROVIDER=okx
export V5_LIVE_ARM=YES
python main.py
```

注意：

- 实盘必须显式设置 `V5_LIVE_ARM=YES`
- 生产数据源必须使用 `okx`
- Dashboard 是运维/监控界面，不是交易前端

## 测试

运行主要测试：

```bash
pytest -q
```

只跑 Dashboard 相关回归：

```bash
pytest tests/test_web_dashboard.py
```

## 生产部署

推荐模型：

- GitHub 是代码真源
- `/home/admin/clawd/v5-prod` 是同步后的运行副本
- `.env`、`.venv/`、`reports/`、`logs/`、服务端缓存属于服务器本地运行态

同步生产发布面：

```bash
python deploy/sync_prod_release.py \
  --host <host> \
  --user root \
  --password '<password>' \
  --remote-root /home/admin/clawd/v5-prod \
  --service-user admin \
  --enable-prod-timer \
  --enable-event-driven-timer
```

当前生产同步会包含：

- `main.py`
- `event_driven_check.py`
- `configs/`
- `deploy/`
- `scripts/`
- `src/`
- `web/`
- 当前生产文档

如需单独安装 user-level systemd：

```bash
bash deploy/install_systemd.sh --user
```

如需用户退出后仍运行：

```bash
sudo loginctl enable-linger admin
```

## 目录说明

主要目录：

- `src/`: 交易核心、执行、风控、regime、因子、报告
- `configs/`: 生产与辅助配置
- `scripts/`: 运维脚本、Dashboard、报告、恢复工具
- `web/`: Dashboard 模板与静态资源
- `deploy/`: systemd unit 与生产同步工具
- `reports/`: 运行输出、SQLite、run artifacts
- `tests/`: 回归测试

仍存在但不是当前生产主路径的内容：

- `study_notes/`
- `v4_export/`
- `scripts/archive/`

## 运维注意事项

- `reports/*` 属于运行态，不要提交到 GitHub
- 不要把 `git pull` 当作生产目录的常规部署方式
- 不要在生产副本里做破坏性 Git 操作
- 如服务器上做了热修，下一次同步前要先回灌到仓库

常看的关键产物：

- `reports/runs/<run_id>/decision_audit.json`
- `reports/runs/<run_id>/summary.json`
- `reports/runs/<run_id>/trades.csv`
- `reports/reconcile_status.json`
- `reports/kill_switch.json`
- `reports/ledger_status.json`
- `reports/ml_runtime_status.json`

## 当前边界

- 仅 OKX 现货
- 不使用杠杆
- 不做空
- ML 默认仍受门控保护，不是永久常开
- 仓库里仍有历史研究内容，但本 README 只描述当前生产链路
