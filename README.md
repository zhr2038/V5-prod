# V5 生产交易仓库

这个仓库现在是 V5 的主仓，不再只是研究目录。它覆盖了 OKX 现货实盘交易、事件驱动检查、风控、对账、Web 看板、ML 训练与门限、以及影子模型实验。

## 当前定位

- GitHub：代码和配置的源头
- 当前生产运行目录示例：`/home/ubuntu/clawd/v5-prod`
- 运行态长期保留在服务器本地：
  - `.env`
  - `.venv/`
  - `reports/`
  - `data/`
  - `logs/`
  - 模型二进制文件

这个仓库故意不上传运行态数据、数据库、缓存和模型二进制。GitHub 保留的是可部署的代码面，不是服务器快照。

## 主入口

- 小时主交易：`main.py`
- 事件驱动检查：`event_driven_check.py`
- Web 看板：`scripts/web_dashboard.py`
- 主配置：`configs/live_prod.yaml`

## 当前能力

- OKX 现货实盘下单，带显式 live arm
- 小时主策略和事件驱动并行
- HMM / 资金费率 / RSS 组合式市场状态判断
- 多策略融合评分、组合构建、换手控制、负期望拦截
- 预检、对账、账本、kill switch、自愈同步
- ML 数据采集、训练、门限、在线归因
- 响应式中文 dashboard

## 运行流程

1. 读取 `.env` 和 `configs/live_prod.yaml`
2. 从 OKX 拉取市场数据和账户状态
3. 计算 alpha、regime、组合目标和风控约束
4. 执行预检、对账和 kill switch 判断
5. 生成订单并通过执行层下发
6. 把订单、成交、仓位、审计和报表写入 `reports/`

## ML 说明

- 正式 ML 只在“最新门限通过”时参与 live 决策
- 训练与门限链路：
  - `scripts/daily_ml_training.py`
  - `scripts/model_promotion_gate.py`
- 正式模型元数据：
  - `models/ml_factor_model_config.json`
  - `models/ml_factor_model_active.txt`
- 模型二进制默认不进 GitHub，需要在服务器本地保存

## 影子模型

当前保留了一个正在测试的 tuned XGBoost 影子模型链路，已经纳入仓库：

- 运行脚本：`scripts/run_shadow_tuned_xgboost.py`
- 小时包装脚本：`scripts/run_shadow_tuned_xgboost_hourly.sh`
- 覆盖配置：`configs/shadow_tuned_xgboost_overrides.yaml`
- 影子模型元数据：
  - `models/ml_factor_model_gpu_tuned.json`
  - `models/ml_factor_model_gpu_tuned_config.json`
- 对应 systemd unit：
  - `deploy/systemd/v5-shadow-tuned-xgboost.user.service`
  - `deploy/systemd/v5-shadow-tuned-xgboost.user.timer`

这个影子模型默认是 dry-run，不参与真钱交易。

## 本地启动

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux/macOS:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

最少环境变量：

```env
EXCHANGE_API_KEY=...
EXCHANGE_API_SECRET=...
EXCHANGE_PASSPHRASE=...
```

本地跑主策略：

```bash
python main.py
```

本地跑 dashboard：

```bash
python scripts/web_dashboard.py
```

默认地址：

- `http://127.0.0.1:5000`

## 测试

全量回归：

```bash
pytest -q
```

常用专项：

```bash
pytest tests/test_web_dashboard.py -q
pytest tests/test_pipeline_marking.py -q
pytest tests/test_alpha_regime_integration.py -q
```

## 生产部署

推荐方式是把服务器当成同步目标，而不是在服务器目录里直接 `git pull`。

同步命令示例：

```bash
python deploy/sync_prod_release.py \
  --host <host> \
  --user <user> \
  --password '***' \
  --remote-root /home/ubuntu/clawd/v5-prod \
  --service-user ubuntu \
  --enable-prod-timer \
  --enable-event-driven-timer
```

部署后重点检查：

- `/health`
- `kill_switch=false`
- `reconcile.ok=true`
- `v5-prod.user.timer`
- `v5-event-driven.timer`
- `v5-web-dashboard.service`

更多部署细节见：

- `docs/CURRENT_PRODUCTION_FLOW.md`
- `docs/PRODUCTION_ONLY_DEPLOYMENT.md`
- `docs/PRODUCTION_MINIMAL_FILES.md`

## 仓库结构

- `src/`：交易、执行、风控、因子、regime、报表核心代码
- `configs/`：生产配置、研究配置、影子模型覆盖配置
- `scripts/`：运维脚本、训练脚本、dashboard、恢复脚本
- `deploy/`：生产同步和 systemd 模板
- `web/`：dashboard 前端模板和静态资源
- `tests/`：核心回归测试
- `docs/`：当前生产流和部署文档
- `models/`：模型元数据；二进制模型由服务器本地保管

## 不在 GitHub 中的内容

下列内容默认不上传：

- `reports/` 运行态
- `data/` 缓存和数据库
- `logs/`
- `.env`
- `.venv/`
- 模型二进制
- 本地备份、归档和临时文件
