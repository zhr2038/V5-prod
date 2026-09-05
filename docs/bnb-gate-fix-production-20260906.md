# BNB 入场与资金费率修复：生产交付

运行代码提交：`d821f5c51142885affe079f95bb84d285062b008`，GitHub 分支 `fix/bnb-participation-gates-20260905`。本文件只补充交付证据，不改变运行代码。逻辑、测试、历史对照和风险详见 [修复报告](bnb-gate-fix-20260905.md)。

2026-09-05 **23:58:32 北京时间**，四文件修复已部署到 `qyun.hrhome.top:/home/ubuntu/clawd/v5-prod`。仅正常 PROTECT 确认轮数从 2 改为 1，资金费率使用实际极端权重覆盖。没有降低 Alpha6/RSI/量能门槛，也没有修改仓位、止损或模拟冷却。

四个修改文件部署后 SHA256 均符合提交字节；另外十个保护文件保持原哈希。配置对比只允许确认轮数这一处语义变化。相关定时器暂停约 **0.706 秒**后全部恢复；活动交易任务未被中断，模拟报价进程 PID 保持 `3148607`、重启计数 0。

验证结果：

- 171 项定向测试通过，0 失败、0 跳过；另有 10 项 Web 市场状态测试和 6 项部署保护测试通过。Ruff 和提交补丁空白检查通过。没有运行完整仓库测试。
- 生产配置加载确认轮数为 1；新进程纯函数复现当时 4% 极端负面案例，资金费率为 Sideways。用当时三个实际投票输入复核，最终组合为 Trending；21 点真实信号通过正常入场门槛。以上属于纯门槛复核，不是实际成交或完整策略回测。
- `/api/market_state`、`/api/command_center`、`/api/positions` 均返回 HTTP 200。市场状态接口明确返回 `classification_version=actual_extreme_breadth_v2`。
- 2026-09-06 **0 点自然运行 `20260906_00` 已结束**，服务 `Result=success`、`ExecMainStatus=0`。实际决策审计使用新的分类版本，整体状态 Trending；BNB 排名 1、目标 15%。因已有仓位仍有正目标且仓位偏差仅约 0.067 个百分点，路由保留仓位、不作细小再平衡。没有新买入，也没有错误清仓；下次小时定时器为 01:00:08。
- BNB 真实买入发生在 **9 月 5 日 23:01:56**，原始成交价 771.5 USDT、成交量 0.020738 BNB，BNB 手续费 0.000020738，成交后数量 0.020717262 BNB。这发生于修复部署之前，不作为本版带来的成交。0 点复核真实 orders/fills/positions 仍为 **3807 / 1475 / 1**。
- 模拟保留原身份 `d6eb8a01a55450f8075535fecab51bbec90435709ebd44d93a312fbe282ab118`、原 SQLite 和策略哈希，0 点已自然推进，仍为 1 次入场、1 个完成闭环、净收益 **+0.13933882 USDT**。模拟当前空仓、无待成交意向；旧 BNB 冷却仍到 06:00:29。没有重置账户或把历史回放合并为前瞻收益。

取消盈利退出冷却的候选未部署，因为同一 60 天、30 bps 成本储备回放结果由 +0.774357 USDT 降至 -0.021508 USDT。此次交付修正的是已确认的误判和重复等待，不能保证以后盈利。较少的确认可能增加震荡入场；保留的 0.40 相对评分门槛仍可能错过上涨早段。

回滚备份：

`/home/ubuntu/clawd/v5-prod/.deploy-backups/bnb-gates-d821f5c51142`

在 qyun 上使用对应脚本恢复四个文件。脚本会核对现版和备份哈希；遇到后续文件漂移或活动交易任务会拒绝覆盖。全部实盘和模拟账本保留，不手工发出订单。

```bash
/home/ubuntu/clawd/v5-prod/.venv/bin/python -B /home/ubuntu/clawd/v5-prod/.deploy-releases/bnb-gates-d821f5c51142/remote_apply.py --manifest /home/ubuntu/clawd/v5-prod/.deploy-releases/bnb-gates-d821f5c51142/manifest.json --rollback
```

没有实际执行生产回滚。原生产 Git checkout 含历史现场修改，因此部署以逐文件哈希为准，没有执行 `git pull` 或 `git reset`。

本地完整证据：`E:\v5-prod\output\bnb-participation-gates-20260905`。服务器交付清单位于 `.deploy-releases/bnb-gates-d821f5c51142`，包含代码清单、原始备份定位、定向测试、部署保护测试和运行校验结果。
