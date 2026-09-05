# V5 交易控制台上线记录 · 2026-09-05

新版已于北京时间 10:20:54 在 `qyun.hrhome.top:5000` 上线。代码提交为 `b6c22cf7a2b8cba6e6461fe2b7334308f4bd6c28`，已推送 GitHub 分支 `feat/web-command-center-20260905`。界面与只读后端共 27 个部署文件逐个通过 SHA256 校验，返回 `DEPLOYED_VERIFIED`。

## 改动

完整设计和数据口径见 `docs/web_command_center_20260905.md`。主要文件为新的 `CommandCenter`、`EquityHistoryChart` 及其样式、`commandFormat` / `commandTypes`、App/API 数据接入、`src/reporting/dashboard_command_center.py` 和 Web 路由，连同入口图标及构建产物。

首页重排为交易总览、行情与机会、持仓与成交、策略验证、系统运行。展示资金、回撤、下一轮决策、逐币筛选/路由原因、72 小时参与统计、真实成交、前瞻模拟结果、后台任务和数据健康。

同时修复了缺失持仓被当成空仓、成交刷新失败清空历史、部分计数没有提示、旧账户/未来观测被当作当前、比例单位错误和 K 线时间混用。`PositionsPanel.tsx` 的历史成交价不再作为当前持仓成本；行情未到达时价格与涨跌显示未知，坐标和末根 K 线时间固定北京时间。

## 验证结果

- Python 测试：`tests/test_dashboard_command_center.py`、`tests/test_dashboard_equity_history.py`、`tests/test_web_dashboard.py`、`tests/test_positions_panel_render.py` 共 **305 项通过**，错误/失败均为 0。
- Node 数据与真实组件渲染回归：`node --experimental-strip-types --test tests/*.test.mjs`，**24 项通过**；覆盖空值/真零、成功空数组/失败保留、过期/未来时间、前瞻身份不匹配及部分观测。
- `npm run build`、`npm run lint`、定向 Ruff、CRLF 识别后的 diff 空白检查通过。K 线额外覆盖 UTC、美国浏览器时区和五种输入时间格式。
- 部署工具 **6 项本地模拟通过**，覆盖漂移拒绝、资源先于 HTML、服务启动等待、文件权限及发布失败自动回滚。
- 生产 `/api/dashboard?view=primary`、deferred、command_center、equity_history、health、trades、positions 共 **7 个接口返回 200**。HTML 与 6 个 JS/CSS 资源的 HTTP 内容哈希全部匹配。
- 真实浏览器已检查桌面和 390×844 手机视口：无整页横向溢出；正常数据无错误告警；成交方向筛选、50 条展开/收起、逐币详情、时间范围、前瞻依据和证据导出列表可用。生产 BTC K 线载入 96 根，显示末根 K 线 `2026-09-05 09:00 北京时间`。浏览器检查未见 error/warn。

10:21—10:22 生产可见账户权益约 106.88 USDT、有效持仓为 0、历史峰值回撤约 19.12%，风险为 PROTECT，近 72 小时真实成交订单为 0。下一次主决策为 11:00。

10:00 自然小时任务已经完成首条新策略前瞻记录，身份和独立账本校验通过：模拟权益 106.8607 USDT、净收益 0、入场/闭环 0，原因是当前没有合格候选。这是首条观察，不构成盈利证明。实盘资金和模拟资金仍分开。

## 运行边界和回滚

只重启 `v5-web-dashboard.service`。生产主决策、风控评估、对账和账本定时器均保持 active/waiting；主决策上一次 oneshot 执行 success/退出 0。没有更改交易策略、风险参数、仓位限额或定时器文件，也没有人工触发下单。

源文件采用带基线校验的逐文件部署，没有对生产脏工作树执行 git pull/reset。生产 Git HEAD 仍为原来的 `15e4956abe09f7259a5b1e0c14f7a90cfea8edd9`；本次运行版本由部署清单及实际文件哈希证明，不将该 HEAD 冒充新代码提交。

保留的主要风险是多接口观测时间不同和历史数据缺口，页面以独立时间、部分数据提示及未知状态表达。旧资源继续保留，供已经打开的页面使用。

备份目录：`/home/ubuntu/clawd/v5-prod/.deploy-backups/web-command-center-b6c22cf7a2b8-20260905T021954Z`。

需要回滚时，在 qyun 的 ubuntu 用户下运行以下命令。它先拒绝后续漂移，再恢复本次 Web 文件及旧 HTML，仅重启 Web 服务；保留新旧带哈希资源。

```sh
/home/ubuntu/clawd/v5-prod/.venv/bin/python -B /home/ubuntu/clawd/v5-prod/.deploy-releases/web-command-center-b6c22cf7a2b8-20260905T021954Z/web_deploy_remote.py --manifest /home/ubuntu/clawd/v5-prod/.deploy-releases/web-command-center-b6c22cf7a2b8-20260905T021954Z/manifest.json --rollback
```

原始证据保存在本机 `E:\v5-prod\output\web-command-center-20260905`：测试 XML、生产接口响应、部署 manifest/transaction、基线哈希、首次前瞻记录和部署工具模拟记录。
