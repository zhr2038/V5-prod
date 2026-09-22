# V5 与 quant-lab 的双模拟账户前瞻验证

此实验只回答一个问题：相同初始资金和共同可见行情下，quant-lab 已提前收到且仍有效的 4 小时 DEFER，是否改善原 V5 小时决策策略的扣成本账户收益。它不改变真实 V5 的交易开关、额度、风控或订单。

## 冻结范围

配置：`configs/research/paired_reference_paper_v1.json`。A 为原 V5 小时决策管道，B 唯一新增处理为 DEFER 否决本次新入场。两者各从 100 USDT 开始，独立现金、持仓、价格峰值、策略状态、模拟成交账本、日内换手/成本预算及冷却记录。B 不排队、不延迟追单；只有下个原规则候选才重新评估。已有持仓加仓和所有退出均不受参考否决。缺失、晚到、过期、不可研究、未知版本的参考按原规则处理并记录原因。

这不是原实盘所有机制的百分百复刻：实验执行共同的小时决策管道，不复制 live event executor 和外部运营开关。自身自动风险状态使用同一个 `AutoRiskGuard.evaluate`，依据各自最近 12 小时审计、实际模拟成交、已实现退出收益和账户历史权益峰值演进，并写入独立 `auto_risk_eval.json` / `auto_risk_guard.json`。冷启动保留原阈值的立即降档保护，至少三轮历史后才允许恢复；缺口或缺失证据不能恢复风险档位。这比原外部 timer 的少于三轮直接跳过更严格，作为两组共同的冻结冷启动规则明确登记。新 100 USDT 账户从 NEUTRAL 自然开始；不复制真实账户因既往亏损形成的 PROTECT。研究每次共同小时决策前评估风险，原实盘外部风险 timer 每 30 分钟评估，这一时钟差异持续显示。报告必须保留 `baseline_scope`，不得用“完整实盘复刻”描述 A。A/B 的输入相同，但分歧后必须各按自身资金和持仓计算信号、退出与仓位；不能复用真实账户 fills 或 A 的退出作为 B 的退出。

正式启动前，必须将配置中的 `reference_contract.analysis_source_identity` 更新为已验证的 quant-lab 发布 worker commit。当前上游新契约为 `qlab.decision.result.v3 / trend-reference-1h-v2 / context-trend-24h-v1 / current-cost-v1 / horizon=4`。六个字段逐一匹配；显示元数据变化不替代来源验证。V5 reference consumer 必须先支持新 result schema。未知版本不会绕过验证。

策略身份由脚本及本地 import 依赖闭包、动态导入模块名、有效非密钥配置、冻结实验配置组成；哈希文件清单保存在 manifest。独立 dashboard/成本展示代码不在闭包中。模型和固定配置文件另有首次观察绑定。任何影响策略的版本漂移必须显式报错，并新建实验，不能偷偷接续历史。

## 运行与账本

Linux 命令（工作目录为发布根）：

```sh
PYTHONPATH=. .venv/bin/python scripts/run_paired_reference_paper.py --output reports/paired_reference_paper/v5-reference-paired-20260922-v1
```

以独立 systemd timer 每分钟触发一次。脚本有非阻塞单写锁，公开行情采集允许网络，策略沙箱禁止网络。它不导入或调用交易 executor，也不需要交易凭据。

共同决策时刻为每小时第 360 秒，最晚第 480 秒。错过则明确记缺口，不回填、不用后来收到的参考改写决定。成交使用决策之后第一次实际观察的 bid/ask，报价最多 30 秒旧，遵守交易所数量步长、最小数量、明确的手续费币种和共同入场资金下限。超过 180 秒观察间隔取消待执行入场，退出只在重新实际观察到报价时模拟执行并保留延迟证据。

主情景单边 10 bps 手续费加 5 bps 滑点，完整往返至少 30 bps，实际 bid/ask 差价额外计入。60/120 bps 是预先登记的成本压力情景，每个情景同样各有 A/B 两账户，不能把它们当成六个策略挑赢家。

输出目录包含：

- `comparison.sqlite`：append-only acquisitions/events，meta 保存身份、账户 checkpoint 和已提交运行状态目录。`events.report` 是 authoritative 完整报告。
- `manifest.json`：身份、哈希清单、冻结配置、真实前瞻起点。
- `latest.json`：与最近 `events.report` 完全一致的展示副本。
- `worker-status.json`：最近运行成功/失败和原因，失败不伪造新账户观察。
- `frame-blobs/`、`input-blobs/`：按内容哈希保存行情、合约、模型等输入。acquisitions 的行情/合约对象是压缩 JSON 引用，解压恢复后原 frame 哈希须一致。
- `generations/`：隔离策略运行状态；每次写入新的目录，账户 checkpoint、事件及目录指针在同一个 SQLite 事务提交。崩溃前未提交目录不能影响前一个有效状态；下次只处理新自然观察。保留最新两个可再生运行状态目录，原始输入和事件不删除。

重复相同观察返回已有结果，不重复成交；同一时间不同输入或倒序输入明确拒绝。`FROZEN.json` 存在时完全停止写入此实验。原旧实验账本不读入、不搬迁、不重算。

## 结论与晋级

主结论是 B 减 A 的完整账户净权益差。已实现收益、未实现收益、可立即变现资产、无法出售残量、费用和回撤分开展示。分批卖出是分配段，只有唯一 campaign 完整退出才增加一次闭环数；价格标签不是交易数。

“避免亏损/错过盈利”只来自同一次双方原策略均产生入场、B 被有效参考否决、A 实际模拟成交且完整退出的 campaign。单边候选、未成交、未退出及资金再分配影响不被伪装成该归因；它们仍反映在账户净差中。

最低样本为 30 个自然日、A 的 20 个完整 campaign、30 个共同入场候选和 10 个完成的匹配否决 campaign；同时检查有效参考覆盖、整点决策覆盖及报价观察覆盖。样本满足后仍需 30/60/120 bps 净差为正、按日分块的 95% 区间下界为正、回撤满足预算和对照约束、剔除最大盈利 campaign 后不依赖单笔暴利。时间和行情状态分段全部保留，不挑最好区间。

通过也只有 `READY_FOR_MANUAL_RESEARCH_REVIEW`，始终 `paper_only=true`、`live_order_effect=none`、`live_execution_eligible=false`，不授予实盘或自动扩容资格。尚未校准的滑点、未纳入的云/API固定成本和执行/风险观察时钟差异必须持续显示。

回滚：停止新双账户 timer，保留全目录证据，恢复旧发布；对真实 V5 无开关或参数回滚需求。不要将失败实验目录清空后以相同实验 id 重启。
