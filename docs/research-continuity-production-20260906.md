# V5 N1/N2 生产交付与 N3 边界 — 2026-09-06

本轮改善研究可信度，尚不能证明净收益改善，也没有授权扩大实盘资金。R1–R4 已修复，本次没有把旧问题重新当成未修复项。N3 只作设计限制说明，冻结 v2 策略、上游绑定、费用、资金及原回撤比较不变。

## 版本与部署

- V5 运行版本：`79c4f2ff8cff76c01321406cd05c401b26005076`，生产目录 `/home/ubuntu/clawd/v5-releases/79c4f2ff8cff76c01321406cd05c401b26005076`。
- 对应 main 合并源：`6e66b8b353c9ae0f2fe53695ba2b53ed5dcf5204`；二者 Git tree 均为 `7b9315094ea4dba3bbcd8cf9f48ee1f67813f2bf`。后续仅补交付文档，无需替换运行代码。
- 实施 PR：[缓存/观察修复 #9](https://github.com/zhr2038/V5-prod/pull/9)，[继承区间质量 #10](https://github.com/zhr2038/V5-prod/pull/10)。
- 420 个生产文件、依赖与 runtime override 校验通过；精简发布 profile 保持 `production-sync-built-web-only-v1`。manifest SHA-256 `4ee3ae312bde01b9aba51db97802ff2b5fab385183ac27d8e4abfcabd8c8b9a6`。
- 有效生产配置 SHA-256 `2e0631f893fa89f70698b9c8cd86ed62a8f8d11926a1758124b52d8ed1b603c2`，切换前后相同；原 participation 身份 `d6eb8a01a55450f8075535fecab51bbec90435709ebd44d93a312fbe282ab118` 相同。两次成功切换均验证实盘、原 participation、前段研究数据库的逻辑数据不变。
- quant-lab 本次没有代码变更。生产 manifest 重新验证：`1159c0a53fec40ec92ae07a0a5be21f22ca80216`、265 文件；main `1ccc9b222566f283b6a057f9b51192306abd90a1`。上游六字段绑定和 `shadow` / `record_only` 不变。

## 修改路径与逻辑

- `src/research/review_forward.py`：失败原始数据按哈希留存；校验后原子发布有效缓存；命中重新检查闭合/连续性；30 秒内不重复远端重试。交易规格/报价与信号可用性分开。
- `src/research/review_comparison.py`：允许明确标注信号缺失的报价观察，维持原有共同入场时间，记录信号缺失和退出覆盖边界。A 不新增分钟策略退出；B/C/D 仍用原 kernel 的硬/时间/试验退出，缺失 EMA 不被伪造。
- `src/research/review_integrity.py`、`configs/research/review_integrity_v1.json`：固定观察规则，计算计划时段、实际有效次数、独立覆盖时段、最大间隔、缺测时长、持仓缺测和未覆盖共同决策；仅用于研究验收。
- `src/research/review_acceptance.py`：同时检查新观察区间与继承区间，数据不足不能晋级人工研究复核；所有状态的实盘权限仍关闭。旧回撤条件没有放宽。
- `scripts/migrate_review_continuation.py`：锁定旧 worker 后接续，保留全检查点、账户文件、意图队列、事件原文和完整源身份链；拒绝 processing 中断、覆盖已有目录、策略/资金变化或静默更换观察规则。
- `web/dashboard/src/components/ReviewComparison.tsx`、`reviewTypes.ts`、构建产物：展示已观测最大回撤、观察完整性、继承区间缺测及源身份接续；页面布局未重做。
- `.github/workflows/ci.yml` 与 `tests/test_review_continuity.py`、`test_review_continuation.py`、`test_review_acceptance.py`：覆盖本轮边界；既有失败用例没有删除。

## 回归与采集证据

- 最初 10 个正式仓库回归在旧代码全部失败，见 `red-tests.xml`。最终相关 116 例通过、0 失败；TypeScript/Vite 构建通过。
- V5 完整 CI：2142 passed / 10 skipped，[发布源 34033793130](https://github.com/zhr2038/V5-prod/actions/runs/34033793130) 与 [main 34034090919](https://github.com/zhr2038/V5-prod/actions/runs/34034090919) 的 test/full-test 均成功。
- quant-lab 复核既有 [main CI 34018496206](https://github.com/zhr2038/quant-lab/actions/runs/34018496206)：664 passed。本轮没有修改中台或重新运行其中台全量本地 pytest。
- 合成输入的真实 collect 回归：H+50 秒尾线/内部缺口被拒绝作为有效缓存；H+55 秒保持单次请求；H+410 秒第二次获取恢复；H+470 秒复用有效缓存；下小时重新采集。另覆盖坏 JSON、跨小时采集、截止之后不入场、已有持仓硬/时间退出及检查点恢复。
- 真实公开 HTTP、真实时钟、隔离故障注入：旧版于 2026-09-06 20:56:55 遇同小时缺尾线缓存后报错；修复版于 2026-09-06 20:57:02 获取有效数据，同一小时内恢复。修复版调用约 3.63 秒。没有调用 `process` 或交易主入口，没有损坏生产缓存。
- 隔离注入缓存 SHA-256 `dd33c7da79725d31c6980cc6c4ee70e5f5cab2352c97a1fb513033e921c6d67e`；恢复采集 SHA-256 `cc5cfb22fa33d3498d9b2703c9a113c99fa42233dfac705ed3c6498069fdc454`。隔离原始文件保留在 `/tmp/v5-public-collector-recovery-20260906-h3slixve`。这是故障注入证据，不能说成生产曾出现坏缓存或据此计算收益。

## 自然运行与真实候选覆盖

核验截至 2026-09-06 21:06:54（北京时间）。当前研究 worker 成功、API 状态 `observed`，前段数据库哈希仍一致。当前小时有效缓存 bar `2026-09-06 21:00:00`；原始数据 SHA-256 `3bb5ca5b158564a824348fc958fb9c112a60bf884bbd894859259cf273edda2b`。

观察规则从 2026-09-06 20:27:58 开始计时，后续源码接续没有重置。计划 40 个时段，实际 41 次有效报价和 41 次有效信号，独立覆盖 40 个时段。重复时段只计一次覆盖，不把额外调用当作更高覆盖率。新规则区间最大间隔 63.957 秒，缺测 0.000 秒。

- 2026-09-06 21:06:54：共同决策在冻结窗口内自然发生；三档 C/D 共享快照一致：True。信号来源 validated_cache；原始采集 SHA-256 `3bb5ca5b158564a824348fc958fb9c112a60bf884bbd894859259cf273edda2b`。

| 往返成本 | 全账户实际候选 | 有效参考覆盖 | 实际参考改动 | 最终源码段新增候选 / 改动 |
|---|---:|---:|---:|---:|
| 30 bps | 1 | 1 | 0 | 0 / 0 |
| 60 bps | 1 | 1 | 0 | 0 / 0 |
| 120 bps | 0 | 0 | 0 | 0 / 0 |

累计的 30/60 bps 候选来自继承区间，同一 SOL 机会在两档成本下分别检验，不能相加成两笔独立机会。HTTP 成功和建议条数不计为候选覆盖。零实际改动仍表示“未形成有效参考对照”，不能宣称中台无增益。实盘、原 participation、A/B/C/D 各账本持续独立，没有把研究结果并入实盘收益。

## 身份与资金接续

1. 原 v2：`0278d6eec70dfe441707d92b0960ca200083bbf471b4c2b335c04f6f2f22f028`，303 条事件截止 2026-09-06 20:26:54。
2. 采集修复段：`cc7ee6f7b4dc9cea367cae61e635e4cd2e5c640880ec3401aa82b1064d5ef15b`，完整保留上述事件；截止 2026-09-06 20:45:54 共 322 条。
3. 当前段：`2b5d55df845044fbe3e7a47eaba94964eace94ad50a284943ecbb943901251d5`，目录 `review-20260906-v2-continuity-79c4f2ff8cff`；当前总事件 344。

原 v2 数据库 SHA-256 `18075fdaf4ee814642e84a8a308106ad0ba304ac9c9700689c3f7b836b3f5647`；第二段冻结数据库 SHA-256 `750206682289867f2e495c88651063cdc361dbbd7b4194bc24a5dc2ad7b06aa2`；当前段继承事件行 SHA-256 `ca509bb6e19baa95330da52d1891c5c4918141af63a677531015d7ef0317bd61`；迁移前后检查点原文 SHA-256 `3564d4069d74244c521ea347a452bc383b57b67f91b4a651472bbe6c0381926b`。两个冻结前段均保留，所有持仓、现金、费用、权益峰值、已实现亏损和待执行意图接续；没有重新注资、清仓、重算成交或重置观察起点。

## 失败、风险与未验证项

- 第一次发布等待已有证据导出任务超过 60 秒，安全退出切换并恢复调度；当时没有迁移账本或切换源码。导出任务于 20:26:30 自然成功结束。此维护等待造成已记录的 117.913 秒观察间隔，按冻结规则计算 57.913 秒缺测。当时 A 及 30/60 bps B/C/D 有持仓；该缺口现在明确进入继承区间验收，不能用随后连续运行掩盖它。
- 继承区间 303 次实际观察、301 个计划时段，质量 `INSUFFICIENT`；这些事件是事后质量核算，前瞻日数恒为零。保留未知区间风险，未改变阈值来求通过。
- 新规则仍远未完成 30 日观察、100 个独立机会和净收益增益检验。报告状态 `INSUFFICIENT_REFERENCE_EXPOSURE`；任何研究状态都不自动修改仓位、PROTECT、硬止损或 live 权限。
- N3：现金/低暴露基线的回撤比较可解释性有限；v2 仍按原条件计算。新风险定义只能在新时间窗前另行版本化，本次没有放宽。
- 辅助读取曾遇系统 Python 缺 numpy，改用已验证运行虚拟环境后通过；一次 journalctl 读取超时，改以实际 service 状态与退出时间核验。两者不是交易故障。旧浏览器在服务重启时有请求中断，新会话已验证加载新页面。
- 存储只读测量：20:18 时数据库 52,342,784 字节，查询约 35 ms；不足一天窗口估计内容写入约 256.6 MB/日，不能当作实测完整一天的磁盘增长。未删除任何研究账本，未实现本轮范围外的历史工作集去重。

## 回滚办法

两次成功激活均在新段产生事件前，实际演练过“切回前一代码目录，再切回新代码”，并确认实盘/原 participation/前段研究逻辑数据一致。当前前一 release 为 `c55848cb807225351fc5925d2b6871f55c64d4be`，更早为 `f31c8c163eebeb416ff8a6792c6a0e8273b048ca`。

当前新段已有自然事件后，如需回退，先暂停研究 timer 并让 worker 结束，保存当前 SQLite、accounts、意图队列、manifest 与 `current.json`；保持当前段冻结。可以独立切回实盘/Web 代码并恢复原调度，但不要启动旧研究检查点来冒充当前连续账户。研究恢复须使用当前完整检查点并经明确的身份/观察规则接续验证；processing 中断不能清除。回滚既不重置真实资金状态，也不丢弃新段研究事件。

完整原始证据留在本地 `E:/v5-prod/output/research-continuity-20260906` 和服务器各 release 的 `release-ops`，见本地 `evidence-index.json`。公开仓库只提交修复、测试和脱敏交付记录，不上传实盘原始账户快照或凭据。
