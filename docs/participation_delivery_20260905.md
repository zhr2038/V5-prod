# V5参与度改造交付记录

代码版本：`def219193c7d53e863193dc486adbb675c52b452`，分支 `feat/participation-redesign-20260905`。GitHub远端分支已核对与该代码提交一致；本记录另行提交，不改变已验证的运行源码。诊断、修改文件、核心逻辑、历史收益及风险详见 [改造报告](participation_redesign_20260905.md)。

2026-09-05 09:14:31北京时间，已部署到唯一V5生产目标 `qyun.hrhome.top:/home/ubuntu/clawd/v5-prod`。部署前9个现有文件逐字节符合审核基线，4个新增文件不存在；本次仅替换这13个文件。现场Git HEAD仍为原部署基线，不能用它代替当前运行源码身份；以逐文件哈希和本次代码版本为准。

三个相关定时器在不到1秒的文件应用窗口内暂停后恢复，主任务/事件任务锁已检查，活动中的任务不会被中断。全部13个目标文件部署后哈希一致，Python编译、生产配置加载、策略配置验证和新运行模块导入成功。`participation.enabled=true`、`mode=forward_paper`；没有直接启动交易主入口，没有手工提交订单，也没有调整实盘仓位上限。

## 验证结果

- 定向测试集244项通过、0失败、0跳过，JUnit耗时36.816秒；最终策略、前瞻运行与配置/存储38项另行复核通过。相关源码及新增测试Ruff通过，提交补丁空白检查通过。未声称完整仓库测试通过。
- 18组固定历史对照已完成。主场景与前瞻配置哈希一致；源输入及脚本哈希与保留结果一致。小时采样不能证明盘中止损可成交，也不能保证亏损上限。
- 09:15:28生产真实资金和历史峰值重新核算回撤19.118294%，保持PROTECT；72小时44个候选，真实成交转化率0。计算只读，风控状态和评估报告未改变。旧独立fills同步游标过期被明确warning，使用当前对账/账本健康证明读取证据新鲜度。
- 09:18:40生产只读数据契约验证 `PASS_READ_ONLY_OBSERVER_CONTRACT_STALE_NOT_FORWARD`：四币完整收盘行情、原生产provider盘口和因子匹配，`data_errors={}`，盘口约1.17秒，9项契约检查通过。
- 09点信号已过15分钟有效期，前瞻更新正确拒绝；未伪造时间，未回填历史收益。真实orders表3806行、fills表1474行，前后逻辑哈希一致；前瞻目录前后均不存在，没有建立虚假队列。只读验证的纯决策演示为hold，不能当成前瞻成交。
- 首次正式前瞻观测安排在原有10:00小时任务中，由生产main自然调用。交付时尚未经历该周期，不能称已完成前瞻成交或收益闭环。每轮会生成 `participation_forward.json`，组合汇总为 `reports/participation/latest.json`，账本为 `reports/participation/forward.sqlite`；随后由现有小时任务持续推进。

## 证据与回滚

本地证据目录：`E:\v5-prod\output\participation-redesign-20260905`。包括部署清单、测试XML、原始及追加行情/决策输入、18组结果、只读风控验证和真实数据契约验证。

服务器交付目录：`/home/ubuntu/clawd/v5-prod/research/participation-redesign-def219193c7d`。已上传完整提交源码归档、输入、历史结果和测试证据；首批28件交付物已逐一核对SHA256。补充契约验证和最终报告另有上传核验记录。

逐文件原始备份：`/home/ubuntu/clawd/v5-prod/.deploy-backups/participation-redesign-def219193c7d`。回滚预检已通过13文件，**未实际回滚**。

必要时在qyun以ubuntu执行以下现成脚本。脚本先检查现有文件仍属于本版本；有后续修改或活动任务时明确失败，避免覆盖。恢复13个文件并恢复原定时器状态，保留前瞻SQLite证据和其他现场改动。

```bash
/home/ubuntu/clawd/v5-prod/.venv/bin/python /home/ubuntu/clawd/v5-prod/research/participation-redesign-def219193c7d/rollback_release.py --apply
```

单独停止新策略观察可将 `participation.enabled` 设为false并保留原账本。若要更换策略参数或源码，需要显式新队列以保持历史身份；不能直接改成live。当前实盘上线的是已证实的风控、成交统计和持有契约修复；新进场内核仍需要足够的前瞻闭环证明收益质量。
