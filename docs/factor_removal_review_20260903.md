# V5 因子拆除结果

已在可运行的**离线精简候选版**中移除 **F4 成交量扩张**：它不再参与候选打分，也不作为该候选通道的独立入场否决条件。配置为[精简候选配置](E:/v5-prod/.worktrees/rally-validation-20260903/configs/research/rally_reentry_reduced.json)，明确记录disabled_factors=[f4_volume_expansion]。**生产实盘尚未切换，生产Alpha权重、PROTECT和定时任务未改动。**

这次没有把“单独删除后收益不变”直接当成“全部一起删除也没影响”。已做联合删除验证，结果明确不支持一次删光。

| 因子 | 此次删除对照 | 处理 |
|---|---|---|
| F1 5日动量 | 单独移除不改变本模型执行结果 | 暂保留；合并剔除会改变排序与收益 |
| F2 20日动量 | 单独移除不改变本模型执行结果 | 暂保留；不能与其余重复项同时盲删 |
| F3 波动率调整收益 | 单独移除后20天30bps净收益从+1.2070降至+0.8203 | 保留 |
| F4 成交量扩张 | 剔除后60天、后20天，在30/60bps下均改善约0.10 USDT | 从精简候选版移除 |
| F5 RSI趋势确认 | 单独移除不改变本模型执行结果 | 保留因子排序贡献；候选通道已取消其独立否决 |
| F6 情绪 | 单独移除不改变本模型执行结果 | 暂保留，不能据此声称长期无价值或有盈利能力 |

收益均为USDT，扣除双边模拟成本：

| 同一趋势持有模型内的组合 | 后20天净盈亏 30bps | 后20天净盈亏 60bps | 全60天净盈亏 30bps | 全60天净盈亏 60bps |
|---|---:|---:|---:|---:|
| 原6因子排序 | +1.2070 | +1.0888 | +0.4189 | -1.1005 |
| 删除F4的候选版 | +1.3078 | +1.1894 | +0.5196 | -0.9999 |
| 合并删除其余5项，仅留F3 | +0.9736 | -0.0579 | +0.3117 | -1.8660 |
| 全删Alpha排序，改用4小时动量 | +1.5716 | +0.7566 | +0.9343 | -1.4134 |

联合删除F1/F2/F4/F5/F6后，高成本场景的后20天从只删F4的+1.1894 USDT变成−0.0579 USDT。保留一个看似主导的因子，并不能替代其余因子在排序接近时的联合信息。全删Alpha、改用价格动量排序，也会使60bps下60天结果更差。

F4的改善集中在8月23–24日三个模拟仓位的变化；60天窗口包含后20天，所以四个数字并非四组独立证据。改善只有约0.10 USDT，不能包装成“已证明F4长期不挣钱”。本次把它从研究候选中剔除，保留原始字段供审计及复现。

删除F4后的候选版，后20天在30bps成本下17笔模拟了结，净盈亏+1.3078 USDT、最大回撤1.40%；但完整60天在60bps下仍为−0.9999 USDT。17笔中含1笔期末按市价扣费了结，且盈利仍集中于一笔ETH行情。平均单笔净收益的95%日区组诊断区间约[-69.7, 231.5]bps，未能排除负期望。该版本未通过实盘就绪条件。

**变更及验证**

- [factor_ablation.py](E:/v5-prod/.worktrees/rally-validation-20260903/src/research/factor_ablation.py)：按真实历史权重重建因子得分，逐因子剔除，仅覆写离线排序，不改原始审计。1,462个完整时段的绝对分重建误差为0，横截面相对分最大误差4.44e-16。
- [ablate_rally_factors.py](E:/v5-prod/.worktrees/rally-validation-20260903/scripts/ablate_rally_factors.py)：完成单因子、全价格排序、合并删除与候选配置导出；选择失败时不会把组合标为通过。
- [rally_reentry_validation.py](E:/v5-prod/.worktrees/rally-validation-20260903/src/research/rally_reentry_validation.py) 和 [validate_rally_reentry.py](E:/v5-prod/.worktrees/rally-validation-20260903/scripts/validate_rally_reentry.py)：支持明确的候选排序及配置执行。候选必须为offline_research并绑定冻结协议的精确SHA256；未知模式或不匹配协议会报错。
- [factor_ablation_protocol.json](E:/v5-prod/.worktrees/rally-validation-20260903/docs/factor_ablation_protocol.json)、[factor_simplification_protocol.json](E:/v5-prod/.worktrees/rally-validation-20260903/docs/factor_simplification_protocol.json)、[rally_reentry_reduced.json](E:/v5-prod/.worktrees/rally-validation-20260903/configs/research/rally_reentry_reduced.json)：记录预设比较、合并删除检验及最终移除项。协议在各自实验前冻结，后续实验明确属于探索性扩展。
- [.gitattributes](E:/v5-prod/.worktrees/rally-validation-20260903/.gitattributes)：为协议JSON固定LF，防止跨平台换行改变候选绑定的文件哈希。
- 两个新增测试文件覆盖因果数据、成交与损益、因子剔除、原始记录不被改写及缺失/无效输入。连同原风险管理测试，**25 tests passed**；针对本轮Python的ruff检查通过。

主要风险仍是短样本、历史试验选择偏差、因子之间的替代关系，以及回放与真实撮合的差异。没有通过这些比较证明任何因子未来一定挣钱或一定亏钱。没有下实盘订单，也没有为了增加交易而清空净值峰值、强制切换风险档位或停用硬止损。

本轮变更保存在独立分支research/rally-validation-20260903。回滚到基线97aabbae或停止使用该离线配置即可撤销候选变更；生产运行没有本轮需要回滚的变更。

复现精简候选：

```powershell
python scripts/validate_rally_reentry.py --history "E:\v5-prod\output\rally-validation-20260903\recorded_history.json.gz" --candles "E:\v5-prod\output\rally-validation-20260903\historical_candles.json.gz" --protocol docs/factor_simplification_protocol.json --candidate-profile configs/research/rally_reentry_reduced.json --output "E:\v5-prod\output\rally-validation-20260903\reduced-replay-check" --observed-equity-peak 132.12063608841112
```

证据：[因子对照结果](E:/v5-prod/output/rally-validation-20260903/factor-simplification-v1/factor_results.json)、[完整CSV](E:/v5-prod/output/rally-validation-20260903/factor-simplification-v1/factor_comparison.csv)、[精简版独立运行结果](E:/v5-prod/output/rally-validation-20260903/reduced-candidate-v1/results.json)、[更早的入场/退出优化报告](E:/v5-prod/output/rally-validation-20260903/optimization_review.md)。
