import type { PairedPaperView } from '../pairedPaperTypes';
import { dateTime, number, ratio } from '../lib/commandFormat';

const states: Record<string, string> = {
  observed: '观测正常', stale: '观测已过期', missing: '等待首个自然周期', invalid: '证据校验未通过',
  worker_failed: '对照任务失败', frozen: '实验已冻结', future: '观测时间异常',
  COLLECTING_FORWARD_EVIDENCE: '正在积累前瞻证据', INSUFFICIENT_EVIDENCE: '证据不足',
  STOP_VERSION_NO_NET_BENEFIT: '当前版本未显示净增益', READY_FOR_MANUAL_RESEARCH_REVIEW: '可作人工研究复核',
};
const accounts: Record<string, string> = { A_original_v5: 'A · 原 V5 小时规则', B_defer_4h: 'B · 加入 4h 等待参考' };
const riskLevels: Record<string, string> = { ATTACK: '进攻', NEUTRAL: '中性', DEFENSE: '防守', PROTECT: '保护' };
const criteria: Record<string, string> = {
  forward_calendar_days: '前瞻天数', completed_control_campaigns: 'A 账户完整闭环',
  matched_candidates: '共同入场候选', completed_matched_veto_campaigns: '已闭环的配对拦截',
  valid_reference_coverage: '有效参考覆盖', decision_coverage: '小时决策覆盖', quote_coverage: '报价观察覆盖',
  net_account_delta_30bps: '30 bps 净收益增量', net_account_delta_60bps: '60 bps 净收益增量',
  net_account_delta_120bps: '120 bps 净收益增量', paired_95pct_lower_bound: '配对区间 95% 下界',
  observed_drawdown_within_budget_and_control: 'B 回撤不超 A 及预算',
  treatment_positive_without_largest_profitable_campaign: '移除最大盈利闭环后的 B 净收益',
};
const value = (input: unknown) => input == null ? '—' : typeof input === 'number' ? number(input, 4) : String(input);

export default function PairedReferencePaper({ data, unavailable }: { data?: PairedPaperView; unavailable: boolean }) {
  const report = data?.report;
  return <section className="cc-review" aria-label="交易参考同资金模拟对照">
    <div className="cc-section-title"><div><span className="cc-section-index">A/B</span><h2>交易参考是否改善账户收益</h2><p>A、B 各自从 100 USDT 开始。B 只用有效的 4 小时“等待”建议过滤新入场，两组的现金、持仓及模拟成交分别记账。</p></div><strong>{unavailable ? '页面数据待确认' : states[data?.status || 'missing'] || data?.status}</strong></div>
    {!report ? <p className="cc-empty">{data?.reason || data?.worker?.detail || '等待新账本的首次自然观测；旧价格标签和旧账户历史不计入本次收益。'}</p> : <>
      <p className="cc-review-notice">{states[report.status] || report.status}。有效起点 {dateTime(report.ledger_start_ts)}，最近观察 {dateTime(report.latest_observed_at)}，累计 {number(report.calendar_days, 2)} 天。</p>
      {data?.worker?.ok === false && <p className="cc-review-notice">任务失败：{data.worker.detail}。下方保留最后一次已校验结果。</p>}
      <div className="cc-paper-metrics"><div><span>B − A 净收益增量 <small>USDT</small></span><b>{number(report.comparison.net_equity_delta_usdt, 4)}</b></div><div><span>配对避开亏损 <small>USDT</small></span><b>{number(report.comparison.avoided_net_loss_usdt, 4)}</b></div><div><span>配对错过盈利 <small>USDT</small></span><b>{number(report.comparison.missed_net_profit_usdt, 4)}</b></div></div>
      <div className="cc-review-scroll"><table><thead><tr><th>独立账户</th><th>权益 USDT</th><th>净收益</th><th>已实现 / 浮动</th><th>已观测最大回撤</th><th>模拟成交 / 完整闭环</th><th>自身模拟风控</th></tr></thead><tbody>{Object.entries(report.accounts).map(([name, account]) => <tr key={name}><th>{accounts[name] || name}</th><td>{number(account.equity_usdt, 4)}</td><td>{number(account.net_equity_increment_usdt, 4)}</td><td>{number(account.realized_pnl_usdt, 4)} / {number(account.unrealized_pnl_usdt, 4)}</td><td>{ratio(account.maximum_drawdown_fraction, 2)}</td><td>{account.actual_simulated_fills} / {account.independent_closed_campaign_count}</td><td>{account.risk_snapshot ? riskLevels[account.risk_snapshot.current_level] || account.risk_snapshot.current_level : '尚未观测'}{account.risk_snapshot?.metrics?.recovery_evidence_ok === false && <small> · 恢复证据不足</small>}</td></tr>)}</tbody></table></div>
      <p className="cc-review-notice">共同候选 {report.references.matched_candidates}，参考导致等待 {report.references.deferred} 次，配对拦截 {report.references.matched_vetoes} 次，其中已闭环 {report.comparison.completed_matched_veto_campaigns} 次。{report.comparison.completed_matched_veto_campaigns === 0 ? '配对闭环为 0，避开亏损与错过盈利尚无完整证据。' : '配对归因只计算已结束的 A 账户交易，其余仓位分歧反映在账户差额中。'}</p>
      <p>有效参考 {report.references.valid} / {report.references.candidates}（{ratio(report.references.valid_coverage_rate, 1)}）；自然小时决策 {report.coverage.observed_decision_hours} / {report.coverage.expected_decision_hours}，缺失 {report.coverage.missed_decision_hours}。缺失或过期参考沿用各自原规则，不补造过去决策。</p>
      <details open><summary>成本压力与人工复核条件</summary><p>基础显式往返成本 {report.cost_model.explicit_roundtrip_cost_bps} bps，观察到的买卖价差另计。{report.cost_model.calibrated_to_real_fills ? '模型已绑定真实成交校准。' : '模型尚未按真实成交校准。'}{!report.cost_model.fixed_operating_cost_included && '未扣云服务等固定费用，因此不是项目最终利润。'}</p><div className="cc-review-scroll"><table><thead><tr><th>往返成本情景</th><th>A 净收益</th><th>B 净收益</th><th>B − A 增量</th></tr></thead><tbody>{Object.entries(report.scenarios).map(([cost, scenario]) => <tr key={cost}><th>{cost} bps</th><td>{number(scenario.accounts.A_original_v5?.net_equity_increment_usdt, 4)}</td><td>{number(scenario.accounts.B_defer_4h?.net_equity_increment_usdt, 4)}</td><td>{number(scenario.comparison.net_equity_delta_usdt, 4)}</td></tr>)}</tbody></table></div><div className="cc-review-scroll"><table><thead><tr><th>复核条件</th><th>实际</th><th>要求</th><th>结果</th></tr></thead><tbody>{report.acceptance.requirements.map(row => <tr key={row.criterion}><th>{criteria[row.criterion] || row.criterion}</th><td>{value(row.actual)}</td><td>{value(row.required)}</td><td>{row.result === 'PASS' ? '已满足' : row.result === 'FAIL' ? '未达到' : '证据不足'}</td></tr>)}</tbody></table></div></details>
      <p className="cc-review-footnote">两组按自身历史每小时评估风控；原实盘外部风控每 30 分钟评估，实验未复刻该时钟。冷启动允许降档，至少三轮历史后才允许恢复，不复制真实账户的风险状态。只复刻冻结的 V5 小时决策，不复现真实账户的事件执行器及外部操作；回撤仅覆盖已观察报价。独立模拟不改变原 V5 实盘运行。30 天是最早研究复核条件之一，仍须满足闭环、覆盖、成本压力、增益与回撤条件；不会自动晋级实盘。</p>
    </>}
  </section>;
}
