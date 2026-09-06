import { useState } from 'react';
import type { ReviewView } from '../reviewTypes';
import { dateTime } from '../lib/commandFormat';

const labels: Record<string, string> = {
  observed: '持续观测', stale: '报告已过期', missing: '尚无已绑定实验', invalid: '证据校验未通过', worker_failed: '研究任务失败',
  INSUFFICIENT_FORWARD_EVIDENCE: '前瞻证据不足', INSUFFICIENT_REFERENCE_EXPOSURE: '参考处理暴露不足',
  INSUFFICIENT_OBSERVATION_EVIDENCE: '持续观察证据不足', SIGNAL_DATA_UNAVAILABLE: '信号数据缺失，保留报价与已声明退出检查',
  observation_integrity: '持续观察完整性', observation_evidence_incomplete: '观察覆盖或持续时间不足',
  inherited_observation_integrity: '继承区间观察完整性', inherited_observation_evidence_incomplete: '继承区间存在缺测或质量证据缺失',
  integrity_policy_binding: '观察规则版本', prospective_days: '本规则前瞻日数', quote_slot_coverage: '有效报价时段覆盖率', signal_slot_coverage: '有效信号时段覆盖率',
  maximum_interval_seconds: '最大观察间隔（秒）', missing_duration_fraction: '缺失时长比例', uncovered_common_decision_count: '未覆盖共同决策数',
  holding_missing_seconds: '持仓期缺测（秒）', holding_signal_unavailable_observations: '持仓期信号缺失观察数',
  RESULT_NOT_SUPPORTED: '结果不支持晋级', READY_FOR_MANUAL_RESEARCH_REVIEW: '可提交人工研究复核，实盘未授权',
  WAITING_COMMON_CUTOFF: '等待共同决策截止点', DECIDED: '已完成共同决策', ALREADY_DECIDED: '本小时已评估，继续观察退出', MISSED_COMMON_DEADLINE: '错过共同截止窗口，本小时不补入场',
  valid: '有效', expired: '过期', no_view: '无观点', version_mismatch: '版本不匹配', late_or_future: '迟到或未来时间',
  PASS: '满足', FAIL: '不满足', INSUFFICIENT: '证据不足',
  forward_calendar_days: '前瞻日数', independent_entry_opportunities: '独立入场机会', distinct_entry_days: '独立入场日',
  valid_reference_coverage_rate: '有效参考覆盖率', matched_reference_candidates: 'C/D 匹配候选', independent_reference_changes: '独立参考改动机会',
  net_equity_delta_usdt: '净权益增量差额', paired_block_bootstrap_95pct_lower_bound_usdt: '配对区间 95% 下界',
  maximum_drawdown_no_greater_than_control: '回撤不高于对照', cost_60bps_net_equity_delta_usdt: '60 bps 成本差额', cost_120bps_net_equity_delta_usdt: '120 bps 成本差额',
  remove_largest_profitable_campaign_net_equity_usdt: '剔除最大盈利交易后净增量', all_observed_time_and_regime_segments_reported: '全部时间与行情分段',
  metric_unavailable: '指标尚不可用', sample_threshold_not_reached: '尚未达到冻结样本数', result_does_not_support_requirement: '结果未满足冻结条件',
  treatment_drawdown_exceeds_control: '回撤超过对照', no_observed_segments: '尚无可报告分段', economic_equity_trial_drawdown: '经济权益触及试验回撤线',
};
const names: Record<string, string> = { A_original_v5: 'A · 原 V5 小时决策', B_participation_v1: 'B · participation 72h', C_hold24_only: 'C · 固定 24h', D_reference_only: 'D · C + 24h DEFER' };
const fmt = (value: unknown, digits = 4): string => value === null || value === undefined ? '—' : typeof value === 'object' ? JSON.stringify(value) : Number.isFinite(Number(value)) ? Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits }) : String(value);

export default function ReviewComparison({ data, unavailable }: { data?: ReviewView; unavailable: boolean }) {
  const [cost, setCost] = useState('30');
  const report = data?.report;
  const funnel = report?.reference_funnel[cost];
  const clock = report?.latest_decision_clock;
  const quality = report?.observation_integrity;
  const legacy = report?.legacy_observation_integrity;
  return <section className="cc-review" aria-label="独立 A B C D 研究实验">
    <div className="cc-section-title"><div><span className="cc-section-index">R1–R4</span><h2>独立 A / B / C / D 对照</h2><p>各自从 100 USDT 开始。与实盘、上方原 participation 的账本和收益分别核算。</p></div><strong>{unavailable ? '页面数据待确认' : labels[data?.status || 'missing'] || data?.status}</strong></div>
    {!report ? <p className="cc-empty">{data?.reason || data?.worker?.detail || '等待已校验的新实验身份与首个自然观测。已有账本继续保留。'}</p> : <>
      <p className="cc-review-notice">{labels[report.status] || report.status}。报告截至 {dateTime(report.latest_observed_at)} · 账户首个观察 {dateTime(report.ledger_start_ts)}</p>
      {report.continuation && <p className="cc-review-notice">采集与报告修复接续：{dateTime(report.continuation.boundary_ts)} 以前属于 <code>{report.continuation.predecessor_identity}</code>。现金、持仓、权益峰值及待执行意图原样接续，旧目录保留；没有重新注资或重算历史成交。</p>}
      {report.signal_data?.status === 'unavailable' && <p className="cc-review-notice">本次 K 线信号不可用：{report.signal_data.reason}。报价估值继续，B/C/D 保留报价硬退出与时间退出；当前小时 EMA 条件未获观察，A 小时决策等待有效数据且不越过共同截止时间。</p>}
      {report.reference_read?.status === 'reference_read_failed' && <p className="cc-review-notice">参考读取失败：{report.reference_read.reason}。本次参考没有否决权限，候选与退出继续按共同规则评估。</p>}
      {data?.worker?.ok === false && <p className="cc-review-notice">任务失败：{data.worker.detail}。以下保留上次观测。</p>}
      <div className="cc-review-clock"><strong>{labels[clock?.status || ''] || clock?.status}</strong><span>共同截止点 {dateTime(clock?.cutoff_ts)} · 最晚 {dateTime(clock?.deadline_ts)}</span><p>C/D 使用同一可观察报价作决策，下一可观察报价模拟成交；迟到参考不回填。</p></div>
      <label className="cc-review-select">往返成本情景 <select value={cost} onChange={e => setCost(e.target.value)}>{Object.keys(report.scenarios).map(v => <option key={v} value={v}>{v} bps</option>)}</select></label>
      <div className="cc-review-scroll"><table><thead><tr><th>独立组合</th><th>资产权益</th><th>经济权益</th><th>即时可执行权益</th><th>受限残余估值</th><th>净权益增量</th><th>已观测最大回撤</th><th>试验状态</th></tr></thead><tbody>{Object.entries(report.scenarios[cost] || {}).map(([name, p]) => <tr key={name}><th>{names[name] || name}</th><td>{fmt(p.asset_equity_usdt)}</td><td>{fmt(p.equity_usdt)}</td><td>{fmt(p.immediately_executable_equity_usdt)}</td><td>{fmt(p.restricted_residual_value_usdt, 6)}</td><td>{fmt(p.net_equity_increment_usdt)}</td><td>{fmt((p.observed_maximum_drawdown_fraction ?? p.maximum_drawdown_fraction) * 100, 2)}%</td><td>{p.trial_halted ? labels[p.trial_stop_reason?.reason || ''] || '试验停止' : '观测中'}</td></tr>)}</tbody></table></div>
      <p className="cc-review-footnote">金额为 USDT。资产权益按 bid 市值；经济权益扣模型退出成本并保留全部数量价值；即时可执行权益另受数量步长和已知交易限制约束。估值不保证实际成交。A 未完整复刻生产事件执行器，组间差异不能全部归因为入场方向。</p>
      <details><summary>受限数量、保留成本与停止原因</summary>{Object.entries(report.scenarios[cost] || {}).map(([name, p]) => <div key={name}><strong>{names[name]}</strong>{Object.keys(p.restricted_positions || {}).length === 0 ? <p>本次没有受限残余。</p> : Object.entries(p.restricted_positions || {}).map(([symbol, r]) => <p key={symbol}>{symbol} · 数量 {fmt(r.quantity, 10)} · 保留成本 {fmt(r.cost_basis_usdt, 6)} USDT · 估计净值 {fmt(r.estimated_net_value_usdt, 6)} USDT · {r.reason}</p>)}{p.trial_stop_reason && <p>{labels[p.trial_stop_reason.reason] || p.trial_stop_reason.reason} · {dateTime(p.trial_stop_reason.observed_at)}</p>}</div>)}</details>
      <h3>参考使用漏斗 · C/D 实际候选，按币种与小时去重</h3>
      <div className="cc-review-funnel">{[['候选机会', funnel?.candidates], ['有效参考覆盖', funnel?.valid_reference_coverage], ['C/D 匹配候选', funnel?.matched_candidates], ['DEFER 可触发', funnel?.defer_eligible], ['实际改变决策', funnel?.decisions_changed], ['事后迟到发现', funnel?.late_arrivals]].map(([label, value]) => <div key={String(label)}><span>{label}</span><b>{fmt(value, 0)}</b></div>)}</div>
      <p>{!funnel?.decisions_changed ? '未形成有效参考对照，不能据此判断中台无增益。' : '已观察到参考处理，净收益增益仍需验收。'} 覆盖率 {funnel?.coverage_rate == null ? '—' : `${fmt(funnel.coverage_rate * 100, 1)}%`}。{Object.entries(funnel?.statuses || {}).map(([k, v]) => `${labels[k] || k} ${v}`).join(' · ')}</p>
      <details open><summary>冻结版本与当前接收状态</summary><p>实验 <code>{report.experiment_id}</code> · 运行身份 <code>{report.identity}</code></p>{Object.entries(report.reference_contract || {}).map(([k, v]) => <p key={k}>{k}：<code>{v}</code></p>)}<p>当前接收：{Object.entries(report.latest_reference_observation || {}).map(([s, r]) => `${s} ${labels[r.status] || r.status}`).join(' · ')}。当前可用不等于过去候选已被覆盖。</p><p>账本：<code>{data?.ledger}</code> · 已冻结旧实验 {data?.frozen_predecessors?.length || 0} 个，收益不拼接。</p></details>
      <h3>逐项研究验收 · 满足后仍需人工复核</h3>
      <details open><summary>持续观察完整性 · {labels[quality?.judgment || 'INSUFFICIENT']}</summary>
        <p>计划 {fmt(quality?.planned_observations, 0)} 次 · 有效报价 {fmt(quality?.valid_quote_observations, 0)} 次 · 有效信号 {fmt(quality?.valid_signal_observations, 0)} 次 · 最大间隔 {fmt(quality?.maximum_interval_seconds, 1)} 秒 · 缺测 {fmt(quality?.missing_duration_seconds, 1)} 秒 · 未覆盖共同决策 {fmt(quality?.uncovered_common_decision_count, 0)} 次。</p>
        <p>规则 <code>{quality?.policy_version}</code> · 前瞻完整性记录自 {dateTime(quality?.start_ts)} 起；此前 {fmt(quality?.legacy_observations, 0)} 次观察保留，但不计作本规则的前瞻验证。</p>
        {!!quality?.legacy_observations && <p>继承区间质量：{labels[legacy?.judgment || 'INSUFFICIENT']}。已保存事件只读统计：计划 {fmt(legacy?.planned_observations, 0)} 次 / 有效 {fmt(legacy?.valid_quote_observations, 0)} 次，最大间隔 {fmt(legacy?.maximum_interval_seconds, 1)} 秒，缺测 {fmt(legacy?.missing_duration_seconds, 1)} 秒，未覆盖共同决策 {fmt(legacy?.uncovered_common_decision_count, 0)} 次。该区间仍参与账户曲线，因此其缺测也会阻止研究验收，不能靠切换源版本消除。</p>}
        {Object.keys(report.scenarios[cost] || {}).map(name => <p key={name}>{names[name]}：持仓期缺测 {fmt(quality?.holding_missing_seconds?.[`${cost}:${name}`], 1)} 秒 · 持仓期信号缺失 {fmt(quality?.holding_signal_unavailable_observations?.[`${cost}:${name}`], 0)} 次</p>)}
        {legacy && <p>继承区间持仓缺测：{Object.keys(report.scenarios[cost] || {}).map(name => `${names[name]} ${fmt(legacy.holding_missing_seconds?.[`${cost}:${name}`], 1)} 秒`).join(' · ')}。这只是已有观察记录的质量核算，没有回放成交或补造缺失价格。</p>}
        <div className="cc-review-scroll"><table><thead><tr><th>观察条件</th><th>实际值</th><th>冻结要求</th><th>结果</th></tr></thead><tbody>{quality?.requirements?.map(r => <tr key={r.criterion}><th>{labels[r.criterion] || r.criterion}</th><td><code>{fmt(r.actual)}</code></td><td><code>{fmt(r.required)}</code></td><td>{labels[r.result] || r.result}</td></tr>)}</tbody></table></div>
        <p>回撤仅来自已观察报价，缺测区间的最坏风险未知。暴露时间沿用上次持仓作估算，不能视为连续监控证明。</p>
      </details>
      <p className="cc-review-footnote">冻结 v2 仍要求回撤不高于对照。对照若长期低暴露或空仓，该条件不足以解释可比风险；现金可作绝对收益基准，新的风险验收定义须另行预先版本化，本页没有放宽旧条件。</p>
      {Object.entries(report.acceptance.comparisons).map(([key, group]) => <details key={key} open={key.startsWith('D_')}><summary>{key.split('_minus_').map(v => names[v] || v).join(' 相对 ')} · {labels[group.status] || group.status}</summary><div className="cc-review-scroll"><table><thead><tr><th>冻结条件</th><th>实际值</th><th>要求</th><th>结果 / 原因</th></tr></thead><tbody>{group.requirements.map(r => <tr key={r.criterion}><th>{labels[r.criterion] || r.criterion}</th><td><code>{fmt(r.actual)}</code></td><td><code>{fmt(r.required)}</code></td><td>{labels[r.result] || r.result}{r.reason ? ` · ${labels[r.reason] || r.reason}` : ''}</td></tr>)}</tbody></table></div></details>)}
      <p className="cc-review-footnote">此处只评估研究可信度与证据。不会改变真实仓位、PROTECT、硬止损或实盘权限。固定云/API 费用尚未计入，项目整体盈利仍未验证。</p>
    </>}
  </section>;
}
