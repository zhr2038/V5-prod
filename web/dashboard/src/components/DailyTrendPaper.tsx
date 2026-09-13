import type { DailyTrendView } from '../dailyTrendTypes';
import { dateTime } from '../lib/commandFormat';

const labels: Record<string, string> = {
  observed: '观测正常',
  stale: '报告已过期',
  missing: '等待首次观测',
  invalid: '证据校验未通过',
  worker_failed: '日线任务失败',
  frozen: '实验已到期冻结',
  WAITING_FOR_START: '等待首个预注册决策日',
  OBSERVING: '90 天前瞻观察中',
  OBSERVING_LATE: '本次观察迟到',
  REVIEW_DUE_FROZEN: '90 天复核到期，账本已冻结',
  buy: '模拟买入',
  sell: '模拟卖出',
  none: '不交易',
  daily_trend_entry: '日线趋势满足',
  signal_exit: '日线趋势退出',
  delayed_signal_exit: '迟到后按首个报价退出',
  review_window_end: '90 天窗口结束',
  entry_skipped_late: '迟到，不补开仓',
  hold: '继续持有',
  cash: '保持现金',
};

const fmt = (value: unknown, digits = 4): string =>
  value === null || value === undefined || value === ''
    ? '—'
    : Number.isFinite(Number(value))
      ? Number(value).toLocaleString('zh-CN', { maximumFractionDigits: digits, minimumFractionDigits: digits })
      : String(value);

const pct = (value: unknown, digits = 2): string =>
  value === null || value === undefined ? '—' : `${fmt(Number(value) * 100, digits)}%`;

export default function DailyTrendPaper({ data, unavailable }: { data?: DailyTrendView; unavailable: boolean }) {
  const report = data?.report;
  return <section className="cc-review" aria-label="BTC ETH 日线趋势独立模拟实验">
    <div className="cc-section-title"><div><span className="cc-section-index">D1</span><h2>BTC / ETH 日线趋势</h2><p>当前唯一新增的前瞻实验。两币各用独立 50 USDT 模拟账户，旧 participation 与 A/B/C/D 已停止写入并保留历史账本。</p></div><strong>{unavailable ? '页面数据待确认' : labels[data?.status || 'missing'] || data?.status}</strong></div>
    {!report ? <p className="cc-empty">{data?.reason || data?.worker?.detail || '等待生产任务建立已绑定的独立账本。'}</p> : <>
      <p className="cc-review-notice">{labels[report.status] || report.status}。最近观测 {dateTime(report.observed_at)}，使用截至 {dateTime(report.completed_bar_ts)} 的已完成 UTC 日线。</p>
      {!!report.cumulative_missed_decision_days && <p className="cc-review-notice">累计缺失 {report.cumulative_missed_decision_days} 个预注册决策日；到期结论将标记为观察覆盖不足，不会回填交易。</p>}
      {data?.worker?.ok === false && <p className="cc-review-notice">任务失败：{data.worker.detail}。以下为最后一次已校验报告。</p>}
      <div className="cc-paper-metrics"><div><span>模拟权益 <small>USDT</small></span><b>{fmt(report.total_equity_usdt, 4)}</b></div><div><span>净权益变化 <small>USDT</small></span><b className={report.net_equity_increment_usdt < 0 ? 'cc-negative' : 'cc-positive'}>{fmt(report.net_equity_increment_usdt, 4)}</b></div><div><span>最大已观测回撤</span><b>{pct(report.observed_maximum_drawdown_fraction)}</b></div></div>
      <div className="cc-review-scroll"><table><thead><tr><th>独立账户</th><th>日线信号</th><th>账户权益 USDT</th><th>净权益变化</th><th>当前暴露</th><th>最大已观测回撤</th><th>模拟成交 / 闭环</th><th>本次动作</th></tr></thead><tbody>{Object.entries(report.sleeves).map(([symbol, sleeve]) => { const action = report.actions.find(row => row.symbol === symbol); return <tr key={symbol}><th>{symbol}</th><td>{sleeve.signal.long ? '持有条件满足' : '现金条件'}<small> · 30 日 {pct(sleeve.signal.momentum_return)}</small></td><td>{fmt(sleeve.portfolio.equity_usdt)}</td><td>{fmt(sleeve.portfolio.net_equity_increment_usdt)}</td><td>{fmt(sleeve.portfolio.gross_exposure_usdt)}</td><td>{pct(sleeve.observed_maximum_drawdown_fraction)}</td><td>{sleeve.actual_simulated_fill_count} / {sleeve.independent_closed_campaign_count}</td><td>{labels[action?.action || 'none'] || action?.action} · {labels[action?.reason || ''] || action?.reason}</td></tr>; })}</tbody></table></div>
      <details open><summary>冻结规则与截止日期</summary><p>只在已完成 UTC 日线收盘高于 100 日简单均线，且过去 30 日收益为正时持有；否则持有现金。每天 UTC 00:10 读取 OKX 公开现货 bid/ask。</p><p>预注册窗口：{dateTime(report.start_utc)} 至 {dateTime(report.review_utc)}。已提交 {report.observation_count} 次日线观察，累计缺失 {report.cumulative_missed_decision_days} 个决策日。显式往返费用与滑点合计 {fmt(report.explicit_roundtrip_cost_bps, 0)} bps，盘口价差另计；累计模型手续费 {fmt(report.modeled_fee_usdt, 6)} USDT。</p><p>证据状态：<code>{report.evidence_status}</code> · 实验身份：<code>{report.identity}</code> · 账本：<code>{data?.ledger}</code></p></details>
      <p className="cc-review-footnote">这是独立模拟账本，不读取交易密钥、不下单、不改变真实仓位，也不会自动晋级实盘。历史回测为筛选依据，不是未来盈利证明；90 天到期仍需人工判断继续、归档或证据不足。</p>
    </>}
  </section>;
}
