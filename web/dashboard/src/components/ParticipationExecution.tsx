import type { ParticipationSummary } from '../commandTypes';
import { age, dateTime, epoch, number, observationState, reasonLabel } from '../lib/commandFormat';

export default function ParticipationExecution({ data, now }: { data: ParticipationSummary; now: number }) {
  if (!data.quote_execution_enabled) return null;
  const worker = data.quote_worker;
  const ready = observationState(worker?.status, epoch(worker?.observed_at), now, 30_000) === 'observed';
  const events = [...(data.events || [])].reverse().slice(0, 6);
  const pending = data.pending;
  const reasons = data.signal_decision?.candidate_reasons;
  return <div className="cc-paper-execution">
    <div className="cc-paper-execution-head">
      <b className={ready ? 'cc-positive' : 'cc-amber'}>{ready ? '持续报价检查运行中' : '报价检查状态待确认'}</b>
      <span>检查间隔 {number(worker?.interval_seconds, 0)} 秒 · 更新于 {dateTime(worker?.observed_at)}</span>
    </div>
    <p>{pending ? `${pending.symbol} ${pending.action === 'exit_intent' ? '退出' : '买入'}意向 · 生成于 ${dateTime(pending.decision_ts)} · ${age(pending.decision_ts, now)}` : '当前没有待执行意向'}<br />小时信号更新于 {dateTime(data.signal_observed_at)}</p>
    {!ready && <p className="cc-amber">暂时无法确认报价检查正常。以实际成交记录为准，持仓估值可能不可用。</p>}
    {worker?.last_error && <p className="cc-muted">行情连接提示：{worker.last_error}</p>}
    {events.length > 0 && <div className="cc-paper-event-list">{events.map((event, index) => {
      const execution = event.execution;
      const executed = execution?.action === 'fill' || execution?.action === 'cancel';
      const action = executed ? execution : event.decision;
      const label = action?.action === 'fill' ? `模拟${action.side === 'buy' ? '买入成交' : '卖出成交'}` : action?.action === 'cancel' ? '意向已取消' : action?.action === 'exit_intent' ? '生成退出意向' : action?.action === 'entry_intent' ? '生成买入意向' : '观察';
      return <div key={`${event.observed_ts}-${index}`}><time>{dateTime(event.observed_ts)}</time><b>{action?.symbol || '—'}</b><span className={action?.action === 'cancel' ? 'cc-amber' : ''}>{label}<small>{action?.action === 'fill' ? `信号至成交 ${number(action.latency_seconds)} 秒` : reasonLabel(action?.reason)}</small></span></div>;
    })}</div>}
    {reasons && <details className="cc-disclosure"><summary>查看小时信号逐币依据</summary><div className="cc-paper-reasons">{Object.entries(reasons).map(([symbol, reason]) => <div key={symbol}><b>{symbol}</b><span>{reasonLabel(reason)}</span><code>{reason}</code></div>)}</div></details>}
  </div>;
}
