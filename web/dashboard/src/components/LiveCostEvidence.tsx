import type { LiveCostView } from '../liveCostTypes';
import { dateTime, number } from '../lib/commandFormat';

const states: Record<string, string> = { observed: '证据已更新', stale: '成本证据已过期', missing: '等待成本观测',
  unavailable: '成本证据不可用', invalid: '成本证据校验未通过', future: '成本观测时间异常' };
const reasons: Record<string, string> = {
  UNVERIFIED_FILL_SOURCE: '成交来源未验证', INVALID_FILL_PRICE_OR_QUANTITY: '成交价格或数量无效',
  INVALID_FILL_SIDE: '成交方向无效', UNSUPPORTED_QUOTE_CURRENCY: '报价币种不支持',
  ORDER_LINK_MISSING: '缺少关联订单', ORDER_IDENTITY_MISMATCH: '订单身份不一致',
  SUBMIT_QUOTE_UNAVAILABLE_OR_INVALID: '下单时盘口缺失或无效',
  QUOTE_OR_ORDER_TIMESTAMP_MISSING: '报价或订单时间缺失',
  SUBMIT_QUOTE_STALE_OR_FUTURE: '下单时盘口过期或时间异常',
  FILL_OUTSIDE_IMMEDIATE_EXECUTION_WINDOW: '成交超出即时执行窗口',
  NON_IMMEDIATE_ORDER_TYPE: '非即时成交订单类型', FEE_CONVERSION_UNAVAILABLE: '费用币种不能可靠折算',
};
const label = (reason: string) => reasons[reason] || reason;

export default function LiveCostEvidence({ data, unavailable }: { data?: LiveCostView; unavailable: boolean }) {
  const report = data?.report;
  return <section className="cc-review" aria-label="真实成交成本证据">
    <div className="cc-section-title"><div><span className="cc-section-index">C</span><h2>真实成交成本</h2><p>把真实成交与对应订单的下单时盘口关联，核对模拟成本假设；同一订单的分笔成交分别保留、订单数去重。</p></div><strong>{unavailable ? '页面数据待确认' : states[data?.status || 'missing'] || data?.status}</strong></div>
    {!report || report.status === 'UNAVAILABLE' ? <p className="cc-empty">{report?.reason || data?.reason || '等待只读成本任务采集有效证据。'}</p> : <>
      <p>最近 {report.window_days} 天 · 更新 {dateTime(report.generated_at_utc)}。{report.status === 'NO_FILLS' && '窗口内没有真实成交，不能据此认为成交成本为零。'}</p>
      <div className="cc-paper-metrics"><div><span>真实分笔成交</span><b>{number(report.fill_count, 0)}</b></div><div><span>可关联成本的分笔 / 订单</span><b>{number(report.qualified_fill_count, 0)} / {number(report.qualified_order_count, 0)}</b></div><div><span>手续费可识别的分笔</span><b>{number(report.fee_known_fill_count, 0)}</b></div></div>
      <div className="cc-review-scroll"><table><thead><tr><th>币种 / 方向</th><th>来源</th><th>有效订单数</th><th>手续费 bps</th><th>有符号滑点 bps</th><th>单边实际成本 bps</th></tr></thead><tbody>{report.groups?.map(group => <tr key={`${group.symbol}:${group.side}:${group.origin}`}><th>{group.symbol} · {group.side === 'buy' ? '买入' : '卖出'}</th><td>{group.origin === 'cost_probe' ? '成本探针' : '策略成交'}</td><td>{group.qualified_order_count}</td><td>{number(group.mean_fee_bps, 3)}</td><td>{number(group.mean_signed_slippage_bps, 3)}</td><td>{number(group.mean_one_way_cost_bps, 3)}</td></tr>)}</tbody></table></div>
      {!!Object.keys(report.missing_reason_counts || {}).length && <p className="cc-review-notice">缺失或不合格原因：{Object.entries(report.missing_reason_counts || {}).map(([reason, count]) => `${label(reason)} ${count}`).join('；')}。缺盘口的成交不按零滑点处理。</p>}
      <details><summary>逐笔预估与实测对照（最近 30 条）</summary><div className="cc-review-scroll"><table><thead><tr><th>时间 / 币种</th><th>方向</th><th>手续费</th><th>滑点</th><th>预估单边</th><th>实测单边</th><th>实测 − 预估</th><th>缺失说明</th></tr></thead><tbody>{report.recent_fills?.map((fill, index) => <tr key={`${fill.fill_ts_ms}:${index}`}><th>{dateTime(fill.fill_ts_ms)} · {fill.symbol}</th><td>{fill.side === 'buy' ? '买入' : '卖出'}</td><td>{number(fill.fee_bps, 3)}</td><td>{number(fill.signed_slippage_bps, 3)}</td><td>{number(fill.expected_one_way_cost_bps, 3)}</td><td>{number(fill.observed_one_way_cost_bps, 3)}</td><td>{number(fill.cost_error_bps, 3)}</td><td>{fill.missing_reasons.map(label).join('；') || '—'}</td></tr>)}</tbody></table></div><p>成本列单位为 bps。预估缺失时保持空缺，不用当前假设回填当时预估。</p></details>
      <p className="cc-review-footnote">这些是单边实际费用与相对下单中间价的有符号滑点，负滑点表示价格改善；不是往返账户收益。市场价格变化与执行冲击尚未分离，不能把有效样本数称为“校准通过”。观测结果不会自动改动实盘费用模型或原 V5 开仓规则。</p>
    </>}
  </section>;
}
