import { useEffect, useId, useMemo, useState } from 'react';
import { AlertCircle, Clock3, LoaderCircle } from 'lucide-react';
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import './equity-history-chart.css';

export interface EquityHistoryChartProps {
  points: { timestamp: string; equity: number }[];
  currentEquity: number | null;
  loading: boolean;
  failed: boolean;
  className?: string;
}

type Period = '7D' | '30D' | 'ALL';
interface ObservedPoint { time: number; equity: number }
interface PlotPoint { time: number; equity: number | null; edge?: boolean }

const DAY = 86_400_000;
const PERIODS: { value: Period; label: string; description: string }[] = [
  { value: '7D', label: '7D', description: '最近7天' },
  { value: '30D', label: '30D', description: '最近30天' },
  { value: 'ALL', label: '全部', description: '全部可用记录' },
];
const AMOUNT = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const PRECISE_AMOUNT = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
const DATE = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit',
});
const FULL_TIME = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
});

function normalizeObservations(points: EquityHistoryChartProps['points'], now: number) {
  const byTime = new Map<number, number | null>();
  let excluded = 0;
  for (const point of points) {
    // A timezone is required: browser-local interpretation would change the evidence.
    const timestamp = typeof point.timestamp === 'string' ? point.timestamp.trim() : '';
    const time = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(timestamp) ? Date.parse(timestamp) : NaN;
    if (!Number.isFinite(time) || time > now || typeof point.equity !== 'number' || !Number.isFinite(point.equity)) {
      excluded += 1;
      continue;
    }
    if (byTime.has(time) && byTime.get(time) !== point.equity) {
      // Conflicting values at one timestamp have no authoritative ordering.
      byTime.set(time, null);
    } else if (!byTime.has(time)) {
      byTime.set(time, point.equity);
    }
  }
  const observed: ObservedPoint[] = [];
  for (const [time, equity] of byTime) {
    if (equity === null) excluded += 1;
    else observed.push({ time, equity });
  }
  observed.sort((a, b) => a.time - b.time);
  return { observed, excluded };
}

function gapThreshold(points: ObservedPoint[]) {
  const intervals = points.slice(1).map((point, index) => point.time - points[index].time).sort((a, b) => a - b);
  const middle = Math.floor(intervals.length / 2);
  const typical = intervals.length ? (intervals.length % 2 ? intervals[middle] : (intervals[middle - 1] + intervals[middle]) / 2) : DAY;
  return Math.min(36 * 3_600_000, Math.max(6 * 3_600_000, typical * 3));
}

function EquityTooltip({ active, payload }: { active?: boolean; payload?: readonly { payload?: PlotPoint }[] }) {
  const point = payload?.find((entry) => typeof entry.payload?.equity === 'number')?.payload;
  if (!active || !point || point.equity === null) return null;
  return (
    <div className="equity-history__tooltip">
      <span>{FULL_TIME.format(point.time)} <span className="equity-history__tooltip-zone">UTC+8</span></span>
      <strong>{PRECISE_AMOUNT.format(point.equity)} <small>USDT</small></strong>
      <span className="equity-history__tooltip-source">账户权益观测</span>
    </div>
  );
}

function ObservationDot({ cx, cy, payload }: { cx?: number; cy?: number; payload?: PlotPoint }) {
  if (cx === undefined || cy === undefined || !payload?.edge || payload.equity === null) return <g />;
  return <circle cx={cx} cy={cy} r={3} fill="#e8b86d" stroke="#101519" strokeWidth={2} />;
}

export function EquityHistoryChart({ points, currentEquity, loading, failed, className = '' }: EquityHistoryChartProps) {
  const [period, setPeriod] = useState<Period>('30D');
  const [now, setNow] = useState(Date.now);
  const titleId = useId();
  const descriptionId = useId();

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 60_000);
    return () => window.clearInterval(timer);
  }, []);

  const normalized = useMemo(() => normalizeObservations(points, now), [points, now]);
  const selected = useMemo(() => {
    const cutoff = period === 'ALL' ? -Infinity : now - (period === '7D' ? 7 : 30) * DAY;
    return normalized.observed.filter((point) => point.time >= cutoff);
  }, [normalized.observed, now, period]);
  const threshold = useMemo(() => gapThreshold(normalized.observed), [normalized.observed]);
  const chart = useMemo(() => {
    const rows: PlotPoint[] = [];
    let gaps = 0;
    for (let index = 0; index < selected.length; index += 1) {
      const point = selected[index];
      const previous = selected[index - 1];
      const next = selected[index + 1];
      if (previous && point.time - previous.time > threshold) {
        // A null path separator is not an equity observation or an interpolated value.
        rows.push({ time: previous.time + (point.time - previous.time) / 2, equity: null });
        gaps += 1;
      }
      rows.push({ ...point, edge: selected.length <= 24 || !previous || !next || point.time - previous.time > threshold || next.time - point.time > threshold });
    }
    const values = selected.map((point) => point.equity);
    const minimum = Math.min(0, ...values);
    const maximum = Math.max(0, ...values);
    // Include zero so a few cents of movement cannot become a dramatic full-height swing.
    const domain: [number, number] = minimum === maximum ? [0, 1] : [minimum < 0 ? minimum * 1.08 : 0, maximum > 0 ? maximum * 1.12 : 0];
    return { rows, gaps, domain };
  }, [selected, threshold]);

  const first = selected[0];
  const last = selected.at(-1);
  const change = selected.length >= 2 ? last!.equity - first.equity : null;
  const changePct = change !== null && first.equity > 0 ? change / first.equity * 100 : null;
  const current = typeof currentEquity === 'number' && Number.isFinite(currentEquity) ? currentEquity : null;
  const periodLabel = PERIODS.find((item) => item.value === period)!.description;
  const startsAt = period === 'ALL' ? Math.min(first?.time ?? now - DAY, now - DAY) : now - (period === '7D' ? 7 : 30) * DAY;
  const newestAge = last ? now - last.time : null;
  const showOldObservation = newestAge !== null && newestAge > threshold;
  const status = loading ? '正在读取权益记录' : failed ? (selected.length ? '历史更新失败 · 显示已有记录' : '历史权益读取失败') : selected.length === 1 ? '只有1次观测，暂不能计算区间变化' : selected.length === 0 ? (normalized.observed.length ? '所选时段没有权益观测' : '尚无可用的历史权益观测') : '';

  return (
    <section className={`equity-history ${className}`.trim()} aria-labelledby={titleId} aria-busy={loading}>
      <header className="equity-history__heading">
        <div>
          <div className="equity-history__eyebrow" id={titleId}>账户权益走势 <span>/ USDT</span></div>
          <div className="equity-history__balance">
            <span>最近可用记录 · 当前 {current === null ? '—' : AMOUNT.format(current)} USDT</span>
          </div>
        </div>
        <div className="equity-history__controls">
          <div className="equity-history__range" role="group" aria-label="权益历史时间范围">
            {PERIODS.map((item) => (
              <button type="button" key={item.value} aria-pressed={period === item.value} aria-label={item.description} onClick={() => setPeriod(item.value)}>
                {item.label}
              </button>
            ))}
          </div>
          <span className="equity-history__source"><i aria-hidden="true" />真实账户记录</span>
        </div>
      </header>

      <div className="equity-history__summary" aria-live="polite" aria-atomic="true">
        <span>{periodLabel}权益变化</span>
        <strong className={change !== null && change > 0 ? 'equity-history__change-positive' : ''}>
          {change === null ? '—' : `${change > 0 ? '+' : change < 0 ? '−' : ''}${AMOUNT.format(Math.abs(change))}`}
          {change !== null && <small> USDT</small>}
        </strong>
        {changePct !== null && <span className="equity-history__percent">{changePct > 0 ? '+' : ''}{changePct.toFixed(2)}%</span>}
        <span className="equity-history__scope">权益变化，非净收益</span>
      </div>

      <p id={descriptionId} className="equity-history__sr-only">
        {periodLabel}内有{selected.length}次有效权益观测，{chart.gaps}处长时间数据间断。
        纵轴包含零；连线只连接相邻观测，间断保持空白，末次记录不会延伸至当前权益。时间均为北京时间。
        可聚焦图表后使用左右方向键查看观测值。
      </p>

      <div className="equity-history__plot" aria-describedby={descriptionId}>
        {selected.length > 0 ? (
          <ResponsiveContainer width="100%" height={250} minWidth={0}>
            <LineChart data={chart.rows} margin={{ top: 18, right: 8, bottom: 4, left: 0 }} accessibilityLayer aria-label={`${periodLabel}真实权益历史曲线`}>
              <CartesianGrid vertical={false} stroke="#293139" strokeDasharray="2 6" />
              <XAxis dataKey="time" type="number" domain={[startsAt, now]} scale="time" tickFormatter={(time: number) => DATE.format(time)} tickLine={false} axisLine={false} tick={{ fill: '#929da5', fontSize: 11 }} minTickGap={40} tickMargin={12} />
              <YAxis domain={chart.domain} tickLine={false} axisLine={false} tick={{ fill: '#929da5', fontSize: 11 }} tickFormatter={(value: number) => new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 2 }).format(value)} width={54} tickMargin={10} tickCount={4} />
              <Tooltip content={<EquityTooltip />} cursor={{ stroke: '#67717b', strokeWidth: 1, strokeDasharray: '3 4' }} isAnimationActive={false} filterNull />
              <Line type="linear" dataKey="equity" stroke="#e8b86d" strokeWidth={1.8} dot={<ObservationDot />} activeDot={{ r: 4, fill: '#e8b86d', stroke: '#101519', strokeWidth: 2 }} connectNulls={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="equity-history__empty" role="status">
            {loading ? <LoaderCircle size={20} className="equity-history__loading" aria-hidden="true" /> : <Clock3 size={20} aria-hidden="true" />}
            <strong>{status}</strong>
            <span>{loading ? '等待真实账户观测' : failed ? '连接恢复后刷新，不以零值代替缺失数据' : '记录到达后将显示实际观测点'}</span>
          </div>
        )}
      </div>

      <footer className="equity-history__footer">
        <div className="equity-history__coverage">
          <span><strong>{selected.length}</strong> 次观测</span>
          {chart.gaps > 0 && <span>{chart.gaps} 处观测间断</span>}
          <span>北京时间 · UTC+8</span>
        </div>
        {last && <span className={showOldObservation ? 'equity-history__stale' : ''}>末次 {FULL_TIME.format(last.time)}</span>}
      </footer>
      {(selected.length > 0 && status) || normalized.excluded > 0 || showOldObservation ? (
        <div className="equity-history__note" role="status">
          <AlertCircle size={12} aria-hidden="true" />
          <span>{[selected.length > 0 ? status : '', showOldObservation ? '末次观测后暂无新记录，曲线保留空白' : '', normalized.excluded > 0 ? `已排除${normalized.excluded}条无效、冲突或未来记录` : ''].filter(Boolean).join('；')}</span>
        </div>
      ) : null}
    </section>
  );
}

export default EquityHistoryChart;
