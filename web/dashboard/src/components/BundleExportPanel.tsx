import { useCallback, useEffect, useMemo, useState } from 'react';
import { Copy, Download, FileArchive, Info, PackageCheck, RefreshCw } from 'lucide-react';
import { api } from '../api';
import type { LiveFollowupBundle, LiveFollowupBundleGenerateResult } from '../types';

function fmtBundleSize(bytes: unknown) {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value <= 0) return '--';
  if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${value.toFixed(0)} B`;
}

function fmtBundleTime(value?: string) {
  const raw = String(value || '').trim();
  if (!raw) return '--';
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw.slice(0, 16).replace('T', ' ');
  return date.toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function shortBundleName(name: string) {
  return name.replace(/^v5_live_followup_bundle_/, '').replace(/\.tar\.gz$/, '');
}

function resultTone(result?: LiveFollowupBundleGenerateResult | null) {
  if (!result) return 'text-[var(--text-dim)]';
  return result.ok ? 'text-emerald-300' : 'text-rose-300';
}

export function BundleExportPanel() {
  const [bundles, setBundles] = useState<LiveFollowupBundle[]>([]);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [lastResult, setLastResult] = useState<LiveFollowupBundleGenerateResult | null>(null);
  const [error, setError] = useState('');
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [copiedName, setCopiedName] = useState('');

  const latest = bundles[0];
  const statusText = useMemo(() => {
    if (generating) return '正在生成今日包';
    if (lastResult) {
      return lastResult.ok
        ? `生成完成 · ${fmtBundleSize(lastResult.size_bytes)} · ${Number(lastResult.elapsed_seconds || 0).toFixed(1)}s`
        : lastResult.error || '生成失败';
    }
    return latest ? `最新 ${fmtBundleTime(latest.mtime_utc)} · ${fmtBundleSize(latest.size_bytes)}` : '暂无可下载包';
  }, [generating, lastResult, latest]);

  const refreshBundles = useCallback(async () => {
    setLoading(true);
    const payload = await api.liveFollowupBundles();
    if (payload?.ok) {
      setBundles(Array.isArray(payload.bundles) ? payload.bundles : []);
      setError('');
    } else {
      setError(payload?.error || '读取 bundle 列表失败');
    }
    setLoading(false);
  }, []);

  const generateBundle = useCallback(async () => {
    if (generating) return;
    setGenerating(true);
    setLastResult(null);
    setError('');
    const result = await api.generateLiveFollowupBundle();
    setLastResult(result);
    if (Array.isArray(result?.bundles)) {
      setBundles(result.bundles);
    } else {
      await refreshBundles();
    }
    if (!result?.ok) {
      setError(result?.error || '生成今日包失败');
    }
    setGenerating(false);
  }, [generating, refreshBundles]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void refreshBundles();
    }, 0);
    return () => window.clearTimeout(timer);
  }, [refreshBundles]);

  useEffect(() => {
    if (!autoRefresh) return undefined;
    const timer = window.setInterval(() => {
      void refreshBundles();
    }, 30000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, refreshBundles]);

  const copyBundleName = useCallback((bundle: LiveFollowupBundle) => {
    void navigator.clipboard?.writeText(bundle.name);
    setCopiedName(bundle.name);
    window.setTimeout(() => setCopiedName(''), 1400);
  }, []);

  return (
    <section className="bundle-export-panel">
      <div className="bundle-export-head">
        <div className="bundle-export-title">
          <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
            <FileArchive className="h-4 w-4" />
            <span>Follow-up bundle / 今日包导出</span>
          </div>
          <div className={`bundle-export-status ${resultTone(lastResult)}`}>{error || statusText}</div>
        </div>
        <div className="bundle-export-actions">
          <label className="bundle-auto-refresh">
            <span>自动刷新</span>
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(event) => setAutoRefresh(event.currentTarget.checked)}
            />
            <i />
          </label>
          <button
            type="button"
            onClick={() => void refreshBundles()}
            disabled={loading || generating}
            className="bundle-icon-button"
            title="刷新列表"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button
            type="button"
            onClick={() => void generateBundle()}
            disabled={generating}
            className="bundle-generate-button"
          >
            <PackageCheck className={`h-4 w-4 ${generating ? 'animate-pulse' : ''}`} />
            <span>{generating ? '生成中' : '生成今日包'}</span>
          </button>
        </div>
      </div>

      <div className="bundle-progress" data-active={generating || loading}>
        <span style={{ width: generating ? '82%' : loading ? '36%' : '100%' }} />
      </div>

      <div className="bundle-table">
        <div className="bundle-table-header">
          <span>文件名</span>
          <span>SHA256</span>
          <span>大小</span>
          <span>生成时间</span>
          <span>操作</span>
        </div>
        {bundles.slice(0, 5).map((bundle) => (
          <div
            key={bundle.name}
            className="bundle-table-row"
          >
            <span className="bundle-file-name">{shortBundleName(bundle.name)}</span>
            <span>
              {bundle.sha256_available ? <em className="bundle-sha-pill">sha256</em> : <em className="bundle-empty">--</em>}
            </span>
            <span>{fmtBundleSize(bundle.size_bytes)}</span>
            <span>{fmtBundleTime(bundle.mtime_utc)}</span>
            <span className="bundle-row-actions">
              <a
                href={bundle.download_url}
                className="bundle-row-button"
              >
                <Download className="h-3.5 w-3.5" />
                <span>下载</span>
              </a>
              <button type="button" className="bundle-row-button" onClick={() => copyBundleName(bundle)}>
                <Copy className="h-3.5 w-3.5" />
                <span>{copiedName === bundle.name ? '已复制' : '复制'}</span>
              </button>
              <a href={bundle.sha256_download_url || bundle.download_url} className="bundle-row-button">
                <Info className="h-3.5 w-3.5" />
                <span>详情</span>
              </a>
            </span>
          </div>
        ))}
        {!bundles.length ? (
          <div className="bundle-empty-state">
            {loading ? '正在读取服务器包列表...' : '服务器上还没有可下载的 follow-up bundle。'}
          </div>
        ) : null}
      </div>
    </section>
  );
}
