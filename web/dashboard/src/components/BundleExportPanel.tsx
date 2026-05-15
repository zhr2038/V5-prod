import { useCallback, useEffect, useMemo, useState } from 'react';
import { Download, FileArchive, PackageCheck, RefreshCw } from 'lucide-react';
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
    void refreshBundles();
  }, [refreshBundles]);

  return (
    <div className="liquid-glass-thick reading-frame tone-sky p-5 flex flex-col gap-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 flex-col gap-1">
          <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
            <FileArchive className="h-4 w-4" />
            <span>Follow-up bundle</span>
          </div>
          <div className="text-xl font-semibold tracking-tight">今日包导出</div>
          <div className={`text-xs ${resultTone(lastResult)}`}>{error || statusText}</div>
        </div>
        <div className="flex shrink-0 gap-2">
          <button
            type="button"
            onClick={() => void refreshBundles()}
            disabled={loading || generating}
            className="liquid-glass-thin tone-pearl inline-flex h-10 items-center justify-center rounded-lg px-3 text-xs font-medium text-[var(--text-soft)] transition hover:border-white/25 disabled:opacity-50"
            title="刷新列表"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button
            type="button"
            onClick={() => void generateBundle()}
            disabled={generating}
            className="liquid-glass-thin tone-sage inline-flex h-10 items-center justify-center gap-2 rounded-lg px-4 text-xs font-semibold text-emerald-100 transition hover:border-emerald-200/30 disabled:opacity-50"
          >
            <PackageCheck className={`h-4 w-4 ${generating ? 'animate-pulse' : ''}`} />
            <span>{generating ? '生成中' : '生成今日包'}</span>
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-2">
        {bundles.slice(0, 5).map((bundle) => (
          <div
            key={bundle.name}
            className="liquid-glass-thin list-row tone-pearl flex flex-col gap-2 px-3 py-3 sm:flex-row sm:items-center sm:justify-between"
          >
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm text-[var(--text-soft)]">{shortBundleName(bundle.name)}</span>
                {bundle.sha256_available ? (
                  <span className="rounded-full border border-emerald-300/20 px-2 py-0.5 text-[10px] text-emerald-300">
                    sha256
                  </span>
                ) : null}
              </div>
              <div className="mt-1 text-xs text-[var(--text-dim)]">
                {fmtBundleTime(bundle.mtime_utc)} · {fmtBundleSize(bundle.size_bytes)}
              </div>
            </div>
            <div className="flex items-center gap-2">
              {bundle.sha256_download_url ? (
                <a
                  href={bundle.sha256_download_url}
                  className="rounded-lg border border-white/10 px-2.5 py-2 text-xs text-[var(--text-dim)] transition hover:border-white/25 hover:text-[var(--text-soft)]"
                >
                  sha
                </a>
              ) : null}
              <a
                href={bundle.download_url}
                className="inline-flex items-center gap-2 rounded-lg border border-white/12 px-3 py-2 text-xs font-medium text-[var(--text-soft)] transition hover:border-[var(--accent)]/45 hover:text-white"
              >
                <Download className="h-3.5 w-3.5" />
                <span>下载</span>
              </a>
            </div>
          </div>
        ))}
        {!bundles.length ? (
          <div className="liquid-glass-thin tone-pearl px-3 py-4 text-sm text-[var(--text-dim)]">
            {loading ? '正在读取服务器包列表...' : '服务器上还没有可下载的 follow-up bundle。'}
          </div>
        ) : null}
      </div>
    </div>
  );
}
