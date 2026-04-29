import { motion } from 'framer-motion';
import { Brain } from 'lucide-react';
import type { MLTrainingData } from '../types';

interface MLBandProps {
  mlTraining?: MLTrainingData | null;
}

const stageConfig = [
  {
    key: 'sampling',
    title: '采样',
    summary: (data: MLTrainingData) =>
      `${Number(data.labeled_samples || 0)} / ${Number(data.samples_needed || 0) || '--'}`,
  },
  {
    key: 'trained',
    title: '训练',
    summary: (data: MLTrainingData) => data.last_training_ts || '未训练',
  },
  {
    key: 'promoted',
    title: '门控',
    summary: (data: MLTrainingData) =>
      Array.isArray(data.promotion_fail_reasons) && data.promotion_fail_reasons.length
        ? '未通过'
        : data.last_promotion_ts
        ? '已通过'
        : '待评估',
  },
  {
    key: 'liveActive',
    title: '实盘',
    summary: (data: MLTrainingData) =>
      data.runtime_reason && data.runtime_reason !== 'ok'
        ? data.runtime_reason
        : data.last_runtime_ts
        ? '已启用'
        : '未启用',
  },
];

export function MLBand({ mlTraining }: MLBandProps) {
  const stages = (mlTraining?.stages || {}) as Record<string, boolean>;
  const progress = mlTraining?.progress_percent || 0;
  const stageTone: Record<string, string> = {
    sampling: 'tone-sage',
    trained: 'tone-sky',
    promoted: 'tone-amber',
    liveActive: 'tone-coral',
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.15 }}
      className="dashboard-section relative z-10 pb-6"
    >
      <div className="max-w-[1780px] mx-auto material-surface material-regular tone-neutral p-4 sm:p-5">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)] mb-4">
          <Brain className="w-4 h-4" />
          <span>ML 训练链路</span>
          <span className="ml-auto font-mono text-[var(--accent)]">{progress.toFixed(0)}%</span>
        </div>

        <div className="ml-progress-track relative mb-5">
          <motion.div
            className="ml-progress-fill absolute inset-y-0 left-0"
            style={{
              width: `${progress}%`,
            }}
            initial={{ width: 0 }}
            animate={{ width: `${progress}%` }}
            transition={{ duration: 0.8 }}
          />
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          {stageConfig.map((stage) => {
            const active = Boolean(stages[stage.key]);
            const note = stage.summary(mlTraining || {});
            return (
              <div
                key={stage.key}
                className={`material-surface material-clear clear-control surface-lift ${stageTone[stage.key] || 'tone-neutral'} px-3 py-3 flex flex-col gap-1`}
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm text-[var(--text-soft)]">{stage.title}</span>
                  <span
                    className={`text-xs px-2 py-0.5 rounded-full border ${
                      active
                        ? 'bg-emerald-500/15 text-emerald-300 border-emerald-400/25'
                        : 'bg-white/5 text-[var(--text-dim)] border-white/10'
                    }`}
                  >
                    {active ? '已到位' : '等待'}
                  </span>
                </div>
                <div className="text-xs text-[var(--text-dim)] truncate">{note}</div>
              </div>
            );
          })}
        </div>
      </div>
    </motion.div>
  );
}
