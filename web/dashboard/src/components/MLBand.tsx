import { motion } from 'framer-motion';
import { Brain } from 'lucide-react';
import type { MLTrainingData } from '../types';

interface MLBandProps {
  mlTraining?: MLTrainingData | null;
}

const stageNames = ['采样', '训练', '门控', '实盘'];

export function MLBand({ mlTraining }: MLBandProps) {
  const stages = mlTraining?.stages || [];
  const progress = mlTraining?.progress_percent || 0;

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.15 }}
      className="relative z-10 px-6 pb-6"
    >
      <div className="glass-card p-5">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)] mb-4">
          <Brain className="w-4 h-4" />
          <span>ML 训练链路</span>
          <span className="ml-auto font-mono text-[var(--accent)]">{progress.toFixed(0)}%</span>
        </div>

        <div className="relative h-2 bg-white/10 rounded-full overflow-hidden mb-5">
          <motion.div
            className="absolute inset-y-0 left-0 rounded-full"
            style={{
              background: 'linear-gradient(90deg, #7fffd4, #8dc4ff, #7fffd4)',
              backgroundSize: '200% 100%',
            }}
            initial={{ width: 0 }}
            animate={{ width: `${progress}%` }}
            transition={{ duration: 0.8 }}
          />
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {stageNames.map((name, idx) => {
            const stage = stages[idx];
            const status = stage?.status || 'pending';
            const isDone = status === 'completed';
            const isRunning = status === 'running';
            return (
              <div
                key={name}
                className={`glass-panel px-3 py-3 flex items-center justify-between gap-2 ${
                  isDone ? 'border-emerald-400/20' : isRunning ? 'border-sky-400/20' : ''
                }`}
              >
                <span className="text-sm text-[var(--text-soft)]">{name}</span>
                <span
                  className={`text-xs px-2 py-0.5 rounded-full border ${
                    isDone
                      ? 'bg-emerald-500/15 text-emerald-300 border-emerald-400/25'
                      : isRunning
                      ? 'bg-sky-500/15 text-sky-300 border-sky-400/25'
                      : 'bg-white/5 text-[var(--text-dim)] border-white/10'
                  }`}
                >
                  {isDone ? '完成' : isRunning ? '进行中' : '等待'}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    </motion.div>
  );
}
