2026-03-13 repository cleanup archive.

This archive holds research-only batches and stale artifacts removed from the
active repository surface during cleanup.

Moved here:
- one-off research configs under `configs/research/`
- shadow tuned XGBoost dry-run assets
- experiment runners under `scripts/`
- experiment modules no longer needed on the active path
- related experiment tests
- historical notes and `v4_export/` output snapshots

Intentionally kept outside this archive:
- active production/runtime code
- `reports/` runtime state
- default research entry configs still referenced by current scripts
- `src/research/walk_forward_optimizer.py`, which is still imported by
  `src/research/task_runner.py`
