from __future__ import annotations

import gzip
import os
import time

import scripts.log_rotator as log_rotator


def test_log_rotator_build_paths_anchor_to_workspace(tmp_path) -> None:
    paths = log_rotator.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.logs_dir == (tmp_path / "logs").resolve()
    assert paths.archive_dir == (tmp_path / "logs" / "archive").resolve()


def test_log_rotator_compresses_old_logs_under_workspace(tmp_path) -> None:
    log_file = tmp_path / "logs" / "worker.20260407.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text("old-log", encoding="utf-8")
    old_ts = time.time() - (log_rotator.KEEP_DAYS + 1) * 86400
    os.utime(log_file, (old_ts, old_ts))

    rotator = log_rotator.LogRotator(workspace=tmp_path)
    rotator.compress_old_logs()

    archive_path = tmp_path / "logs" / "archive" / "worker.20260407.log.gz"
    assert not log_file.exists()
    assert archive_path.exists()
    with gzip.open(archive_path, "rt", encoding="utf-8") as fh:
        assert fh.read() == "old-log"
