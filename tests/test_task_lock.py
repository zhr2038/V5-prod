from __future__ import annotations

import fcntl

import scripts.task_lock as task_lock


def test_task_lock_failed_acquire_preserves_existing_lock_contents(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(task_lock, "LOCK_DIR", tmp_path)
    lock_file = tmp_path / "job.lock"
    lock_file.write_text("123\n2026-01-01T00:00:00\n", encoding="utf-8")

    def fake_flock(fd, flags):
        raise BlockingIOError()

    monkeypatch.setattr(task_lock.fcntl, "flock", fake_flock)

    lock = task_lock.TaskLock("job")

    assert lock.acquire() is False
    assert lock_file.read_text(encoding="utf-8") == "123\n2026-01-01T00:00:00\n"


def test_task_lock_release_without_ownership_keeps_existing_lock_file(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(task_lock, "LOCK_DIR", tmp_path)
    lock_file = tmp_path / "job.lock"
    lock_file.write_text("123\n2026-01-01T00:00:00\n", encoding="utf-8")

    lock = task_lock.TaskLock("job")
    lock.release()

    assert lock_file.exists()
    assert lock_file.read_text(encoding="utf-8") == "123\n2026-01-01T00:00:00\n"
