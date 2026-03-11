from __future__ import annotations

from src.research.recorder import ResearchRecorder, find_latest_task_run, load_latest_task_record


def test_research_recorder_persists_latest_run_artifacts(tmp_path) -> None:
    recorder = ResearchRecorder(base_dir=tmp_path)

    run1 = recorder.start_run(task_name="ml_training", task_config={"task": {"name": "ml_training"}})
    run1.write_json("metrics.json", {"score": 1})
    recorder.finalize_run(run1, status="completed", summary={"score": 1})

    run2 = recorder.start_run(task_name="ml_training", task_config={"task": {"name": "ml_training"}})
    run2.write_json("metrics.json", {"score": 2})
    recorder.finalize_run(run2, status="completed", summary={"score": 2})

    latest_run = find_latest_task_run("ml_training", base_dir=tmp_path)
    latest_metrics = load_latest_task_record("ml_training", "metrics.json", base_dir=tmp_path)

    assert latest_run == run2.run_dir
    assert latest_metrics == {"score": 2}
