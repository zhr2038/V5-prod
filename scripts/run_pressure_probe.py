#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _cpu_times() -> tuple[int, int]:
    with Path("/proc/stat").open("r", encoding="utf-8") as handle:
        parts = handle.readline().split()[1:9]
    values = [int(part) for part in parts]
    idle = values[3] + values[4]
    total = sum(values)
    return idle, total


def _mem_available_mb() -> float | None:
    with Path("/proc/meminfo").open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("MemAvailable:"):
                return round(int(line.split()[1]) / 1024.0, 1)
    return None


def _loadavg() -> tuple[float, float, float]:
    with Path("/proc/loadavg").open("r", encoding="utf-8") as handle:
        parts = handle.read().split()[:3]
    return (float(parts[0]), float(parts[1]), float(parts[2]))


def _top_processes(limit: int = 8) -> list[str]:
    result = subprocess.run(
        ["ps", "-eo", "pid,comm,%cpu,%mem", "--sort=-%cpu"],
        capture_output=True,
        text=True,
        check=False,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return lines[: max(2, int(limit))]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--output-dir", default="reports/research/remote_pressure")
    parser.add_argument("--sample-seconds", type=float, default=5.0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("missing command after --")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = f"{args.label}_{stamp}"
    monitor_path = output_dir / f"{base}_monitor.jsonl"
    stdout_path = output_dir / f"{base}_stdout.log"
    stderr_path = output_dir / f"{base}_stderr.log"
    summary_path = output_dir / f"{base}_summary.json"

    started_at = time.time()
    started_iso = _utc_now()
    prev_idle, prev_total = _cpu_times()
    samples: list[dict[str, Any]] = []

    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
        process = subprocess.Popen(
            command,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
        )

        while process.poll() is None:
            time.sleep(max(0.5, float(args.sample_seconds)))
            idle, total = _cpu_times()
            cpu_pct = 0.0 if total == prev_total else 100.0 * (1.0 - ((idle - prev_idle) / max(1, total - prev_total)))
            prev_idle, prev_total = idle, total
            load1, load5, load15 = _loadavg()
            sample = {
                "ts": int(time.time()),
                "cpu_pct": round(cpu_pct, 2),
                "load1": load1,
                "load5": load5,
                "load15": load15,
                "mem_available_mb": _mem_available_mb(),
                "top": _top_processes(),
            }
            samples.append(sample)
            with monitor_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")

    ended_at = time.time()
    status = int(process.returncode or 0)
    peak_cpu = max((float(sample.get("cpu_pct") or 0.0) for sample in samples), default=0.0)
    mean_cpu = sum((float(sample.get("cpu_pct") or 0.0) for sample in samples), 0.0) / max(1, len(samples))
    peak_load1 = max((float(sample.get("load1") or 0.0) for sample in samples), default=0.0)
    min_mem = min((float(sample.get("mem_available_mb") or 0.0) for sample in samples), default=0.0)

    summary = {
        "label": args.label,
        "command": command,
        "status": status,
        "started_at": started_iso,
        "ended_at": _utc_now(),
        "elapsed_sec": round(ended_at - started_at, 2),
        "sample_seconds": float(args.sample_seconds),
        "sample_count": len(samples),
        "peak_cpu_pct": round(peak_cpu, 2),
        "mean_cpu_pct": round(mean_cpu, 2),
        "peak_load1": round(peak_load1, 2),
        "min_mem_available_mb": round(min_mem, 1),
        "monitor_path": str(monitor_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }
    _write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
