from __future__ import annotations

import csv
import io
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


PAPER_STRATEGY_PROPOSAL_FILENAME = "paper_strategy_proposals.csv"


@dataclass(frozen=True)
class ProposalSource:
    path: Path
    rows: list[dict[str, Any]]
    is_archive: bool
    mtime_ms: int


def read_paper_strategy_proposals(
    *,
    run_path: str | Path,
    reports_dir: str | Path,
    diagnostics: Any,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Read exactly one authoritative proposal generation.

    Archives represent a complete quant-lab snapshot and therefore take
    precedence over loose CSV files. Multiple generations are never unioned.
    """
    if not bool(
        getattr(diagnostics, "quant_lab_paper_strategy_proposals_enabled", True)
    ):
        return []
    configured = (
        getattr(diagnostics, "quant_lab_paper_strategy_proposals_paths", None)
        or default_proposal_paths()
    )
    max_age_minutes = float(
        getattr(
            diagnostics,
            "quant_lab_paper_strategy_proposals_max_age_minutes",
            1440.0,
        )
        or 1440.0
    )
    observed_ms = now_ms or int(datetime.now(UTC).timestamp() * 1000)
    cutoff_ms = int(observed_ms - max_age_minutes * 60_000)
    archives: list[ProposalSource] = []
    direct: list[ProposalSource] = []
    seen_paths: set[Path] = set()
    run = Path(run_path)
    reports = Path(reports_dir)
    for raw_path in configured:
        for path in candidate_paths(str(raw_path), run_path=run, reports_dir=reports):
            resolved = path.resolve()
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)
            if not path.is_file():
                continue
            try:
                mtime_ms = int(path.stat().st_mtime * 1000)
            except OSError:
                continue
            if mtime_ms < cutoff_ms:
                continue
            rows = read_raw_csv_path(
                path,
                target_filename=PAPER_STRATEGY_PROPOSAL_FILENAME,
            )
            if not rows:
                continue
            source = ProposalSource(
                path=resolved,
                rows=rows,
                is_archive=is_archive_path(path),
                mtime_ms=mtime_ms,
            )
            (archives if source.is_archive else direct).append(source)

    sources = archives or direct
    if not sources:
        return []
    latest_mtime_ms = max(source.mtime_ms for source in sources)
    current_rows = [
        row
        for source in sources
        if source.mtime_ms == latest_mtime_ms
        for row in source.rows
    ]
    return dedupe_proposal_rows(current_rows)


def default_proposal_paths() -> list[str]:
    return [
        "/var/lib/v5-prod/paper_strategy_proposals.csv",
        "/var/lib/v5-prod/quant_lab_latest_bundle.zip",
        "/var/lib/v5-prod/quant_lab_latest_bundle.tar.gz",
        "paper_strategy_proposals.csv",
        "quant_lab/paper_strategy_proposals.csv",
        "quant_lab_latest/paper_strategy_proposals.csv",
        "quant_lab/latest/reports/paper_strategy_proposals.csv",
        "reports/paper_strategy_proposals.csv",
        "reports/quant_lab_latest/paper_strategy_proposals.csv",
        "reports/quant_lab/latest/reports/paper_strategy_proposals.csv",
        "reports/quant_lab_latest_bundle.zip",
        "reports/quant_lab_latest_bundle.tar.gz",
        "reports/quant_lab/latest_bundle.zip",
        "reports/quant_lab/latest_bundle.tar.gz",
    ]


def candidate_paths(
    raw_path: str,
    *,
    run_path: Path,
    reports_dir: Path,
) -> list[Path]:
    path = Path(str(raw_path or "").strip())
    if not str(path):
        return []
    if path.is_absolute():
        return [path]
    candidates = [reports_dir / path, run_path / path, Path.cwd() / path]
    parts = path.parts
    if parts and parts[0].lower() == "reports":
        candidates.append(reports_dir.parent / path)
        candidates.append(
            reports_dir / Path(*parts[1:]) if len(parts) > 1 else reports_dir
        )
    return list(dict.fromkeys(candidates))


def dedupe_proposal_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for raw in rows:
        row = dict(raw)
        proposal_id = str(row.get("proposal_id") or row.get("strategy_id") or "")
        proposal_hash = str(row.get("proposal_hash") or "")
        if proposal_id and proposal_hash:
            key = ("structured", proposal_id, proposal_hash)
        else:
            key = (
                "legacy",
                proposal_id,
                str(row.get("strategy_candidate") or ""),
                str(row.get("symbol") or ""),
                str(row.get("recommended_mode") or ""),
                str(row.get("suggested_horizon") or ""),
                str(row.get("entry_conditions") or ""),
            )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def is_archive_path(path: Path) -> bool:
    return path.name.lower().endswith((".zip", ".tar", ".tar.gz", ".tgz"))


def read_raw_csv_path(
    path: Path,
    *,
    target_filename: str,
) -> list[dict[str, Any]]:
    lower_name = path.name.lower()
    if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        try:
            with tarfile.open(path, "r:*") as archive:
                members = {
                    member.name: member
                    for member in archive.getmembers()
                    if member.isfile()
                }
                rows: list[dict[str, Any]] = []
                for member_name in archive_csv_members(members, target_filename):
                    extracted = archive.extractfile(members[member_name])
                    if extracted is None:
                        continue
                    with extracted:
                        with io.TextIOWrapper(
                            extracted,
                            encoding="utf-8",
                            newline="",
                        ) as handle:
                            rows.extend(
                                raw_csv_rows(
                                    handle,
                                    source_path=f"{path}:{member_name}",
                                )
                            )
                return rows
        except (OSError, tarfile.TarError, UnicodeError, csv.Error):
            return []
    if lower_name.endswith(".zip"):
        try:
            with zipfile.ZipFile(path) as archive:
                members = [name for name in archive.namelist() if not name.endswith("/")]
                rows = []
                for member_name in archive_csv_members(members, target_filename):
                    with archive.open(member_name) as extracted:
                        with io.TextIOWrapper(
                            extracted,
                            encoding="utf-8",
                            newline="",
                        ) as handle:
                            rows.extend(
                                raw_csv_rows(
                                    handle,
                                    source_path=f"{path}:{member_name}",
                                )
                            )
                return rows
        except (OSError, zipfile.BadZipFile, KeyError, UnicodeError, csv.Error):
            return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return raw_csv_rows(handle, source_path=str(path))
    except (OSError, UnicodeError, csv.Error):
        return []


def archive_csv_members(names: Iterable[str], target_filename: str) -> list[str]:
    normalized = [(name, str(name).replace("\\", "/")) for name in names]
    report_target = f"reports/{target_filename}"
    primary = [name for name, clean in normalized if clean.endswith(report_target)]
    if primary:
        return primary
    return [name for name, clean in normalized if clean.endswith(target_filename)]


def raw_csv_rows(handle: Any, *, source_path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in csv.DictReader(handle):
        if not row:
            continue
        payload = dict(row)
        payload["source_path"] = source_path
        rows.append(payload)
    return rows
