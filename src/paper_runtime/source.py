from __future__ import annotations

import csv
import hashlib
import io
import json
import re
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


@dataclass(frozen=True)
class ProposalSnapshot:
    rows: list[dict[str, Any]]
    proposal_snapshot_id: str = ""
    proposal_snapshot_sha256: str = ""
    snapshot_generated_at: str = ""
    fetched_at: str = ""
    proposal_count: int = 0
    proposal_ids: tuple[str, ...] = ()
    proposal_hashes: tuple[str, ...] = ()
    source_quant_lab_commit: str = ""
    quant_lab_contract_version: str = ""
    source_kind: str = ""
    source_path: str = ""
    identity_valid: bool = False

    def state_payload(self) -> dict[str, Any]:
        return {
            "proposal_snapshot_id": self.proposal_snapshot_id,
            "proposal_snapshot_sha256": self.proposal_snapshot_sha256,
            "proposal_snapshot_generated_at": self.snapshot_generated_at,
            "fetched_at": self.fetched_at,
            "proposal_count": self.proposal_count,
            "proposal_ids": list(self.proposal_ids),
            "proposal_hashes": list(self.proposal_hashes),
            "source_quant_lab_commit": self.source_quant_lab_commit,
            "quant_lab_contract_version": self.quant_lab_contract_version,
            "source_kind": self.source_kind,
            "source_path": self.source_path,
            "identity_valid": self.identity_valid,
        }


class ProposalSnapshotError(RuntimeError):
    pass


def read_paper_strategy_snapshot(
    *,
    run_path: str | Path,
    reports_dir: str | Path,
    diagnostics: Any,
    cfg: Any,
    now_ms: int | None = None,
) -> ProposalSnapshot:
    observed_ms = now_ms or int(datetime.now(UTC).timestamp() * 1000)
    fetched_at = datetime.fromtimestamp(observed_ms / 1000.0, UTC).isoformat()
    if bool(
        getattr(
            diagnostics,
            "quant_lab_paper_strategy_proposals_api_enabled",
            False,
        )
    ):
        return _read_proposal_snapshot_api(
            cfg=cfg,
            diagnostics=diagnostics,
            fetched_at=fetched_at,
        )
    rows = read_paper_strategy_proposals(
        run_path=run_path,
        reports_dir=reports_dir,
        diagnostics=diagnostics,
        now_ms=observed_ms,
    )
    return _snapshot_from_local_rows(rows, fetched_at=fetched_at)


def _read_proposal_snapshot_api(
    *,
    cfg: Any,
    diagnostics: Any,
    fetched_at: str,
) -> ProposalSnapshot:
    qcfg = getattr(cfg, "quant_lab", None)
    if qcfg is None or not bool(getattr(qcfg, "enabled", False)):
        raise ProposalSnapshotError("proposal_snapshot_api_quant_lab_disabled")
    from src.quant_lab_client.client import QuantLabClient

    client = QuantLabClient.from_config(
        qcfg,
        run_id=f"paper-proposal-snapshot:{fetched_at}",
        phase="paper_proposal_reader",
    )
    if client.api_token is None:
        raise ProposalSnapshotError("proposal_snapshot_api_token_missing")
    endpoint = str(
        getattr(
            diagnostics,
            "quant_lab_paper_strategy_proposals_api_path",
            "/v1/paper-strategy/proposals",
        )
        or "/v1/paper-strategy/proposals"
    )
    response = client.get_json(endpoint)
    if not bool(response.ok) or not isinstance(response.data, Mapping):
        raise ProposalSnapshotError("proposal_snapshot_api_invalid_response")
    return _snapshot_from_api_payload(
        response.data,
        fetched_at=fetched_at,
        source_path=f"api:{endpoint}",
    )


def _snapshot_from_api_payload(
    payload: Mapping[str, Any],
    *,
    fetched_at: str,
    source_path: str,
) -> ProposalSnapshot:
    raw_rows = payload.get("proposals")
    if not isinstance(raw_rows, list) or any(
        not isinstance(row, Mapping) for row in raw_rows
    ):
        raise ProposalSnapshotError("proposal_snapshot_members_invalid")
    rows = [dict(row) for row in raw_rows]
    snapshot_id = str(payload.get("proposal_snapshot_id") or "").strip()
    snapshot_sha = str(payload.get("proposal_snapshot_sha256") or "").strip().lower()
    generated_at = _utc_timestamp(payload.get("snapshot_generated_at"))
    source_commit = str(payload.get("source_quant_lab_commit") or "").strip()
    proposal_ids = _string_tuple(payload.get("proposal_ids"))
    proposal_hashes = tuple(value.lower() for value in _string_tuple(payload.get("proposal_hashes")))
    try:
        proposal_count = int(payload.get("proposal_count"))
    except (TypeError, ValueError) as exc:
        raise ProposalSnapshotError("proposal_snapshot_count_invalid") from exc
    if not snapshot_id or not re.fullmatch(r"[0-9a-f]{64}", snapshot_sha):
        raise ProposalSnapshotError("proposal_snapshot_identity_missing")
    if not generated_at:
        raise ProposalSnapshotError("proposal_snapshot_generated_at_invalid")
    members = sorted(
        (
            str(row.get("proposal_id") or "").strip(),
            str(row.get("proposal_hash") or "").strip().lower(),
        )
        for row in rows
    )
    if any(not proposal_id or not re.fullmatch(r"[0-9a-f]{64}", proposal_hash) for proposal_id, proposal_hash in members):
        raise ProposalSnapshotError("proposal_snapshot_member_identity_invalid")
    if len(set(members)) != len(members):
        raise ProposalSnapshotError("proposal_snapshot_duplicate_member")
    observed_ids = tuple(proposal_id for proposal_id, _proposal_hash in members)
    observed_hashes = tuple(proposal_hash for _proposal_id, proposal_hash in members)
    if (
        proposal_count != len(rows)
        or proposal_ids != observed_ids
        or proposal_hashes != observed_hashes
    ):
        raise ProposalSnapshotError("proposal_snapshot_membership_mismatch")
    material = {
        "proposal_ids": list(proposal_ids),
        "proposal_hashes": list(proposal_hashes),
        "proposal_count": proposal_count,
        "source_quant_lab_commit": source_commit,
    }
    expected_sha = hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if snapshot_sha != expected_sha or snapshot_id != f"proposal-snapshot:{snapshot_sha[:24]}":
        raise ProposalSnapshotError("proposal_snapshot_hash_mismatch")
    contract_versions = {
        str(row.get("contract_version") or "").strip() for row in rows
    } - {""}
    quant_lab_contract_version = str(
        payload.get("quant_lab_contract_version")
        or payload.get("contract_version")
        or (next(iter(contract_versions)) if len(contract_versions) == 1 else "")
    ).strip()
    for row in rows:
        row_snapshot_id = str(row.get("proposal_snapshot_id") or snapshot_id).strip()
        row_snapshot_sha = str(
            row.get("proposal_snapshot_sha256") or snapshot_sha
        ).strip().lower()
        row_generated_at = _utc_timestamp(
            row.get("snapshot_generated_at") or generated_at
        )
        if (
            row_snapshot_id != snapshot_id
            or row_snapshot_sha != snapshot_sha
            or row_generated_at != generated_at
        ):
            raise ProposalSnapshotError("proposal_snapshot_member_metadata_mismatch")
        row.update(
            {
                "proposal_snapshot_id": snapshot_id,
                "proposal_snapshot_sha256": snapshot_sha,
                "snapshot_generated_at": generated_at,
                "source_quant_lab_commit": source_commit,
                "source_path": source_path,
            }
        )
    return ProposalSnapshot(
        rows=rows,
        proposal_snapshot_id=snapshot_id,
        proposal_snapshot_sha256=snapshot_sha,
        snapshot_generated_at=generated_at,
        fetched_at=fetched_at,
        proposal_count=proposal_count,
        proposal_ids=proposal_ids,
        proposal_hashes=proposal_hashes,
        source_quant_lab_commit=source_commit,
        quant_lab_contract_version=quant_lab_contract_version,
        source_kind="canonical_api",
        source_path=source_path,
        identity_valid=True,
    )


def _snapshot_from_local_rows(
    rows: list[dict[str, Any]],
    *,
    fetched_at: str,
) -> ProposalSnapshot:
    snapshot_ids = {str(row.get("proposal_snapshot_id") or "").strip() for row in rows} - {""}
    snapshot_shas = {
        str(row.get("proposal_snapshot_sha256") or "").strip().lower()
        for row in rows
    } - {""}
    generated_values = {
        _utc_timestamp(row.get("snapshot_generated_at")) for row in rows
    } - {""}
    source_path = str(rows[0].get("source_path") or "") if rows else ""
    if len(snapshot_ids) != 1 or len(snapshot_shas) != 1 or len(generated_values) != 1:
        return ProposalSnapshot(
            rows=rows,
            fetched_at=fetched_at,
            proposal_count=len(rows),
            proposal_ids=tuple(str(row.get("proposal_id") or "") for row in rows),
            proposal_hashes=tuple(str(row.get("proposal_hash") or "") for row in rows),
            source_kind="legacy_local",
            source_path=source_path,
            identity_valid=False,
        )
    members = sorted(
        (
            str(row.get("proposal_id") or "").strip(),
            str(row.get("proposal_hash") or "").strip().lower(),
        )
        for row in rows
    )
    payload = {
        "proposal_snapshot_id": next(iter(snapshot_ids)),
        "proposal_snapshot_sha256": next(iter(snapshot_shas)),
        "snapshot_generated_at": next(iter(generated_values)),
        "proposal_count": len(rows),
        "proposal_ids": [proposal_id for proposal_id, _proposal_hash in members],
        "proposal_hashes": [proposal_hash for _proposal_id, proposal_hash in members],
        "source_quant_lab_commit": str(rows[0].get("source_quant_lab_commit") or ""),
        "proposals": rows,
    }
    return _snapshot_from_api_payload(
        payload,
        fetched_at=fetched_at,
        source_path=source_path,
    )


def _string_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            decoded = []
        value = decoded
    if not isinstance(value, list):
        return ()
    return tuple(str(item or "").strip() for item in value)


def _utc_timestamp(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        return ""
    return parsed.astimezone(UTC).isoformat()


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
