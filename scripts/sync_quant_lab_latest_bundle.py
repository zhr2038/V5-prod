from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any


DEFAULT_STATUS_URL = "http://qyun2.hrhome.top:8027/web-v2/expert-pack/status"
DEFAULT_OUTPUT_DIR = "/var/lib/v5-prod"
DEFAULT_OUTPUT_NAME = "quant_lab_latest_bundle.zip"
PROPOSAL_MEMBER = "reports/paper_strategy_proposals.csv"


def _read_url(url: str, *, timeout: float) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "v5-quant-lab-bundle-sync/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - production URL is operator-configured.
        return resp.read()


def _load_status(status_url: str, *, timeout: float) -> dict[str, Any]:
    payload = _read_url(status_url, timeout=timeout)
    try:
        data = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"invalid expert-pack status JSON from {status_url}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"expert-pack status must be a JSON object: {status_url}")
    return data


def _download_url(status: dict[str, Any], status_url: str) -> str:
    raw = str(
        status.get("available_download_url")
        or status.get("latest_download_url")
        or status.get("download_url")
        or ""
    ).strip()
    if not raw:
        raise RuntimeError("expert-pack status did not expose a download URL")
    return urllib.parse.urljoin(status_url, raw)


def _proposal_row_count(bundle_path: Path) -> int:
    try:
        with zipfile.ZipFile(bundle_path) as zf:
            if PROPOSAL_MEMBER not in zf.namelist():
                return -1
            text = zf.read(PROPOSAL_MEMBER).decode("utf-8-sig")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"downloaded expert pack is not a valid zip: {bundle_path}") from exc
    rows = list(csv.DictReader(io.StringIO(text)))
    return len(rows)


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        tmp = Path(tmp_name)
        if tmp.exists():
            tmp.unlink()


def sync_latest_bundle(
    *,
    status_url: str,
    output_dir: Path,
    output_name: str,
    timeout: float,
    allow_missing_proposals: bool = False,
) -> dict[str, Any]:
    status = _load_status(status_url, timeout=timeout)
    if str(status.get("storage_location") or "").strip().lower() == "nas_only":
        return {
            "status": "skipped",
            "reason": "nas_only_pack_bytes_are_not_proxied_by_qyun2",
            "status_url": status_url,
            "storage_location": "nas_only",
            "cloud_zip_present": bool(status.get("cloud_zip_present")),
            "local_bundle_unchanged": True,
            "pack_name": status.get("available_pack_name")
            or status.get("latest_pack_name")
            or "",
            "export_date": status.get("export_date") or "",
            "state": status.get("state") or "",
            "live_order_effect": "none",
        }
    url = _download_url(status, status_url)
    payload = _read_url(url, timeout=timeout)
    target = output_dir / output_name
    _atomic_write(target, payload)
    digest = hashlib.sha256(payload).hexdigest()
    proposal_rows = _proposal_row_count(target)
    if proposal_rows <= 0 and not allow_missing_proposals:
        raise RuntimeError(
            f"downloaded expert pack has no {PROPOSAL_MEMBER} rows; "
            "refusing to publish an empty proposal source"
        )
    sha_path = target.with_suffix(target.suffix + ".sha256")
    sha_path.write_text(f"{digest}  {target.name}\n", encoding="utf-8")
    result = {
        "status": "ok",
        "status_url": status_url,
        "download_url": url,
        "output_path": str(target),
        "sha256": digest,
        "proposal_rows": proposal_rows,
        "pack_name": status.get("available_pack_name") or status.get("latest_pack_name") or "",
        "export_date": status.get("export_date") or "",
        "state": status.get("state") or "",
    }
    target.with_suffix(target.suffix + ".json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync latest quant-lab expert pack for V5 paper-only readers.")
    parser.add_argument("--status-url", default=os.environ.get("QUANT_LAB_EXPERT_PACK_STATUS_URL", DEFAULT_STATUS_URL))
    parser.add_argument("--output-dir", default=os.environ.get("V5_QUANT_LAB_BUNDLE_DIR", DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-name", default=os.environ.get("V5_QUANT_LAB_BUNDLE_NAME", DEFAULT_OUTPUT_NAME))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("V5_QUANT_LAB_BUNDLE_SYNC_TIMEOUT", "30")))
    parser.add_argument("--allow-missing-proposals", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = sync_latest_bundle(
            status_url=str(args.status_url),
            output_dir=Path(args.output_dir),
            output_name=str(args.output_name),
            timeout=float(args.timeout),
            allow_missing_proposals=bool(args.allow_missing_proposals),
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
