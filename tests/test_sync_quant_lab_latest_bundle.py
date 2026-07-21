from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from scripts import sync_quant_lab_latest_bundle as syncer


class _FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return self.payload


def _zip_payload(members: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in members.items():
            zf.writestr(name, text)
    return buf.getvalue()


def test_sync_latest_bundle_downloads_status_pack_and_validates_proposals(monkeypatch, tmp_path: Path) -> None:
    pack = _zip_payload(
        {
            syncer.PROPOSAL_MEMBER: (
                "proposal_id,strategy_candidate,symbol,recommended_mode\n"
                "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1,v5.f3_dominant_entry,SOL-USDT,paper\n"
            )
        }
    )
    status = {
        "available_download_url": "/web-v2/expert-pack/download/latest.zip",
        "available_pack_name": "quant_lab_expert_pack_2026-07-04.zip",
        "export_date": "2026-07-04",
        "state": "succeeded",
    }

    def fake_urlopen(req, timeout):
        url = req.full_url
        if url.endswith("/web-v2/expert-pack/status"):
            return _FakeResponse(json.dumps(status).encode("utf-8"))
        if url.endswith("/web-v2/expert-pack/download/latest.zip"):
            return _FakeResponse(pack)
        raise AssertionError(url)

    monkeypatch.setattr(syncer.urllib.request, "urlopen", fake_urlopen)

    result = syncer.sync_latest_bundle(
        status_url="http://qyun2.hrhome.top:8027/web-v2/expert-pack/status",
        output_dir=tmp_path,
        output_name="quant_lab_latest_bundle.zip",
        timeout=1.0,
    )

    target = tmp_path / "quant_lab_latest_bundle.zip"
    assert target.exists()
    assert result["proposal_rows"] == 1
    assert result["download_url"] == "http://qyun2.hrhome.top:8027/web-v2/expert-pack/download/latest.zip"
    assert target.with_suffix(".zip.sha256").exists()
    assert json.loads(target.with_suffix(".zip.json").read_text(encoding="utf-8"))["proposal_rows"] == 1


def test_sync_latest_bundle_refuses_empty_proposal_pack(monkeypatch, tmp_path: Path) -> None:
    pack = _zip_payload({syncer.PROPOSAL_MEMBER: "proposal_id,strategy_candidate\n"})
    status = {"available_download_url": "/download/latest.zip"}

    def fake_urlopen(req, timeout):
        if req.full_url.endswith("/status"):
            return _FakeResponse(json.dumps(status).encode("utf-8"))
        return _FakeResponse(pack)

    monkeypatch.setattr(syncer.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="no reports/paper_strategy_proposals.csv rows"):
        syncer.sync_latest_bundle(
            status_url="http://qyun2.hrhome.top/status",
            output_dir=tmp_path,
            output_name="quant_lab_latest_bundle.zip",
            timeout=1.0,
        )


def test_sync_latest_bundle_explicitly_skips_nas_only_pack(monkeypatch, tmp_path: Path) -> None:
    status = {
        "storage_location": "nas_only",
        "cloud_zip_present": False,
        "available_download_url": "http://192.168.1.15:8788/download/private-pack",
        "available_pack_name": "quant_lab_expert_pack_2026-07-21.zip",
        "export_date": "2026-07-21",
        "state": "accepted_on_nas",
    }
    requested_urls: list[str] = []

    def fake_urlopen(req, timeout):
        requested_urls.append(req.full_url)
        return _FakeResponse(json.dumps(status).encode("utf-8"))

    monkeypatch.setattr(syncer.urllib.request, "urlopen", fake_urlopen)

    result = syncer.sync_latest_bundle(
        status_url="http://qyun2.hrhome.top:8027/web-v2/expert-pack/status",
        output_dir=tmp_path,
        output_name="quant_lab_latest_bundle.zip",
        timeout=1.0,
    )

    assert result == {
        "status": "skipped",
        "reason": "nas_only_pack_bytes_are_not_proxied_by_qyun2",
        "status_url": "http://qyun2.hrhome.top:8027/web-v2/expert-pack/status",
        "storage_location": "nas_only",
        "cloud_zip_present": False,
        "local_bundle_unchanged": True,
        "pack_name": "quant_lab_expert_pack_2026-07-21.zip",
        "export_date": "2026-07-21",
        "state": "accepted_on_nas",
        "live_order_effect": "none",
    }
    assert requested_urls == ["http://qyun2.hrhome.top:8027/web-v2/expert-pack/status"]
    assert not (tmp_path / "quant_lab_latest_bundle.zip").exists()
