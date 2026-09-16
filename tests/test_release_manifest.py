import hashlib
import os

import pytest

from scripts.verify_release_manifest import verify


def test_startup_rejects_changed_code_and_runtime_permission(tmp_path):
    source = tmp_path / "main.py"
    source.write_text("print('version')")
    manifest = {"schema_version": "review.release.v1", "code_revision": "a"*40,
                "files": {"main.py": hashlib.sha256(source.read_bytes()).hexdigest()},
                "required_runtime_values": {"mode.json": {"mode": "shadow"}}}
    (tmp_path / "mode.json").write_text('{"mode":"shadow"}')
    assert verify(tmp_path, manifest, runtime=True)["verified"]
    (tmp_path / "mode.json").write_text('{"mode":"enforce"}')
    with pytest.raises(ValueError, match="runtime_override_mismatch"):
        verify(tmp_path, manifest, runtime=True)
    source.write_text("print('modified')")
    with pytest.raises(ValueError, match="hash_mismatch"):
        verify(tmp_path, manifest)


def test_verifier_never_accepts_files_outside_release(tmp_path):
    manifest = {"schema_version": "review.release.v1", "code_revision": "a"*40, "files": {"../state/orders.sqlite": "b"*64}}
    with pytest.raises(ValueError, match="outside_release"):
        verify(tmp_path, manifest)


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits are enforced on production")
def test_verifier_rejects_wrong_file_mode(tmp_path):
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/sh\necho ok\n")
    script.chmod(0o644)
    manifest = {
        "schema_version": "review.release.v1",
        "code_revision": "a" * 40,
        "files": {"run.sh": hashlib.sha256(script.read_bytes()).hexdigest()},
        "file_modes": {"run.sh": 0o755},
    }

    with pytest.raises(ValueError, match="mode_mismatch:644!=755"):
        verify(tmp_path, manifest)
