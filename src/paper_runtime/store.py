from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class PaperRuntimeStore:
    def __init__(self, state_path: str | Path) -> None:
        self.path = Path(state_path)

    def load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {"schema_version": "v5.paper_runtime_state.v1", "trackers": {}}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not isinstance(
            payload.get("trackers"), dict
        ):
            raise ValueError("invalid paper runtime state file")
        return payload

    def save(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
                json.dump(
                    payload,
                    stream,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, self.path)
        except Exception:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass
            raise
