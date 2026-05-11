from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Hashable, Optional, Tuple


@dataclass
class TTLCache:
    ttl_seconds: int = 60
    _items: Dict[Hashable, Tuple[float, Any]] = field(default_factory=dict)

    def get(self, key: Hashable) -> Optional[Any]:
        if self.ttl_seconds <= 0:
            return None
        item = self._items.get(key)
        if item is None:
            return None
        expires_at, value = item
        if expires_at < time.time():
            self._items.pop(key, None)
            return None
        return value

    def set(self, key: Hashable, value: Any) -> None:
        if self.ttl_seconds <= 0:
            return
        self._items[key] = (time.time() + float(self.ttl_seconds), value)

    def clear(self) -> None:
        self._items.clear()
