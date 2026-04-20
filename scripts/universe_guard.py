#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path


@dataclass(frozen=True)
class GuardPaths:
    workspace: Path
    reports_dir: Path
    configs_dir: Path
    universe_path: Path
    blacklist_path: Path


def build_paths(workspace: Path | None = None) -> GuardPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    cfg = load_runtime_config(project_root=root)
    config_path = (root / "configs" / "live_prod.yaml").resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    universe_cfg = cfg.get("universe")
    if universe_cfg is not None and not isinstance(universe_cfg, dict):
        raise ValueError(f"runtime config universe section is invalid: {config_path}")
    universe_cfg = universe_cfg or {}
    universe_path = Path(
        resolve_runtime_path(
            universe_cfg.get("cache_path"),
            default="reports/universe_cache.json",
            project_root=root,
        )
    ).resolve()
    blacklist_path = Path(
        resolve_runtime_path(
            universe_cfg.get("blacklist_path"),
            default="configs/blacklist.json",
            project_root=root,
        )
    ).resolve()
    reports_dir = universe_path.parent.resolve()
    configs_dir = blacklist_path.parent.resolve()
    return GuardPaths(
        workspace=root,
        reports_dir=reports_dir,
        configs_dir=configs_dir,
        universe_path=universe_path,
        blacklist_path=blacklist_path,
    )


# 高风险币种特征
SUSPICIOUS_PATTERNS = [
    "PEPE",
    "DOGE",
    "SHIB",
    "FLOKI",
    "BONK",
    "WIF",
    "BOME",
    "PROMPT",
    "SPACE",
    "KITE",
    "WLFI",
    "MERL",
    "J",
    "AGLD",
    "USDT/USDT",
    "USDG",
    "XAUT",
]

# 必须有良好的历史
MIN_LISTING_DAYS = 90


def _resolve_path(value: str | Path | None, *, default: Path) -> Path:
    if value is None:
        return default
    path = Path(value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def check_universe(
    universe_path: str | Path | None = None,
    blacklist_path: str | Path | None = None,
) -> bool:
    """检查币池，标记可疑币种。"""
    defaults = build_paths()
    path = _resolve_path(universe_path, default=defaults.universe_path)
    resolved_blacklist_path = _resolve_path(blacklist_path, default=defaults.blacklist_path)
    if not path.exists():
        print(f"未找到币池文件: {path}")
        return False

    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)

    symbols = data.get("symbols", [])
    suspicious: list[tuple[str, str]] = []

    for sym in symbols:
        sym_upper = str(sym).upper()
        for pattern in SUSPICIOUS_PATTERNS:
            if pattern in sym_upper:
                suspicious.append((str(sym), pattern))
                break

    print("=" * 60)
    print("币池守卫检查报告")
    print("=" * 60)
    print(f"币池总数: {len(symbols)}")
    print(f"可疑币种: {len(suspicious)}")
    print()

    if suspicious:
        print("发现可疑币种:")
        for sym, reason in suspicious:
            print(f"  - {sym} (匹配: {reason})")
        print()
        print("建议操作:")
        print(f"  1. 将这些币加入 {resolved_blacklist_path}")
        print("  2. 重新生成币池")
        return False

    print("币池检查通过，无可疑币种")
    return True


def auto_blacklist_suspicious(
    *,
    universe_path: str | Path | None = None,
    blacklist_path: str | Path | None = None,
) -> list[str]:
    """自动将可疑币加入黑名单。"""
    defaults = build_paths()
    resolved_universe_path = _resolve_path(universe_path, default=defaults.universe_path)
    resolved_blacklist_path = _resolve_path(blacklist_path, default=defaults.blacklist_path)

    if not resolved_universe_path.exists():
        return []

    with resolved_universe_path.open(encoding="utf-8") as handle:
        data = json.load(handle)

    symbols = data.get("symbols", [])

    if resolved_blacklist_path.exists():
        with resolved_blacklist_path.open(encoding="utf-8") as handle:
            blacklist = json.load(handle)
    else:
        blacklist = {"symbols": []}

    existing = {str(symbol) for symbol in blacklist.get("symbols", [])}
    added: list[str] = []

    for sym in symbols:
        sym_text = str(sym)
        sym_upper = sym_text.upper()
        for pattern in SUSPICIOUS_PATTERNS:
            if pattern in sym_upper:
                if sym_text not in existing:
                    blacklist.setdefault("symbols", []).append(sym_text)
                    added.append(sym_text)
                    existing.add(sym_text)
                break

    if added:
        resolved_blacklist_path.parent.mkdir(parents=True, exist_ok=True)
        with resolved_blacklist_path.open("w", encoding="utf-8") as handle:
            json.dump(blacklist, handle, indent=2, ensure_ascii=False)
        print(f"已自动将 {len(added)} 个可疑币加入黑名单:")
        for symbol in added:
            print(f"  - {symbol}")

    return added


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-blacklist", action="store_true", help="自动加入黑名单")
    parser.add_argument("--universe-path", help="币池文件路径")
    parser.add_argument("--blacklist-path", help="黑名单文件路径")
    args = parser.parse_args()

    if args.auto_blacklist:
        auto_blacklist_suspicious(
            universe_path=args.universe_path,
            blacklist_path=args.blacklist_path,
        )
        return

    ok = check_universe(universe_path=args.universe_path, blacklist_path=args.blacklist_path)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
