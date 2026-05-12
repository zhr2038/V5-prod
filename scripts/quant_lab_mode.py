from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.quant_lab_client.mode import (  # noqa: E402
    QuantLabMode,
    quant_lab_mode_needs_fallback_confirmation,
    resolve_mode_path,
    resolve_quant_lab_mode,
    write_quant_lab_mode_override,
)


def _mode_values() -> list[str]:
    return [mode.value for mode in QuantLabMode]


def _is_yes(value: str | None) -> bool:
    return str(value or "").strip().upper() == "YES"


def _confirmation_for_mode(mode: QuantLabMode, args: argparse.Namespace) -> tuple[bool, str | None, str | None]:
    if mode == QuantLabMode.ENFORCE:
        if _is_yes(args.confirm_enforce):
            return True, "cli:confirm_enforce", None
        if _is_yes(os.getenv("V5_QUANT_LAB_CONFIRM_ENFORCE")):
            return True, "env:V5_QUANT_LAB_CONFIRM_ENFORCE", None
        return (
            False,
            None,
            "setting quant-lab mode=enforce requires --confirm-enforce YES "
            "or V5_QUANT_LAB_CONFIRM_ENFORCE=YES",
        )
    if mode in {QuantLabMode.COST_ONLY, QuantLabMode.PERMISSION_ONLY}:
        if _is_yes(args.confirm_gated_mode):
            return True, "cli:confirm_gated_mode", None
        if _is_yes(os.getenv("V5_QUANT_LAB_CONFIRM_GATED_MODE")):
            return True, "env:V5_QUANT_LAB_CONFIRM_GATED_MODE", None
        return (
            False,
            None,
            "setting quant-lab mode=cost_only/permission_only requires --confirm-gated-mode YES "
            "or V5_QUANT_LAB_CONFIRM_GATED_MODE=YES",
        )
    return False, None, None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show or set V5 quant-lab integration mode")
    sub = parser.add_subparsers(dest="command", required=True)

    show = sub.add_parser("show")
    show.add_argument("--config", default="configs/config.yaml")

    set_cmd = sub.add_parser("set")
    set_cmd.add_argument("--mode", required=True, choices=_mode_values())
    set_cmd.add_argument("--reason", required=True)
    set_cmd.add_argument("--updated-by", default=os.getenv("USER") or os.getenv("USERNAME") or "operator")
    set_cmd.add_argument("--path", default="state/quant_lab_mode.json")
    set_cmd.add_argument("--config", default="configs/config.yaml")
    set_cmd.add_argument("--confirm-unsafe-fallback", action="store_true")
    set_cmd.add_argument("--confirm-enforce", default=None, metavar="YES")
    set_cmd.add_argument("--confirm-gated-mode", default=None, metavar="YES")

    args = parser.parse_args(argv)
    if args.command == "show":
        cfg = load_config(args.config)
        resolution = resolve_quant_lab_mode(cfg)
        print(json.dumps(resolution.to_dict(), ensure_ascii=False, indent=2))
        return 0

    cfg = load_config(args.config)
    target_mode = QuantLabMode(args.mode)
    confirmed, confirmation_method, confirmation_error = _confirmation_for_mode(target_mode, args)
    if confirmation_error:
        parser.error(confirmation_error)
    if quant_lab_mode_needs_fallback_confirmation(cfg.quant_lab, target_mode) and not args.confirm_unsafe_fallback:
        parser.error(
            "--confirm-unsafe-fallback is required when setting quant-lab mode to "
            "permission_only/enforce while fail_policy=allow_local_fallback"
        )

    target = write_quant_lab_mode_override(
        mode=args.mode,
        reason=args.reason,
        updated_by=args.updated_by,
        path=args.path,
        confirm_unsafe_fallback=args.confirm_unsafe_fallback,
        confirmed=confirmed,
        confirmation_method=confirmation_method,
    )
    payload = json.loads(resolve_mode_path(target).read_text(encoding="utf-8"))
    payload["path"] = str(resolve_mode_path(target))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
