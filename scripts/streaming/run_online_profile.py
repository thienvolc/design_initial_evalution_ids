from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.orchestration.profile_service import (  # noqa: E402
    ProfileExecutionOptions,
    ProfileListFilters,
    list_profiles,
    run_profile,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an online evaluation profile from YAML config")
    parser.add_argument(
        "--profile-config",
        type=str,
        default="experiments/streaming/profiles/local_profiles.yaml",
        help="Path to profile config YAML (default: local profiles for low-resource machine)",
    )
    parser.add_argument("--profile", type=str, default="", help="Profile name to run")
    parser.add_argument("--list", action="store_true", help="List available profiles")
    parser.add_argument("--list-official-only", action="store_true", help="List only official profiles")
    parser.add_argument("--list-ablation-only", action="store_true", help="List only ablation profiles")
    parser.add_argument("--list-light-only", action="store_true", help="List only light profiles")
    parser.add_argument("--list-heavy-only", action="store_true", help="List only heavy profiles")
    parser.add_argument(
        "--allow-heavy",
        action="store_true",
        help="Allow executing heavy profiles (dry-run does not require this flag)",
    )
    parser.add_argument(
        "--skip-gates",
        action="store_true",
        help="Skip pass/fail gate evaluation even if profile defines meta.pass_fail",
    )
    parser.add_argument(
        "--gate-only",
        action="store_true",
        help="Skip execution and evaluate pass/fail using existing summary_csv artifact",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print resolved command without executing")
    parser.add_argument("--python-exe", type=str, default=sys.executable, help="Python executable for host runtime")
    return parser.parse_args()


def _print_gate_lines(lines: list[str]) -> None:
    for line in lines:
        print(line, flush=True)


def main() -> int:
    args = parse_args()

    if args.list_light_only and args.list_heavy_only:
        raise ValueError("Use only one of --list-light-only or --list-heavy-only")
    if args.list_official_only and args.list_ablation_only:
        raise ValueError("Use only one of --list-official-only or --list-ablation-only")
    if args.skip_gates and args.gate_only:
        raise ValueError("--skip-gates cannot be combined with --gate-only")

    if args.list:
        filters = ProfileListFilters(
            official_only=args.list_official_only,
            ablation_only=args.list_ablation_only,
            light_only=args.list_light_only,
            heavy_only=args.list_heavy_only,
        )
        for item in list_profiles(args.profile_config, filters):
            tags = f"[{item.mode}/{item.resource_class}]"
            print(f"{item.name} {tags}{' - ' + item.description if item.description else ''}")
        return 0

    if not args.profile.strip():
        raise ValueError("--profile is required unless --list is used")

    execution_result = run_profile(
        ProfileExecutionOptions(
            profile_config_path=args.profile_config,
            profile_name=args.profile,
            python_executable=args.python_exe,
            allow_heavy=args.allow_heavy,
            skip_gates=args.skip_gates,
            gate_only=args.gate_only,
            dry_run=args.dry_run,
        )
    )

    if execution_result.blocked_reason:
        print(execution_result.blocked_reason, flush=True)
        return execution_result.exit_code

    print("Resolved command:")
    print(" ".join(execution_result.command), flush=True)

    if args.gate_only:
        print("Gate-only mode: skipped execution, evaluating existing summary CSV.", flush=True)
        _print_gate_lines(execution_result.gate_lines)
        return execution_result.exit_code

    if args.dry_run:
        return 0

    if args.skip_gates:
        print("Gate evaluation skipped by --skip-gates.", flush=True)
        return 0

    _print_gate_lines(execution_result.gate_lines)
    return execution_result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

