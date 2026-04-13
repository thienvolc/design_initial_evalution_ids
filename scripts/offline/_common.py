from __future__ import annotations

import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
SRC_DIR = PROJECT_ROOT / "src"


def ensure_src_on_path() -> None:
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))


def project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def print_section(title: str, *, width: int = 80) -> None:
    line = "=" * width
    print(f"\n{line}")
    print(title)
    print(line)


ensure_src_on_path()
