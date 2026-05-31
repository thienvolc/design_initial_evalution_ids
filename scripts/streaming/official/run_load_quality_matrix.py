from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.load_quality import LOAD_QUALITY_CONFIG  # noqa: E402
from ids_platform.streaming.evaluation.matrices.load_quality_matrix import run_load_quality_matrix  # noqa: E402


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("run_load_quality_matrix.py is config-driven and accepts no CLI arguments.")
    return run_load_quality_matrix(LOAD_QUALITY_CONFIG)


if __name__ == "__main__":
    raise SystemExit(main())
