from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.layer_c import LAYER_C_CONFIG  # noqa: E402
from ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix import run  # noqa: E402


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("run_layer_c_matrix.py is config-driven and accepts no CLI arguments.")
    return run(LAYER_C_CONFIG)


if __name__ == "__main__":
    raise SystemExit(main())
