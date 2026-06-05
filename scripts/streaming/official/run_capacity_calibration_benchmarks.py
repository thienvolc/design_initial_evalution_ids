from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.calibration import build_capacity_load_threshold_config  # noqa: E402
from ids_platform.streaming.evaluation.matrices.capacity_calibration_matrix import (  # noqa: E402
    run_capacity_calibration_matrix,
)


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit(
            "run_capacity_calibration_benchmarks.py is config-driven and accepts no CLI arguments."
        )

    config = build_capacity_load_threshold_config()
    print(f"running {config.name}", flush=True)
    return int(run_capacity_calibration_matrix(config))


if __name__ == "__main__":
    raise SystemExit(main())
