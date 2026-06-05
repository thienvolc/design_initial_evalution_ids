from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.overload_degradation import (  # noqa: E402
    build_overload_degradation_smoke_config,
)
from ids_platform.streaming.evaluation.matrices.overload_degradation_matrix import (  # noqa: E402
    run_overload_degradation_matrix,
)


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("run_overload_degradation.py is config-driven and accepts no CLI arguments.")
    return run_overload_degradation_matrix(build_overload_degradation_smoke_config())


if __name__ == "__main__":
    raise SystemExit(main())
