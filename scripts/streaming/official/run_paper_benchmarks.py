from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.calibration import (  # noqa: E402
    build_capacity_calibration_main_configs,
)
from ids_platform.streaming.config.fault_recovery import build_fault_recovery_main_config  # noqa: E402
from ids_platform.streaming.config.model_feature_tradeoff import (  # noqa: E402
    build_model_feature_tradeoff_main_config,
)
from ids_platform.streaming.config.overload_degradation import (  # noqa: E402
    build_overload_degradation_main_config,
)
from ids_platform.streaming.evaluation.matrices.capacity_calibration_matrix import (  # noqa: E402
    run_capacity_calibration_matrix,
)
from ids_platform.streaming.evaluation.matrices.fault_recovery_matrix import (  # noqa: E402
    run_fault_recovery_matrix,
)
from ids_platform.streaming.evaluation.matrices.model_feature_tradeoff_matrix import (  # noqa: E402
    run_model_feature_tradeoff_matrix,
)
from ids_platform.streaming.evaluation.matrices.overload_degradation_matrix import (  # noqa: E402
    run_overload_degradation_matrix,
)


BenchmarkStep = tuple[str, Callable[[], object], Callable[[object], int]]


BENCHMARK_STEPS: tuple[BenchmarkStep, ...] = (
    (
        "model_feature_tradeoff",
        build_model_feature_tradeoff_main_config,
        run_model_feature_tradeoff_matrix,
    ),
    (
        "fault_recovery",
        build_fault_recovery_main_config,
        run_fault_recovery_matrix,
    ),
    (
        "overload_degradation",
        build_overload_degradation_main_config,
        run_overload_degradation_matrix,
    ),
)


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("run_paper_benchmarks.py is config-driven and accepts no CLI arguments.")

    for config in build_capacity_calibration_main_configs(repeats=3, summary_suffix="_3run"):
        print(f"running {config.name}", flush=True)
        exit_code = run_capacity_calibration_matrix(config)
        if exit_code != 0:
            return int(exit_code)

    for step_name, build_config, run_matrix in BENCHMARK_STEPS:
        print(f"running {step_name}", flush=True)
        exit_code = run_matrix(build_config())
        if exit_code != 0:
            return int(exit_code)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
