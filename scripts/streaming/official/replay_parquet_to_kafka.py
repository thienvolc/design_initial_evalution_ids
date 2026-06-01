from __future__ import annotations

"""Debug-only replay entrypoint. Official runs go through matrix scripts."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.capacity import CAPACITY_CONFIG  # noqa: E402
from ids_platform.streaming.replay.runner import run_replay_job  # noqa: E402


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("replay_parquet_to_kafka.py is config-driven and accepts no CLI arguments.")
    return run_replay_job(CAPACITY_CONFIG.runs[0].benchmark.replay)


if __name__ == "__main__":
    raise SystemExit(main())
