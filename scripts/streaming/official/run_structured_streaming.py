from __future__ import annotations

"""Debug-only runtime entrypoint. Official runs go through matrix scripts."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.runtime import build_runtime_config  # noqa: E402
from ids_platform.streaming.runtime.structured_streaming_job import run_structured_streaming_job  # noqa: E402


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit("run_structured_streaming.py is config-driven and accepts no CLI arguments.")
    return run_structured_streaming_job(build_runtime_config())


if __name__ == "__main__":
    raise SystemExit(main())
