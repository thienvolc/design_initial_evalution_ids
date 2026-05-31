from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.command import render_structured_stream_command


class StreamingRuntimeCommandTests(unittest.TestCase):
    def test_render_structured_stream_command_uses_config_entrypoint_only(self) -> None:
        self.assertEqual(
            render_structured_stream_command(python_exe="python"),
            ["python", "scripts/streaming/official/run_structured_streaming.py"],
        )


if __name__ == "__main__":
    unittest.main()
