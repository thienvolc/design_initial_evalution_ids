from __future__ import annotations


def render_structured_stream_command(*, python_exe: str) -> list[str]:
    return [
        python_exe,
        "scripts/streaming/official/run_structured_streaming.py",
    ]
