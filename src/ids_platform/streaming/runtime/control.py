from __future__ import annotations

from pathlib import Path

from ids_platform.common.paths import PROJECT_ROOT


def shutdown_request_path(run_tag: str) -> Path:
    safe_run_tag = str(run_tag or "").strip() or "unknown_run"
    return PROJECT_ROOT / "artifacts" / "streaming" / "control" / f"{safe_run_tag}.stop"


def clear_shutdown_request(run_tag: str) -> None:
    path = shutdown_request_path(run_tag)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def write_shutdown_request(run_tag: str) -> Path:
    path = shutdown_request_path(run_tag)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("stop\n", encoding="utf-8")
    return path
