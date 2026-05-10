from __future__ import annotations

import time
from pathlib import Path


def poll_timeout_seconds(*, end_time: float, max_poll_seconds: float = 1.0, time_module=time) -> float:
    remaining = end_time - time_module.time()
    if remaining <= 0:
        return 0.0
    return max(0.05, min(max_poll_seconds, remaining))


def wait_for_process_startup(
    process,
    *,
    startup_wait_sec: int,
    poll_seconds: float = 0.25,
    ready_log_path: str = "",
    ready_pattern: str = "",
    ready_log_start_offset: int | None = None,
    require_ready_marker: bool = False,
    time_module=time,
) -> bool:
    wait_seconds = max(float(startup_wait_sec), 0.0)
    if wait_seconds <= 0:
        return process.poll() is None

    def _ready_marker_seen() -> bool:
        normalized_path = str(ready_log_path or "").strip()
        normalized_pattern = str(ready_pattern or "").strip()
        if not normalized_path or not normalized_pattern:
            return False
        try:
            start_offset = ready_log_start_offset
            if start_offset is None:
                start_offset = int(getattr(process, "ids_log_start_offset", 0) or 0)
            content = Path(normalized_path).read_bytes()
            if start_offset > 0:
                content = content[max(int(start_offset), 0):]
            return normalized_pattern.encode("utf-8") in content
        except Exception:
            return False

    end_time = time_module.time() + wait_seconds
    while time_module.time() < end_time:
        if process.poll() is not None:
            return False
        if _ready_marker_seen():
            return True
        time_module.sleep(poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds, time_module=time_module))
    if require_ready_marker:
        return process.poll() is None and _ready_marker_seen()
    return process.poll() is None


def describe_process_startup_state(
    process,
    *,
    log_path: str = "",
    ready_pattern: str = "",
    max_lines: int = 8,
) -> str:
    parts: list[str] = []
    try:
        exit_code = process.poll()
    except Exception:
        exit_code = None
    parts.append(f"process_alive={exit_code is None}")
    if exit_code is not None:
        parts.append(f"exit_code={exit_code}")

    normalized_pattern = str(ready_pattern or "").strip()
    normalized_path = str(log_path or "").strip()
    if normalized_pattern and normalized_path:
        try:
            start_offset = int(getattr(process, "ids_log_start_offset", 0) or 0)
            content = Path(normalized_path).read_text(encoding="utf-8", errors="replace")
            if start_offset > 0:
                content = content[start_offset:]
            parts.append(f"ready_marker_seen={normalized_pattern in content}")
            tail_lines = [line.strip() for line in content.splitlines() if line.strip()]
            if tail_lines:
                rendered_tail = " | ".join(tail_lines[-max(int(max_lines), 1):])
                parts.append(f"log_tail={rendered_tail}")
        except Exception:
            parts.append("log_tail_unavailable=true")
    elif normalized_path:
        try:
            content = Path(normalized_path).read_text(encoding="utf-8", errors="replace")
            tail_lines = [line.strip() for line in content.splitlines() if line.strip()]
            if tail_lines:
                rendered_tail = " | ".join(tail_lines[-max(int(max_lines), 1):])
                parts.append(f"log_tail={rendered_tail}")
        except Exception:
            parts.append("log_tail_unavailable=true")
    return "; ".join(parts)


def wait_for_log_patterns(
    *,
    process,
    log_path: str,
    patterns: list[str],
    timeout_sec: float,
    poll_seconds: float = 0.25,
    log_start_offset: int | None = None,
    time_module=time,
) -> str:
    normalized_path = str(log_path or "").strip()
    normalized_patterns = [str(pattern).strip() for pattern in patterns if str(pattern).strip()]
    if not normalized_path or not normalized_patterns:
        return ""

    path = Path(normalized_path)
    start_offset = log_start_offset
    if start_offset is None:
        start_offset = int(getattr(process, "ids_log_start_offset", 0) or 0)

    encoded_patterns = [(pattern, pattern.encode("utf-8")) for pattern in normalized_patterns]
    end_time = time_module.time() + max(float(timeout_sec), 0.0)
    while time_module.time() < end_time:
        if process is not None and process.poll() is not None:
            return ""
        try:
            content = path.read_bytes()
        except OSError:
            content = b""
        if start_offset > 0:
            content = content[max(int(start_offset), 0):]
        for pattern_text, pattern_bytes in encoded_patterns:
            if pattern_bytes in content:
                return pattern_text
        time_module.sleep(poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds, time_module=time_module))
    return ""


def wait_for_log_quiescence(
    *,
    process,
    log_path: str,
    idle_sec: float,
    timeout_sec: float,
    poll_seconds: float = 0.5,
    time_module=time,
) -> bool:
    normalized_path = str(log_path or "").strip()
    if not normalized_path:
        return False

    path = Path(normalized_path)
    end_time = time_module.time() + max(float(timeout_sec), 0.0)
    last_size = None
    last_change_at = time_module.time()

    while time_module.time() < end_time:
        if process is not None and process.poll() is not None:
            return False
        try:
            current_size = path.stat().st_size
        except OSError:
            current_size = -1
        if last_size is None or current_size != last_size:
            last_size = current_size
            last_change_at = time_module.time()
        elif (time_module.time() - last_change_at) >= max(float(idle_sec), 0.0):
            return True
        time_module.sleep(poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds, time_module=time_module))
    return False
