from __future__ import annotations


def apply_trigger(writer, *, available_now: bool, trigger_interval: str):
    if available_now:
        return writer.trigger(availableNow=True)
    normalized_interval = str(trigger_interval).strip()
    if not normalized_interval:
        raise ValueError("trigger_interval must be non-empty when available_now is disabled")
    return writer.trigger(processingTime=normalized_interval)


def safe_tag(raw: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in raw.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "default"
