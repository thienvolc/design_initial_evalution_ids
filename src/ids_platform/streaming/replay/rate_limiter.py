import time

from ids_platform.streaming.replay.config import ReplayRatePlan


def sleep_for_rate_limit(*, rate: ReplayRatePlan, sent_rows: int, started: float) -> None:
    if not rate.is_throttled():
        return

    actual_elapsed = time.perf_counter() - started
    expected_elapsed = rate.expected_elapsed_for_rows(sent_rows)
    sleep_seconds = expected_elapsed - actual_elapsed

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
