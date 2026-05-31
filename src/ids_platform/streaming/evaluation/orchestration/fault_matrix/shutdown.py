from __future__ import annotations


def stop_stream_process(
    process,
    *,
    wait_for_stream_shutdown_fn,
    cleanup_stream_processes_fn,
    wait_for_process_exit_fn,
    stop_background_process_fn,
) -> None:
    if process is None:
        return

    execution_mode = str(getattr(process, "ids_execution_mode", "") or "").strip().lower()
    run_tag = str(getattr(process, "ids_run_tag", "") or "").strip()

    stop_background_process_fn(process)
    if wait_for_process_exit_fn(process, timeout_sec=15):
        return

    cleanup_stream_processes_fn(run_tag=run_tag, execution_mode=execution_mode or "host")
    wait_for_stream_shutdown_fn(
        run_tag=run_tag,
        execution_mode=execution_mode or "host",
        timeout_sec=45,
        poll_sec=0.5,
        settle_sec=2.0,
    )
