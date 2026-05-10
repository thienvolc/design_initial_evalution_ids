from __future__ import annotations


def stop_stream_process(
    process,
    *,
    write_shutdown_request_fn,
    time_module,
    wait_for_stream_shutdown_fn,
    cleanup_stream_processes_fn,
    wait_for_process_exit_fn,
    stop_background_process_fn,
) -> None:
    if process is None:
        return

    execution_mode = str(getattr(process, "ids_execution_mode", "") or "").strip().lower()
    run_tag = str(getattr(process, "ids_run_tag", "") or "").strip()

    if execution_mode == "docker" and run_tag:
        write_shutdown_request_fn(run_tag)
        graceful_deadline = time_module.time() + 45.0
        while time_module.time() < graceful_deadline:
            if process.poll() is not None:
                stop_background_process_fn(process)
                return
            if wait_for_stream_shutdown_fn(
                run_tag=run_tag,
                execution_mode="docker",
                timeout_sec=1,
                poll_sec=0.25,
                settle_sec=0.0,
            ):
                break
            time_module.sleep(0.25)
        else:
            cleanup_stream_processes_fn(run_tag=run_tag, execution_mode="docker")
            wait_for_stream_shutdown_fn(
                run_tag=run_tag,
                execution_mode="docker",
                timeout_sec=45,
                poll_sec=0.5,
                settle_sec=2.0,
            )

        if wait_for_process_exit_fn(process, timeout_sec=45):
            stop_background_process_fn(process)
            return

    stop_background_process_fn(process)
