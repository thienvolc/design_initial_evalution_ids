from __future__ import annotations


def restart_service(name: str, *, project_root, run_command_or_raise_fn) -> None:
    run_command_or_raise_fn(["docker", "compose", "restart", name], cwd=project_root, timeout=180)


def wait_for_kafka_topics_ready(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: int,
    poll_sec: float,
    execution_mode: str,
    consecutive_successes: int,
    normalized_execution_mode_fn,
    docker_wait_for_kafka_topics_ready_fn,
    host_wait_for_kafka_topics_ready_fn,
    run_command_fn,
    sleep_fn,
    project_root,
    time_module,
) -> bool:
    normalized_bootstrap = str(bootstrap_servers or "").strip()
    normalized_topics = [str(topic).strip() for topic in topic_names if str(topic).strip()]
    if not normalized_bootstrap or not normalized_topics:
        return False

    if normalized_execution_mode_fn(execution_mode) == "docker":
        return docker_wait_for_kafka_topics_ready_fn(
            run_command_fn=run_command_fn,
            sleep_fn=sleep_fn,
            project_root=project_root,
            bootstrap_servers=normalized_bootstrap,
            topic_names=normalized_topics,
            timeout_sec=timeout_sec,
            poll_sec=poll_sec,
            consecutive_successes=consecutive_successes,
            time_module=time_module,
        )

    return host_wait_for_kafka_topics_ready_fn(
        bootstrap_servers=normalized_bootstrap,
        topic_names=normalized_topics,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        consecutive_successes=consecutive_successes,
        time_module=time_module,
    )


def describe_kafka_topics_state(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    execution_mode: str,
    normalized_execution_mode_fn,
    docker_describe_kafka_topics_state_fn,
    host_describe_kafka_topics_state_fn,
    run_command_fn,
    project_root,
) -> str:
    normalized_bootstrap = str(bootstrap_servers or "").strip()
    normalized_topics = [str(topic).strip() for topic in topic_names if str(topic).strip()]
    if not normalized_bootstrap:
        return "bootstrap_missing"
    if not normalized_topics:
        return "topics_missing"

    if normalized_execution_mode_fn(execution_mode) == "docker":
        return docker_describe_kafka_topics_state_fn(
            run_command_fn=run_command_fn,
            project_root=project_root,
            bootstrap_servers=normalized_bootstrap,
            topic_names=normalized_topics,
        )

    return host_describe_kafka_topics_state_fn(
        bootstrap_servers=normalized_bootstrap,
        topic_names=normalized_topics,
    )


def wait_for_kafka_bootstrap_ready(
    *,
    bootstrap_servers: str,
    timeout_sec: int,
    poll_sec: float,
    consecutive_successes: int,
    execution_mode: str,
    normalized_execution_mode_fn,
    docker_wait_for_kafka_bootstrap_ready_fn,
    host_wait_for_kafka_bootstrap_ready_fn,
    run_command_fn,
    sleep_fn,
    project_root,
    time_module,
) -> bool:
    normalized_bootstrap = str(bootstrap_servers or "").strip()
    if not normalized_bootstrap:
        return False

    if normalized_execution_mode_fn(execution_mode) == "docker":
        return docker_wait_for_kafka_bootstrap_ready_fn(
            run_command_fn=run_command_fn,
            sleep_fn=sleep_fn,
            project_root=project_root,
            bootstrap_servers=normalized_bootstrap,
            timeout_sec=timeout_sec,
            poll_sec=poll_sec,
            consecutive_successes=consecutive_successes,
            time_module=time_module,
        )

    return host_wait_for_kafka_bootstrap_ready_fn(
        bootstrap_servers=normalized_bootstrap,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        consecutive_successes=consecutive_successes,
        time_module=time_module,
    )
