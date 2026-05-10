from __future__ import annotations


def docker_wait_for_kafka_topics_ready(
    *,
    run_command_fn,
    sleep_fn,
    project_root,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    consecutive_successes: int = 2,
    time_module,
) -> bool:
    probe_code = (
        "import sys;"
        "from confluent_kafka.admin import AdminClient;"
        "bootstrap=sys.argv[1];"
        "topics=[topic for topic in sys.argv[2:] if topic.strip()];"
        "admin=AdminClient({'bootstrap.servers': bootstrap});"
        "md=admin.list_topics(timeout=10.0);"
        "all_ready=True;"
        "for topic_name in topics:"
        "    topic_md=md.topics.get(topic_name);"
        "    if topic_md is None:"
        "        all_ready=False;"
        "        break;"
        "    partitions=getattr(topic_md,'partitions',{}) or {};"
        "    if not partitions:"
        "        all_ready=False;"
        "        break;"
        "    for partition_md in partitions.values():"
        "        leader=getattr(partition_md,'leader',-1);"
        "        if leader is None or int(leader) < 0:"
        "            all_ready=False;"
        "            break;"
        "    if not all_ready:"
        "        break;"
        "sys.exit(0 if all_ready else 1)"
    )
    deadline = time_module.time() + max(int(timeout_sec), 1)
    success_count = 0
    required_successes = max(int(consecutive_successes), 1)
    while time_module.time() < deadline:
        result = run_command_fn(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "ids-dev",
                "python",
                "-c",
                probe_code,
                bootstrap_servers,
                *topic_names,
            ],
            cwd=project_root,
            timeout=20,
        )
        if result.returncode == 0:
            success_count += 1
            if success_count >= required_successes:
                return True
        else:
            success_count = 0
        sleep_fn(max(float(poll_sec), 0.25))
    return False


def host_wait_for_kafka_topics_ready(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    consecutive_successes: int = 2,
    time_module,
) -> bool:
    try:
        from confluent_kafka.admin import AdminClient
    except Exception:
        return False

    deadline = time_module.time() + max(int(timeout_sec), 1)
    success_count = 0
    required_successes = max(int(consecutive_successes), 1)
    while time_module.time() < deadline:
        try:
            admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10.0)
            all_ready = True
            for topic_name in topic_names:
                topic_metadata = metadata.topics.get(topic_name)
                if topic_metadata is None:
                    all_ready = False
                    break
                partitions = getattr(topic_metadata, "partitions", {}) or {}
                if not partitions:
                    all_ready = False
                    break
                for partition_metadata in partitions.values():
                    leader = getattr(partition_metadata, "leader", -1)
                    if leader is None or int(leader) < 0:
                        all_ready = False
                        break
                if not all_ready:
                    break
            if all_ready:
                success_count += 1
                if success_count >= required_successes:
                    return True
            else:
                success_count = 0
        except Exception:
            success_count = 0
        time_module.sleep(max(float(poll_sec), 0.25))
    return False


def docker_describe_kafka_topics_state(
    *,
    run_command_fn,
    project_root,
    bootstrap_servers: str,
    topic_names: list[str],
) -> str:
    probe_code = "\n".join(
        [
            "import sys",
            "from confluent_kafka.admin import AdminClient",
            "bootstrap = sys.argv[1]",
            "topics = [topic for topic in sys.argv[2:] if topic.strip()]",
            "try:",
            "    admin = AdminClient({'bootstrap.servers': bootstrap})",
            "    md = admin.list_topics(timeout=10.0)",
            "except Exception as exc:",
            "    print(f'admin_error:{type(exc).__name__}:{exc}')",
            "    raise SystemExit(0)",
            "brokers = getattr(md, 'brokers', {}) or {}",
            "if not brokers:",
            "    print('brokers_missing')",
            "    raise SystemExit(0)",
            "problems = []",
            "for topic_name in topics:",
            "    topic_md = md.topics.get(topic_name)",
            "    if topic_md is None:",
            "        problems.append(f'topic_missing:{topic_name}')",
            "        continue",
            "    partitions = getattr(topic_md, 'partitions', {}) or {}",
            "    if not partitions:",
            "        problems.append(f'partitions_missing:{topic_name}')",
            "        continue",
            "    for partition_id, partition_md in partitions.items():",
            "        leader = getattr(partition_md, 'leader', -1)",
            "        if leader is None or int(leader) < 0:",
            "            problems.append(f'leader_unavailable:{topic_name}:{partition_id}')",
            "if problems:",
            "    print(';'.join(problems))",
            "else:",
            "    print('ready')",
        ]
    )
    result = run_command_fn(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "ids-dev",
            "python",
            "-c",
            probe_code,
            bootstrap_servers,
            *topic_names,
        ],
        cwd=project_root,
        timeout=20,
    )
    output = (result.stdout or "").strip()
    if output:
        return output.splitlines()[-1].strip()
    stderr = (result.stderr or "").strip()
    return stderr.splitlines()[-1].strip() if stderr else "probe_failed"


def host_describe_kafka_topics_state(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
) -> str:
    try:
        from confluent_kafka.admin import AdminClient
    except Exception as exc:
        return f"admin_import_error:{type(exc).__name__}"

    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10.0)
    except Exception as exc:
        return f"admin_error:{type(exc).__name__}:{exc}"

    brokers = getattr(metadata, "brokers", {}) or {}
    if not brokers:
        return "brokers_missing"

    problems: list[str] = []
    for topic_name in topic_names:
        topic_metadata = metadata.topics.get(topic_name)
        if topic_metadata is None:
            problems.append(f"topic_missing:{topic_name}")
            continue
        partitions = getattr(topic_metadata, "partitions", {}) or {}
        if not partitions:
            problems.append(f"partitions_missing:{topic_name}")
            continue
        for partition_id, partition_metadata in partitions.items():
            leader = getattr(partition_metadata, "leader", -1)
            if leader is None or int(leader) < 0:
                problems.append(f"leader_unavailable:{topic_name}:{partition_id}")
    return ";".join(problems) if problems else "ready"


def docker_wait_for_kafka_bootstrap_ready(
    *,
    run_command_fn,
    sleep_fn,
    project_root,
    bootstrap_servers: str,
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    consecutive_successes: int = 2,
    time_module,
) -> bool:
    probe_code = (
        "import sys,time;"
        "from confluent_kafka.admin import AdminClient;"
        "bootstrap=sys.argv[1];"
        "admin=AdminClient({'bootstrap.servers': bootstrap});"
        "md=admin.list_topics(timeout=10.0);"
        "brokers=getattr(md,'brokers',{}) or {};"
        "sys.exit(0 if brokers else 1)"
    )
    deadline = time_module.time() + max(int(timeout_sec), 1)
    success_count = 0
    required_successes = max(int(consecutive_successes), 1)
    while time_module.time() < deadline:
        result = run_command_fn(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "ids-dev",
                "python",
                "-c",
                probe_code,
                bootstrap_servers,
            ],
            cwd=project_root,
            timeout=20,
        )
        if result.returncode == 0:
            success_count += 1
            if success_count >= required_successes:
                return True
        else:
            success_count = 0
        sleep_fn(max(float(poll_sec), 0.25))
    return False


def host_wait_for_kafka_bootstrap_ready(
    *,
    bootstrap_servers: str,
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    consecutive_successes: int = 2,
    time_module,
) -> bool:
    try:
        from confluent_kafka.admin import AdminClient
    except Exception:
        return False

    deadline = time_module.time() + max(int(timeout_sec), 1)
    success_count = 0
    required_successes = max(int(consecutive_successes), 1)
    while time_module.time() < deadline:
        try:
            admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10.0)
            brokers = getattr(metadata, "brokers", {}) or {}
            if brokers:
                success_count += 1
                if success_count >= required_successes:
                    return True
            else:
                success_count = 0
        except Exception:
            success_count = 0
        time_module.sleep(max(float(poll_sec), 0.25))
    return False
