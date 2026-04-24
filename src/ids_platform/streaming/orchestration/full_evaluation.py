from __future__ import annotations

from dataclasses import dataclass

from ids_platform.common.subprocess import run_command_or_raise


def compose_fault_scenarios(repeats: int) -> list[str]:
    scenarios: list[str] = []
    for _ in range(max(repeats, 1)):
        scenarios.extend(["kafka_restart", "spark_process_restart", "producer_restart", "network_slowdown"])
    return scenarios


def append_positive_arg(command: list[str], flag: str, value: int | float) -> None:
    if float(value) > 0:
        command.extend([flag, str(value)])


def append_nonempty_arg(command: list[str], flag: str, value: str) -> None:
    text = str(value).strip()
    if text:
        command.extend([flag, text])


def build_layer_a_command(args, python_executable: str) -> list[str]:
    command = [
        python_executable,
        "scripts/streaming/run_layer_a_matrix.py",
        "--config",
        args.config,
        "--repeats",
        str(max(args.layer_a_repeats, 1)),
        "--max-rows",
        str(max(args.layer_a_max_rows, 1)),
        "--batch-size",
        str(max(args.layer_a_batch_size, 1)),
        "--summary-csv",
        args.layer_a_summary_csv,
    ]
    append_positive_arg(command, "--metrics-timeout-sec", int(args.layer_a_metrics_timeout_sec))
    append_positive_arg(command, "--metrics-idle-sec", int(args.layer_a_metrics_idle_sec))
    append_positive_arg(command, "--warmup-rows", int(args.layer_a_warmup_rows))
    append_positive_arg(command, "--warmup-rows-per-sec", float(args.layer_a_warmup_rows_per_sec))
    append_nonempty_arg(command, "--warmup-rate-schedule", args.layer_a_warmup_rate_schedule)
    return command


def build_layer_b_command(args, python_executable: str) -> list[str]:
    command = [
        python_executable,
        "scripts/streaming/run_layer_b_matrix.py",
        "--config",
        args.config,
        "--repeats",
        str(max(args.layer_b_repeats, 1)),
        "--max-rows",
        str(max(args.layer_b_max_rows, 1)),
        "--batch-size",
        str(max(args.layer_b_batch_size, 1)),
        "--summary-csv",
        args.layer_b_summary_csv,
    ]
    append_positive_arg(command, "--metrics-timeout-sec", int(args.layer_b_metrics_timeout_sec))
    append_positive_arg(command, "--metrics-idle-sec", int(args.layer_b_metrics_idle_sec))
    append_positive_arg(command, "--warmup-rows", int(args.layer_b_warmup_rows))
    append_positive_arg(command, "--warmup-rows-per-sec", float(args.layer_b_warmup_rows_per_sec))
    append_nonempty_arg(command, "--warmup-rate-schedule", args.layer_b_warmup_rate_schedule)
    return command


def build_layer_c_command(args, python_executable: str) -> list[str]:
    return [
        python_executable,
        "scripts/streaming/run_layer_c_matrix.py",
        "--config",
        args.config,
        "--execution-mode",
        "docker",
        "--scenarios",
        *compose_fault_scenarios(args.layer_c_repeats),
        "--warmup-rows",
        str(max(args.layer_c_warmup_rows, 1)),
        "--post-fault-rows",
        str(max(args.layer_c_post_fault_rows, 1)),
        "--batch-size",
        str(max(args.layer_c_batch_size, 1)),
        "--summary-csv",
        args.layer_c_summary_csv,
    ]


def build_watermark_command(args, python_executable: str) -> list[str]:
    command = [
        python_executable,
        "scripts/streaming/run_watermark_matrix.py",
        "--config",
        args.config,
        "--drop-late-events",
        "--summary-csv",
        args.watermark_summary_csv,
    ]
    append_positive_arg(command, "--metrics-timeout-sec", int(args.watermark_metrics_timeout_sec))
    append_positive_arg(command, "--metrics-idle-sec", int(args.watermark_metrics_idle_sec))
    append_positive_arg(command, "--warmup-rows", int(args.watermark_warmup_rows))
    append_positive_arg(command, "--warmup-rows-per-sec", float(args.watermark_warmup_rows_per_sec))
    append_nonempty_arg(command, "--warmup-rate-schedule", args.watermark_warmup_rate_schedule)
    return command


def build_load_quality_command(args, python_executable: str) -> list[str]:
    command = [
        python_executable,
        "scripts/streaming/run_load_quality_matrix.py",
        "--config",
        args.config,
        "--summary-csv",
        args.load_quality_summary_csv,
    ]
    append_positive_arg(command, "--metrics-timeout-sec", int(args.load_quality_metrics_timeout_sec))
    append_positive_arg(command, "--metrics-idle-sec", int(args.load_quality_metrics_idle_sec))
    append_positive_arg(command, "--warmup-rows", int(args.load_quality_warmup_rows))
    append_positive_arg(command, "--warmup-rows-per-sec", float(args.load_quality_warmup_rows_per_sec))
    append_nonempty_arg(command, "--warmup-rate-schedule", args.load_quality_warmup_rate_schedule)
    return command


def build_report_command(args, python_executable: str) -> list[str]:
    return [
        python_executable,
        "scripts/streaming/build_online_report.py",
        "--layer-a",
        args.layer_a_summary_csv,
        "--layer-b",
        args.layer_b_summary_csv,
        "--layer-c",
        args.layer_c_summary_csv,
        "--watermark-summary",
        args.watermark_summary_csv if args.include_watermark_matrix else "",
        "--load-quality-summary",
        args.load_quality_summary_csv if args.include_load_quality else "",
        "--out-md",
        args.report_md,
        "--out-json",
        args.report_json,
    ]


@dataclass(frozen=True)
class FullEvaluationOptions:
    config: str = "configs/streaming/online.yaml"
    layer_a_repeats: int = 3
    layer_a_max_rows: int = 1000
    layer_a_batch_size: int = 500
    layer_a_warmup_rows: int = 0
    layer_a_warmup_rows_per_sec: float = 0.0
    layer_a_warmup_rate_schedule: str = ""
    layer_a_metrics_timeout_sec: int = 60
    layer_a_metrics_idle_sec: int = 5
    layer_a_summary_csv: str = "artifacts/streaming/online/layer_a_summary_final.csv"
    layer_b_repeats: int = 3
    layer_b_max_rows: int = 400
    layer_b_batch_size: int = 200
    layer_b_warmup_rows: int = 0
    layer_b_warmup_rows_per_sec: float = 0.0
    layer_b_warmup_rate_schedule: str = ""
    layer_b_metrics_timeout_sec: int = 45
    layer_b_metrics_idle_sec: int = 5
    layer_b_summary_csv: str = "artifacts/streaming/online/layer_b_summary_final.csv"
    layer_c_repeats: int = 3
    layer_c_warmup_rows: int = 600
    layer_c_post_fault_rows: int = 600
    layer_c_batch_size: int = 300
    layer_c_summary_csv: str = "artifacts/streaming/online/layer_c_summary_final.csv"
    include_watermark_matrix: bool = False
    include_load_quality: bool = False
    watermark_summary_csv: str = "artifacts/streaming/online/watermark_summary_final.csv"
    watermark_warmup_rows: int = 0
    watermark_warmup_rows_per_sec: float = 0.0
    watermark_warmup_rate_schedule: str = ""
    watermark_metrics_timeout_sec: int = 90
    watermark_metrics_idle_sec: int = 15
    load_quality_summary_csv: str = "artifacts/streaming/online/load_quality_summary_final.csv"
    load_quality_warmup_rows: int = 0
    load_quality_warmup_rows_per_sec: float = 0.0
    load_quality_warmup_rate_schedule: str = ""
    load_quality_metrics_timeout_sec: int = 90
    load_quality_metrics_idle_sec: int = 8
    report_md: str = "artifacts/streaming/online/online_evaluation_report_final.md"
    report_json: str = "artifacts/streaming/online/online_evaluation_report_final.json"


def run_full_evaluation(options: FullEvaluationOptions, *, python_executable: str) -> int:
    layer_a_command = build_layer_a_command(options, python_executable)
    layer_b_command = build_layer_b_command(options, python_executable)
    layer_c_command = build_layer_c_command(options, python_executable)
    report_command = build_report_command(options, python_executable)
    watermark_command = build_watermark_command(options, python_executable)
    load_quality_command = build_load_quality_command(options, python_executable)

    total_steps = 4 + int(bool(options.include_watermark_matrix)) + int(bool(options.include_load_quality))
    step = 1

    print(f"[{step}/{total_steps}] Running Layer A matrix", flush=True)
    run_command_or_raise(layer_a_command)
    step += 1

    print(f"[{step}/{total_steps}] Running Layer B matrix", flush=True)
    run_command_or_raise(layer_b_command)
    step += 1

    print(f"[{step}/{total_steps}] Running Layer C matrix", flush=True)
    run_command_or_raise(layer_c_command)
    step += 1

    if options.include_watermark_matrix:
        print(f"[{step}/{total_steps}] Running watermark matrix", flush=True)
        run_command_or_raise(watermark_command)
        step += 1

    if options.include_load_quality:
        print(f"[{step}/{total_steps}] Running load quality matrix", flush=True)
        run_command_or_raise(load_quality_command)
        step += 1

    print(f"[{step}/{total_steps}] Building consolidated report", flush=True)
    run_command_or_raise(report_command)

    print("Completed one-shot online evaluation.", flush=True)
    print(f"Layer A summary: {options.layer_a_summary_csv}", flush=True)
    print(f"Layer B summary: {options.layer_b_summary_csv}", flush=True)
    print(f"Layer C summary: {options.layer_c_summary_csv}", flush=True)
    if options.include_watermark_matrix:
        print(f"Watermark summary: {options.watermark_summary_csv}", flush=True)
    if options.include_load_quality:
        print(f"Load quality summary: {options.load_quality_summary_csv}", flush=True)
    print(f"Report markdown: {options.report_md}", flush=True)
    print(f"Report json: {options.report_json}", flush=True)
    return 0

