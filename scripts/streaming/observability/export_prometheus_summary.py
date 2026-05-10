from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.observability.prometheus_exporter import (  # noqa: E402
    PrometheusExporterOptions,
    run_prometheus_exporter,
)


def parse_args() -> argparse.Namespace:
    default_config = PROJECT_ROOT / "configs" / "streaming" / "streaming.yaml"
    parser = argparse.ArgumentParser(description="Export live runtime telemetry from Kafka metrics topic as Prometheus gauges")
    parser.add_argument("--port", type=int, default=9108, help="Port to expose /metrics")
    parser.add_argument("--refresh-sec", type=int, default=15, help="Poll window in seconds for runtime metrics")
    parser.add_argument("--stale-sec", type=int, default=300, help="Expire stale run labels after this many seconds")
    parser.add_argument(
        "--offset-reset",
        type=str,
        default="latest",
        choices=["earliest", "latest"],
        help="Kafka offset reset policy for the exporter consumer; use latest for live telemetry, earliest only for backlog inspection",
    )
    parser.add_argument("--config", type=str, default=str(default_config), help="Streaming config used to resolve bootstrap servers and metrics topic")
    parser.add_argument("--bootstrap-servers", type=str, default="", help="Override Kafka bootstrap servers")
    parser.add_argument("--metrics-topic", type=str, default="", help="Override Kafka metrics topic")
    parser.add_argument("--once", action="store_true", help="Run one refresh and exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_prometheus_exporter(
        PrometheusExporterOptions(
            config_path=Path(args.config) if args.config else None,
            bootstrap_servers=str(args.bootstrap_servers).strip() or None,
            metrics_topic=str(args.metrics_topic).strip() or None,
            port=args.port,
            refresh_seconds=args.refresh_sec,
            stale_seconds=args.stale_sec,
            offset_reset=args.offset_reset,
            once=args.once,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())

