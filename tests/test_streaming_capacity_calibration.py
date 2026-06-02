from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.calibration import (  # noqa: E402
    CAPACITY_CALIBRATION_CONFIG,
    CAPACITY_CALIBRATION_PRACTICAL_SLO,
    CAPACITY_CALIBRATION_SMOKE_CONFIG,
    CapacityCalibrationConfig,
    CapacitySloConfig,
    build_capacity_calibration_main_config,
)
from ids_platform.streaming.evaluation.matrices.capacity_calibration_matrix import (  # noqa: E402
    evaluate_capacity_slo,
    trigger_interval_ms,
)
from ids_platform.streaming.evaluation.matrices.common import (  # noqa: E402
    filter_metrics_by_phase,
    summarize_runtime_metrics,
)


def _metrics_rows(*, p50: float = 200.0, p95: float = 900.0, batch_wall: float = 100.0, lag=None) -> list[dict]:
    lag_values = [0, 0, 0] if lag is None else lag
    return [
        {
            "batch_wall_ms": batch_wall,
            "latency_ms": {
                "end_to_end": {
                    "p50": p50,
                    "p95": p95,
                },
            },
            "kafka": {
                "lag_records_total": lag_value,
            },
        }
        for lag_value in lag_values
    ]


def _phase_metrics_rows() -> list[dict]:
    return [
        {
            "benchmark_phase": "warmup",
            "rows": 1_000,
            "rows_per_sec": 250.0,
            "batch_wall_ms": 2_000.0,
            "latency_ms": {"end_to_end": {"p50": 2_000.0, "p95": 2_000.0}},
            "kafka": {"lag_records_total": 1_000},
        },
        {
            "benchmark_phase": "measure",
            "rows": 1_000,
            "rows_per_sec": 1_200.0,
            "batch_wall_ms": 300.0,
            "latency_ms": {"end_to_end": {"p50": 300.0, "p95": 300.0}},
            "kafka": {"lag_records_total": 400},
        },
        {
            "benchmark_phase": "measure",
            "rows": 1_000,
            "rows_per_sec": 1_100.0,
            "batch_wall_ms": 350.0,
            "latency_ms": {"end_to_end": {"p50": 350.0, "p95": 350.0}},
            "kafka": {"lag_records_total": 0},
        },
    ]


def _summary(*, rows_total: int = 1_000, actual_rps: float = 950.0, p95: float = 900.0) -> dict:
    return {
        "rows_total": rows_total,
        "rows_per_sec_avg": actual_rps,
        "e2e_p95_ms_max": p95,
        "kafka_lag_records_max": 0,
        "batch_count": 3,
    }


class CapacityCalibrationTests(unittest.TestCase):
    def test_default_calibration_config_is_smoke(self) -> None:
        self.assertIs(CAPACITY_CALIBRATION_CONFIG, CAPACITY_CALIBRATION_SMOKE_CONFIG)
        self.assertIsInstance(CAPACITY_CALIBRATION_CONFIG, CapacityCalibrationConfig)
        self.assertEqual(CAPACITY_CALIBRATION_CONFIG.name, "capacity_calibration_smoke")
        self.assertEqual(len(CAPACITY_CALIBRATION_CONFIG.runs), 1)
        benchmark = CAPACITY_CALIBRATION_CONFIG.runs[0].benchmark
        self.assertEqual(benchmark.runtime.spark.master, "local[4]")
        self.assertEqual(benchmark.replay.phase, "measure")
        self.assertIsNotNone(benchmark.warmup_replay)
        self.assertEqual(benchmark.warmup_replay.phase, "warmup")
        self.assertEqual(benchmark.warmup_replay.source.table.num_rows, 1_000)
        self.assertEqual(
            [run.target_rps for run in CAPACITY_CALIBRATION_CONFIG.runs],
            [500.0],
        )
        self.assertIs(CAPACITY_CALIBRATION_CONFIG.slo, CAPACITY_CALIBRATION_PRACTICAL_SLO)

    def test_main_calibration_config_is_reproducible_operating_point_plan(self) -> None:
        config = build_capacity_calibration_main_config()

        self.assertEqual(config.name, "capacity_calibration_main")
        self.assertEqual(len(config.runs), 18)
        self.assertEqual(config.slo.max_p95_e2e_ms, 2_000.0)
        self.assertEqual(config.slo.max_batch_wall_trigger_ratio, 1.0)
        self.assertEqual({run.target_rps for run in config.runs}, {500.0, 750.0, 1000.0})
        self.assertEqual({run.mode for run in config.runs}, {"pass_through", "random_forest_full"})
        self.assertEqual({run.benchmark.repeat_index for run in config.runs}, {1, 2, 3})
        self.assertTrue(
            all(run.benchmark.runtime.spark.master == "local[4]" for run in config.runs)
        )
        self.assertTrue(
            all(run.benchmark.runtime.kafka.max_offsets_per_trigger == 1_000 for run in config.runs)
        )
        self.assertEqual({run.benchmark.replay.source.batch_size for run in config.runs}, {500})
        self.assertTrue(all(run.benchmark.warmup_replay is not None for run in config.runs))
        self.assertEqual(
            {run.benchmark.warmup_replay.source.table.num_rows for run in config.runs},
            {5_000, 7_500, 10_000},
        )
        self.assertEqual({run.benchmark.replay.phase for run in config.runs}, {"measure"})
        self.assertEqual({run.benchmark.warmup_replay.phase for run in config.runs}, {"warmup"})
        self.assertEqual(
            [run.benchmark.run_tag for run in config.runs],
            [run.benchmark.run_tag for run in build_capacity_calibration_main_config().runs],
        )

    def test_trigger_interval_parser_supports_common_units(self) -> None:
        self.assertEqual(trigger_interval_ms("500 milliseconds"), 500.0)
        self.assertEqual(trigger_interval_ms("1 second"), 1_000.0)
        self.assertEqual(trigger_interval_ms("2 seconds"), 2_000.0)
        self.assertEqual(trigger_interval_ms("1 minute"), 60_000.0)

    def test_slo_passes_when_operational_metrics_meet_thresholds(self) -> None:
        result = evaluate_capacity_slo(
            summary=_summary(),
            metrics_rows=_metrics_rows(),
            target_rps=1_000,
            expected_rows=1_000,
            trigger_interval="1 second",
            slo=CapacitySloConfig(),
        )

        self.assertEqual(result["slo_status"], "pass")
        self.assertEqual(result["failure_reason"], "")
        self.assertEqual(result["throughput_ratio"], 0.95)

    def test_slo_fails_for_high_p95_latency(self) -> None:
        result = evaluate_capacity_slo(
            summary=_summary(p95=2_000),
            metrics_rows=_metrics_rows(p95=2_000),
            target_rps=1_000,
            expected_rows=1_000,
            trigger_interval="1 second",
            slo=CapacitySloConfig(),
        )

        self.assertEqual(result["slo_status"], "fail")
        self.assertIn("p95_e2e_exceeded", result["failure_reason"])

    def test_slo_fails_for_low_throughput_and_missing_rows(self) -> None:
        result = evaluate_capacity_slo(
            summary=_summary(rows_total=800, actual_rps=700),
            metrics_rows=_metrics_rows(),
            target_rps=1_000,
            expected_rows=1_000,
            trigger_interval="1 second",
            slo=CapacitySloConfig(),
        )

        self.assertEqual(result["slo_status"], "fail")
        self.assertIn("rows_below_expected", result["failure_reason"])
        self.assertIn("throughput_below_target", result["failure_reason"])

    def test_slo_fails_when_batch_wall_exceeds_trigger_budget(self) -> None:
        result = evaluate_capacity_slo(
            summary=_summary(),
            metrics_rows=_metrics_rows(batch_wall=900),
            target_rps=1_000,
            expected_rows=1_000,
            trigger_interval="1 second",
            slo=CapacitySloConfig(),
        )

        self.assertEqual(result["slo_status"], "fail")
        self.assertIn("batch_wall_exceeds_trigger_budget", result["failure_reason"])

    def test_slo_fails_when_lag_monotonically_increases(self) -> None:
        result = evaluate_capacity_slo(
            summary=_summary(),
            metrics_rows=_metrics_rows(lag=[0, 100, 250]),
            target_rps=1_000,
            expected_rows=1_000,
            trigger_interval="1 second",
            slo=CapacitySloConfig(),
        )

        self.assertEqual(result["slo_status"], "fail")
        self.assertIn("lag_increasing", result["failure_reason"])

    def test_slo_uses_measure_phase_instead_of_skipping_first_batch(self) -> None:
        metrics_rows = _phase_metrics_rows()
        measure_rows = filter_metrics_by_phase(metrics_rows, "measure")
        measure_summary = summarize_runtime_metrics(measure_rows)

        result = evaluate_capacity_slo(
            summary=measure_summary,
            metrics_rows=measure_rows,
            target_rps=1_000,
            expected_rows=2_000,
            trigger_interval="1 second",
            slo=CapacitySloConfig(),
        )

        self.assertEqual(result["slo_status"], "pass")
        self.assertEqual(measure_summary["rows_total"], 2_000)

    def test_calibration_entrypoint_is_config_driven(self) -> None:
        script = PROJECT_ROOT / "scripts/streaming/official/run_capacity_calibration.py"
        content = script.read_text(encoding="utf-8")

        self.assertIn("CAPACITY_CALIBRATION_CONFIG", content)
        self.assertIn("run_capacity_calibration_matrix", content)
        self.assertNotIn("argparse", content)


if __name__ == "__main__":
    unittest.main()
