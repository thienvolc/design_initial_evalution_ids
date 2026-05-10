from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.benchmark.contract import (
    BenchmarkPlan,
    BenchmarkRun,
    FairnessError,
    FairnessLayer,
    load_benchmark_plan,
    materialize_run_params,
    validate_fairness,
)


class BenchmarkContractTests(unittest.TestCase):
    def test_materialize_run_params_merges_layer_fixed_values(self) -> None:
        plan = BenchmarkPlan(
            name="test",
            output_root=Path("artifacts"),
            detect_script=Path(__file__),
            detect_config=Path(__file__),
            primary_latency_metric="end_to_end_p95_ms",
            sustainable_throughput_metric="rows_per_second",
            feature_sets={"full"},
            model_profiles={"logistic_regression"},
            layers={"A": FairnessLayer(name="A", varied_group={"batch_size"}, fixed={"feature_set": "full", "models": ["logistic_regression"]})},
            runs=[BenchmarkRun(run_id="run-1", layer="A", params={"batch_size": 500})],
        )

        params = materialize_run_params(plan, plan.runs[0])
        self.assertEqual(params["feature_set"], "full")
        self.assertEqual(params["batch_size"], 500)

    def test_validate_fairness_rejects_illegal_varied_keys(self) -> None:
        plan = BenchmarkPlan(
            name="test",
            output_root=Path("artifacts"),
            detect_script=Path(__file__),
            detect_config=Path(__file__),
            primary_latency_metric="end_to_end_p95_ms",
            sustainable_throughput_metric="rows_per_second",
            feature_sets={"full"},
            model_profiles={"logistic_regression"},
            layers={"A": FairnessLayer(name="A", varied_group={"batch_size"}, fixed={"feature_set": "full", "models": ["logistic_regression"]})},
            runs=[BenchmarkRun(run_id="run-1", layer="A", params={"max_rows": 1000})],
        )

        with self.assertRaises(FairnessError):
            validate_fairness(plan)

    def test_load_benchmark_plan_reads_yaml_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            detect_script = root / "detect.py"
            detect_config = root / "detect.yaml"
            benchmark_yaml = root / "benchmark.yaml"
            detect_script.write_text("print('ok')\n", encoding="utf-8")
            detect_config.write_text("runtime: {}\n", encoding="utf-8")
            benchmark_yaml.write_text(
                "\n".join(
                    [
                        "benchmark:",
                        "  name: sample",
                        "  output_root: artifacts/streaming/benchmark",
                        f"  detect_script: {detect_script.name}",
                        f"  detect_config: {detect_config.name}",
                        "contracts:",
                        "  primary_latency_metric: end_to_end_p95_ms",
                        "  sustainable_throughput_metric: rows_per_second",
                        "  feature_sets: [full]",
                        "  model_profiles: [logistic_regression]",
                        "  fairness_layers:",
                        "    A:",
                        "      varied_group: [batch_size]",
                        "      fixed:",
                        "        feature_set: full",
                        "        models: [logistic_regression]",
                        "matrix:",
                        "  A:",
                        "    - run_id: run-1",
                        "      layer: A",
                        "      batch_size: 500",
                    ]
                ),
                encoding="utf-8",
            )

            plan = load_benchmark_plan(benchmark_yaml, root)
            self.assertEqual(plan.name, "sample")
            self.assertEqual(len(plan.runs), 1)
            self.assertEqual(plan.runs[0].run_id, "run-1")


if __name__ == "__main__":
    unittest.main()
