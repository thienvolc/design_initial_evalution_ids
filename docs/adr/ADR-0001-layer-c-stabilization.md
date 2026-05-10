# ADR-0001: Layer C Streaming Fault Matrix Stabilization

- Status: Accepted
- Date: 2026-04-24
- Scope: Layer C local/online fault evaluation flow
- Owners: Streaming evaluation pipeline

## Context

Layer C is the streaming fault-matrix layer used to evaluate recovery behavior for:

- `kafka_restart`
- `spark_process_restart`
- `producer_restart`

The original implementation had repeated failures and ambiguous metrics during local runs on Windows host orchestration with Docker-backed subprocesses. The primary problems were not model-quality problems. They were runtime-orchestration, checkpoint-ownership, shutdown, and observability problems.

The user goal was to make Layer C stable enough to support:

- trustworthy local regression runs
- clean summary/timeseries artifacts
- defensible reporting for later paper writing

## Decision Summary

We kept the Layer C orchestrator on the host, but standardized the Layer C SUT/replay subprocesses to run in Docker.

We then stabilized Layer C by:

- fixing host-vs-container bootstrap resolution for post-run metric collection
- adding runtime log capture for each Layer C run tag
- removing unsupported Spark checkpoint file manager configuration
- hardening readiness detection to avoid false positives from reused log files
- adding cooperative shutdown support for Docker-backed stream processes
- using input sentinel for natural end-of-run shutdown where appropriate
- preventing false recovery success caused by terminal-marker payloads
- validating warmup/recovery metrics before marking scenario status `ok`
- increasing run-tag uniqueness to avoid accidental checkpoint/log/control collisions
- tightening teardown semantics so graceful stop requires real stop markers, not only sentinel receipt

## Execution Model

### Final model

Layer C does not run fully inside Docker as a single controller process.

Instead:

- host Python runs `scripts/streaming/official/run_streaming_profile.py`
- host orchestration runs `scripts/streaming/official/run_layer_c_matrix.py`
- fault injection is controlled from the host
- streaming SUT and replay subprocesses are started with `execution_mode=docker`
- Kafka inside container network uses `kafka:29092`
- host-side post-run collectors use host-resolvable bootstrap mapping

### Why this model was kept

Full-container Layer C orchestration would make Docker fault injection harder and would remove the host’s control over restart scenarios. The host orchestrator is therefore intentional, but all SUT/replay subprocesses still use container runtime so that runtime behavior matches the rest of the streaming stack.

## Problems Observed

### 1. Host/Container Kafka bootstrap mismatch

Symptoms:

- host-side metrics collector attempted to read from `kafka:29092`
- Windows host could not resolve container-internal DNS name
- result was `no_matching_metrics` or warmup metric missing

Root cause:

- Layer C orchestrator is host-side
- but metric collection logic initially reused container bootstrap assumptions

Resolution:

- Layer C post-run collector now resolves bootstrap for `execution_mode="host"` during host collection

Files:

- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`

### 2. Unsupported Spark checkpoint file manager config

Symptoms:

- Docker run failed with `ClassNotFoundException`
- class `org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager` was not available

Root cause:

- configuration forced a checkpoint manager class incompatible with the actual Spark/PySpark runtime in container
- container runtime was observed to be PySpark `4.1.1`

Resolution:

- removed forced checkpoint file manager override

Files:

- `src/ids_platform/streaming/runtime/structured_streaming_job.py`

### 3. False readiness caused by reused log file content

Symptoms:

- restart path reported stream ready even when the new process was not actually ready
- log-based readiness matched old `job_start` lines from previous process lifetime using the same run tag/log file

Root cause:

- readiness check searched the entire runtime log file
- Layer C restart uses the same run tag and same log file

Resolution:

- background processes now record `ids_log_start_offset`
- readiness scanning only searches new log content written after process start

Files:

- `src/ids_platform/common/subprocess.py`
- `src/ids_platform/streaming/evaluation/matrices/common.py`

### 4. Checkpoint concurrency during `spark_process_restart`

Symptoms:

- `CONCURRENT_STREAM_LOG_UPDATE`
- `FileAlreadyExistsException` on checkpoint offsets
- multiple queries appeared to own the same checkpoint path

Root cause:

- restarted instance began before the previous streaming process had truly released checkpoint ownership
- external shutdown was too weak and too late

Resolution:

- added cooperative host-to-container shutdown request via control file
- host requests shutdown first
- job stops itself and releases queries/checkpoints
- force cleanup remains only as fallback

Files:

- `src/ids_platform/streaming/runtime/control.py`
- `src/ids_platform/streaming/evaluation/orchestration/fault_matrix.py`
- `src/ids_platform/streaming/runtime/structured_streaming_job.py`

### 5. Shutdown between micro-batches causing broken restart state

Symptoms:

- `InterruptedException`
- `FileFormatWriter abort`
- Spark query shutdown warnings
- later Kafka progress NPE on restarted stream

Root cause:

- stream was being stopped while a batch was still active or not fully quiesced

Resolution:

- added a quiescence gate before `spark_process_restart`
- restart is delayed until stream activity settles enough to reduce mid-batch interruption risk

Important note:

- current quiescence signal is based on log inactivity
- this is a pragmatic proxy, not a perfect batch-level truth signal

Files:

- `src/ids_platform/streaming/evaluation/matrices/common.py`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`

### 6. False positive recovery from terminal marker payloads

Symptoms:

- warmup or recovery could be considered successful when only a `run_completed`/`final` payload existed

Root cause:

- `read_first_matching_metric()` returned the first matching payload for `run_tag`
- it did not exclude terminal-marker messages

Resolution:

- terminal markers are now skipped in metrics reader
- Layer C also validates metric payload shape before accepting warmup/recovery success

Validation rules:

- payload must be a dict
- `event_type` must be `batch_metrics` if present
- `rows > 0`

Files:

- `src/ids_platform/streaming/evaluation/reporting/metrics_reader.py`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`

### 7. Run-tag collision risk

Symptoms:

- possible reuse of checkpoint paths, control files, and logs under quick reruns

Root cause:

- run tag originally used second-level timestamp precision

Resolution:

- run tag now uses millisecond precision

Files:

- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`

### 8. Sentinel support existed but Layer C did not use it correctly

Symptoms:

- end-of-scenario cleanup still used external stop
- `producer_restart` and post-recovery teardown generated task kills and abort noise

Root cause:

- stream did not consistently rely on input sentinel for natural end-of-run termination
- replay emitted sentinel by default, but Layer C did not control when it should or should not be emitted

Resolution:

- Layer C stream starts with `stop_on_input_sentinel=True` where appropriate
- warmup replay disables sentinel emission
- only the final replay segment of a scenario emits sentinel
- `producer_restart`:
  - first post-fault segment: no sentinel
  - second post-fault segment: sentinel enabled
- `spark_process_restart`:
  - initial pre-fault stream: sentinel disabled
  - restarted stream: sentinel enabled for final end-of-scenario shutdown

Files:

- `src/ids_platform/streaming/evaluation/orchestration/fault_matrix.py`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`
- `src/ids_platform/streaming/runtime/structured_streaming_job.py`

### 9. `input_sentinel_seen` was an insufficient graceful-stop marker

Symptoms:

- scenario business logic succeeded
- but process teardown still fell back to external stop
- Spark then emitted shutdown-side errors during sink/query termination

Root cause:

- `input_sentinel_seen` only means the control record was observed
- it does not mean all sink queries have completed and the process has fully stopped

Resolution:

- Layer C teardown no longer accepts `input_sentinel_seen` as sufficient
- graceful shutdown is recognized only when one of the following appears:
  - `stop_condition_met ... reason=input_sentinel`
  - `job_stop`

Additional changes:

- longer graceful wait before fallback
- longer shutdown wait before force-stop

Files:

- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`
- `src/ids_platform/streaming/evaluation/matrices/common.py`

## Documentation and Profile Changes

Note:

- Layer C implementation is now split across `src/ids_platform/streaming/evaluation/matrices/layer_c/`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py` remains as the compatibility facade for existing imports/tests

Updated:

- `scripts/streaming/official/run_layer_c_matrix.py`
- `experiments/streaming/profiles/local_profiles.yaml`
- `experiments/streaming/profiles/streaming_profiles.yaml`
- `scripts/streaming/README.md`

Effective contract:

- Layer C profile remains host-orchestrated
- Layer C subprocess runtime is Docker-backed
- default `--execution-mode` for Layer C matrix runner is `docker`

## Observability Changes

Added or improved:

- per-run runtime logs under `logs/streaming/runtime/<run_tag>.log`
- Layer C timeseries export under `artifacts/streaming/metrics_timeseries/<run_tag>.csv`
- clearer phase logs for:
  - warmup metric wait
  - stream restart start/done
  - graceful-stop marker detection
  - metrics collection progress/end

## Academic Validity Notes

These changes are considered runtime-validity fixes, not model-tuning changes.

They do not intentionally change:

- model artifact
- feature set
- fault scenario semantics
- checkpoint reuse semantics for restart recovery

They do change:

- whether the experiment actually runs according to intended protocol
- whether metric collection can falsely pass or falsely miss results
- whether shutdown/restart artifacts contaminate recovery evaluation

For reporting purposes:

- this work should be described as instrumentation and orchestration stabilization
- not as model optimization

## Remaining Known Risks

The following risks remain and should be tracked:

### 1. Spark 4.1.1 shutdown fragility

Even after orchestration fixes, Spark 4.1.1 can still produce poor shutdown behavior in multi-query streaming jobs if termination happens at an unlucky point.

Observed patterns historically included:

- `InterruptedException`
- `FileFormatWriter abort`
- `TaskKilled`
- `RejectedExecutionException`
- query-thread `StackOverflowError`

Current mitigation:

- prefer natural sentinel-based shutdown
- delay fallback external stop

### 2. Quiescence proxy is still heuristic

`wait_for_log_quiescence()` is a practical proxy, not a proof that no micro-batch is active.

A stronger future design would use:

- batch-progress watermarking
- explicit source-progress polling
- dedicated internal shutdown state

### 3. Layer C summary is based on SUT-emitted metrics

Current summary remains derived from `ids.metrics` emitted by the SUT. This is acceptable for current evaluation, but it should be disclosed in reports as the operational source of truth.

## Final Outcome at This Stage

After the fixes above, `layer_c_local_light` reached the following desired state:

- all three scenarios completed
- summary CSV written successfully
- timeseries CSV written per scenario
- profile gate passed
- `spark_process_restart` and `producer_restart` both ended with graceful input-sentinel stop markers rather than cleanup-time external stop

## Files Most Relevant to Layer C Stabilization

- `scripts/streaming/official/run_layer_c_matrix.py`
- `scripts/streaming/README.md`
- `experiments/streaming/profiles/local_profiles.yaml`
- `experiments/streaming/profiles/streaming_profiles.yaml`
- `src/ids_platform/common/subprocess.py`
- `src/ids_platform/streaming/runtime/control.py`
- `src/ids_platform/streaming/evaluation/orchestration/fault_matrix.py`
- `src/ids_platform/streaming/evaluation/matrices/common.py`
- `src/ids_platform/streaming/evaluation/matrices/layer_c/`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`
- `src/ids_platform/streaming/evaluation/reporting/metrics_reader.py`
- `src/ids_platform/streaming/runtime/structured_streaming_job.py`

## Follow-up

Recommended next actions:

1. Keep this ADR as the baseline reference for Layer C fault-evaluation semantics.
2. If full-profile or scale-up runs expose new issues, append a new ADR instead of rewriting this one.
3. In the paper/report, explicitly state that Layer C was stabilized through orchestration/runtime fixes before final measurements were accepted.
