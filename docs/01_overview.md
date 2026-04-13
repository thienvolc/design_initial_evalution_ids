> Legacy note: this file is historical context, not the canonical architecture reference.
> Use [README.md](README.md), [architecture.md](architecture.md), and [evaluation_methodology.md](evaluation_methodology.md) for the current project boundary and execution model.

## 1. List papers
- 02 Distributed Intrusion Detection System using Kafka and Spark Streaming
- 03 Performance Evaluation of Intrusion Detection Streaming Transactions Using Apache Kafka and Spark Streaming
- 04 A Comparative Study of Two-Stage Intrusion Detection Using Modern Machine Learning Approaches on the CSE-CIC-IDS2018 Dataset
- 05 CSE-CIC-IDS2018 Dataset
- 06 Toward Scalable Security Intrusion Detection with Spark-Based Classifiers on Hadoop YARN
- 07 Benchmarking Distributed Stream Data Processing

Scope áp dụng (Option A):
- Chỉ triển khai bài toán binary detection (Benign vs Attack)
- Không triển khai multiclass online trong pha benchmark hệ thống


## 2. Metrics
### Performance
- end-to-end latency
- processing latency / micro-batch duration
- throughput (records/sec)
- Kafka consumer lag
- drop/retry/failure count
- CPU
- RAM
- executor utilization
- processing-time latency p50/p95/p99
- event-time latency p50/p95/p99
- trigger duration
- end-to-end alert delay

### Throughput
- input throughput
- processed throughput
- output throughput
- sustainable throughput

### Pressure / queueing
- Kafka lag
- queue growth rate
- watermark delay
- late event ratio

### Reliability
- retry count
- reconnect count
- duplicate ratio
- failed batches
- restart recovery time

### Detection
- accuracy
- precision / recall / F1
- confusion matrix
- binary recall cho attack
- false positive rate (FPR)
- false negative rate (FNR)
- degradation under load

### Cost/scalability
- throughput per core
- latency per partition
- accuracy degradation under load
- resource cost per 10k events/sec


## 3. Experiments
### Kafka-side
- partitions: 1 / 2 / 4 / 8
- producer rate

Ghi chú reliability scope:
- Với topology 1 broker, không benchmark replication/failover liên broker

### Spark-side
- trigger interval
- number of executors
- executor cores
- executor memory
- maxOffsetsPerTrigger và trigger-based backpressure control

### Model-side
- LR vs RF
- feature full vs reduced/PCA

### Load-side
- low / medium / high event rate
- balanced vs imbalanced attack distribution
- bursty vs steady stream


## 4. Architecture
### 4.1. Event source layer
1. Đọc flow records từ CSE-CIC-IDS2018 CSV
2. Map mỗi record thành JSON/Avro event
3. Push vào Kafka theo tốc độ cấu hình được

### 4.2. Messaging layer
- Kafka topics
- partitioning theo flow key
- idempotent producer
- retry/backoff config

Topics:
- ids.raw.flows
- ids.clean.flows
- ids.predictions.binary
- ids.alerts
- ids.metrics
Partition by key:
- source IP
- 5-tuple hash
- flow ID

### 4.3. Stream processing layer
- event-time aware processing
- watermark

1. Stage 1 — Ingestion & validation
- Read from Kafka
- Parse schema
- Drop malformed rows
- Normalize null / type / timestamp
- Add event time, ingest time

2. Stage 2 — Feature preparation
- Chọn subset feature đúng với model đã train
- Chuẩn hóa/encode y như offline pipeline
- Tính một số feature dẫn xuất nếu cần

3. Stage 3 — Inference
- Benign vs Attack
- Score/probability + ngưỡng cảnh báo

### 4.4. Pressure control layer
- maxOffsetsPerTrigger
- producer pacing
- adaptive throttling
- degradation policy

### 4.5. Reliability layer
- checkpointing
- offset replay
- retry/backoff
- reconnect strategy
- idempotent sink / dedup policy

### 4.6. Observability & benchmark layer
- event-time latency
- processing-time latency
- sustainable throughput
- lag
- resource usage
- detection quality


## 5. Models
1. Offline
- PCA hoặc selected-features
- Binary: Logistic Regression hoặc Random Forest

2. Online
- Spark UDF / PipelineModel để inference binary


## 6. Physical
- Node 1: Kafka broker + controller + producer/replay
- Node 2: Spark driver + 1 executor
- Node 3: Spark executor + storage/serving


## 7. Stack
- Dataset: CSE-CIC-IDS2018
- Replay: Python producer
- Message bus: Apache Kafka
- Stream processing: Spark Structured Streaming
- Models: Binary LR / RF
- Cluster manager: standalone trước
- Metrics: Prometheus/Grafana hoặc log-based metrics
- Storage: Parquet/Delta/CSV for experiment outputs
- Visualization: simple dashboard/API

