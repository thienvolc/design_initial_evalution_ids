## 1. Scope lập trình (Option A)
- Chỉ triển khai binary detection: Benign vs Attack.
- Không triển khai multiclass topic hoặc multiclass inference trong pipeline online.
- Chuẩn stream processing dùng Spark Structured Streaming (không trộn API DStreams).

## 2. Event contract tối thiểu
### 2.1 Timestamp bắt buộc
Mỗi event cần ít nhất 3 mốc thời gian:
- event_time: thời điểm flow thực sự xảy ra (dùng cho event-time latency).
- ingest_time: thời điểm record vào stream processor.
- emit_time: thời điểm prediction/alert được xuất ra.

### 2.2 Trường khuyến nghị cho mỗi record
- flow_id hoặc khóa flow tương đương.
- flow_start_time (hoặc timestamp đại diện đầu flow).
- source_ip, destination_ip, source_port, destination_port, protocol.
- feature vector đã chuẩn hóa theo offline preprocessor.
- label_binary (0/1) cho tập có nhãn khi đánh giá offline/nearline.
- prediction_label và prediction_score cho output online.

## 3. Topic design cho binary pipeline
### 3.1 Topic đề xuất
- ids.raw.flows
- ids.clean.flows
- ids.predictions.binary
- ids.alerts
- ids.metrics

### 3.2 Không dùng trong Option A
- Không tạo topic multiclass prediction trong benchmark chính.

## 4. Spark Structured Streaming settings
### 4.1 Trigger và intake control
- Dùng trigger interval cố định để benchmark công bằng.
- Dùng maxOffsetsPerTrigger để giới hạn tốc độ đọc từ Kafka.
- Điều chỉnh theo profile low/medium/high load.

### 4.2 Event-time processing
- Định nghĩa watermark dựa trên event_time.
- Log late event ratio và watermark delay theo từng đợt chạy.

### 4.3 Reliability
- Bật checkpointing cho query state và offset.
- Định nghĩa retry policy cho sink ghi prediction/alert.
- Với topology 1 broker: không benchmark failover liên broker.

## 5. Kafka producer/consumer config cần chốt
### 5.1 Backoff và retry
- reconnect.backoff.ms
- reconnect.backoff.max.ms
- retry.backoff.ms
- retry.backoff.max.ms

### 5.2 Producer durability
- Bật idempotent producer.
- Cấu hình acks phù hợp mục tiêu benchmark latency/reliability.

## 6. Model serving rules
- Chỉ serve model binary (LR hoặc RF theo artifact đã train).
- Offline preprocessor và online transform phải cùng thứ tự feature.
- Không thay đổi ngưỡng cảnh báo giữa các lần benchmark nếu không ghi rõ trong log thí nghiệm.

## 7. Checklist trước khi chạy benchmark
- Xác nhận schema input khớp contract.
- Xác nhận mapping label về binary nhất quán.
- Xác nhận checkpoint path trống hoặc chủ động reuse theo thiết kế thử nghiệm.
- Xác nhận metrics output có đủ p50/p95/p99, throughput, lag, FPR, FNR.
- Xác nhận load profile có cả steady và bursty.

## 8. Checklist sau khi chạy benchmark
- Thu thập confusion matrix và detection metrics (precision, recall, F1, FPR, FNR).
- So sánh latency event-time và processing-time theo từng mức tải.
- So sánh full feature set và reduced feature set trong cùng điều kiện tải.
- Ghi rõ degradation point (ngưỡng tải bắt đầu giảm chất lượng hoặc tăng lag đột biến).

