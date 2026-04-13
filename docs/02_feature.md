## 1. Phạm vi feature (Option A)
- Bài toán chỉ là binary detection: Benign vs Attack.
- Không tối ưu theo từng lớp attack riêng lẻ trong online inference.
- Feature engineering phải nhất quán giữa offline training và online serving.

## 2. Nguyên tắc chọn feature
- Ưu tiên feature có tín hiệu mạnh cho tấn công khối lượng lớn, scan, flood và automation.
- Giảm feature dư thừa để giảm chi phí xử lý trên Spark Structured Streaming.
- Tránh các trường có tỷ lệ null/zero quá cao hoặc phụ thuộc mạnh vào môi trường host.
- Duy trì 2 cấu hình để benchmark: full feature set và reduced feature set.

## 3. Nhóm feature nên giữ
### 3.1 Core volume and speed
| Feature | Vai trò |
| --- | --- |
| Flow Duration | Phân biệt burst ngắn và session kéo dài |
| Flow Byts/s | Cường độ dữ liệu theo thời gian |
| Flow Pkts/s | Tín hiệu mạnh cho flood |
| Tot Fwd Pkts | Hoạt động chiều client -> server |
| Tot Bwd Pkts | Hoạt động chiều server -> client |

### 3.2 Packet size behavior
| Feature | Vai trò |
| --- | --- |
| Pkt Len Mean | Kích thước gói trung bình |
| Pkt Len Std | Độ biến thiên kích thước gói |
| Fwd Pkt Len Mean | Hành vi gói ở chiều gửi |
| Bwd Pkt Len Mean | Hành vi gói ở chiều nhận |

### 3.3 Inter-arrival timing
| Feature | Vai trò |
| --- | --- |
| Flow IAT Mean | Nhịp gửi tổng thể của flow |
| Flow IAT Std | Mức đều/không đều của traffic |
| Fwd IAT Mean | Nhịp gửi phía forward |
| Bwd IAT Mean | Nhịp phản hồi phía backward |

### 3.4 TCP control signals
| Feature | Vai trò |
| --- | --- |
| SYN Flag Cnt | Dấu hiệu scan hoặc SYN flood |
| ACK Flag Cnt | Hành vi kết nối hợp lệ |
| RST Flag Cnt | Kết nối bị reset bất thường |

### 3.5 Direction imbalance
| Feature | Vai trò |
| --- | --- |
| Down/Up Ratio | Mất cân bằng client/server |

## 4. Nhóm feature nên loại bỏ
### 4.1 Redundant features
- Subflow Fwd Pkts
- Subflow Fwd Byts
- Subflow Bwd Pkts
- Subflow Bwd Byts

Lý do: gần như trùng ngữ nghĩa với tổng packet/byte theo chiều, làm tăng chiều dữ liệu nhưng ít thêm thông tin.

### 4.2 Bulk and unstable statistics
- Fwd Byts/b Avg
- Fwd Pkts/b Avg
- Fwd Blk Rate Avg
- Bwd Byts/b Avg
- Bwd Pkts/b Avg
- Bwd Blk Rate Avg

Lý do: thường nhiều giá trị 0, chất lượng thống kê kém ổn định cho production.

### 4.3 Low-signal or noisy fields
- Fwd Header Len
- Bwd Header Len
- Init Fwd Win Byts
- Init Bwd Win Byts
- Active Mean / Std / Max / Min
- Idle Mean / Std / Max / Min

Lý do: phụ thuộc môi trường hệ điều hành hoặc ít đóng góp tương xứng chi phí tính toán trong luồng online.

### 4.4 Rare/degenerate fields
- Fwd Pkt Len Min
- Bwd Pkt Len Min
- Pkt Len Min
- URG Flag Cnt
- CWE Flag Count
- ECE Flag Cnt

Lý do: nhiều giá trị thoái hóa (0 hoặc gần như luôn 0), hiệu quả phân loại thấp.

### 4.5 Duplicate rate logic
- Fwd Pkts/s
- Bwd Pkts/s

Lý do: với Option A, giữ Flow Pkts/s là đủ để đại diện cường độ gói trên toàn flow.

## 5. Cấu hình feature cho benchmark
### 5.1 Full set
- Dùng toàn bộ feature hợp lệ sau bước làm sạch.
- Mục tiêu: tối đa chất lượng detection.

### 5.2 Reduced set
- Dùng các nhóm feature lõi ở mục 3.
- Mục tiêu: đo trade-off giữa chất lượng và latency/throughput.

## 6. Ràng buộc triển khai
- Danh sách feature online phải khớp chính xác với manifest và preprocessor đã lưu từ offline.
- Không thêm feature mới trong online pipeline nếu không retrain model.
- Khi so sánh full vs reduced, phải giữ nguyên data split và cấu hình load để kết quả công bằng.
