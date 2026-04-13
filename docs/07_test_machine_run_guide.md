> Legacy note: this guide is historical and narrower than the current canonical runbook.
> Use [execution_runbook.md](execution_runbook.md) for the current execution order and [evaluation_methodology.md](evaluation_methodology.md) for source-of-truth boundaries.

# Hướng dẫn chạy trên máy test bằng profile (không dùng Prometheus/Grafana/Streamlit)

## 1. Mục tiêu
- Dùng profile có sẵn trong repo để chạy từng layer.
- Chạy stack chính bằng Docker Compose: zookeeper, kafka, ids-dev.
- Không chạy stack quan sát/dashboard:
  - Prometheus
  - Grafana
  - Streamlit

## 2. Yêu cầu máy test
- Windows 10/11.
- Docker Desktop (WSL2 backend).
- Git.
- Python 3.11+ trên host (launcher cần Python host cho profile runtime=host như Layer C).

Khuyến nghị tài nguyên:
- CPU >= 4 cores
- RAM >= 8 GB (khuyến nghị 16 GB)
- Disk trống >= 10 GB

## 3. Cài Docker Desktop
1. Cài Docker Desktop từ trang chính thức của Docker.
2. Bật WSL2 backend.
3. Mở Docker Desktop đến khi Engine = Running.
4. Kiểm tra:

```powershell
docker --version
docker compose version
```

## 4. Chuẩn bị source

```powershell
git clone <repo-url> design_initial_evalution_ids
cd design_initial_evalution_ids
Copy-Item .env.example .env -Force
```

Nếu cần mount theo đường dẫn tuyệt đối, sửa `.env`:

```dotenv
PROJECT_DIR=D:/path/to/design_initial_evalution_ids
```

## 5. Docker compose (cách nhanh, khuyến nghị)
Một lệnh để vừa build image vừa bật stack tối thiểu:

```powershell
docker compose up -d --build zookeeper kafka ids-dev
docker compose ps
docker compose exec -T ids-dev pkill -f run_structured_streaming.py ; true
```

## 6. Docker compose (cách tách bước, tuỳ chọn)
Nếu muốn tách riêng build và run:

```powershell
docker compose build ids-dev
docker compose up -d zookeeper kafka ids-dev
docker compose ps
docker compose exec -T ids-dev pkill -f run_structured_streaming.py ; true
```

Khi cần build sạch hoàn toàn:

```powershell
docker compose build --no-cache ids-dev
```

## 7. Chuẩn bị Python host cho profile launcher
Launcher chạy từ host. Với profile runtime=docker thì launcher gọi `docker compose exec ...`; với runtime=host (Layer C) launcher chạy script bằng Python host.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

$PY = ".\.venv\Scripts\python.exe"
```

## 8. Chạy theo profile có sẵn (từng layer)
Máy test dùng online profiles:

```powershell
$PROFILE_CFG = "experiments/streaming/profiles/online_profiles.yaml"

# kiểm tra danh sách profile hợp lệ trước khi chạy
& $PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG --list --list-official-only
```

Thứ tự chạy đề xuất:

```powershell
# Gate bắt buộc
& $PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG --profile smoke_gate

# Layer A
& $PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG --profile layer_a_500k --allow-heavy

# Layer B
& $PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG --profile layer_b_500k --allow-heavy

# Layer C
& $PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG --profile layer_c_700k_fault --allow-heavy
```

Ghi chú:
- `layer_c_700k_fault` có runtime=host nhưng vẫn chạy qua cùng launcher.
- Các profile heavy bắt buộc thêm `--allow-heavy`, nếu không launcher sẽ từ chối chạy.
- Không cần gọi trực tiếp `run_online_layer_*` khi đã dùng profile launcher.

Lưu ý quan trọng khi chạy `layer_a_500k` và `layer_b_500k`:
- Đây là profile nặng, thời gian chạy dài.
- File summary chỉ được ghi ở cuối toàn bộ profile, không ghi dần theo từng vòng.
- Vì vậy trong lúc đang chạy bạn có thể chưa thấy file CSV xuất hiện ngay.

Ước lượng thời gian để có CSV (trên máy test trung bình):
- `layer_a_500k`: khoảng 35-70 phút.
- `layer_b_500k`: khoảng 45-90 phút.
- Nếu metrics không về đúng run_tag và chạm timeout nhiều vòng:
  - `layer_a_500k`: có thể lên ~100-110 phút.
  - `layer_b_500k`: có thể lên ~140-150 phút.

Vì sao có cảm giác "treo":
- Profile có `trace_stream_run_seconds: 420` và warmup lớn.
- Profile có `metrics_timeout_sec: 1500` (25 phút) cho mỗi vòng nếu không đọc được metrics đúng run_tag.
- Khi stream đã kết thúc, container `ids-dev` có thể giảm RAM xuống thấp (ví dụ ~28 MiB) nhưng tiến trình launcher vẫn đang chờ metrics timeout.

Nếu máy test yếu hoặc cần kiểm tra nhanh:
- Chạy `smoke_gate` trước để xác nhận luồng metrics còn hoạt động.
- Tạm ưu tiên profile nhẹ hơn (ví dụ `layer_b_light`) để xác nhận end-to-end trước khi chạy full 500k.

## 9. Tổng hợp báo cáo sau khi chạy xong A/B/C
Với online profiles hiện tại, summary mặc định nằm trong `artifacts/streaming/scale_up`.

```powershell
docker compose exec ids-dev python scripts/streaming/build_online_report.py `
  --layer-a artifacts/streaming/scale_up/layer_a_summary_500k.csv `
  --layer-b artifacts/streaming/scale_up/layer_b_summary_500k.csv `
  --layer-c artifacts/streaming/scale_up/layer_c_summary_700k_fault.csv `
  --out-md artifacts/streaming/scale_up/online_evaluation_report_test_machine.md `
  --out-json artifacts/streaming/scale_up/online_evaluation_report_test_machine.json
```

Kiểm tra output:

```powershell
Get-ChildItem artifacts/streaming/scale_up
```

## 10. Kết thúc phiên chạy

```powershell
docker compose exec -T ids-dev pkill -f run_structured_streaming.py ; true
docker compose down
```

## 11. Không chạy trong runbook này
- `docker compose -f ops/observability/docker-compose.observability.yaml up -d`
- `python scripts/streaming/export_prometheus_summary.py ...`
- `python -m streamlit run apps/streaming_dashboard.py`

Runbook này chỉ dùng profile để chạy benchmark theo layer và xuất artifact CSV/MD/JSON.


