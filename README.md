# Streaming IDS Experimental Pipeline

Repository này chứa pipeline thực nghiệm cho IDS dạng luồng trên Kafka và Spark Structured Streaming. Luồng chính gồm:

1. Cài môi trường Python 3.11.
2. Giải nén artifacts đã lưu trữ nếu có `artifacts.zip`.
3. Dùng `data/gold/splits` đã chuẩn bị sẵn để train lại model khi cần.
4. Khởi động Kafka/Spark runtime bằng Docker Compose.
5. Chạy các benchmark streaming.
6. Tổng hợp bảng và hình cho báo cáo.

## Yêu cầu môi trường

- Windows PowerShell hoặc PowerShell 7.
- Python `>=3.11,<3.12`.
- Docker Desktop đã bật WSL 2 backend.
- Dữ liệu split đã có trong `data/gold/splits`, hoặc raw CSV trong `data/raw` nếu muốn tái tạo từ đầu.

Các thư mục dữ liệu và artifact lớn được ignore khỏi Git, gồm `data/**/*.parquet`, `artifacts/streaming/**`, `artifacts/offline/spark/**` và `paper/**`.

## Cài đặt Python

Khuyến nghị dùng `uv` vì repo đã có `pyproject.toml` và `uv.lock`.

### Cách 1: uv

```powershell
py -3.11 -m pip install uv
uv venv --python 3.11
uv sync
```

Kích hoạt môi trường nếu muốn chạy lệnh Python thủ công:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Cách 2: pip + venv

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Kiểm tra nhanh:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m compileall -q src scripts
```

## Khôi phục artifact đã lưu trữ

Nếu repo đi kèm file `artifacts.zip`, giải nén file này ở project root để khôi phục các artifact đã tạo sẵn:

```powershell
Expand-Archive -Path artifacts.zip -DestinationPath . -Force
```

Sau khi giải nén, kiểm tra các thư mục chính:

```powershell
Get-ChildItem artifacts
Get-ChildItem artifacts\offline\spark
Get-ChildItem artifacts\streaming\evaluation
```

Nếu `artifacts.zip` đã chứa kết quả offline và streaming mới nhất, có thể chạy thẳng bước tổng hợp bảng/hình. Nếu muốn tái chạy benchmark, vẫn nên train hoặc kiểm tra lại offline Spark artifacts trước.

## Tải raw data CSE-CIC-IDS2018

Dataset CSE-CIC-IDS2018 được công bố trên AWS Open Data ở bucket `s3://cse-cic-ids2018/`. AWS Registry ghi bucket này ở region `ca-central-1` và cho phép truy cập bằng `--no-sign-request`, tức không cần AWS account.

Chỉ cần tải raw data khi muốn tái tạo `data/silver` và `data/gold/splits` từ đầu. Nếu `data/gold/splits` đã được chuẩn bị sẵn, có thể bỏ qua mục này và chạy phase 3 để train model.

Cài AWS CLI trên Windows:

```powershell
winget install -e --id Amazon.AWSCLI
aws --version
```

Kiểm tra bucket:

```powershell
aws s3 ls --no-sign-request --region ca-central-1 s3://cse-cic-ids2018/
```

Pipeline offline phase 1 đọc các CSV trực tiếp trong `data/raw/*.csv`. Vì vậy, nếu chỉ cần dữ liệu đặc trưng đã gán nhãn cho ML, tải thư mục `Processed Traffic Data for ML Algorithms` rồi copy các CSV về `data/raw`:

```powershell
New-Item -ItemType Directory -Force data\raw | Out-Null

aws s3 sync `
  --no-sign-request `
  --region ca-central-1 `
  "s3://cse-cic-ids2018/Processed Traffic Data for ML Algorithms/" `
  "data/raw/_aws_processed" `
  --exclude "*" `
  --include "*.csv"

Get-ChildItem data\raw\_aws_processed -Recurse -Filter *.csv |
  Copy-Item -Destination data\raw -Force

Remove-Item data\raw\_aws_processed -Recurse -Force
Get-ChildItem data\raw -Filter *.csv
```

Không nên sync toàn bộ bucket trừ khi cần PCAP/log/raw files đầy đủ, vì dung lượng rất lớn và không cần cho pipeline train hiện tại. Nếu thật sự cần toàn bộ dữ liệu:

```powershell
aws s3 sync --no-sign-request --region ca-central-1 s3://cse-cic-ids2018/ data/raw/cse-cic-ids2018_full
```

## Offline training

Offline pipeline có 3 phase:

- Phase 1: ingest và clean dữ liệu thô thành silver parquet.
- Phase 2: chia silver thành các split gold.
- Phase 3: train, calibration threshold, test evaluation và export Spark artifacts.

Thông thường, nếu `data/gold/splits` đã có sẵn, chỉ cần train lại artifacts hiện dùng cho báo cáo:

```powershell
.\scripts\runbooks\offline_train_full.ps1
.\scripts\runbooks\offline_train_reduced.ps1
```

Chỉ chạy lại từ raw CSV khi cần tái tạo toàn bộ dữ liệu:

```powershell
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 1 2 3 --feature-set full --models logistic_regression gradient_boosting random_forest
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 3 --feature-set reduced --models random_forest
```

Kết quả offline chính nằm dưới:

- `artifacts/offline/spark/full`
- `artifacts/offline/spark/reduced`

## Khởi động Docker Compose

Build và bật các service cần cho benchmark streaming:

```powershell
docker compose up -d --build zookeeper kafka ids-dev
docker compose ps
```

Trong container, Kafka bootstrap mặc định là `kafka:29092`. Từ host, Kafka được expose ở `localhost:9092`.

Khi cần dọn runtime sau benchmark:

```powershell
docker compose down
```

Nếu cần xóa cả volume Kafka/Zookeeper để chạy rất sạch:

```powershell
docker compose down -v
```

## Chạy benchmark streaming

Runbook tổng hợp chạy các benchmark chính phục vụ báo cáo:

```powershell
.\scripts\runbooks\streaming_paper_benchmarks.ps1
```

Runbook này tự tạo `IDS_STREAMING_TOPIC_NAMESPACE` theo timestamp để tránh topic cũ làm nhiễu kết quả. Các nhóm benchmark chính gồm:

- Capacity calibration.
- Model-feature tradeoff.
- Fault recovery.
- Overload degradation.

Nếu muốn chạy từng nhóm riêng:

```powershell
.\scripts\runbooks\streaming_capacity_calibration_3run.ps1
.\scripts\runbooks\streaming_model_feature_tradeoff_3run.ps1
.\scripts\runbooks\streaming_fault_recovery_3run.ps1
.\scripts\runbooks\streaming_overload_degradation_3run.ps1
```

Các kết quả streaming chính nằm dưới:

- `artifacts/streaming/evaluation`
- `artifacts/streaming/predictions`
- `artifacts/streaming/kafka_lag_timeseries`

## Tổng hợp bảng và hình

Sau khi benchmark chạy xong, build bảng và hình cho báo cáo:

```powershell
.\scripts\runbooks\streaming_build_paper_plots.ps1
```

Script này đọc latest summary CSV và artifacts hiện có để sinh:

- Bảng paper: `artifacts/streaming/evaluation/paper_tables`
- Hình paper: `paper/figures/plots`
- Manifest nguồn dữ liệu: `artifacts/streaming/evaluation/paper_tables/input_manifest.md`

## Luồng chạy khuyến nghị

```powershell
# 1. Cài môi trường
py -3.11 -m pip install uv
uv venv --python 3.11
uv sync

# 2. Khôi phục artifacts nếu có file lưu trữ
Expand-Archive -Path artifacts.zip -DestinationPath . -Force

# 3. Train offline artifacts từ data/gold/splits đã chuẩn bị
.\scripts\runbooks\offline_train_full.ps1
.\scripts\runbooks\offline_train_reduced.ps1

# 4. Start Docker services
docker compose up -d --build zookeeper kafka ids-dev

# 5. Run streaming benchmarks
.\scripts\runbooks\streaming_paper_benchmarks.ps1

# 6. Build report tables/plots
.\scripts\runbooks\streaming_build_paper_plots.ps1
```

## Ghi chú khi upload source

Không upload dữ liệu và artifact lớn. Chỉ commit source code, config, runbook và tài liệu cần thiết. Các thư mục sau là output tái sinh được:

- `data/**/*.parquet`
- `artifacts/offline/spark/**`
- `artifacts/streaming/**`
- `paper/**`
- `logs/**`
