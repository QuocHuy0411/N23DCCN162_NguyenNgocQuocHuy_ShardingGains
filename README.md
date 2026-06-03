# Sharding Gains

Sharding Gains là dự án mô phỏng và benchmark cơ sở dữ liệu phân tán sử dụng PostgreSQL, Docker Compose và Python.

Mục tiêu chính của dự án:

- Mô phỏng phân mảnh ngang dữ liệu `User_Logs`.
- So sánh thời gian truy vấn khi chạy với 1, 2 và 4 logical shard.
- Minh họa cơ chế fallback từ primary sang replica khi một shard primary bị dừng.
- Lưu kết quả benchmark ra CSV/JSON và hiển thị bằng dashboard Streamlit.

## HUONG DAN CHAY DU AN

Chạy các lệnh sau trong thư mục gốc của dự án:

### Chạy nhanh

```bash
pip install -r requirements.txt
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
streamlit run dashboard.py
```

Sau khi chạy lệnh cuối, mở dashboard tại địa chỉ Streamlit in ra trên terminal, thường là:

```text
http://localhost:8501
```

Kết quả benchmark được lưu tại:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

### Chạy từng bước

1. Cài thư viện Python:

```bash
pip install -r requirements.txt
```

2. Khởi động 8 container PostgreSQL:

```bash
docker compose up -d --build
docker ps
```

3. Sinh dữ liệu benchmark:

```bash
python -m coordinator.main generate --rows 1000000
```

Muốn sinh lại dữ liệu từ đầu:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

4. Khởi tạo schema:

```bash
python -m coordinator.main init-db
```

5. Nạp dữ liệu vào các shard:

```bash
python -m coordinator.main load
```

6. Chạy benchmark:

```bash
python -m coordinator.main benchmark
```

Chỉ chạy một kịch bản cụ thể:

```bash
python -m coordinator.main benchmark --nodes 4
```

Đổi số lần chạy mỗi kịch bản:

```bash
python -m coordinator.main benchmark --runs 5
```

7. Mở dashboard:

```bash
streamlit run dashboard.py
```

### Chạy thử nhanh với dữ liệu ít hơn

```bash
python -m coordinator.main generate --rows 100000 --force
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark --runs 5
streamlit run dashboard.py
```

Truy vấn benchmark chính đếm số log theo từng `user_id`:

```sql
WITH per_user_action AS (
    SELECT user_id, action, COUNT(*) AS action_count
    FROM user_logs_n{n}
    GROUP BY user_id, action
)
SELECT user_id, SUM(action_count) AS log_count
FROM per_user_action
GROUP BY user_id;
```

## Kiến trúc hệ thống

Hệ thống gồm một coordinator viết bằng Python và 8 container PostgreSQL:

```text
Python Coordinator
|-- Sinh dữ liệu CSV
|-- Khởi tạo schema
|-- Chia dữ liệu theo shard
|-- Nạp dữ liệu vào primary và replica
|-- Chạy benchmark song song
|-- Fallback primary -> replica khi truy vấn
|-- Gộp kết quả từ các shard
|-- Lưu kết quả CSV/JSON
`-- Dashboard Streamlit

PostgreSQL
|-- shard1_primary  |-- shard1_replica
|-- shard2_primary  |-- shard2_replica
|-- shard3_primary  |-- shard3_replica
`-- shard4_primary  `-- shard4_replica
```

Thông tin các node:

| Logical shard | Primary | Replica | Port primary | Port replica |
|---:|---|---|---:|---:|
| 1 | `shard1_primary` | `shard1_replica` | `5433` | `5443` |
| 2 | `shard2_primary` | `shard2_replica` | `5434` | `5444` |
| 3 | `shard3_primary` | `shard3_replica` | `5435` | `5445` |
| 4 | `shard4_primary` | `shard4_replica` | `5436` | `5446` |

Thông tin đăng nhập PostgreSQL:

```text
Database: userlogs
User:     benchmark
Password: benchmark
Host:     localhost
```

Lưu ý: replica trong dự án này không phải PostgreSQL streaming replication. Khi load dữ liệu, coordinator ghi cùng một partition vào primary và replica tương ứng. Cách này phù hợp với benchmark vì dữ liệu tĩnh và workload chỉ đọc.

## Cấu trúc thư mục

```text
.
|-- README.md
|-- AI-AGENT.md
|-- TAI_LIEU_THIET_KE.md
|-- docker-compose.yml
|-- dashboard.py
|-- requirements.txt
|
|-- coordinator/
|   |-- main.py
|   |-- config.py
|   |-- dataset_generator.py
|   |-- loader.py
|   |-- router.py
|   |-- benchmark.py
|   |-- db.py
|   |-- merger.py
|   `-- reporter.py
|
|-- db/
|   `-- init.sql
|
|-- data/
|   `-- user_logs.csv
|
`-- results/
    |-- benchmark_results.csv
    `-- benchmark_results.json
```

## Yêu cầu môi trường

Cần cài trước:

- Docker Desktop
- Docker Compose
- Python 3.10 trở lên
- `pip`

Kiểm tra nhanh:

```bash
docker --version
docker compose version
python --version
pip --version
```

Các thư viện Python chính:

- `psycopg2-binary`: kết nối PostgreSQL
- `tabulate`: in bảng kết quả trên terminal
- `streamlit`, `pandas`, `plotly`: dashboard

## Kết quả benchmark

CLI in bảng kết quả gồm các thông tin chính:

| Cột | Ý nghĩa |
|---|---|
| Số shard | Số logical shard của kịch bản |
| Thời gian chạy | Danh sách thời gian từng lần chạy, đơn vị giây |
| Trung bình | Thời gian trung bình |
| Trung vị | Thời gian dùng để tính speedup |
| P99 | Tail latency theo nearest-rank percentile |
| Mức tăng tốc | `T1 / Tn` |
| Hiệu suất | `speedup / số shard` |
| Số log đếm được | Tổng log sau khi coordinator gộp kết quả |
| Độ đầy đủ | `counted_logs / expected_logs * 100%` |
| S1..S4 | Nguồn dữ liệu dùng cho từng logical shard |

Ký hiệu nguồn shard:

| Ký hiệu | Ý nghĩa |
|---|---|
| `P` | Đọc từ primary |
| `R` | Primary lỗi, đọc từ replica |
| ô trống | Cả primary và replica đều lỗi |
| `-` | Shard không dùng trong kịch bản |
| `P/R` | Nguồn đọc thay đổi giữa các lần chạy |

Speedup và efficiency dùng thời gian trung vị:

```text
Speedup(n) = median_time_1_shard / median_time_n_shards
Efficiency(n) = Speedup(n) / n
```

## Mô hình chi phí

Ngoài thời gian chạy, dự án còn lưu một mô hình chi phí thực nghiệm:

```text
Cost = IO + CPU + Comm
```

Các thành phần:

| Thành phần | Cách đo |
|---|---|
| IO | Tổng PostgreSQL blocks từ `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` |
| CPU | Thời gian thực thi query trên shard cộng thời gian merge tại coordinator |
| Comm | Lượng dữ liệu kết quả truyền từ shard về coordinator, ước lượng bằng JSON bytes |
| Total Cost | `IO + CPU + Comm KB` |

Đây là chi phí thực nghiệm để liên hệ với mô hình chi phí trong cơ sở dữ liệu phân tán, không phải cost nội bộ tuyệt đối của PostgreSQL optimizer.

## Cách phân mảnh dữ liệu

Quy tắc router:

```text
nodes = 1: shard_id = 1
nodes = 2 hoặc 4: shard_id = (user_id % nodes) + 1
```

Ý nghĩa:

- Kịch bản 1 shard dùng shard 1.
- Kịch bản 2 shard dùng shard 1 và shard 2.
- Kịch bản 4 shard dùng shard 1, 2, 3 và 4.

Coordinator query các shard song song bằng `ThreadPoolExecutor`, sau đó merge kết quả theo `user_id`.

## Demo fallback

### Trạng thái bình thường

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S1 = P, S2 = P, S3 = P, S4 = P
Độ đầy đủ = 100%
```

### Dừng một primary

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 = R
Độ đầy đủ = 100%
```

### Dừng cả primary và replica của một shard

```bash
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 trống
Độ đầy đủ < 100%
Benchmark vẫn chạy xong và có cảnh báo kết quả một phần
```

### Khôi phục shard

```bash
docker start shard2_primary
docker start shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 = P
Độ đầy đủ = 100%
```

## Xử lý lỗi thường gặp

### Docker chưa chạy

Khởi động Docker Desktop, đợi Docker Engine sẵn sàng, rồi chạy lại:

```bash
docker compose up -d
```

### Port bị chiếm

Dự án dùng các port:

```text
5433, 5434, 5435, 5436
5443, 5444, 5445, 5446
```

Nếu port bị chiếm, dừng dịch vụ đang dùng port đó hoặc sửa đồng bộ trong `docker-compose.yml` và `coordinator/config.py`.

### Chưa có dataset

Nếu `load` báo không tìm thấy `data/user_logs.csv`, chạy:

```bash
python -m coordinator.main generate --rows 1000000
```

### Speedup là N/A

Nguyên nhân thường là chưa có baseline 1 shard đầy đủ. Chạy:

```bash
python -m coordinator.main benchmark
```

hoặc:

```bash
python -m coordinator.main benchmark --nodes 1
```

### Độ đầy đủ dưới 100%

Có ít nhất một logical shard không đọc được từ cả primary và replica.

Kiểm tra container:

```bash
docker ps -a
```

Khởi động lại node bị dừng, ví dụ:

```bash
docker start shard2_primary
docker start shard2_replica
```

Rồi chạy lại benchmark:

```bash
python -m coordinator.main benchmark --nodes 4
```

## Reset sạch

Dừng container:

```bash
docker compose down
```

Dừng container và xóa volume database:

```bash
docker compose down -v
```

Sau khi xóa volume, chạy lại từ đầu:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```
