# Sharding Gains

Sharding Gains là dự án mô phỏng và benchmark cơ sở dữ liệu phân tán dùng PostgreSQL, Docker Compose và Python. Mục tiêu chính là cho thấy phân mảnh ngang giúp cải thiện thời gian truy vấn tổng hợp như thế nào, đồng thời minh họa cơ chế fallback từ primary sang replica khi một shard bị dừng thủ công.

Dữ liệu benchmark là bảng log người dùng `User_Logs` được sinh tự động. Truy vấn chính đếm số log theo từng `user_id`:

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

Dự án so sánh 3 kịch bản:

| Kịch bản | Bảng dữ liệu | Số logical shard | Ý nghĩa |
|---:|---|---:|---|
| 1 shard | `user_logs_n1` | 1 | Baseline |
| 2 shard | `user_logs_n2` | 2 | Phân mảnh trung bình |
| 4 shard | `user_logs_n4` | 4 | Phân mảnh lớn nhất trong dự án |

## 1. Kiến trúc

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

Replica trong dự án không phải PostgreSQL streaming replication. Khi load dữ liệu, coordinator ghi cùng một partition vào primary và replica tương ứng. Cách này phù hợp với benchmark vì dữ liệu tĩnh và workload chỉ đọc.

## 2. Cấu trúc thư mục

```text
.
|-- README.md
|-- AI-AGENT.md
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

## 3. Yêu cầu môi trường

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

Cài thư viện Python:

```bash
pip install -r requirements.txt
```

Các thư viện chính:

- `psycopg2-binary`: kết nối PostgreSQL
- `tabulate`: in bảng kết quả trên terminal
- `streamlit`, `pandas`, `plotly`: dashboard

## 4. Chạy nhanh từ đầu

Từ thư mục gốc dự án:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
streamlit run dashboard.py
```

Sau khi chạy xong, kết quả được lưu tại:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

Nếu dữ liệu và database đã được chuẩn bị từ trước, chạy lại benchmark chỉ cần:

```bash
python -m coordinator.main benchmark
```

## 5. Các lệnh chính

### Khởi động PostgreSQL

```bash
docker compose up -d --build
```

Nếu container đã tồn tại và không đổi cấu hình Docker:

```bash
docker compose up -d
```

### Sinh dữ liệu

```bash
python -m coordinator.main generate --rows 1000000
```

Lệnh này tạo `data/user_logs.csv`. Mặc định dữ liệu gồm:

- 1.000.000 dòng
- 100.000 user có thể xuất hiện
- các action: `login`, `logout`, `view_product`, `search`, `add_to_cart`, `checkout`
- thời gian log nằm trong năm bắt đầu từ `2025-01-01`
- seed cố định để có thể tái lập dữ liệu

Nếu file đã tồn tại, chương trình sẽ dùng lại. Muốn sinh lại:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

### Khởi tạo schema

```bash
python -m coordinator.main init-db
```

Lệnh này tạo 3 bảng trên toàn bộ primary và replica:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

Mỗi bảng có các cột:

```text
id, user_id, action, created_at
```

và index theo `user_id`.

### Nạp dữ liệu vào shard

```bash
python -m coordinator.main load
```

Luồng nạp dữ liệu:

1. Đọc `data/user_logs.csv`.
2. Chia dữ liệu thành các chunk theo số shard của từng kịch bản.
3. Truncate bảng tương ứng trên các node đang dùng.
4. Copy dữ liệu vào primary.
5. Copy cùng dữ liệu vào replica.

Giữ lại chunk tạm để kiểm tra:

```bash
python -m coordinator.main load --keep-chunks
```

### Chạy benchmark

Chạy toàn bộ 3 kịch bản 1, 2 và 4 shard:

```bash
python -m coordinator.main benchmark
```

Chỉ chạy một kịch bản:

```bash
python -m coordinator.main benchmark --nodes 4
```

Đổi số lần chạy mỗi kịch bản:

```bash
python -m coordinator.main benchmark --runs 5
```

Mặc định:

```text
runs = 20
```

## 6. Dashboard

Sau khi có kết quả benchmark, mở dashboard:

```bash
streamlit run dashboard.py
```

Dashboard đọc file:

```text
results/benchmark_results.json
```

Nếu chưa có file kết quả, dashboard sẽ nhắc chạy:

```bash
python -m coordinator.main benchmark
```

Dashboard hiển thị:

- tổng số log và số user
- số lần benchmark
- bảng thời gian từng lần chạy
- bảng tóm tắt benchmark
- biểu đồ thời gian trung vị
- biểu đồ speedup thực tế so với lý tưởng
- biểu đồ efficiency
- heatmap thời gian từng lần chạy
- bảng và biểu đồ mô hình chi phí Özsu

## 7. Kết quả benchmark

CLI in bảng gồm các thông tin chính:

| Cột | Ý nghĩa |
|---|---|
| Số shard | Số logical shard của kịch bản |
| Thời gian chạy | Danh sách thời gian từng lần chạy, đơn vị giây |
| Trung bình | Thời gian trung bình |
| Trung vị | Thời gian đại diện dùng để tính speedup |
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
| `trống` hoặc ô trống | Cả primary và replica đều lỗi |
| `-` | Shard không dùng trong kịch bản |
| `P/R` | Nguồn đọc thay đổi giữa các lần chạy |

Speedup và efficiency dùng thời gian trung vị:

```text
Speedup(n) = median_time_1_shard / median_time_n_shards
Efficiency(n) = Speedup(n) / n
```

Nếu chạy riêng `--nodes 2` hoặc `--nodes 4`, chương trình sẽ cố đọc baseline 1 shard từ `results/benchmark_results.json`. Nếu chưa có baseline đầy đủ, speedup và efficiency sẽ là `N/A`.

## 8. Mô hình chi phí Özsu

Ngoài thời gian chạy, dự án còn lưu và hiển thị một mô hình chi phí thực nghiệm:

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

## 9. Cách phân mảnh dữ liệu

Quy tắc router:

```text
nodes = 1: shard_id = 1
nodes = 2 hoặc 4: shard_id = (user_id % nodes) + 1
```

Ý nghĩa:

- kịch bản 1 shard dùng shard 1
- kịch bản 2 shard dùng shard 1 và shard 2
- kịch bản 4 shard dùng shard 1, 2, 3 và 4

Coordinator query các shard song song bằng `ThreadPoolExecutor`. Kết quả từng shard có dạng:

```text
user_id, log_count
```

Sau đó coordinator merge bằng cộng dồn:

```text
global_counts[user_id] += log_count
```

## 10. Demo lỗi và fallback

Dự án không tự động tắt container. Người dùng tự dừng node bằng Docker, sau đó chạy benchmark lại.

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

Coordinator thử primary trước. Nếu primary lỗi hoặc timeout, coordinator thử replica.

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

## 11. File kết quả

CSV và JSON lưu các trường chính:

```text
nodes
run_times_seconds
mean_time_seconds
median_time_seconds
p99_time_seconds
speedup
efficiency
counted_logs
expected_logs
completeness_percent
io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written
cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time
comm_rows_formula_rows_returned_to_coordinator
comm_kb_formula_comm_bytes_div_1024
total_cost_formula_io_plus_cpu_plus_comm_kb
s1_source
s2_source
s3_source
s4_source
notes
```

JSON còn có:

```json
{
  "dataset_rows": 1000000,
  "baseline_median_time_seconds": 0.0,
  "dataset": {
    "expected_logs": 1000000,
    "distinct_users": 100000
  },
  "benchmark_config": {
    "runs": 20,
    "scenarios": [1, 2, 4],
    "time_unit": "seconds"
  },
  "benchmark_results": []
}
```

## 12. Xử lý lỗi thường gặp

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

## 13. Reset sạch

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
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

Nếu `data/user_logs.csv` vẫn còn, không cần sinh lại dataset. Nếu muốn sinh lại:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

## 14. Kịch bản demo đề xuất

Chạy benchmark đầy đủ:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
streamlit run dashboard.py
```

Demo fallback sang replica:

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
streamlit run dashboard.py
```

Demo kết quả một phần:

```bash
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
streamlit run dashboard.py
```

Khôi phục:

```bash
docker start shard2_primary
docker start shard2_replica
python -m coordinator.main benchmark --nodes 4
```
