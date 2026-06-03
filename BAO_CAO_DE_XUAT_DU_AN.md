# Distributed Database Project Proposal

Due Date: [Điền ngày nộp - Week 3]

Project ID & Category: [Điền mã đề tài] - Horizontal Fragmentation / Sharding Benchmark

## 1. Project Identity

Team Name: Sharding Gains

Team Members: [Điền tên thành viên 1], [Điền tên thành viên 2]

Project Title: Sharding Gains: Benchmark phân mảnh ngang dữ liệu User Logs trên PostgreSQL phân tán

## 2. Objective & Problem Statement

### The "Why"

Dự án giải quyết bài toán đánh giá hiệu quả của phân mảnh ngang trong cơ sở dữ liệu phân tán. Cụ thể, hệ thống kiểm tra liệu việc chia bảng log người dùng thành nhiều shard có giúp giảm thời gian truy vấn tổng hợp hay không, đồng thời vẫn đảm bảo kết quả đầy đủ khi một node primary gặp lỗi.

Bài toán chính của dự án là: với một bảng `User_Logs` có kích thước lớn, khi cần đếm tổng số log theo từng `user_id`, cấu hình 1 shard, 2 shard và 4 shard khác nhau như thế nào về thời gian thực thi, speedup, efficiency, chi phí xử lý và độ đầy đủ dữ liệu.

### Core Logic

Thuật toán chính được triển khai là phân mảnh ngang theo khóa `user_id` kết hợp truy vấn song song và merge kết quả tại coordinator.

Quy tắc định tuyến shard:

```text
nodes = 1: shard_id = 1
nodes = 2 hoặc 4: shard_id = (user_id % nodes) + 1
```

Mỗi logical shard có một primary và một replica. Khi truy vấn, coordinator ưu tiên đọc từ primary. Nếu primary lỗi, hệ thống fallback sang replica. Nếu cả primary và replica của một shard đều lỗi, benchmark vẫn tiếp tục chạy nhưng báo độ đầy đủ dữ liệu thấp hơn 100%.

## 3. Dataset Specification

Source: Dataset được sinh nội bộ bằng module `coordinator/dataset_generator.py`, lưu tại `data/user_logs.csv`. Dự án không phụ thuộc dataset ngoài.

Size: 1.000.000 dòng, khoảng 42,77 MB theo file hiện tại `data/user_logs.csv`.

Schema:

| Attribute | Type | Description |
|---|---|---|
| `id` | `BIGINT` | Mã định danh log, khóa chính. |
| `user_id` | `INT` | Mã người dùng, dùng làm khóa phân mảnh và khóa tổng hợp. |
| `action` | `VARCHAR(50)` | Hành động của người dùng, gồm `login`, `logout`, `view_product`, `search`, `add_to_cart`, `checkout`. |
| `created_at` | `TIMESTAMP` | Thời điểm phát sinh log. |

Thông số sinh dữ liệu:

| Parameter | Value |
|---|---:|
| Số dòng mặc định | 1.000.000 |
| Số user phân biệt | 100.000 |
| Random seed | 20260531 |
| Thời gian log | Từ `2025-01-01`, trải trong 1 năm |

Fragmentation Strategy:

Dữ liệu được phân mảnh ngang theo `user_id`. Với scenario 1 shard, toàn bộ dữ liệu nằm ở shard 1. Với scenario 2 hoặc 4 shard, mỗi dòng được đưa vào shard theo công thức modulo `(user_id % nodes) + 1`. Coordinator tạo các chunk CSV theo shard, sau đó nạp từng chunk vào bảng tương ứng trên primary và replica.

Dự án dùng ba bảng benchmark riêng:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

Cách này cho phép lưu đồng thời dữ liệu của cả ba scenario 1, 2 và 4 shard mà không cần reload mỗi khi chuyển cấu hình benchmark.

## 4. System Architecture

Nodes:

Hệ thống mô phỏng 4 logical shard. Mỗi logical shard gồm 1 primary và 1 replica, tổng cộng 8 container PostgreSQL.

| Logical shard | Primary | Replica | Primary port | Replica port |
|---:|---|---|---:|---:|
| 1 | `shard1_primary` | `shard1_replica` | 5433 | 5443 |
| 2 | `shard2_primary` | `shard2_replica` | 5434 | 5444 |
| 3 | `shard3_primary` | `shard3_replica` | 5435 | 5445 |
| 4 | `shard4_primary` | `shard4_replica` | 5436 | 5446 |

Communication Layer:

Coordinator Python giao tiếp với các PostgreSQL node qua TCP database connection bằng thư viện `psycopg2`. Trong quá trình benchmark, coordinator dùng `ThreadPoolExecutor` để gửi truy vấn song song tới nhiều shard.

Storage:

Dữ liệu được lưu vật lý trong các PostgreSQL container chạy bằng Docker Compose. Mỗi container có Docker volume riêng. Dataset gốc là file CSV tại `data/user_logs.csv`; kết quả benchmark được lưu tại:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

Kiến trúc tổng quát:

```text
Python Coordinator
|-- Generate dataset CSV
|-- Init database schema
|-- Split data by shard
|-- Load data into primary and replica
|-- Run parallel benchmark
|-- Fallback primary -> replica
|-- Merge shard results
|-- Save CSV/JSON reports
`-- Streamlit dashboard

PostgreSQL nodes
|-- shard1_primary  |-- shard1_replica
|-- shard2_primary  |-- shard2_replica
|-- shard3_primary  |-- shard3_replica
`-- shard4_primary  `-- shard4_replica
```

## 5. Tech Stack & Implementation Plan

Programming Language:

Python 3.10+.

Deployment:

Localhost processes kết hợp Docker Compose. PostgreSQL chạy trong 8 Docker container, coordinator và dashboard chạy trực tiếp bằng Python trên máy local.

Libraries/Frameworks:

| Technology | Role |
|---|---|
| PostgreSQL 16 Alpine | Database engine cho từng shard. |
| Docker Compose | Khởi tạo và quản lý 8 database node. |
| Python | Coordinator, router, loader, benchmark runner. |
| `psycopg2-binary` | Kết nối PostgreSQL và thực thi query/COPY. |
| `ThreadPoolExecutor` | Gửi truy vấn song song tới các shard. |
| `tabulate` | In bảng kết quả benchmark trên terminal. |
| Streamlit | Dashboard hiển thị kết quả benchmark. |
| Pandas, Plotly | Xử lý dữ liệu và trực quan hóa biểu đồ. |

Implementation Plan:

1. Sinh dataset `User_Logs` bằng seed cố định để kết quả benchmark có thể tái lập.
2. Khởi tạo schema trên toàn bộ primary và replica.
3. Chia dữ liệu theo số shard của từng scenario: 1, 2 và 4.
4. Nạp dữ liệu vào primary và replica bằng PostgreSQL `COPY`.
5. Chạy truy vấn tổng hợp song song trên các shard.
6. Merge kết quả `(user_id, log_count)` tại coordinator.
7. Đo thời gian, speedup, efficiency, P99 latency, completeness và cost model.
8. Lưu kết quả CSV/JSON và hiển thị bằng dashboard Streamlit.

Truy vấn benchmark chính:

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

## 6. Success Metrics & Analysis

Quantitative Metric:

Dự án đo các chỉ số định lượng sau:

| Metric | Meaning |
|---|---|
| `mean_time_seconds` | Thời gian chạy trung bình của mỗi scenario. |
| `median_time_seconds` | Thời gian trung vị, dùng làm cơ sở tính speedup. |
| `p99_time_seconds` | Tail latency theo nearest-rank percentile. |
| `speedup` | `T1 / Tn`, so sánh với baseline 1 shard. |
| `efficiency` | `speedup / số shard`. |
| `counted_logs` | Tổng số log đếm được sau khi merge. |
| `completeness_percent` | `counted_logs / expected_logs * 100%`. |
| `cost_units` | Mô hình chi phí thực nghiệm `IO + CPU + Comm`. |

Kết quả benchmark hiện tại với 1.000.000 log, chạy 20 lần mỗi scenario:

| Shards | Median (s) | Mean (s) | P99 (s) | Speedup | Efficiency | Completeness | Total Cost |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1.125801 | 1.195648 | 2.104571 | 1.000000 | 1.000000 | 100% | 24385.005293 |
| 2 | 0.703924 | 0.740455 | 1.037792 | 1.599321 | 0.799661 | 100% | 17586.779839 |
| 4 | 0.443871 | 0.479214 | 0.898562 | 2.536324 | 0.634081 | 100% | 14154.792512 |

Nhận xét:

1. Khi tăng từ 1 shard lên 2 shard, median time giảm từ 1.125801 giây xuống 0.703924 giây, speedup đạt khoảng 1,60 lần.
2. Khi tăng từ 1 shard lên 4 shard, median time giảm xuống 0.443871 giây, speedup đạt khoảng 2,54 lần.
3. Completeness luôn đạt 100%, chứng tỏ hệ thống tăng tốc mà không làm mất dữ liệu trong điều kiện bình thường.
4. Speedup không tuyến tính tuyệt đối vì vẫn tồn tại overhead tại coordinator, chi phí merge, chi phí truyền kết quả và độ lệch tốc độ giữa các container.
5. Total cost giảm khi tăng số shard, từ khoảng 24385 ở 1 shard xuống 14155 ở 4 shard.

The "Failure" Scenario:

Dự án mô phỏng lỗi phân tán bằng cách dừng một hoặc nhiều PostgreSQL container trong lúc benchmark.

Kịch bản 1: Dừng một primary.

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
```

Kết quả kỳ vọng: coordinator phát hiện primary của shard 2 không khả dụng, chuyển sang đọc từ `shard2_replica`. Nguồn đọc của S2 hiển thị là `R`, completeness vẫn đạt 100%.

Kịch bản 2: Dừng cả primary và replica của một shard.

```bash
docker stop shard2_primary
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kết quả kỳ vọng: shard 2 không có dữ liệu trả về, benchmark vẫn hoàn tất nhưng completeness thấp hơn 100%. Điều này chứng minh hệ thống không crash khi mất một logical shard, đồng thời có cơ chế báo kết quả một phần.

## 7. Project Milestones

Milestone 1 (Week 5): Environment setup and data fragmentation complete.

- Hoàn thiện Docker Compose với 8 PostgreSQL container.
- Tạo schema `user_logs_n1`, `user_logs_n2`, `user_logs_n4`.
- Sinh dataset `User_Logs` 1.000.000 dòng.
- Hoàn thiện router phân mảnh ngang theo `user_id`.
- Nạp dữ liệu vào primary và replica bằng `COPY`.

Milestone 2 (Week 8): Core algorithm operational.

- Hoàn thiện coordinator CLI với các lệnh `generate`, `init-db`, `load`, `benchmark`.
- Chạy truy vấn song song trên 1, 2 và 4 logical shard.
- Merge kết quả theo `user_id` tại coordinator.
- Đo mean, median, P99, speedup, efficiency và completeness.
- Lưu kết quả benchmark ra CSV/JSON.

Milestone 3 (Week 12): Failure handling and benchmarking complete.

- Hoàn thiện fallback từ primary sang replica.
- Mô phỏng lỗi bằng cách dừng container primary/replica.
- Kiểm chứng completeness khi primary lỗi và khi cả logical shard lỗi.
- Hoàn thiện dashboard Streamlit để trực quan hóa kết quả.
- Phân tích benchmark, cost model `IO + CPU + Comm` và viết báo cáo tổng kết.

