# Ngữ cảnh AI Agent - Sharding Gains

Tài liệu này dành cho AI agent hoặc maintainer tiếp tục sửa dự án. Mục tiêu là nắm nhanh kiến trúc hiện tại, các ràng buộc thiết kế và những điểm không nên làm lệch khi cập nhật code.

## 1. Tổng quan

Sharding Gains là dự án benchmark cơ sở dữ liệu phân tán dùng:

- PostgreSQL chạy bằng Docker Compose
- Python coordinator
- dữ liệu log người dùng sinh tổng hợp
- phân mảnh ngang theo `user_id`
- fallback từ primary sang replica tại thời điểm truy vấn
- báo cáo CLI, CSV, JSON và dashboard Streamlit

Workload chính là truy vấn tổng hợp số log theo `user_id`. Query trong code dùng CTE hai bước:

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

Kết quả cuối cùng tương đương đếm log theo từng user. Phần `(user_id, action)` là bước tổng hợp trung gian.

Các kịch bản benchmark:

| Nodes | Bảng | Logical shard |
|---:|---|---:|
| 1 | `user_logs_n1` | shard 1 |
| 2 | `user_logs_n2` | shard 1, 2 |
| 4 | `user_logs_n4` | shard 1, 2, 3, 4 |

## 2. Trạng thái công nghệ hiện tại

Đang có:

- Docker Compose với 8 container PostgreSQL 16 Alpine
- 4 logical shard, mỗi shard có 1 primary và 1 replica
- application-level replication trong lúc load dữ liệu
- Python CLI ở `coordinator/main.py`
- Streamlit dashboard ở `dashboard.py`
- connection pool trước khi đo benchmark
- query song song bằng `ThreadPoolExecutor`
- fallback primary -> replica khi query
- lưu kết quả vào `results/benchmark_results.csv` và `results/benchmark_results.json`
- mô hình chi phí thực nghiệm Özsu: `Cost = IO + CPU + Comm`

Không có:

- PostgreSQL streaming replication
- distributed write transaction
- 2PC hoặc 3PC
- API server
- lệnh CLI `ui`
- script tự động dừng hoặc khởi động container để demo lỗi
- health-check phase riêng trước benchmark

Nếu cần mở dashboard, dùng trực tiếp:

```bash
streamlit run dashboard.py
```

## 3. Cấu trúc file

```text
.
|-- AI-AGENT.md
|-- README.md
|-- docker-compose.yml
|-- dashboard.py
|-- requirements.txt
|
|-- coordinator/
|   |-- __init__.py
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
|   |-- .gitkeep
|   `-- user_logs.csv
|
`-- results/
    |-- .gitkeep
    |-- benchmark_results.csv
    `-- benchmark_results.json
```

## 4. Docker và database

`docker-compose.yml` định nghĩa 8 PostgreSQL container:

| Shard | Primary | Replica | Primary port | Replica port |
|---:|---|---|---:|---:|
| 1 | `shard1_primary` | `shard1_replica` | `5433` | `5443` |
| 2 | `shard2_primary` | `shard2_replica` | `5434` | `5444` |
| 3 | `shard3_primary` | `shard3_replica` | `5435` | `5445` |
| 4 | `shard4_primary` | `shard4_replica` | `5436` | `5446` |

Credential dùng chung:

```text
POSTGRES_DB=userlogs
POSTGRES_USER=benchmark
POSTGRES_PASSWORD=benchmark
```

Mỗi container có volume riêng. `db/init.sql` được mount vào `/docker-entrypoint-initdb.d/init.sql`, nhưng file init này chỉ tự chạy khi volume mới được tạo. Lệnh `python -m coordinator.main init-db` có thể apply schema lại thủ công trên toàn bộ endpoint.

## 5. Schema

`db/init.sql` tạo 3 bảng:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

Schema giống nhau:

```sql
id BIGINT PRIMARY KEY,
user_id INT NOT NULL,
action VARCHAR(50) NOT NULL,
created_at TIMESTAMP NOT NULL
```

Mỗi bảng có index theo `user_id`.

## 6. Cấu hình trung tâm

File `coordinator/config.py` chứa:

- đường dẫn `data`, `results`, `db/init.sql`
- credential PostgreSQL
- `DEFAULT_ROWS = 1_000_000`
- `USER_ID_COUNT = 100_000`
- `RANDOM_SEED = 20260531`
- `ACTIONS`
- `SCENARIOS = (1, 2, 4)`
- `DEFAULT_RUNS = 20`
- `EXPECTED_LOGS = DEFAULT_ROWS`
- timeout kết nối và statement
- `POOL_MIN_CONNECTIONS = 1`
- `POOL_MAX_CONNECTIONS = 1`
- mapping `TABLE_BY_NODES`
- dataclass `DbEndpoint`, `LogicalShard`
- mapping `SHARDS`

Nếu đổi port, tên container hoặc số shard, phải cập nhật đồng bộ `docker-compose.yml`, `coordinator/config.py`, README và các phần dashboard/reporter liên quan.

## 7. CLI hiện tại

File `coordinator/main.py` hỗ trợ các lệnh:

```bash
python -m coordinator.main generate --rows 1000000
python -m coordinator.main generate --rows 1000000 --force
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main load --keep-chunks
python -m coordinator.main benchmark
python -m coordinator.main benchmark --nodes 4
python -m coordinator.main benchmark --runs 5
```

`--nodes` chỉ nhận `1`, `2`, `4`. `--runs` phải lớn hơn `0`.

Hiện không có subcommand `ui`.

## 8. Sinh dữ liệu

File `coordinator/dataset_generator.py` sinh `data/user_logs.csv` với cột:

```text
id,user_id,action,created_at
```

Đặc điểm:

- `id` tăng tuần tự từ 1
- `user_id` random từ 1 đến 100.000
- `action` lấy từ `ACTIONS`
- `created_at` random trong khoảng một năm từ `2025-01-01`
- seed cố định để tái lập
- không ghi đè file đã có, trừ khi dùng `--force`

## 9. Router và phân mảnh

File `coordinator/router.py` dùng quy tắc:

```python
if nodes == 1:
    shard_id = 1
else:
    shard_id = (user_id % nodes) + 1
```

Lưu ý:

- shard id bắt đầu từ 1
- kịch bản 1 shard luôn dùng shard 1
- kịch bản 2 shard chỉ dùng shard 1 và 2
- kịch bản 4 shard dùng shard 1 đến 4

## 10. Load dữ liệu

File `coordinator/loader.py` thực hiện:

1. Kiểm tra `data/user_logs.csv`.
2. Xóa `data/load_chunks` cũ nếu có.
3. Với từng scenario `1`, `2`, `4`:
   - tạo chunk CSV theo shard
   - truncate bảng tương ứng trên primary và replica của các shard đang dùng
   - dùng PostgreSQL `COPY` để nạp chunk vào primary
   - nạp cùng chunk vào replica
4. Xóa chunk tạm, trừ khi dùng `--keep-chunks`.

Replica là database độc lập có cùng dữ liệu với primary sau bước load, không phải replica streaming.

## 11. DB helper

File `coordinator/db.py` chứa:

- `connect`
- `EndpointConnectionPool`
- `run_sql`
- `run_init_sql`
- `truncate_table`
- `copy_csv_to_table`
- `query_grouped_counts`
- `query_grouped_counts_with_pool`
- `explain_grouped_counts_cost_with_pool`
- `iter_all_endpoints`

Timeout hiện tại:

```text
connect_timeout = 2 seconds
statement_timeout = 30000 ms
```

Pool hiện chỉ cần 1 connection mỗi endpoint vì trong một benchmark run, mỗi logical shard chỉ query một endpoint tại một thời điểm.

## 12. Benchmark core

File `coordinator/benchmark.py` là lõi đo hiệu năng.

Dataclass chính:

- `ShardQueryResult`
- `RunResult`
- `ScenarioResult`
- `LogicalShardPools`

Luồng một scenario:

1. Validate nodes.
2. Tạo pool cho primary và replica của các active shard.
3. Chạy `runs` lần.
4. Mỗi lần dùng `ThreadPoolExecutor(max_workers=nodes)`.
5. Mỗi logical shard thử query primary trước.
6. Nếu primary lỗi hoặc timeout, thử replica.
7. Nếu replica cũng lỗi, trả rows rỗng cho shard đó.
8. Merge kết quả tại coordinator.
9. Tính `counted_logs` và `completeness_percent`.
10. Thu thập cost bằng `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.
11. Tính mean, median, P99, speedup, efficiency và cost trung bình.

Benchmark đo:

- thời gian query song song
- thời gian fetch kết quả
- thời gian merge tại coordinator

Benchmark không đo:

- thời gian sinh dữ liệu
- thời gian load dữ liệu
- thời gian khởi động Docker
- chi phí tạo kết nối lặp lại cho từng query, vì pool được chuẩn bị trước scenario

## 13. Baseline và speedup

Khi chạy toàn bộ:

```bash
python -m coordinator.main benchmark
```

scenario 1 shard đầy đủ sẽ làm baseline.

Khi chạy riêng:

```bash
python -m coordinator.main benchmark --nodes 4
```

code cố đọc baseline cũ từ `results/benchmark_results.json`.

Nếu không có baseline đầy đủ:

```text
speedup = N/A
efficiency = N/A
```

Không nên xem speedup là so sánh công bằng nếu `completeness_percent < 100`.

## 14. Mô hình chi phí Özsu

Code hiện có mô hình:

```text
Cost = IO + CPU + Comm
```

Nguồn dữ liệu:

- IO lấy từ block metrics trong `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`
- CPU là actual time của query trên shard cộng thời gian merge tại coordinator
- Comm là số dòng và bytes kết quả trả về coordinator, bytes ước lượng bằng JSON
- Total Cost là `IO + CPU + Comm KB`

Các field liên quan trong CSV/JSON:

```text
io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written
cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time
comm_rows_formula_rows_returned_to_coordinator
comm_kb_formula_comm_bytes_div_1024
total_cost_formula_io_plus_cpu_plus_comm_kb
```

Đây là cost thực nghiệm phục vụ báo cáo môn cơ sở dữ liệu phân tán, không phải PostgreSQL planner cost tuyệt đối.

## 15. Reporter và file kết quả

File `coordinator/reporter.py`:

- in bảng benchmark CLI
- in chú giải `P`, `R`, `trống`, `-`
- cảnh báo khi dùng replica
- cảnh báo khi kết quả không đầy đủ
- in bảng cost model
- lưu CSV
- lưu JSON
- giữ baseline median time để các lần chạy riêng vẫn tính được speedup

Kết quả lưu tại:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

JSON có cả `benchmark_results` cho dashboard mới và `results` dạng phẳng để tương thích.

## 16. Dashboard

File `dashboard.py` dùng Streamlit, pandas và Plotly.

Luồng:

1. Đọc `results/benchmark_results.json`.
2. Nếu chưa có file, hiển thị lệnh cần chạy thay vì crash.
3. Chuẩn hóa dữ liệu từ `benchmark_results` hoặc fallback sang `results`.
4. Hiển thị metric cards.
5. Hiển thị bảng thời gian từng lần chạy.
6. Hiển thị bảng tóm tắt benchmark.
7. Hiển thị chart median time, speedup, efficiency, heatmap.
8. Hiển thị bảng và chart cost model Özsu.

Chạy dashboard bằng:

```bash
streamlit run dashboard.py
```

Không thêm lại lệnh `python -m coordinator.main ui` nếu chưa bổ sung thật trong parser.

## 17. Demo lỗi thủ công

Không tự động dừng container bằng code. Demo đúng là người dùng tự chạy Docker command.

Dừng một primary:

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 = R
Độ đầy đủ = 100%
```

Dừng replica của cùng shard:

```bash
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 trống
Độ đầy đủ < 100%
Benchmark không crash
Có cảnh báo kết quả một phần
```

Khôi phục:

```bash
docker start shard2_primary
docker start shard2_replica
python -m coordinator.main benchmark --nodes 4
```

## 18. Quy tắc khi sửa tiếp

Giữ các nguyên tắc sau:

- Không thêm 2PC/3PC nếu chỉ benchmark workload đọc.
- Không biến replica thành streaming replication nếu chưa có yêu cầu rõ ràng.
- Không bỏ fallback primary -> replica.
- Không query shard tuần tự trong benchmark; phải giữ query song song.
- Không tính thời gian generate/load/Docker startup vào benchmark.
- Không để chương trình crash khi một logical shard mất cả primary và replica.
- Không xóa logic lưu baseline trong JSON nếu vẫn cần chạy riêng `--nodes 2` hoặc `--nodes 4`.
- Không thêm lệnh README trừ khi parser thực sự hỗ trợ.
- Nếu đổi query benchmark, cập nhật `db.py`, README, dashboard và giải thích cost nếu bị ảnh hưởng.
- Nếu đổi schema, cập nhật `db/init.sql`, loader, README và reset-volume guidance.
- Nếu đổi số rows mặc định, cập nhật `DEFAULT_ROWS`, `EXPECTED_LOGS`, README và mô tả dashboard.
- Nếu đổi số shard, cần sửa nhiều nơi: Docker Compose, config, router, loader, reporter, dashboard, README.

## 19. Điểm dễ nhầm

- `shard_id_for_user` trả shard id bắt đầu từ 1.
- Với `nodes=1`, không dùng modulo mà đưa toàn bộ dữ liệu vào shard 1.
- `user_logs_n2` không được load vào shard 3 và 4.
- `user_logs_n4` được load vào cả 4 shard.
- Replica chỉ có dữ liệu sau khi chạy `load`.
- Nếu chạy `docker compose down -v`, cần `init-db` và `load` lại.
- Nếu `benchmark --nodes 4` hiện `N/A` speedup, thường là chưa có baseline 1 shard đầy đủ trong JSON.
- Nếu source hiển thị `P/R`, nghĩa là nguồn đọc thay đổi giữa các lần chạy trong cùng scenario.
- Nếu completeness dưới 100%, không nên so sánh speedup như kết quả đầy đủ.

## 20. Tiêu chí dự án hoạt động đúng

Dự án được xem là đúng với thiết kế hiện tại khi:

- Docker Compose tạo đủ 8 PostgreSQL container.
- `generate` tạo hoặc reuse `data/user_logs.csv`.
- `init-db` tạo bảng trên toàn bộ primary và replica.
- `load` nạp dữ liệu cho cả 3 kịch bản 1, 2, 4 shard.
- `benchmark` chạy được toàn bộ hoặc từng scenario.
- Mỗi scenario có đủ số run theo `--runs`.
- CLI in bảng thời gian, speedup, efficiency, completeness và source `S1..S4`.
- CLI in bảng cost model Özsu.
- CSV/JSON được lưu đúng.
- Dashboard đọc JSON và hiển thị bảng/biểu đồ.
- Dừng một primary thì dùng replica và hiện `R`.
- Dừng cả primary và replica của một shard thì kết quả một phần, có cảnh báo và chương trình vẫn kết thúc bình thường.
