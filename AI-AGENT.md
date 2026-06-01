# Ngữ cảnh AI Agent - Sharding Gains

Tài liệu này mô tả trạng thái hiện tại của dự án để AI agent hoặc maintainer tiếp theo có thể đọc nhanh, hiểu đúng kiến trúc, và sửa code mà không làm lệch mục tiêu ban đầu.

## 1. Tổng quan dự án

**Sharding Gains** là project benchmark cơ sở dữ liệu phân tán dùng PostgreSQL, Docker Compose và Python coordinator để chứng minh hiệu quả mở rộng ngang bằng phân mảnh ngang dữ liệu.

Dữ liệu benchmark là bảng `User_Logs` sinh tổng hợp, mặc định **1.000.000 dòng**. Workload chính là truy vấn tổng hợp số log theo `user_id`. Trong code hiện tại, truy vấn được viết theo dạng CTE hai bước:

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

Truy vấn này vẫn trả kết quả cuối cùng tương đương yêu cầu `COUNT(*) GROUP BY user_id`, đồng thời giữ rõ bước tổng hợp theo `(user_id, action)`.

Project benchmark 3 layout:

| Scenario | Bảng dùng | Logical shards | Mục đích |
|---:|---|---:|---|
| 1 shard | `user_logs_n1` | 1 | Baseline |
| 2 shards | `user_logs_n2` | 2 | So sánh split trung bình |
| 4 shards | `user_logs_n4` | 4 | Split lớn nhất trong project |

Kết quả benchmark gồm:

- thời gian từng run;
- median time;
- speedup;
- efficiency;
- tổng log đếm được;
- completeness;
- nguồn đọc của từng shard: `P`, `R`, trống, hoặc `-`;
- file kết quả CSV/JSON trong `results/`.

## 2. Công nghệ và ràng buộc thiết kế

Đang dùng:

- Docker Compose;
- PostgreSQL 16 Alpine;
- Python coordinator;
- `psycopg2-binary`;
- `tabulate`;
- terminal output, không có web UI.

Không triển khai:

- 2PC;
- 3PC;
- API server;
- web dashboard;
- dashboard lỗi riêng;
- bảng lỗi riêng;
- health check riêng trước benchmark;
- script tự động `docker stop` / `docker start` node;
- PostgreSQL streaming replication.

Replication hiện tại là **application-level replication**: khi load dữ liệu, coordinator copy cùng partition vào primary và replica tương ứng. Vì dataset tĩnh và workload chỉ đọc, cách này đủ cho mục tiêu demo fallback.

## 3. Cấu trúc thư mục hiện tại

```text
.
|-- AI-AGENT.md
|-- README.md
|-- docker-compose.yml
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

## 4. Docker và PostgreSQL nodes

`docker-compose.yml` định nghĩa 8 PostgreSQL containers:

| Logical shard | Primary container | Replica container | Primary port | Replica port |
|---:|---|---|---:|---:|
| 1 | `shard1_primary` | `shard1_replica` | `5433` | `5443` |
| 2 | `shard2_primary` | `shard2_replica` | `5434` | `5444` |
| 3 | `shard3_primary` | `shard3_replica` | `5435` | `5445` |
| 4 | `shard4_primary` | `shard4_replica` | `5436` | `5446` |

Database credential dùng chung:

```text
POSTGRES_DB=userlogs
POSTGRES_USER=benchmark
POSTGRES_PASSWORD=benchmark
```

Mỗi container có volume riêng. `db/init.sql` được mount vào `/docker-entrypoint-initdb.d/init.sql`.

## 5. Schema

`db/init.sql` tạo 3 bảng có schema giống nhau:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

Mỗi bảng có:

```sql
id BIGINT PRIMARY KEY,
user_id INT NOT NULL,
action VARCHAR(50) NOT NULL,
created_at TIMESTAMP NOT NULL
```

Mỗi bảng có index theo `user_id`.

Lưu ý: nếu volume PostgreSQL đã tồn tại trước khi sửa `init.sql`, Docker entrypoint không tự chạy lại init script. Khi cần reset schema từ đầu, dùng:

```bash
docker compose down -v
docker compose up -d --build
```

Hoặc chạy:

```bash
python -m coordinator.main init-db
```

để apply schema lên toàn bộ primary và replica.

## 6. Các module Python

### `coordinator/config.py`

Chứa cấu hình trung tâm:

- path `data/`, `results/`, `db/init.sql`;
- DB credential;
- `DEFAULT_ROWS = 1_000_000`;
- `USER_ID_COUNT = 100_000`;
- `RANDOM_SEED = 20260531`;
- `SCENARIOS = (1, 2, 4)`;
- `DEFAULT_RUNS = 3`;
- timeout DB;
- mapping `TABLE_BY_NODES`;
- dataclass `DbEndpoint`;
- dataclass `LogicalShard`;
- mapping `SHARDS`.

Nếu đổi port hoặc tên container trong Docker Compose, phải cập nhật `SHARDS` tương ứng.

### `coordinator/main.py`

Định nghĩa CLI:

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

`--nodes` chỉ nhận `1`, `2`, hoặc `4`. `--runs` phải lớn hơn `0`.

### `coordinator/dataset_generator.py`

Sinh `data/user_logs.csv` với các cột:

```text
id,user_id,action,created_at
```

Đặc điểm:

- `id` chạy từ 1 đến số dòng yêu cầu;
- `user_id` random từ 1 đến 100.000;
- `action` lấy từ `ACTIONS`;
- `created_at` nằm trong năm bắt đầu từ `2025-01-01`;
- random seed cố định để tái lập;
- nếu file đã tồn tại thì không ghi đè, trừ khi dùng `--force`.

### `coordinator/router.py`

Quy tắc phân mảnh:

```python
if nodes == 1:
    shard_id = 1
else:
    shard_id = (user_id % nodes) + 1
```

Mapping thực tế:

- `nodes=1`: toàn bộ dữ liệu vào shard 1;
- `nodes=2`: dùng shard 1 và shard 2;
- `nodes=4`: dùng shard 1, 2, 3, 4.

### `coordinator/loader.py`

Luồng load:

1. kiểm tra `data/user_logs.csv`;
2. tạo các chunk CSV tạm trong `data/load_chunks`;
3. với từng scenario `1`, `2`, `4`:
   - chia dữ liệu theo `router.shard_id_for_user`;
   - truncate bảng tương ứng trên primary và replica của active shards;
   - dùng PostgreSQL `COPY` để load chunk vào primary;
   - copy cùng chunk vào replica tương ứng;
4. xóa `data/load_chunks` sau khi load, trừ khi dùng `--keep-chunks`.

Không load dữ liệu vào shard không tham gia scenario. Ví dụ `user_logs_n2` chỉ được load vào shard 1 và shard 2.

### `coordinator/db.py`

Chứa helper DB:

- `connect`;
- `EndpointConnectionPool`;
- `run_sql`;
- `run_init_sql`;
- `truncate_table`;
- `copy_csv_to_table`;
- `query_grouped_counts`;
- `query_grouped_counts_with_pool`;
- `iter_all_endpoints`.

Timeout hiện tại:

```text
connect_timeout = 2 seconds
statement_timeout = 30000 ms
```

Benchmark đang dùng `EndpointConnectionPool` để chuẩn bị connection trước khi đo thời gian. Pool max hiện tại là `1` connection mỗi endpoint vì mỗi logical shard chỉ có một worker query endpoint đó trong mỗi run.

### `coordinator/benchmark.py`

Đây là phần lõi benchmark.

Các dataclass chính:

- `ShardQueryResult`;
- `RunResult`;
- `ScenarioResult`;
- `LogicalShardPools`.

Luồng mỗi scenario:

1. build pool cho primary và replica của các active shards;
2. chạy `runs` lần;
3. mỗi run dùng `ThreadPoolExecutor(max_workers=nodes)`;
4. query các logical shard song song;
5. trong từng shard:
   - thử primary trước;
   - nếu primary lỗi hoặc timeout, thử replica;
   - nếu replica chạy được, source là `R`;
   - nếu cả hai lỗi, source là trống và rows rỗng;
6. merge rows bằng `merger.merge_count_rows`;
7. tính counted logs và completeness;
8. lấy median time;
9. tính speedup và efficiency nếu có baseline.

Baseline:

- Khi chạy toàn bộ scenario, scenario 1 shard đầy đủ sẽ làm baseline.
- Khi chạy riêng `--nodes 2` hoặc `--nodes 4`, code cố đọc baseline cũ từ `results/benchmark_results.json`.
- Nếu không có complete baseline, speedup và efficiency là `N/A`.

### `coordinator/merger.py`

Merge kết quả từ các shard bằng dictionary:

```python
global_counts[user_id] += log_count
```

Điều này an toàn ngay cả khi một `user_id` xuất hiện ở nhiều nguồn ngoài dự kiến.

### `coordinator/reporter.py`

In bảng terminal và lưu kết quả.

Terminal table có các cột:

```text
Nodes
Thời gian chạy (giây)
Trung vị
Mức tăng tốc
Hiệu suất
Số log đếm được
Độ đầy đủ
S1
S2
S3
S4
```

Chú giải:

```text
P = dùng primary
R = dùng replica
trống = primary và replica đều không khả dụng
- = shard không dùng trong kịch bản này
```

Nếu có replica được dùng, reporter in ghi chú. Nếu có shard thiếu hoặc độ đầy đủ < 100%, reporter in cảnh báo kết quả một phần.

Kết quả lưu vào:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

JSON cũng lưu `baseline_median_time_seconds` để lần chạy `--nodes 4` riêng vẫn có thể tính speedup nếu trước đó đã có baseline hợp lệ.

## 7. Luồng chạy chuẩn

Từ trạng thái sạch:

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

Sau khi đã setup và load dữ liệu, benchmark lại chỉ cần:

```bash
python -m coordinator.main benchmark
```

Chạy riêng 4 shard:

```bash
python -m coordinator.main benchmark --nodes 4
```

Chạy 5 lần mỗi scenario:

```bash
python -m coordinator.main benchmark --runs 5
```

## 8. Demo lỗi thủ công

Không viết code tự tắt container. Người dùng tự thao tác Docker rồi chạy benchmark lại.

### Tắt một primary

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 = R
Độ đầy đủ = 100%
```

### Tắt cả primary và replica của một shard

```bash
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 trống
Độ đầy đủ < 100%
Cảnh báo kết quả một phần
Benchmark không crash
```

### Khôi phục

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

## 9. Quy tắc khi sửa tiếp dự án

Giữ các nguyên tắc sau:

- Không thêm 2PC/3PC vì workload không có distributed write transaction.
- Không thêm web UI hoặc API server nếu không có yêu cầu mới rõ ràng.
- Không thêm health-check table hoặc bảng lỗi riêng.
- Không tạo script tự động dừng node.
- Không làm benchmark crash khi một shard chết.
- Không bỏ fallback primary -> replica.
- Không chạy query các shard tuần tự trong benchmark; phải giữ query song song.
- Không tính thời gian generate/load/start Docker vào benchmark.
- Không xóa logic lưu baseline trong JSON nếu vẫn muốn chạy riêng `--nodes 4` mà có speedup.
- Nếu đổi số lượng rows mặc định, cập nhật đồng bộ `DEFAULT_ROWS`, `EXPECTED_LOGS`, README và mô tả output.
- Nếu đổi query benchmark, cập nhật README, reporter header và tài liệu này.
- Nếu đổi port/container, cập nhật cả `docker-compose.yml` và `coordinator/config.py`.

## 10. Các điểm dễ nhầm

- `shard_id_for_user` trả về shard id bắt đầu từ 1, không phải 0.
- Với `nodes=1`, toàn bộ rows vào shard 1, không dùng công thức modulo.
- Replica không phải PostgreSQL streaming replica; nó là một DB container độc lập được load cùng dữ liệu.
- `init.sql` chỉ tự chạy khi volume PostgreSQL mới được tạo, nhưng lệnh `init-db` có thể chạy lại schema thủ công.
- `load` truncate và load lại các bảng đang dùng cho từng scenario.
- `benchmark --nodes 4` có thể hiện `N/A` speedup nếu chưa từng có baseline 1 shard hợp lệ trong JSON.
- Mức tăng tốc khi độ đầy đủ < 100% không nên xem là so sánh công bằng vì dữ liệu bị thiếu.

## 11. Tiêu chí trạng thái đúng

Project được xem là đúng với thiết kế hiện tại khi:

- Docker Compose tạo đủ 8 PostgreSQL containers;
- `generate` tạo hoặc reuse `data/user_logs.csv`;
- `init-db` tạo bảng trên toàn bộ primary/replica;
- `load` load đủ dữ liệu vào active primary/replica cho 3 scenario;
- `benchmark` chạy được 1, 2, 4 shards;
- mỗi scenario có đủ số run theo `--runs`;
- bảng terminal có median, speedup, efficiency, counted, completeness và source `S1..S4`;
- kết quả được lưu ra CSV/JSON;
- tắt một primary thì fallback sang replica và hiện `R`;
- tắt cả primary và replica của một shard thì shard đó trống, độ đầy đủ giảm, có cảnh báo, và chương trình vẫn in kết quả cuối.
