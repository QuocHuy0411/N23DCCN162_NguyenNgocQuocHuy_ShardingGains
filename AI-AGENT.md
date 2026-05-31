# Yêu cầu triển khai dự án: Horizontal Scaling Efficiency - Sharding Gains

## 1. Bối cảnh dự án

Tôi đang làm một dự án môn Cơ sở dữ liệu phân tán với chủ đề:

**Horizontal Scaling Efficiency: “Sharding Gains”**

Mục tiêu của dự án là xây dựng một hệ thống cơ sở dữ liệu phân tán mô phỏng bằng Docker và PostgreSQL để đo hiệu quả mở rộng ngang bằng phương pháp phân mảnh ngang dữ liệu.

Dataset là bảng `User_Logs` gồm **1.000.000 bản ghi log người dùng**.

Truy vấn benchmark chính là:

```sql
SELECT user_id, COUNT(*)
FROM user_logs
GROUP BY user_id;
```

Dự án cần so sánh hiệu năng khi dữ liệu được chia trên:

* 1 shard
* 2 shard
* 4 shard

Sau đó tính:

* thời gian chạy từng lần
* thời gian trung vị
* speedup
* efficiency

Ngoài phần benchmark bình thường, dự án cần có replication để tôi có thể **tự tắt shard bằng Docker** rồi chạy lại benchmark, quan sát hệ thống phản ứng.

---

## 2. Công nghệ bắt buộc sử dụng

Sử dụng các công nghệ sau:

* Docker
* Docker Compose
* PostgreSQL
* Python làm coordinator
* Terminal output, không cần giao diện web

Không sử dụng:

* 2PC
* 3PC
* Web UI
* API server
* Dashboard lỗi riêng
* Cơ chế tự động tạo lỗi
* Cơ chế tự động tắt node
* Bảng lỗi riêng
* Health check riêng trước khi chạy benchmark

---

## 3. Tư tưởng thiết kế chính

Tôi muốn hệ thống theo hướng:

> Tôi là người chủ động tắt shard bằng Docker, sau đó bấm chạy benchmark lại. Chương trình không tự tạo lỗi, không tự tắt container, không có bảng lỗi riêng. Bảng benchmark dùng chung cho cả trường hợp bình thường và trường hợp có shard bị tắt.

Khi chạy benchmark:

* Shard nào chạy được thì in kết quả bình thường.
* Nếu primary của shard bị tắt thì thử đọc từ replica.
* Nếu replica đọc được thì ô shard đó hiển thị `R`.
* Nếu primary đọc được thì ô shard đó hiển thị `P`.
* Nếu cả primary và replica của shard đều tắt thì ô shard đó để trống.
* Các shard còn chạy được vẫn tiếp tục được query.
* Không được để chương trình crash chỉ vì một shard bị tắt.
* Không được dừng toàn bộ benchmark nếu một shard không phản hồi.
* Bên dưới bảng kết quả cần ghi chú rõ nếu kết quả bị thiếu dữ liệu.

---

## 4. Kiến trúc tổng thể cần triển khai

Thiết kế hệ thống gồm:

```text
Python Coordinator
 ├── Dataset Generator
 ├── Data Loader
 ├── Shard Router
 ├── Benchmark Runner
 ├── Query-time Fallback Handler
 ├── Result Merger
 └── Terminal Reporter

PostgreSQL Docker Containers
 ├── shard1_primary
 ├── shard1_replica
 ├── shard2_primary
 ├── shard2_replica
 ├── shard3_primary
 ├── shard3_replica
 ├── shard4_primary
 └── shard4_replica
```

Mỗi logical shard có 2 PostgreSQL container:

```text
shard1 = shard1_primary + shard1_replica
shard2 = shard2_primary + shard2_replica
shard3 = shard3_primary + shard3_replica
shard4 = shard4_primary + shard4_replica
```

Replica trong dự án này dùng theo kiểu đơn giản ở mức ứng dụng:

* Khi load dữ liệu vào primary shard, cũng load cùng dữ liệu đó vào replica tương ứng.
* Không cần PostgreSQL streaming replication thật.
* Dataset là dữ liệu tĩnh để benchmark nên application-level replication là đủ.
* Workload chính là truy vấn đọc, không có giao dịch ghi phân tán.

---

## 5. Ràng buộc rất quan trọng

### 5.1. Không dùng 2PC/3PC

Không triển khai 2PC hoặc 3PC.

Lý do: dự án chỉ benchmark truy vấn đọc/tổng hợp `COUNT GROUP BY user_id`, không có transaction ghi đồng thời trên nhiều shard.

### 5.2. Không thiết kế health check riêng

Không tạo module kiểu:

```text
HealthChecker kiểm tra trước shard nào UP/DOWN
```

Không in bảng health check riêng.

Tuy nhiên vẫn cần dùng:

* `try/except`
* `connect_timeout`
* `statement_timeout`

để tránh chương trình bị treo khi query vào một container đã bị tắt.

Đây không được xem là health check riêng, mà chỉ là xử lý lỗi tại thời điểm query.

### 5.3. Không tự động tạo lỗi

Không viết code tự động:

```bash
docker stop ...
docker start ...
```

Không tạo script tự động kill node.

Việc tắt/bật node sẽ do tôi tự làm thủ công bằng Docker.

### 5.4. Không có bảng lỗi riêng

Không tạo bảng kiểu:

```text
FAILURE TEST RESULT
```

Tất cả trường hợp bình thường và lỗi đều dùng cùng một bảng benchmark.

---

## 6. Cấu trúc thư mục mong muốn

Tạo project với cấu trúc gợi ý như sau:

```text
sharding-gains/
│
├── docker-compose.yml
├── README.md
├── requirements.txt
│
├── coordinator/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── dataset_generator.py
│   ├── loader.py
│   ├── router.py
│   ├── benchmark.py
│   ├── db.py
│   ├── merger.py
│   └── reporter.py
│
├── db/
│   └── init.sql
│
├── data/
│   └── user_logs.csv
│
└── results/
    ├── benchmark_results.csv
    └── benchmark_results.json
```

Có thể điều chỉnh tên file nếu cần, nhưng phải giữ logic rõ ràng.

---

## 7. Docker Compose

Tạo `docker-compose.yml` với 8 PostgreSQL containers:

```text
shard1_primary
shard1_replica
shard2_primary
shard2_replica
shard3_primary
shard3_replica
shard4_primary
shard4_replica
```

Thông tin database dùng chung:

```text
POSTGRES_DB=userlogs
POSTGRES_USER=benchmark
POSTGRES_PASSWORD=benchmark
```

Port gợi ý:

```text
shard1_primary  -> localhost:5433
shard1_replica  -> localhost:5443

shard2_primary  -> localhost:5434
shard2_replica  -> localhost:5444

shard3_primary  -> localhost:5435
shard3_replica  -> localhost:5445

shard4_primary  -> localhost:5436
shard4_replica  -> localhost:5446
```

Mỗi container có volume riêng để dữ liệu không bị lẫn nhau.

Không cần cấu hình PostgreSQL streaming replication.

Không cần Docker healthcheck.

---

## 8. Schema database

Tạo bảng User Logs cho từng scenario benchmark.

Để có thể benchmark cả 1 shard, 2 shard, 4 shard mà không phải reload lại dữ liệu liên tục, nên tạo 3 bảng:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

Schema các bảng giống nhau:

```sql
CREATE TABLE IF NOT EXISTS user_logs_n1 (
    id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_logs_n1_user_id
ON user_logs_n1(user_id);
```

Tương tự cho:

```text
user_logs_n2
user_logs_n4
```

Mỗi bảng tương ứng một cách chia dữ liệu:

* `user_logs_n1`: dùng cho benchmark 1 shard
* `user_logs_n2`: dùng cho benchmark 2 shard
* `user_logs_n4`: dùng cho benchmark 4 shard

---

## 9. Dataset Generator

Tạo dataset gồm đúng:

```text
1.000.000 dòng User_Logs
```

Schema dữ liệu:

```text
id
user_id
action
created_at
```

Yêu cầu:

* `id` chạy từ 1 đến 1.000.000.
* `user_id` có số lượng là 100.000.
* `action` chọn ngẫu nhiên từ một số giá trị như:

  * login
  * logout
  * view_product
  * search
  * add_to_cart
  * checkout
* `created_at` là timestamp hợp lệ.
* Dùng random seed cố định để kết quả có thể tái lập.
* Lưu dataset vào:

```text
data/user_logs.csv
```

Nếu file đã tồn tại thì không cần sinh lại, trừ khi có tham số `--force`.

---

## 10. Quy tắc phân mảnh ngang

Dùng hash-based horizontal fragmentation theo `user_id`.

Công thức:

```text
shard_index = user_id % number_of_shards
```

Mapping:

```text
0 -> shard1
1 -> shard2
2 -> shard3
3 -> shard4
```

Với 1 shard:

```text
Tất cả dữ liệu vào shard1
```

Với 2 shard:

```text
user_id % 2 = 0 -> shard1
user_id % 2 = 1 -> shard2
```

Với 4 shard:

```text
user_id % 4 = 0 -> shard1
user_id % 4 = 1 -> shard2
user_id % 4 = 2 -> shard3
user_id % 4 = 3 -> shard4
```

---

## 11. Data Loader

Khi load dữ liệu:

### Với scenario 1 shard

Load toàn bộ 1.000.000 dòng vào:

```text
shard1_primary.user_logs_n1
shard1_replica.user_logs_n1
```

Các shard khác không cần dữ liệu cho bảng `user_logs_n1`.

### Với scenario 2 shard

Chia dữ liệu theo `user_id % 2`, load vào:

```text
shard1_primary.user_logs_n2
shard1_replica.user_logs_n2

shard2_primary.user_logs_n2
shard2_replica.user_logs_n2
```

### Với scenario 4 shard

Chia dữ liệu theo `user_id % 4`, load vào:

```text
shard1_primary.user_logs_n4
shard1_replica.user_logs_n4

shard2_primary.user_logs_n4
shard2_replica.user_logs_n4

shard3_primary.user_logs_n4
shard3_replica.user_logs_n4

shard4_primary.user_logs_n4
shard4_replica.user_logs_n4
```

Trước khi load lại, cần truncate các bảng liên quan để tránh trùng dữ liệu.

Dữ liệu trên replica phải giống primary tương ứng.

---

## 12. Benchmark Query

Với mỗi scenario `n` trong `{1, 2, 4}`, query bảng tương ứng:

```text
n = 1 -> user_logs_n1
n = 2 -> user_logs_n2
n = 4 -> user_logs_n4
```

Query cần chạy trên từng shard:

```sql
SELECT user_id, COUNT(*) AS log_count
FROM user_logs_n{n}
GROUP BY user_id;
```

Ví dụ với 4 shard:

* Gửi query đến shard1
* Gửi query đến shard2
* Gửi query đến shard3
* Gửi query đến shard4

Các query phải chạy song song, không chạy tuần tự.

Dùng Python:

```text
ThreadPoolExecutor
```

hoặc cơ chế tương đương.

Lý do: nếu query tuần tự thì khó thể hiện lợi ích của sharding.

---

## 13. Query-time fallback logic

Không health check trước.

Khi benchmark cần query một logical shard, làm như sau:

```text
1. Thử query primary.
2. Nếu primary query thành công:
   - source = "P"
   - dùng kết quả từ primary.
3. Nếu primary lỗi hoặc timeout:
   - thử query replica.
4. Nếu replica query thành công:
   - source = "R"
   - dùng kết quả từ replica.
5. Nếu cả primary và replica đều lỗi:
   - source = ""
   - result = []
   - counted = 0
   - không crash chương trình.
```

Pseudo-code:

```python
def query_logical_shard(logical_shard, table_name):
    try:
        result = query_database(logical_shard.primary, table_name)
        return {
            "source": "P",
            "rows": result,
            "counted": sum(row["log_count"] for row in result)
        }
    except Exception:
        pass

    try:
        result = query_database(logical_shard.replica, table_name)
        return {
            "source": "R",
            "rows": result,
            "counted": sum(row["log_count"] for row in result)
        }
    except Exception:
        return {
            "source": "",
            "rows": [],
            "counted": 0
        }
```

Phải có timeout ngắn, ví dụ:

```text
connect_timeout = 2 giây
statement_timeout = 30 giây
```

để tránh chương trình bị treo lâu khi container đã bị tắt.

---

## 14. Cách đo thời gian

Mỗi scenario chạy 3 lần.

Ví dụ:

```text
1 shard -> chạy 3 lần
2 shard -> chạy 3 lần
4 shard -> chạy 3 lần
```

Mỗi lần đo wall-clock time bằng Python:

```python
time.perf_counter()
```

Thời gian đo phải bao gồm:

* gửi query song song đến các shard
* nhận kết quả
* merge kết quả ở coordinator

Không tính thời gian:

* sinh dataset
* load dữ liệu
* khởi động Docker

Với mỗi scenario, lưu:

```text
run1_time
run2_time
run3_time
median_time
```

Median là trung vị của 3 lần chạy.

---

## 15. Merge kết quả

Do dữ liệu được shard theo `user_id`, mỗi `user_id` chỉ thuộc về một logical shard trong cùng scenario.

Vì vậy merge kết quả có thể thực hiện bằng cách nối kết quả từ các shard.

Tuy nhiên để an toàn, vẫn có thể merge bằng dictionary:

```text
global_counts[user_id] += log_count
```

Sau khi merge, tính:

```text
counted_logs = tổng tất cả log_count
```

Với kết quả đầy đủ:

```text
counted_logs = 1.000.000
```

Nếu một logical shard bị mất cả primary và replica thì:

```text
counted_logs < 1.000.000
```

---

## 16. Công thức tính benchmark

### Speedup

```text
Speedup(n) = T1 / Tn
```

Trong đó:

```text
T1 = median time của scenario 1 shard
Tn = median time của scenario n shard
```

### Efficiency

```text
Efficiency(n) = Speedup(n) / n
```

Với 1 shard:

```text
Speedup = 1.00
Efficiency = 1.00
```

Nếu baseline 1 shard không có kết quả đầy đủ thì speedup và efficiency nên hiển thị `N/A`.

Nếu một scenario có completeness < 100%, vẫn có thể in speedup/efficiency nhưng phải ghi chú bên dưới rằng kết quả không nên so sánh trực tiếp vì dữ liệu bị thiếu.

---

## 17. Terminal output bắt buộc

Kết quả phải in ra terminal bằng một bảng duy nhất dùng cho cả benchmark bình thường và benchmark khi có shard bị tắt.

Bảng nên có dạng:

```text
================ SHARDING BENCHMARK ================

Query: SELECT user_id, COUNT(*) FROM user_logs GROUP BY user_id
Dataset: 1,000,000 User_Logs
Runs per scenario: 3
Representative time: Median

+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
| Nodes | Run times (seconds)    | Median | Speedup | Efficiency | Counted | Completeness | S1 | S2 | S3 | S4 |
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
| 1     | [6.12, 6.03, 6.18]     | 6.12   | 1.00    | 1.00       | 1000000 | 100%         | P  | -  | -  | -  |
| 2     | [3.41, 3.36, 3.48]     | 3.41   | 1.79    | 0.89       | 1000000 | 100%         | P  | P  | -  | -  |
| 4     | [1.95, 1.88, 1.92]     | 1.92   | 3.19    | 0.80       | 1000000 | 100%         | P  | P  | P  | P  |
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
```

Ý nghĩa cột shard:

```text
P  = đọc từ primary
R  = đọc từ replica
   = để trống, nghĩa là cả primary và replica đều không đọc được
-  = shard không dùng trong scenario đó
```

Bên dưới bảng luôn in chú thích:

```text
Legend:
P = primary used
R = replica used
blank = primary and replica unavailable
- = shard not used in this scenario
```

Nếu có replica được dùng, in thêm note:

```text
Note:
Some primary shards were unavailable, so replica nodes were used.
```

Nếu có shard bị trống, in thêm note:

```text
Warning:
Some logical shards were unavailable because both primary and replica failed.
The result is partial and should not be compared as a complete benchmark.
```

---

## 18. Ví dụ khi tôi tự tắt primary shard

Tôi sẽ tự chạy:

```bash
docker stop shard2_primary
```

Sau đó chạy lại:

```bash
python -m coordinator.main benchmark --nodes 4
```

Kết quả mong muốn:

```text
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
| Nodes | Run times (seconds)    | Median | Speedup | Efficiency | Counted | Completeness | S1 | S2 | S3 | S4 |
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
| 4     | [2.04, 2.01, 2.06]     | 2.04   | 2.94    | 0.74       | 1000000 | 100%         | P  | R  | P  | P  |
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
```

Ý nghĩa:

```text
S2 hiển thị R vì shard2_primary bị tắt nhưng shard2_replica vẫn còn.
Completeness vẫn là 100% vì replica có đủ dữ liệu.
```

---

## 19. Ví dụ khi tôi tự tắt cả primary và replica

Tôi sẽ tự chạy:

```bash
docker stop shard2_primary
docker stop shard2_replica
```

Sau đó chạy:

```bash
python -m coordinator.main benchmark --nodes 4
```

Kết quả mong muốn:

```text
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
| Nodes | Run times (seconds)    | Median | Speedup | Efficiency | Counted | Completeness | S1 | S2 | S3 | S4 |
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
| 4     | [1.51, 1.49, 1.53]     | 1.51   | 3.97    | 0.99       | 750000  | 75%          | P  |    | P  | P  |
+-------+------------------------+--------+---------+------------+---------+--------------+----+----+----+----+
```

Ý nghĩa:

```text
S2 để trống vì cả primary và replica của shard2 đều không đọc được.
Counted chỉ còn khoảng 750000.
Completeness chỉ còn khoảng 75%.
Phải ghi chú đây là partial result, không phải hệ thống nhanh hơn thật.
```

---

## 20. CLI commands cần hỗ trợ

Hỗ trợ các lệnh sau:

### Khởi động Docker

```bash
docker compose up -d --build
```

### Sinh dataset

```bash
python -m coordinator.main generate --rows 1000000
```

Nếu file đã tồn tại:

```bash
python -m coordinator.main generate --rows 1000000 --force
```

### Khởi tạo bảng

```bash
python -m coordinator.main init-db
```

### Load dữ liệu

```bash
python -m coordinator.main load
```

Lệnh này load dữ liệu cho cả 3 scenario:

```text
n = 1
n = 2
n = 4
```

vào cả primary và replica.

### Chạy benchmark tất cả scenario

```bash
python -m coordinator.main benchmark
```

Mặc định chạy:

```text
nodes = 1, 2, 4
runs = 3
```

### Chạy riêng một scenario

```bash
python -m coordinator.main benchmark --nodes 4
```

### Chạy với số lần đo tùy chỉnh

```bash
python -m coordinator.main benchmark --runs 5
```

---

## 21. Lệnh demo lỗi thủ công cần ghi vào README

README phải hướng dẫn tôi tự test lỗi như sau.

### Test bình thường

```bash
docker compose up -d --build
python -m coordinator.main generate --rows 1000000
python -m coordinator.main init-db
python -m coordinator.main load
python -m coordinator.main benchmark
```

Kỳ vọng:

```text
Completeness = 100%
S1/S2/S3/S4 đều là P ở scenario 4 shard
```

### Test tắt 1 primary shard

```bash
docker stop shard2_primary
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 = R
Completeness = 100%
```

### Test tắt cả primary và replica của một shard

```bash
docker stop shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 để trống
Completeness khoảng 75%
Có warning partial result
```

### Bật lại shard

```bash
docker start shard2_primary
docker start shard2_replica
python -m coordinator.main benchmark --nodes 4
```

Kỳ vọng:

```text
S2 = P
Completeness = 100%
```

---

## 22. File kết quả

Ngoài in terminal, lưu thêm kết quả vào:

```text
results/benchmark_results.csv
results/benchmark_results.json
```

Các cột nên có:

```text
nodes
run_times_seconds
median_time_seconds
speedup
efficiency
counted_logs
expected_logs
completeness_percent
s1_source
s2_source
s3_source
s4_source
notes
```

Không cần tạo file kết quả lỗi riêng.

Dữ liệu lỗi và bình thường đều nằm chung trong benchmark results.

---

## 23. README bắt buộc phải có

README cần giải thích:

1. Dự án làm gì.
2. Kiến trúc gồm những container nào.
3. Cách sharding theo `user_id % number_of_shards`.
4. Cách replication hoạt động.
5. Vì sao không dùng 2PC/3PC.
6. Cách chạy Docker.
7. Cách generate dataset.
8. Cách load dữ liệu.
9. Cách chạy benchmark.
10. Cách tự test lỗi bằng `docker stop`.
11. Ý nghĩa các cột trong bảng terminal.
12. Ý nghĩa `P`, `R`, blank, `-`.
13. Vì sao khi mất cả primary và replica thì kết quả bị thiếu.
14. Vì sao không nên so sánh speedup khi completeness < 100%.

---

## 24. Tiêu chí hoàn thành

Dự án được xem là hoàn thành nếu đáp ứng đủ các tiêu chí sau:

### Benchmark bình thường

* Chạy được Docker Compose.
* Có 8 PostgreSQL containers.
* Sinh được 1.000.000 dòng User_Logs.
* Load được dữ liệu vào primary và replica.
* Benchmark được 1 shard, 2 shard, 4 shard.
* Mỗi scenario chạy 3 lần.
* Hiển thị đủ 3 thời gian chạy.
* Tính median.
* Tính speedup.
* Tính efficiency.
* In bảng kết quả trên terminal.
* Lưu kết quả ra CSV/JSON.

### Replication và lỗi thủ công

* Nếu tắt một primary shard, benchmark không crash.
* Nếu primary bị tắt nhưng replica còn, chương trình đọc từ replica.
* Bảng hiển thị `R` ở shard tương ứng.
* Completeness vẫn 100%.
* Nếu tắt cả primary và replica của một shard, benchmark không crash.
* Ô shard đó để trống.
* Các shard còn lại vẫn query được.
* Counted logs giảm.
* Completeness giảm.
* Có warning bên dưới bảng.
* Không có bảng lỗi riêng.

### Ràng buộc thiết kế

* Không dùng 2PC.
* Không dùng 3PC.
* Không tự động tắt node.
* Không tạo failure dashboard.
* Không tạo health check riêng.
* Không tạo bảng failure test riêng.
* Không làm web UI.

---

## 25. Ghi chú triển khai quan trọng

Khi benchmark scenario 4 shard, nếu shard2 chết hoàn toàn thì các shard còn lại vẫn phải query:

```text
shard1 -> query được
shard2 -> không query được, để trống
shard3 -> query được
shard4 -> query được
```

Không được vì shard2 lỗi mà dừng toàn bộ chương trình.

Khi một shard lỗi, kết quả cuối cùng vẫn phải được in.

Nếu không thể kết nối DB, phải bắt exception và chuyển sang replica hoặc trả kết quả rỗng.

Nên dùng package như:

```text
psycopg2-binary
tabulate hoặc rich
pandas tùy chọn
```

Nếu dùng package nào thì ghi vào `requirements.txt`.

---

## 26. Kết quả mong muốn cuối cùng

Sau khi hoàn thành, tôi muốn có một project có thể demo theo luồng:

```text
1. docker compose up -d --build
2. generate 1 triệu User_Logs
3. init-db
4. load dữ liệu vào primary và replica
5. chạy benchmark bình thường
6. thấy bảng so sánh 1, 2, 4 shard
7. tự docker stop shard2_primary
8. chạy benchmark lại
9. thấy S2 chuyển thành R
10. tự docker stop shard2_replica
11. chạy benchmark lại
12. thấy S2 để trống, completeness giảm
13. docker start lại shard2_primary và shard2_replica
14. chạy benchmark lại
15. thấy hệ thống quay về đầy đủ
```

Mục tiêu cuối cùng là chứng minh:

```text
- Sharding giúp giảm thời gian xử lý truy vấn aggregation khi tăng số shard.
- Speedup tăng khi tăng số shard, nhưng efficiency có thể giảm do overhead.
- Replication giúp hệ thống vẫn trả kết quả đầy đủ khi primary shard bị tắt.
- Nếu mất cả primary và replica của một shard, hệ thống vẫn không crash nhưng chỉ trả partial result.
- Bảng benchmark terminal thể hiện được đầy đủ thời gian, speedup, efficiency, counted logs, completeness và shard source.
```

Hãy triển khai đầy đủ theo mô tả trên. Sau khi code xong, hãy chạy thử các lệnh cơ bản để đảm bảo project hoạt động, sau đó cập nhật README hướng dẫn tôi chạy lại từ đầu.
