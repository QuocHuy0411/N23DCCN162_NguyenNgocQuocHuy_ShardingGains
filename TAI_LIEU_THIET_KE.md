# Tài liệu đặc tả thiết kế hệ thống Sharding Gains

## 1. Mục tiêu và phạm vi

Sharding Gains là hệ thống mô phỏng và benchmark cơ sở dữ liệu phân tán cho bảng log người dùng `User_Logs`. Dự án được xây dựng bằng PostgreSQL, Docker Compose, Python và Streamlit nhằm chứng minh lợi ích của phân mảnh ngang khi xử lý truy vấn tổng hợp trên tập dữ liệu lớn.

Hệ thống tập trung vào bốn mục tiêu kỹ thuật:

- Phân mảnh ngang dữ liệu theo `user_id`.
- So sánh hiệu năng giữa các cấu hình 1, 2 và 4 logical shard.
- Thực thi truy vấn song song tại nhiều shard và hợp nhất kết quả tại coordinator.
- Minh họa khả năng chịu lỗi bằng cơ chế fallback từ primary sang replica.

Phạm vi hiện tại là workload đọc, dữ liệu tĩnh và benchmark truy vấn tổng hợp. Replica trong dự án là bản sao dữ liệu được nạp bởi coordinator khi load, không phải PostgreSQL streaming replication. Cách thiết kế này phù hợp với mục tiêu môn học vì làm rõ các thành phần cốt lõi của hệ cơ sở dữ liệu phân tán mà không đưa thêm độ phức tạp của replication thật, transaction phân tán hoặc failover tự động cấp hạ tầng.

## 2. Tổng quan kiến trúc

Hệ thống gồm hai tầng chính:

- Tầng lưu trữ: 8 container PostgreSQL, đại diện cho 4 logical shard. Mỗi logical shard có một primary và một replica.
- Tầng điều phối: Python coordinator chịu trách nhiệm sinh dữ liệu, khởi tạo schema, chia dữ liệu, nạp dữ liệu, chạy benchmark, fallback, merge kết quả và xuất báo cáo.

Sơ đồ khái quát:

```text
                    +----------------------+
                    |  Streamlit Dashboard |
                    +----------^-----------+
                               |
                    +----------+-----------+
                    |   Python Coordinator |
                    | generate/init/load   |
                    | route/query/merge    |
                    | benchmark/report     |
                    +----------+-----------+
                               |
          +--------------------+--------------------+
          |                    |                    |
   +------+-----+       +------+-----+       +------+-----+
   | Shard 1    |       | Shard 2    |       | Shard 3/4  |
   | P + R      |       | P + R      |       | P + R      |
   +------------+       +------------+       +------------+
```

Các node PostgreSQL:

| Logical shard | Primary | Replica | Port primary | Port replica |
|---:|---|---|---:|---:|
| 1 | `shard1_primary` | `shard1_replica` | 5433 | 5443 |
| 2 | `shard2_primary` | `shard2_replica` | 5434 | 5444 |
| 3 | `shard3_primary` | `shard3_replica` | 5435 | 5445 |
| 4 | `shard4_primary` | `shard4_replica` | 5436 | 5446 |

Tất cả node dùng chung database `userlogs`, user `benchmark`, password `benchmark`. Mỗi container có volume riêng, nhờ đó các shard độc lập về lưu trữ và có thể bật/tắt từng node để demo lỗi.

## 3. Cấu trúc module

| Thành phần | Vai trò |
|---|---|
| `docker-compose.yml` | Khai báo 8 container PostgreSQL và volume riêng cho từng node. |
| `db/init.sql` | Tạo các bảng benchmark và index theo `user_id`. |
| `coordinator/config.py` | Cấu hình tập trung: đường dẫn, số dòng, scenario, endpoint database, timeout, mapping bảng. |
| `coordinator/main.py` | CLI entrypoint cho các lệnh `generate`, `init-db`, `load`, `benchmark`. |
| `coordinator/dataset_generator.py` | Sinh dữ liệu CSV có seed cố định để benchmark tái lập được. |
| `coordinator/router.py` | Xác định shard đích theo số node và `user_id`. |
| `coordinator/loader.py` | Chia CSV thành chunk theo shard và nạp bằng PostgreSQL `COPY`. |
| `coordinator/db.py` | Kết nối PostgreSQL, connection pool, chạy query, `COPY`, `EXPLAIN`. |
| `coordinator/benchmark.py` | Lõi benchmark: query song song, fallback, merge, đo latency, speedup, cost. |
| `coordinator/merger.py` | Hợp nhất kết quả `(user_id, log_count)` từ các shard. |
| `coordinator/reporter.py` | In bảng CLI và lưu kết quả CSV/JSON. |
| `dashboard.py` | Dashboard Streamlit trực quan hóa thời gian, speedup, efficiency, completeness và cost. |

Thiết kế module có tính tách biệt rõ: router không phụ thuộc DB, loader không chứa logic benchmark, benchmark không tự sinh dữ liệu, reporter chỉ lo xuất kết quả. Điều này giúp hệ thống dễ kiểm chứng từng phần và dễ mở rộng.

## 4. Mô hình dữ liệu

Schema được khai báo trong `db/init.sql`. Dự án tạo ba bảng có cùng cấu trúc:

```text
user_logs_n1
user_logs_n2
user_logs_n4
```

Mỗi bảng có các cột:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `id` | `BIGINT PRIMARY KEY` | Định danh log. |
| `user_id` | `INT NOT NULL` | Khóa phân mảnh và khóa tổng hợp. |
| `action` | `VARCHAR(50) NOT NULL` | Hành động của người dùng. |
| `created_at` | `TIMESTAMP NOT NULL` | Thời điểm phát sinh log. |

Mỗi bảng có index trên `user_id`. Đây là lựa chọn phù hợp vì truy vấn benchmark nhóm dữ liệu theo người dùng. Dù PostgreSQL vẫn có thể phải quét nhiều dữ liệu cho truy vấn tổng hợp toàn bảng, index này phản ánh đúng định hướng thiết kế: `user_id` là khóa truy cập và phân phối chính.

Việc tạo bảng riêng cho từng scenario (`n1`, `n2`, `n4`) là một quyết định thực dụng. Nó cho phép cùng một hệ thống lưu đồng thời dữ liệu cho ba cấu hình benchmark, tránh phải reload dữ liệu mỗi khi chuyển từ 1 shard sang 2 hoặc 4 shard.

## 5. Sinh dữ liệu và khả năng tái lập

Module `dataset_generator.py` sinh file `data/user_logs.csv` với mặc định:

| Tham số | Giá trị |
|---|---:|
| Số dòng | 1.000.000 |
| Số user | 100.000 |
| Seed random | 20260531 |
| Khoảng thời gian | Từ `2025-01-01`, trải trong một năm |
| Action | `login`, `logout`, `view_product`, `search`, `add_to_cart`, `checkout` |

Seed cố định là điểm quan trọng trong thiết kế benchmark. Nó giúp các lần chạy có cùng dữ liệu đầu vào, từ đó kết quả giữa 1, 2 và 4 shard có thể so sánh được. Nếu dữ liệu thay đổi ngẫu nhiên giữa các scenario, speedup sẽ không còn phản ánh riêng tác động của sharding.

## 6. Chiến lược phân mảnh

Router nằm trong `coordinator/router.py`:

```text
nodes = 1: shard_id = 1
nodes = 2 hoặc 4: shard_id = (user_id % nodes) + 1
```

Đây là chiến lược hash/modulo sharding đơn giản. Với dữ liệu có `user_id` phân bố đa dạng, modulo giúp dữ liệu được chia tương đối đều giữa các shard. Vì truy vấn benchmark tổng hợp theo `user_id`, toàn bộ log của cùng một user sẽ luôn đi về cùng một shard trong một scenario. Điều này giảm nhu cầu trao đổi dữ liệu giữa các shard trong lúc tính toán cục bộ.

Ưu điểm của chiến lược này:

- Dễ hiểu, dễ hiện thực và dễ giải thích trong báo cáo.
- Không cần metadata phức tạp để tra cứu shard.
- Routing có độ phức tạp `O(1)`.
- Phù hợp với truy vấn nhóm theo `user_id`.
- Dễ mở rộng từ 1 lên 2 và 4 shard trong môi trường benchmark.

Giới hạn của modulo sharding là khi thay đổi số shard, nhiều khóa có thể đổi shard đích. Trong hệ thống production, có thể cân nhắc consistent hashing để giảm chi phí rebalance. Tuy nhiên với mục tiêu benchmark cố định 1/2/4 shard, modulo là lựa chọn hợp lý và minh bạch.

## 7. Quy trình nạp dữ liệu

Module `loader.py` xử lý toàn bộ quá trình load:

1. Kiểm tra tồn tại file `data/user_logs.csv`.
2. Xóa thư mục chunk cũ nếu có.
3. Với từng scenario `1`, `2`, `4`, tạo các file chunk theo shard.
4. Truncate bảng tương ứng trên primary và replica của các shard đang dùng.
5. Dùng PostgreSQL `COPY` để nạp chunk vào primary.
6. Nạp cùng chunk vào replica tương ứng.
7. Xóa chunk tạm nếu không dùng `--keep-chunks`.

Việc dùng `COPY` thay vì insert từng dòng là một điểm mạnh đáng kể. `COPY` là cơ chế bulk load tối ưu của PostgreSQL, giảm overhead giao tiếp client-server và phù hợp với dataset 1.000.000 dòng.

Coordinator chủ động nạp cùng dữ liệu vào primary và replica. Điều này tạo ra mô hình "application-level replication" đủ tốt cho workload đọc tĩnh. Ưu điểm của cách này là dễ demo: khi primary bị dừng, replica vẫn có cùng partition dữ liệu và benchmark vẫn có thể hoàn tất với độ đầy đủ 100%.

## 8. Truy vấn benchmark

Truy vấn chính được định nghĩa trong `db.py`:

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

Truy vấn này gồm hai bước:

- Mỗi shard nhóm theo `(user_id, action)` để tạo số lượng hành động theo user.
- Sau đó cộng lại theo `user_id` để thu được tổng số log của từng user.

Trong hệ phân tán, mỗi shard chỉ xử lý phần dữ liệu của mình. Coordinator nhận các dòng `(user_id, log_count)` và cộng dồn theo `user_id`. Vì dữ liệu đã được partition theo `user_id`, mỗi user thường chỉ xuất hiện ở một shard trong cùng scenario, nhưng hàm merge vẫn được viết tổng quát để cộng dồn nếu có trùng khóa.

## 9. Thực thi song song

Benchmark sử dụng `ThreadPoolExecutor(max_workers=nodes)`. Với `nodes = 4`, coordinator gửi truy vấn đồng thời tới 4 logical shard. Tổng thời gian chạy không còn là tổng thời gian của từng shard như khi query tuần tự; nó xấp xỉ thời gian của shard chậm nhất cộng với overhead điều phối, truyền kết quả và merge.

Đây là điểm ưu việt cốt lõi của thiết kế:

- Mỗi shard quét và tổng hợp ít dữ liệu hơn.
- CPU và I/O của nhiều PostgreSQL container được tận dụng đồng thời.
- Coordinator chỉ nhận kết quả đã tổng hợp, không nhận toàn bộ log thô.
- Truy vấn tổng hợp toàn cục được phân rã thành các truy vấn cục bộ độc lập.

Connection pool được tạo trước khi đo benchmark cho từng scenario. Vì vậy thời gian đo tập trung vào truy vấn và merge, không bị nhiễu nhiều bởi chi phí mở kết nối lặp lại.

## 10. Cơ chế chịu lỗi

Mỗi logical shard có hai endpoint:

- Primary: nguồn đọc ưu tiên.
- Replica: nguồn đọc dự phòng.

Luồng xử lý của một shard:

1. Coordinator thử query primary.
2. Nếu primary lỗi, timeout hoặc không kết nối được, coordinator query replica.
3. Nếu replica thành công, shard được đánh dấu nguồn đọc là `R`.
4. Nếu cả primary và replica đều lỗi, shard trả kết quả rỗng, benchmark vẫn chạy tiếp.

Cách xử lý này làm hệ thống có graceful degradation. Khi chỉ mất primary, kết quả vẫn đầy đủ. Khi mất cả primary và replica của một shard, hệ thống không crash mà báo completeness thấp hơn 100%, giúp người vận hành biết kết quả là một phần.

Các ký hiệu nguồn đọc:

| Ký hiệu | Ý nghĩa |
|---|---|
| `P` | Đọc từ primary. |
| `R` | Primary lỗi, đọc từ replica. |
| Trống | Cả primary và replica đều không khả dụng. |
| `-` | Shard không dùng trong scenario. |
| `P/R` | Nguồn đọc thay đổi giữa các lần chạy. |

Đây là thiết kế có giá trị trình diễn cao vì có thể dừng một container bằng `docker stop shard2_primary` và quan sát benchmark chuyển sang replica mà không cần thay code.

## 11. Hợp nhất kết quả

Module `merger.py` nhận danh sách kết quả từ các shard và cộng dồn:

```text
global_counts[user_id] += log_count
```

Thiết kế merge đơn giản nhưng đúng với bản chất truy vấn phân tán: tính toán đẩy xuống shard, coordinator chỉ làm bước tổng hợp cuối. Cách làm này tốt hơn việc kéo toàn bộ log thô về coordinator vì giảm đáng kể dữ liệu truyền qua mạng và giảm tải xử lý tập trung.

Sau merge, hệ thống tính:

```text
completeness = counted_logs / expected_logs * 100
```

Chỉ số completeness là điểm mạnh của benchmark vì nó không chỉ đo nhanh/chậm mà còn kiểm tra kết quả có đầy đủ hay không. Một hệ thống chạy nhanh nhưng mất dữ liệu sẽ được phát hiện ngay.

## 12. Bộ chỉ số benchmark

Mỗi scenario được chạy nhiều lần, mặc định 20 lần. Hệ thống lưu:

| Chỉ số | Ý nghĩa |
|---|---|
| `run_times_seconds` | Thời gian từng lần chạy. |
| `mean_time_seconds` | Thời gian trung bình. |
| `median_time_seconds` | Thời gian đại diện để tính speedup. |
| `p99_time_seconds` | Tail latency theo nearest-rank percentile. |
| `speedup` | `T1 / Tn`. |
| `efficiency` | `speedup / n`. |
| `counted_logs` | Tổng log sau merge. |
| `completeness_percent` | Tỷ lệ dữ liệu đọc được. |
| `S1..S4` | Nguồn đọc của từng logical shard. |

Median được chọn làm thời gian đại diện vì ổn định hơn mean khi có outlier do Docker, cache, I/O hoặc dao động hệ điều hành. P99 được giữ lại để quan sát lần chạy chậm gần cực trị.

## 13. Mô hình chi phí thực nghiệm

Dự án áp dụng mô hình chi phí theo tinh thần cơ sở dữ liệu phân tán:

```text
Cost = IO + CPU + Comm
```

Trong hệ thống:

| Thành phần | Cách đo |
|---|---|
| IO | Tổng block từ `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`: hit, read, temp read, temp written. |
| CPU | Thời gian thực thi query trên shard cộng thời gian merge tại coordinator. |
| Comm | Dữ liệu kết quả truyền từ shard về coordinator, ước lượng bằng JSON bytes. |
| Total Cost | `IO + CPU + Comm KB`. |

Mô hình này không phải cost nội bộ tuyệt đối của PostgreSQL optimizer. Nó là cost thực nghiệm để giải thích trade-off của thiết kế phân tán: khi tăng shard, IO mỗi shard giảm, thời gian truy vấn giảm, nhưng chi phí điều phối và merge vẫn tồn tại.

## 14. Kết quả benchmark hiện tại

Theo file `results/benchmark_results.json` được tạo ngày 02/06/2026, dataset có 1.000.000 log và benchmark chạy 20 lần cho mỗi scenario.

| Số shard | Median (s) | Mean (s) | P99 (s) | Speedup | Efficiency | Completeness | Total Cost |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1.125801 | 1.195648 | 2.104571 | 1.000000 | 1.000000 | 100% | 24385.005293 |
| 2 | 0.703924 | 0.740455 | 1.037792 | 1.599321 | 0.799661 | 100% | 17586.779839 |
| 4 | 0.443871 | 0.479214 | 0.898562 | 2.536324 | 0.634081 | 100% | 14154.792512 |

Nhận xét:

- 2 shard nhanh hơn 1 shard khoảng 1,60 lần theo median.
- 4 shard nhanh hơn 1 shard khoảng 2,54 lần theo median.
- Completeness đều đạt 100%, nghĩa là tăng tốc không đánh đổi bằng mất dữ liệu.
- Total Cost giảm từ khoảng 24385 xuống 14155 khi tăng từ 1 lên 4 shard.
- Speedup không tuyến tính tuyệt đối vì vẫn có overhead coordinator, truyền kết quả, merge, Docker network và thời gian chờ shard chậm nhất.

Kết quả này chứng minh thiết kế sharding có hiệu quả thực tế với workload tổng hợp: dữ liệu được chia nhỏ, xử lý song song và hợp nhất có kiểm soát.

## 15. Dashboard quan sát

`dashboard.py` đọc `results/benchmark_results.json` và hiển thị:

- Metric tổng quan về số log, số user, số lần benchmark.
- Bảng thời gian từng lần chạy.
- Bảng tóm tắt mean, median, P99, speedup, efficiency, completeness.
- Biểu đồ median time theo số shard.
- Biểu đồ speedup thực tế so với lý tưởng.
- Biểu đồ efficiency.
- Heatmap thời gian từng lần chạy.
- Bảng và biểu đồ mô hình chi phí `IO + CPU + Comm`.

Dashboard giúp kết quả benchmark không chỉ tồn tại dưới dạng log CLI mà có thể phân tích trực quan. Đây là điểm ưu việt ở góc độ vận hành và trình bày: người dùng nhìn được cả hiệu năng, độ ổn định, nguồn đọc và chi phí.

## 16. Các điểm ưu việt của thiết kế

### 16.1. Tách biệt rõ trách nhiệm

Mỗi module đảm nhiệm một vai trò riêng. Cấu hình nằm ở `config.py`, router nằm ở `router.py`, DB helper nằm ở `db.py`, benchmark nằm ở `benchmark.py`, dashboard nằm ngoài coordinator. Điều này giúp code dễ đọc, dễ kiểm thử và dễ sửa từng phần mà không ảnh hưởng toàn hệ thống.

### 16.2. Thiết kế benchmark công bằng

Cùng một dataset được dùng cho cả 1, 2 và 4 shard. Dữ liệu có seed cố định, số lần chạy lặp lại nhiều lần và dùng median làm đại diện. Nhờ đó kết quả phản ánh tác động của sharding rõ hơn so với chạy một lần duy nhất.

### 16.3. Tận dụng song song hóa tự nhiên

Truy vấn tổng hợp được đẩy xuống từng shard, sau đó coordinator merge kết quả. Đây là cách tiếp cận đúng với hệ phân tán: xử lý gần nơi dữ liệu nằm, hạn chế kéo dữ liệu thô về node trung tâm.

### 16.4. Có khả năng chịu lỗi ở tầng ứng dụng

Fallback primary sang replica giúp hệ thống vẫn phục vụ truy vấn khi một primary lỗi. Nếu mất cả hai node của một shard, benchmark vẫn hoàn tất và báo completeness thấp. Cách này tốt hơn việc dừng toàn bộ hệ thống khi một node gặp sự cố.

### 16.5. Quan sát được cả hiệu năng và độ đúng

Hệ thống không chỉ báo thời gian chạy mà còn báo `counted_logs`, `expected_logs` và `completeness_percent`. Đây là điểm quan trọng vì benchmark phân tán phải kiểm tra cả tốc độ lẫn tính đầy đủ của kết quả.

### 16.6. Có mô hình chi phí giải thích được

Việc đo IO, CPU và Comm giúp kết quả benchmark có cơ sở phân tích. Người đọc không chỉ thấy "4 shard nhanh hơn" mà còn thấy chi phí tổng giảm và hiểu các thành phần tạo nên chi phí.

### 16.7. Dễ demo và tái tạo

Docker Compose tạo toàn bộ môi trường chỉ bằng một lệnh. CLI tách thành các bước rõ ràng: generate, init-db, load, benchmark. Người dùng có thể dừng từng container để demo fallback. Kết quả lưu CSV/JSON và dashboard đọc lại được.

### 16.8. Thiết kế vừa đủ cho mục tiêu học thuật

Dự án không cố hiện thực mọi cơ chế production như 2PC, streaming replication, rebalancing hoặc SQL router tổng quát. Thay vào đó, nó tập trung vào các khái niệm cốt lõi: phân mảnh, coordinator, query song song, merge, fallback và đo hiệu năng. Đây là lựa chọn tốt vì giảm nhiễu và làm rõ bài toán chính.

## 17. Giới hạn hiện tại

Các giới hạn cần ghi nhận rõ:

- Replica là bản sao được nạp dữ liệu bởi coordinator, chưa phải streaming replication.
- Workload chủ yếu là đọc và dữ liệu tĩnh.
- Chưa có transaction ghi phân tán.
- Chưa có rebalance tự động khi thay đổi số shard.
- Modulo sharding có thể gây di chuyển nhiều dữ liệu nếu mở rộng số shard trong hệ thống thật.
- Chưa có health-check riêng trước benchmark; lỗi được phát hiện khi query.
- Dashboard đọc file kết quả có sẵn, chưa phải dashboard thời gian thực.
- Query benchmark cố định, chưa hỗ trợ SQL tùy ý.

Những giới hạn này không làm mất giá trị của thiết kế hiện tại. Chúng cho thấy dự án được giới hạn có chủ đích để tập trung vào benchmark sharding và minh họa hệ phân tán.

## 18. Hướng mở rộng

Các hướng nâng cấp hợp lý:

- Thêm PostgreSQL streaming replication để replica phản ánh đúng cơ chế production.
- Bổ sung workload ghi và kiểm tra nhất quán dữ liệu.
- Thêm health-check để đánh dấu trạng thái node trước khi benchmark.
- Thêm consistent hashing để giảm chi phí rebalance.
- Bổ sung truy vấn lọc theo thời gian hoặc action.
- Đo thêm throughput, CPU/memory container và network latency.
- Tạo API hoặc giao diện điều khiển để chạy benchmark từ dashboard.
- Lưu lịch sử nhiều lần benchmark để so sánh theo thời gian.

## 19. Kết luận

Sharding Gains có thiết kế ưu việt ở tính rõ ràng, khả năng tái lập và khả năng chứng minh hiệu quả của phân mảnh ngang. Hệ thống chia dữ liệu theo khóa phù hợp, xử lý truy vấn song song trên nhiều PostgreSQL shard, merge kết quả tại coordinator, theo dõi độ đầy đủ dữ liệu và hỗ trợ fallback sang replica khi primary lỗi.

Kết quả benchmark hiện tại cho thấy khi tăng từ 1 lên 4 shard, median time giảm từ 1.125801 giây xuống 0.443871 giây, speedup đạt 2.536324 lần và completeness vẫn đạt 100%. Điều này chứng minh thiết kế không chỉ đúng về mặt kiến trúc mà còn tạo ra cải thiện hiệu năng đo được trong thực nghiệm.
