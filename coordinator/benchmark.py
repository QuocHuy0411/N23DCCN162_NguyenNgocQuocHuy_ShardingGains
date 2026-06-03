from __future__ import annotations

import statistics
import time
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from coordinator.config import (
    DEFAULT_RUNS,
    EXPECTED_LOGS,
    RESULTS_JSON,
    SCENARIOS,
    TABLE_BY_NODES,
    LogicalShard,
)
from coordinator.db import (
    EndpointConnectionPool,
    explain_grouped_counts_cost_with_pool,
    query_grouped_counts_with_pool,
)
from coordinator.merger import merge_count_rows
from coordinator.router import active_shards, validate_nodes


@dataclass
#Lớp ShardQueryResult lưu kết quả truy vấn của một logical shard trong một lần benchmark, bao gồm thông tin về shard_id, nguồn dữ liệu (primary hoặc replica), các hàng kết quả, số lượng log đã đếm được, lỗi nếu có, và các chỉ số chi phí như IO blocks, CPU ms, số hàng truyền về và số byte truyền về.
class ShardQueryResult:
    """Lưu kết quả truy vấn của một logical shard trong một lần benchmark."""

    shard_id: int
    source: str
    rows: list[tuple]
    counted: int
    error: str | None = None
    io_blocks: int = 0
    cpu_ms: float = 0.0
    comm_rows: int = 0
    comm_bytes: int = 0
    cost_error: str | None = None


@dataclass
#Lớp RunResult lưu toàn bộ chỉ số của một lần chạy benchmark cho một kịch bản, bao gồm thời gian chạy, kết quả truy vấn của từng shard, số lượng log đã đếm được, độ đầy đủ dữ liệu, thời gian merge kết quả, và các chỉ số chi phí tổng hợp như IO blocks, CPU ms, số hàng truyền về và số byte truyền về.
class RunResult:
    """Lưu toàn bộ chỉ số của một lần chạy benchmark cho một kịch bản."""

    elapsed_seconds: float
    shard_results: list[ShardQueryResult]
    counted_logs: int
    completeness_percent: float
    merge_time_ms: float
    io_blocks: int
    cpu_ms: float
    comm_rows: int
    comm_bytes: int
    cost_units: float


@dataclass
#Lớp ScenarioResult lưu kết quả tổng hợp sau nhiều lần chạy của một kịch bản shard, bao gồm số lượng nodes, thời gian chạy của từng lần, các chỉ số thống kê như mean, median và p99 time, tốc độ tăng tốc và hiệu suất so với baseline, số lượng log đã đếm được, độ đầy đủ dữ liệu, nguồn đọc của từng shard, các chỉ số chi phí trung bình và các ghi chú liên quan đến kết quả benchmark.
class ScenarioResult:
    """Lưu kết quả tổng hợp sau nhiều lần chạy của một kịch bản shard."""

    nodes: int
    run_times: list[float]
    mean_time: float
    median_time: float
    p99_time: float
    speedup: float | None
    efficiency: float | None
    counted_logs: int
    expected_logs: int
    completeness_percent: float
    shard_sources: dict[int, str]
    io_blocks: float
    cpu_ms: float
    comm_rows: float
    comm_bytes: float
    comm_kb: float
    cost_units: float
    notes: list[str]


@dataclass
#Lớp LogicalShardPools giữ connection pool primary và replica cho một logical shard, bao gồm shard_id, connection pool cho primary endpoint và connection pool cho replica endpoint. Lớp này được sử dụng để quản lý kết nối đến các shard trong quá trình chạy benchmark.
class LogicalShardPools:
    """Giữ connection pool primary và replica cho một logical shard."""

    shard_id: int
    primary: EndpointConnectionPool
    replica: EndpointConnectionPool


def build_pools_for_scenario(nodes: int) -> dict[int, LogicalShardPools]:
    #Hàm build_pools_for_scenario để tạo connection pool cho toàn bộ shard đang dùng trong kịch bản.
    """Tạo connection pool cho toàn bộ shard đang dùng trong kịch bản."""
    pools: dict[int, LogicalShardPools] = {}
    for shard in active_shards(nodes):
        pools[shard.shard_id] = LogicalShardPools(
            shard_id=shard.shard_id,
            primary=EndpointConnectionPool(shard.primary),
            replica=EndpointConnectionPool(shard.replica),
        )
    return pools


def close_scenario_pools(pools: dict[int, LogicalShardPools]) -> None:
#Khóa tất cả connection pool sau khi chạy xong một kịch bản để giải phóng tài nguyên và đảm bảo rằng các kết nối đến các shard được đóng lại một cách an toàn sau khi hoàn thành công việc.
    """Đóng tất cả connection pool sau khi chạy xong một kịch bản."""
    for shard_pools in pools.values():
        shard_pools.primary.closeall()
        shard_pools.replica.closeall()


def query_logical_shard(
    #Hàm query_logical_shard để truy vấn một logical shard cụ thể, ưu tiên truy vấn từ primary và nếu có lỗi thì sẽ fallback sang replica của cùng shard. 
    #Kết quả trả về sẽ bao gồm thông tin về shard_id, nguồn dữ liệu đã truy vấn (primary hoặc replica), các hàng kết quả, số lượng log đã đếm được, và lỗi nếu có.
    logical_shard: LogicalShard,
    table_name: str,
    shard_pools: LogicalShardPools,
) -> ShardQueryResult:
    """Truy vấn primary trước, nếu lỗi thì fallback sang replica của cùng shard."""
    try:
        rows = query_grouped_counts_with_pool(shard_pools.primary, table_name)
        #Truy vấn dữ liệu từ primary endpoint của shard bằng cách sử dụng connection pool đã tạo. 
        #Kết quả sẽ là một danh sách các tuple, trong đó mỗi tuple chứa user_id và số lượng log tương ứng từ primary endpoint.
        return ShardQueryResult(
        #Trả về kết quả truy vấn dưới dạng một đối tượng ShardQueryResult, bao gồm shard_id, nguồn dữ liệu (P cho primary), các hàng kết quả, số lượng log đã đếm được, và lỗi nếu có.
            shard_id=logical_shard.shard_id,
            source="P",
            rows=rows,
            counted=sum(int(row[1]) for row in rows),
        )
    except Exception as primary_error:
        #Nếu có lỗi xảy ra khi truy vấn từ primary endpoint, sẽ bắt lỗi và lưu thông tin lỗi vào biến primary_error. 
        #Sau đó, sẽ cố gắng truy vấn từ replica endpoint của cùng shard để đảm bảo rằng dữ liệu vẫn có thể được truy cập ngay cả khi primary gặp sự cố.
        try:
            rows = query_grouped_counts_with_pool(shard_pools.replica, table_name)
            #Truy vấn dữ liệu từ replica endpoint của shard bằng cách sử dụng connection pool đã tạo.
            return ShardQueryResult(
            #Trả về kết quả truy vấn từ replica dưới dạng một đối tượng ShardQueryResult, bao gồm shard_id, nguồn dữ liệu (R cho replica), các hàng kết quả, số lượng log đã đếm được, và lỗi nếu có.
                shard_id=logical_shard.shard_id,
                source="R",
                rows=rows,
                counted=sum(int(row[1]) for row in rows),
                error=f"primary lỗi: {primary_error}",
            )
        except Exception as replica_error:
        #Nếu có lỗi xảy ra khi truy vấn từ replica endpoint, sẽ bắt lỗi và lưu thông tin lỗi vào biến replica_error.
            return ShardQueryResult(
                shard_id=logical_shard.shard_id,
                source="",
                rows=[],
                counted=0,
                error=f"primary lỗi: {primary_error}; replica lỗi: {replica_error}",
            )


def _estimate_comm_bytes(rows: list[tuple]) -> int:
    """Ước tính số byte truyền từ shard về coordinator bằng kích thước JSON."""
    payload = json.dumps(rows, separators=(",", ":"), default=str)
    return len(payload.encode("utf-8"))


def collect_logical_shard_cost(
    #Hàm collect_logical_shard_cost để đo lường chi phí IO, CPU và truyền dữ liệu cho một shard đã truy vấn thành công trong lần chạy benchmark.
    shard_result: ShardQueryResult,
    table_name: str,
    shard_pools: LogicalShardPools,
) -> ShardQueryResult:
    """Đo IO, CPU và Comm cho shard đã truy vấn thành công trong lần chạy."""
    shard_result.comm_rows = len(shard_result.rows)
    #Đếm số lượng hàng kết quả trả về từ shard, đây là một phần của chi phí truyền dữ liệu giữa shard và coordinator.
    shard_result.comm_bytes = _estimate_comm_bytes(shard_result.rows)
    #Ước tính số byte truyền từ shard về coordinator bằng cách tính kích thước của dữ liệu trả về khi được chuyển đổi thành JSON. 
    #Đây là một cách để đánh giá chi phí truyền dữ liệu giữa shard và coordinator.

    if shard_result.source == "":#Kiểm tra lỗi truy vấn trước khi đo chi phí
        return shard_result

    pool = shard_pools.primary if shard_result.source == "P" else shard_pools.replica
    #Chọn connection pool tương ứng với nguồn dữ liệu đã truy vấn thành công (primary hoặc replica) để đo chi phí.
    try:
        metrics = explain_grouped_counts_cost_with_pool(pool, table_name)
        #Thực thi câu lệnh EXPLAIN để đo chi phí IO và CPU của truy vấn trên shard, sử dụng connection pool đã chọn. 
        #Kết quả sẽ bao gồm các chỉ số chi phí như số block IO và thời gian CPU thực tế.
        shard_result.io_blocks = metrics.io_blocks 
        #Lưu số block IO vào kết quả shard.
        shard_result.cpu_ms = metrics.actual_total_time_ms 
        #Lưu thời gian CPU thực tế vào kết quả shard.
    except Exception as exc:
        shard_result.cost_error = str(exc)
    return shard_result


def _nearest_rank_p99(values: list[float]) -> float:
    """Tính p99 bằng phương pháp nearest-rank từ danh sách thời gian chạy."""
    sorted_values = sorted(values)
    index = math.ceil(0.99 * len(sorted_values)) - 1 
    #Tính chỉ số của phần tử p99 trong danh sách đã sắp xếp bằng cách sử dụng phương pháp nearest-rank, trong đó index được tính bằng cách lấy phần trăm (0.99) nhân với độ dài của danh sách và sau đó làm tròn lên để lấy phần nguyên lớn nhất.
    return sorted_values[max(0, min(index, len(sorted_values) - 1))]


def run_single_benchmark(
    nodes: int,
    pools: dict[int, LogicalShardPools],
) -> RunResult:
    """Chạy một lần benchmark: query song song, merge kết quả và tính cost."""
    validate_nodes(nodes)
    table_name = TABLE_BY_NODES[nodes]
    shards = active_shards(nodes)

    started_at = time.perf_counter()
    shard_results: list[ShardQueryResult] = []

    with ThreadPoolExecutor(max_workers=nodes) as executor:
        futures = {
            executor.submit(
                query_logical_shard,
                shard,
                table_name,
                pools[shard.shard_id],
            ): shard
            for shard in shards
        }
        for future in as_completed(futures):
            shard_results.append(future.result())

    shard_results.sort(key=lambda item: item.shard_id)
    merge_started_at = time.perf_counter()
    merged_counts = merge_count_rows([result.rows for result in shard_results])
    merge_time_ms = (time.perf_counter() - merge_started_at) * 1000
    counted_logs = sum(merged_counts.values())
    elapsed_seconds = time.perf_counter() - started_at
    completeness_percent = counted_logs / EXPECTED_LOGS * 100

    with ThreadPoolExecutor(max_workers=nodes) as executor:
        cost_futures = {
            executor.submit(
                collect_logical_shard_cost,
                shard_result,
                table_name,
                pools[shard_result.shard_id],
            ): shard_result
            for shard_result in shard_results
        }
        for future in as_completed(cost_futures):
            future.result()

    io_blocks = sum(result.io_blocks for result in shard_results)
    #Tổng hợp số block IO từ tất cả các shard bằng cách cộng dồn số block IO của từng shard.
    shard_cpu_ms = sum(result.cpu_ms for result in shard_results)
    #Tổng hợp thời gian CPU từ tất cả các shard bằng cách cộng dồn thời gian CPU của từng shard.
    cpu_ms = shard_cpu_ms + merge_time_ms
    #Tổng hợp thời gian CPU từ tất cả các shard và thời gian merge.
    comm_rows = sum(result.comm_rows for result in shard_results)
    #Tổng hợp số hàng truyền về từ tất cả các shard bằng cách cộng dồn số hàng của từng shard.
    comm_bytes = sum(result.comm_bytes for result in shard_results)
    #Tổng hợp số byte truyền về từ tất cả các shard bằng cách cộng dồn số byte của từng shard.
    cost_units = io_blocks + cpu_ms + (comm_bytes / 1024)

    return RunResult(
        elapsed_seconds=elapsed_seconds,
        shard_results=shard_results,
        counted_logs=counted_logs,
        completeness_percent=completeness_percent,
        merge_time_ms=merge_time_ms,
        io_blocks=io_blocks,
        cpu_ms=cpu_ms,
        comm_rows=comm_rows,
        comm_bytes=comm_bytes,
        cost_units=cost_units,
    )


def _notes_for_sources(
    shard_sources: dict[int, str],
    completeness_percent: float,
    source_changed: bool,
) -> list[str]:
    """Tạo các ghi chú/cảnh báo dựa trên nguồn đọc P/R và độ đầy đủ dữ liệu."""
    notes: list[str] = []
    if any("R" in source for source in shard_sources.values()):
        notes.append("Một số primary shard không khả dụng nên hệ thống đã dùng replica.")
    if source_changed:
        notes.append("Nguồn đọc dữ liệu thay đổi giữa các lần chạy.")
    if "" in shard_sources.values() or any("trống" in source for source in shard_sources.values()):
        notes.append(
            "Một số logical shard không khả dụng vì cả primary và replica đều lỗi."
        )
    if completeness_percent < 100:
        notes.append(
            "Kết quả là một phần và không nên so sánh như benchmark đầy đủ."
        )
    return notes


def _format_source_history(sources: list[str]) -> str:
    """Tóm tắt lịch sử nguồn đọc của shard thành P, R, trống hoặc P/R."""
    ordered = [source for source in ("P", "R", "") if source in sources]
    if not ordered:
        return "-"
    if len(ordered) == 1:
        return ordered[0]
    return "/".join("trống" if source == "" else source for source in ordered)


def _source_summary(nodes: int, run_results: list[RunResult]) -> tuple[dict[int, str], bool]:
    """Tổng hợp nguồn đọc của từng shard qua nhiều lần chạy benchmark."""
    shard_sources = {index: "-" for index in range(1, 5)}
    source_changed = False
    for shard_id in range(1, nodes + 1):
        sources = [
            shard_result.source
            for run_result in run_results
            for shard_result in run_result.shard_results
            if shard_result.shard_id == shard_id
        ]
        shard_sources[shard_id] = _format_source_history(sources)
        if len(set(sources)) > 1:
            source_changed = True
    return shard_sources, source_changed


def run_scenario(nodes: int, runs: int, baseline_time: float | None) -> ScenarioResult:
    """Chạy một kịch bản shard nhiều lần và tính các chỉ số thống kê tổng hợp."""
    run_results = []
    print(f"Đang chuẩn bị connection pool cho nodes={nodes}...")
    pools = build_pools_for_scenario(nodes)
    try:
        for index in range(1, runs + 1):
            print(f"Đang chạy benchmark: nodes={nodes}, lần={index}/{runs}")
            run_results.append(run_single_benchmark(nodes, pools))
    finally:
        close_scenario_pools(pools)

    run_times = [result.elapsed_seconds for result in run_results]
    mean_time = statistics.mean(run_times)
    median_time = statistics.median(run_times)
    p99_time = _nearest_rank_p99(run_times)
    representative = run_results[-1]
    shard_sources, source_changed = _source_summary(nodes, run_results)
    io_blocks = statistics.mean(result.io_blocks for result in run_results)
    cpu_ms = statistics.mean(result.cpu_ms for result in run_results)
    comm_rows = statistics.mean(result.comm_rows for result in run_results)
    comm_bytes = statistics.mean(result.comm_bytes for result in run_results)
    comm_kb = comm_bytes / 1024
    cost_units = statistics.mean(result.cost_units for result in run_results)

    speedup = None
    efficiency = None
    if baseline_time is not None and baseline_time > 0:
        speedup = baseline_time / median_time
        efficiency = speedup / nodes

    notes = _notes_for_sources(
        shard_sources,
        representative.completeness_percent,
        source_changed,
    )
    return ScenarioResult(
        nodes=nodes,
        run_times=run_times,
        mean_time=mean_time,
        median_time=median_time,
        p99_time=p99_time,
        speedup=speedup,
        efficiency=efficiency,
        counted_logs=representative.counted_logs,
        expected_logs=EXPECTED_LOGS,
        completeness_percent=representative.completeness_percent,
        shard_sources=shard_sources,
        io_blocks=io_blocks,
        cpu_ms=cpu_ms,
        comm_rows=comm_rows,
        comm_bytes=comm_bytes,
        comm_kb=comm_kb,
        cost_units=cost_units,
        notes=notes,
    )


def run_benchmark(nodes: int | None = None, runs: int = DEFAULT_RUNS) -> list[ScenarioResult]:
    """Chạy benchmark cho một kịch bản cụ thể hoặc toàn bộ kịch bản đã cấu hình."""
    scenarios = [nodes] if nodes is not None else list(SCENARIOS)
    for scenario in scenarios:
        validate_nodes(scenario)

    results: list[ScenarioResult] = []
    baseline_time: float | None = None

    if nodes is not None and nodes != 1:
        baseline_time = load_saved_baseline_time()

    for scenario in scenarios:
        result = run_scenario(scenario, runs, baseline_time)
        if scenario == 1 and result.completeness_percent >= 100:
            result.speedup = 1.0
            result.efficiency = 1.0
            baseline_time = result.median_time
        elif scenario != 1 and baseline_time is not None:
            result.speedup = baseline_time / result.median_time
            result.efficiency = result.speedup / scenario
        results.append(result)

    return results


def load_saved_baseline_time() -> float | None:
    """Đọc median time của kịch bản 1 shard đã lưu để tính speedup khi cần."""
    if not RESULTS_JSON.exists():
        return None

    try:
        payload = json.loads(RESULTS_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    baseline = payload.get("baseline_median_time_seconds")
    if (
        payload.get("dataset_rows") == EXPECTED_LOGS
        and isinstance(baseline, (int, float))
        and baseline > 0
    ):
        return float(baseline)

    result_rows = payload.get("benchmark_results", payload.get("results", []))
    for row in result_rows:
        if (
            row.get("nodes") == 1
            and row.get("expected_logs") == EXPECTED_LOGS
            and row.get("completeness_percent") == 100
        ):
            median = row.get("median_time_seconds")
            if isinstance(median, (int, float)) and median > 0:
                return float(median)

    return None
