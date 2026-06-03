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
class ShardQueryResult:
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
class RunResult:
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
class ScenarioResult:
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
class LogicalShardPools:
    shard_id: int
    primary: EndpointConnectionPool
    replica: EndpointConnectionPool


def build_pools_for_scenario(nodes: int) -> dict[int, LogicalShardPools]:
    pools: dict[int, LogicalShardPools] = {}
    for shard in active_shards(nodes):
        pools[shard.shard_id] = LogicalShardPools(
            shard_id=shard.shard_id,
            primary=EndpointConnectionPool(shard.primary),
            replica=EndpointConnectionPool(shard.replica),
        )
    return pools


def close_scenario_pools(pools: dict[int, LogicalShardPools]) -> None:
    for shard_pools in pools.values():
        shard_pools.primary.closeall()
        shard_pools.replica.closeall()


def query_logical_shard(
    logical_shard: LogicalShard,
    table_name: str,
    shard_pools: LogicalShardPools,
) -> ShardQueryResult:
    try:
        rows = query_grouped_counts_with_pool(shard_pools.primary, table_name)
        return ShardQueryResult(
            shard_id=logical_shard.shard_id,
            source="P",
            rows=rows,
            counted=sum(int(row[1]) for row in rows),
        )
    except Exception as primary_error:
        try:
            rows = query_grouped_counts_with_pool(shard_pools.replica, table_name)
            return ShardQueryResult(
                shard_id=logical_shard.shard_id,
                source="R",
                rows=rows,
                counted=sum(int(row[1]) for row in rows),
                error=f"primary lỗi: {primary_error}",
            )
        except Exception as replica_error:
            return ShardQueryResult(
                shard_id=logical_shard.shard_id,
                source="",
                rows=[],
                counted=0,
                error=f"primary lỗi: {primary_error}; replica lỗi: {replica_error}",
            )


def _estimate_comm_bytes(rows: list[tuple]) -> int:#Ước tính số byte cần truyền để gửi kết quả từ logical shard về coordinator, bằng cách chuyển đổi kết quả thành JSON và đo kích thước của payload
    payload = json.dumps(rows, separators=(",", ":"), default=str)
    return len(payload.encode("utf-8"))


def collect_logical_shard_cost(
    shard_result: ShardQueryResult,
    table_name: str,
    shard_pools: LogicalShardPools,
) -> ShardQueryResult:
    shard_result.comm_rows = len(shard_result.rows)
    shard_result.comm_bytes = _estimate_comm_bytes(shard_result.rows)

    if shard_result.source == "":
        return shard_result

    pool = shard_pools.primary if shard_result.source == "P" else shard_pools.replica
    try:
        metrics = explain_grouped_counts_cost_with_pool(pool, table_name)
        shard_result.io_blocks = metrics.io_blocks
        shard_result.cpu_ms = metrics.actual_total_time_ms
    except Exception as exc:
        shard_result.cost_error = str(exc)
    return shard_result


def _nearest_rank_p99(values: list[float]) -> float:
    sorted_values = sorted(values)
    index = math.ceil(0.99 * len(sorted_values)) - 1
    return sorted_values[max(0, min(index, len(sorted_values) - 1))]


def run_single_benchmark(
    nodes: int,
    pools: dict[int, LogicalShardPools],
) -> RunResult:
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
    shard_cpu_ms = sum(result.cpu_ms for result in shard_results)
    cpu_ms = shard_cpu_ms + merge_time_ms
    comm_rows = sum(result.comm_rows for result in shard_results)
    comm_bytes = sum(result.comm_bytes for result in shard_results)
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
    ordered = [source for source in ("P", "R", "") if source in sources]
    if not ordered:
        return "-"
    if len(ordered) == 1:
        return ordered[0]
    return "/".join("trống" if source == "" else source for source in ordered)


def _source_summary(nodes: int, run_results: list[RunResult]) -> tuple[dict[int, str], bool]:
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
