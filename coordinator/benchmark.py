from __future__ import annotations

import statistics
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from coordinator.config import (
    EXPECTED_LOGS,
    RESULTS_JSON,
    SCENARIOS,
    TABLE_BY_NODES,
    LogicalShard,
)
from coordinator.db import EndpointConnectionPool, query_grouped_counts_with_pool
from coordinator.merger import merge_count_rows
from coordinator.router import active_shards, validate_nodes


@dataclass
class ShardQueryResult:
    shard_id: int
    source: str
    rows: list[tuple]
    counted: int
    error: str | None = None


@dataclass
class RunResult:
    elapsed_seconds: float
    shard_results: list[ShardQueryResult]
    counted_logs: int
    completeness_percent: float


@dataclass
class ScenarioResult:
    nodes: int
    run_times: list[float]
    median_time: float
    speedup: float | None
    efficiency: float | None
    counted_logs: int
    expected_logs: int
    completeness_percent: float
    shard_sources: dict[int, str]
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
                error=f"primary failed: {primary_error}",
            )
        except Exception as replica_error:
            return ShardQueryResult(
                shard_id=logical_shard.shard_id,
                source="",
                rows=[],
                counted=0,
                error=f"primary failed: {primary_error}; replica failed: {replica_error}",
            )


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
    merged_counts = merge_count_rows([result.rows for result in shard_results])
    counted_logs = sum(merged_counts.values())
    elapsed_seconds = time.perf_counter() - started_at
    completeness_percent = counted_logs / EXPECTED_LOGS * 100

    return RunResult(
        elapsed_seconds=elapsed_seconds,
        shard_results=shard_results,
        counted_logs=counted_logs,
        completeness_percent=completeness_percent,
    )


def _notes_for_sources(shard_sources: dict[int, str], completeness_percent: float) -> list[str]:
    notes: list[str] = []
    if "R" in shard_sources.values():
        notes.append("Some primary shards were unavailable, so replica nodes were used.")
    if "" in shard_sources.values():
        notes.append(
            "Some logical shards were unavailable because both primary and replica failed."
        )
    if completeness_percent < 100:
        notes.append(
            "The result is partial and should not be compared as a complete benchmark."
        )
    return notes


def run_scenario(nodes: int, runs: int, baseline_time: float | None) -> ScenarioResult:
    run_results = []
    print(f"Preparing connection pools for nodes={nodes}...")
    pools = build_pools_for_scenario(nodes)
    try:
        for index in range(1, runs + 1):
            print(f"Running benchmark: nodes={nodes}, run={index}/{runs}")
            run_results.append(run_single_benchmark(nodes, pools))
    finally:
        close_scenario_pools(pools)

    run_times = [result.elapsed_seconds for result in run_results]
    median_time = statistics.median(run_times)
    representative = run_results[-1]
    shard_sources = {index: "-" for index in range(1, 5)}
    for shard_result in representative.shard_results:
        shard_sources[shard_result.shard_id] = shard_result.source

    speedup = None
    efficiency = None
    if baseline_time is not None and baseline_time > 0:
        speedup = baseline_time / median_time
        efficiency = speedup / nodes

    notes = _notes_for_sources(shard_sources, representative.completeness_percent)

    return ScenarioResult(
        nodes=nodes,
        run_times=run_times,
        median_time=median_time,
        speedup=speedup,
        efficiency=efficiency,
        counted_logs=representative.counted_logs,
        expected_logs=EXPECTED_LOGS,
        completeness_percent=representative.completeness_percent,
        shard_sources=shard_sources,
        notes=notes,
    )


def run_benchmark(nodes: int | None = None, runs: int = 3) -> list[ScenarioResult]:
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

    for row in payload.get("results", []):
        if (
            row.get("nodes") == 1
            and row.get("expected_logs") == EXPECTED_LOGS
            and row.get("completeness_percent") == 100
        ):
            median = row.get("median_time_seconds")
            if isinstance(median, (int, float)) and median > 0:
                return float(median)

    return None
