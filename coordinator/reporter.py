from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from typing import Any

from tabulate import tabulate

from coordinator.benchmark import ScenarioResult, load_saved_baseline_time
from coordinator.config import (
    DEFAULT_ROWS,
    EXPECTED_LOGS,
    RESULTS_CSV,
    RESULTS_DIR,
    RESULTS_JSON,
)


def _format_seconds(value: float) -> str:
    return f"{value:.2f}"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}"


def _format_completeness(value: float) -> str:
    if abs(value - round(value)) < 0.005:
        return f"{round(value):.0f}%"
    return f"{value:.2f}%"


def _format_run_times(values: list[float]) -> str:
    return "[" + ", ".join(_format_seconds(value) for value in values) + "]"


def print_benchmark_table(results: list[ScenarioResult], runs: int) -> None:
    print()
    print("================ SHARDING BENCHMARK ================")
    print()
    print("Query: SELECT user_id, COUNT(*) FROM user_logs GROUP BY user_id")
    print(f"Dataset: {DEFAULT_ROWS:,} User_Logs")
    print(f"Runs per scenario: {runs}")
    print("Representative time: Median")
    print()

    headers = [
        "Nodes",
        "Run times (seconds)",
        "Median",
        "Speedup",
        "Efficiency",
        "Counted",
        "Completeness",
        "S1",
        "S2",
        "S3",
        "S4",
    ]
    rows = []
    for result in results:
        rows.append(
            [
                result.nodes,
                _format_run_times(result.run_times),
                _format_seconds(result.median_time),
                _format_ratio(result.speedup),
                _format_ratio(result.efficiency),
                result.counted_logs,
                _format_completeness(result.completeness_percent),
                result.shard_sources[1],
                result.shard_sources[2],
                result.shard_sources[3],
                result.shard_sources[4],
            ]
        )

    print(tabulate(rows, headers=headers, tablefmt="grid"))
    print()
    print("Legend:")
    print("P = primary used")
    print("R = replica used")
    print("blank = primary and replica unavailable")
    print("- = shard not used in this scenario")

    used_replica = any("R" in result.shard_sources.values() for result in results)
    missing_shard = any("" in result.shard_sources.values() for result in results)
    incomplete = any(result.completeness_percent < 100 for result in results)
    missing_speedup = any(result.speedup is None for result in results)

    if used_replica:
        print()
        print("Note:")
        print("Some primary shards were unavailable, so replica nodes were used.")

    if missing_shard or incomplete:
        print()
        print("Warning:")
        print("Some logical shards were unavailable because both primary and replica failed.")
        print("The result is partial and should not be compared as a complete benchmark.")

    if missing_speedup:
        print()
        print("Note:")
        print("Speedup and efficiency are N/A when no complete 1-shard baseline is available.")


def _row_for_result(result: ScenarioResult) -> dict[str, Any]:
    return {
        "nodes": result.nodes,
        "run_times_seconds": [round(value, 6) for value in result.run_times],
        "median_time_seconds": round(result.median_time, 6),
        "speedup": None if result.speedup is None else round(result.speedup, 6),
        "efficiency": None if result.efficiency is None else round(result.efficiency, 6),
        "counted_logs": result.counted_logs,
        "expected_logs": result.expected_logs,
        "completeness_percent": round(result.completeness_percent, 6),
        "s1_source": result.shard_sources[1],
        "s2_source": result.shard_sources[2],
        "s3_source": result.shard_sources[3],
        "s4_source": result.shard_sources[4],
        "notes": " | ".join(result.notes),
    }


def save_results(results: list[ScenarioResult]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    rows = [_row_for_result(result) for result in results]
    fieldnames = [
        "nodes",
        "run_times_seconds",
        "median_time_seconds",
        "speedup",
        "efficiency",
        "counted_logs",
        "expected_logs",
        "completeness_percent",
        "s1_source",
        "s2_source",
        "s3_source",
        "s4_source",
        "notes",
    ]

    with RESULTS_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            csv_row = dict(row)
            csv_row["run_times_seconds"] = json.dumps(csv_row["run_times_seconds"])
            writer.writerow(csv_row)

    existing_baseline = load_saved_baseline_time()
    current_baseline = next(
        (
            result.median_time
            for result in results
            if result.nodes == 1 and result.completeness_percent >= 100
        ),
        None,
    )
    baseline = current_baseline if current_baseline is not None else existing_baseline

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_rows": EXPECTED_LOGS,
        "baseline_median_time_seconds": baseline,
        "results": rows,
    }
    RESULTS_JSON.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print()
    print(f"Saved CSV results to: {RESULTS_CSV}")
    print(f"Saved JSON results to: {RESULTS_JSON}")
