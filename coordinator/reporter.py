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
    SCENARIOS,
    USER_ID_COUNT,
)


def _format_seconds(value: float) -> str:
    """Định dạng thời gian giây cho bảng kết quả trên terminal."""
    return f"{value:.2f}"


def _format_ratio(value: float | None) -> str:
    """Định dạng tỷ lệ speedup/efficiency hoặc trả N/A khi chưa có baseline."""
    if value is None:
        return "N/A"
    return f"{value:.2f}"


def _format_completeness(value: float) -> str:
    """Định dạng phần trăm độ đầy đủ dữ liệu của kết quả benchmark."""
    if abs(value - round(value)) < 0.005:
        return f"{round(value):.0f}%"
    return f"{value:.2f}%"


def _format_run_times(values: list[float]) -> str:
    """Định dạng danh sách thời gian từng lần chạy thành chuỗi ngắn."""
    return "[" + ", ".join(_format_seconds(value) for value in values) + "]"


def _format_cost(value: float) -> str:
    """Định dạng giá trị cost model với 2 chữ số thập phân."""
    return f"{value:.2f}"


def print_benchmark_table(results: list[ScenarioResult], runs: int) -> None:
    """In bảng benchmark và bảng cost model ra terminal."""
    print()
    print("================ BENCHMARK SHARDING ================")
    print()
    print("Truy vấn: WITH per_user_action AS (...) SELECT user_id, SUM(action_count) FROM per_user_action GROUP BY user_id")
    print(f"Dataset: {DEFAULT_ROWS:,} User_Logs")
    print(f"Số lần chạy mỗi kịch bản: {runs}")
    print("Thời gian đại diện: trung vị")
    print()

    headers = [
        "Số shard",
        "Thời gian chạy (giây)",
        "Trung bình",
        "Trung vị",
        "P99",
        "Mức tăng tốc",
        "Hiệu suất",
        "Số log đếm được",
        "Độ đầy đủ",
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
                _format_seconds(result.mean_time),
                _format_seconds(result.median_time),
                _format_seconds(result.p99_time),
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
    print("Chú giải:")
    print("P = dùng primary")
    print("R = dùng replica")
    print("trống = primary và replica đều không khả dụng")
    print("- = shard không dùng trong kịch bản này")

    used_replica = any(
        any("R" in source for source in result.shard_sources.values())
        for result in results
    )
    missing_shard = any(
        any(source == "" or "trống" in source for source in result.shard_sources.values())
        for result in results
    )
    incomplete = any(result.completeness_percent < 100 for result in results)
    missing_speedup = any(result.speedup is None for result in results)

    if used_replica:
        print()
        print("Ghi chú:")
        print("Một số primary shard không khả dụng nên hệ thống đã dùng replica.")

    if missing_shard or incomplete:
        print()
        print("Cảnh báo:")
        print("Một số logical shard không khả dụng vì cả primary và replica đều lỗi.")
        print("Kết quả là một phần và không nên so sánh như benchmark đầy đủ.")

    if missing_speedup:
        print()
        print("Ghi chú:")
        print("Mức tăng tốc và hiệu suất là N/A khi chưa có baseline 1 shard đầy đủ.")

    print()
    print("============= COST MODEL OZSU: Cost = IO + CPU + Comm =============")
    cost_headers = [
        "So shard",
        "IO blocks = hit + read + temp_read + temp_written",
        "CPU ms = shard_actual_time + coordinator_merge_time",
        "Comm rows = rows returned to coordinator",
        "Comm KB = comm_bytes / 1024",
        "Total Cost = IO + CPU + Comm KB",
    ]
    cost_rows = []
    for result in results:
        cost_rows.append(
            [
                result.nodes,
                _format_cost(result.io_blocks),
                _format_cost(result.cpu_ms),
                _format_cost(result.comm_rows),
                _format_cost(result.comm_kb),
                _format_cost(result.cost_units),
            ]
        )
    print(tabulate(cost_rows, headers=cost_headers, tablefmt="grid"))
    print()
    print("Chu giai cost:")
    print("IO: PostgreSQL blocks from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON).")
    print("CPU: shard query actual time plus coordinator merge time.")
    print("Comm: result data returned from shards to the coordinator, estimated as JSON bytes.")
    print("Total Cost: experimental normalized cost for Ozsu's model, not PostgreSQL's internal absolute cost.")


def _row_for_result(result: ScenarioResult) -> dict[str, Any]:
    """Chuyển ScenarioResult thành một dòng phẳng để ghi CSV."""
    return {
        "nodes": result.nodes,
        "run_times_seconds": [round(value, 6) for value in result.run_times],
        "mean_time_seconds": round(result.mean_time, 6),
        "median_time_seconds": round(result.median_time, 6),
        "p99_time_seconds": round(result.p99_time, 6),
        "speedup": None if result.speedup is None else round(result.speedup, 6),
        "efficiency": None if result.efficiency is None else round(result.efficiency, 6),
        "counted_logs": result.counted_logs,
        "expected_logs": result.expected_logs,
        "completeness_percent": round(result.completeness_percent, 6),
        "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written": round(result.io_blocks, 6),
        "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time": round(result.cpu_ms, 6),
        "comm_rows_formula_rows_returned_to_coordinator": round(result.comm_rows, 6),
        "comm_kb_formula_comm_bytes_div_1024": round(result.comm_kb, 6),
        "total_cost_formula_io_plus_cpu_plus_comm_kb": round(result.cost_units, 6),
        "s1_source": result.shard_sources[1],
        "s2_source": result.shard_sources[2],
        "s3_source": result.shard_sources[3],
        "s4_source": result.shard_sources[4],
        "notes": " | ".join(result.notes),
    }


def _json_result_for_result(result: ScenarioResult) -> dict[str, Any]:
    """Chuyển ScenarioResult thành cấu trúc JSON giàu ngữ nghĩa cho dashboard."""
    row = _row_for_result(result)
    return {
        "nodes": result.nodes,
        "run_times_seconds": row["run_times_seconds"],
        "mean_time_seconds": row["mean_time_seconds"],
        "median_time_seconds": row["median_time_seconds"],
        "p99_time_seconds": row["p99_time_seconds"],
        "speedup": row["speedup"],
        "efficiency": row["efficiency"],
        "counted_logs": result.counted_logs,
        "expected_logs": result.expected_logs,
        "completeness_percent": row["completeness_percent"],
        "cost_model_ozsu": {
            "formula": "Cost = IO + CPU + Comm",
            "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written": row[
                "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written"
            ],
            "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time": row[
                "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time"
            ],
            "comm_rows_formula_rows_returned_to_coordinator": row[
                "comm_rows_formula_rows_returned_to_coordinator"
            ],
            "comm_kb_formula_comm_bytes_div_1024": row[
                "comm_kb_formula_comm_bytes_div_1024"
            ],
            "total_cost_formula_io_plus_cpu_plus_comm_kb": row[
                "total_cost_formula_io_plus_cpu_plus_comm_kb"
            ],
        },
        "shards": {
            "s1": result.shard_sources[1],
            "s2": result.shard_sources[2],
            "s3": result.shard_sources[3],
            "s4": result.shard_sources[4],
        },
        "notes": result.notes,
    }


def save_results(results: list[ScenarioResult]) -> None:
    """Lưu kết quả benchmark ra CSV và JSON trong thư mục results."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    rows = [_row_for_result(result) for result in results]
    fieldnames = [
        "nodes",
        "run_times_seconds",
        "mean_time_seconds",
        "median_time_seconds",
        "p99_time_seconds",
        "speedup",
        "efficiency",
        "counted_logs",
        "expected_logs",
        "completeness_percent",
        "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written",
        "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time",
        "comm_rows_formula_rows_returned_to_coordinator",
        "comm_kb_formula_comm_bytes_div_1024",
        "total_cost_formula_io_plus_cpu_plus_comm_kb",
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

    benchmark_results = [_json_result_for_result(result) for result in results]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_rows": EXPECTED_LOGS,
        "baseline_median_time_seconds": baseline,
        "dataset": {
            "expected_logs": EXPECTED_LOGS,
            "distinct_users": USER_ID_COUNT,
        },
        "benchmark_config": {
            "runs": max((len(result.run_times) for result in results), default=0),
            "scenarios": list(SCENARIOS),
            "time_unit": "seconds",
        },
        "benchmark_results": benchmark_results,
        "results": rows,
    }
    RESULTS_JSON.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print()
    print(f"Đã lưu kết quả CSV vào: {RESULTS_CSV}")
    print(f"Đã lưu kết quả JSON vào: {RESULTS_JSON}")
