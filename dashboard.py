from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


RESULTS_JSON = Path("results/benchmark_results.json")

def _format_seconds(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.4f}"


def _format_ratio(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.2f}"


def _format_percent(value: Any) -> str:
    if value is None:
        return "N/A"
    value = float(value)
    if abs(value - round(value)) < 0.005:
        return f"{round(value):.0f}%"
    return f"{value:.2f}%"


def _load_payload() -> dict[str, Any] | None:
    if not RESULTS_JSON.exists():
        return None
    try:
        return json.loads(RESULTS_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _normalize_results(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if "benchmark_results" in payload:
        return payload["benchmark_results"]

    normalized = []
    for row in payload.get("results", []):
        run_times = row.get("run_times_seconds", [])
        notes = row.get("notes", "")
        normalized.append(
            {
                "nodes": row.get("nodes"),
                "run_times_seconds": run_times,
                "mean_time_seconds": row.get("mean_time_seconds"),
                "median_time_seconds": row.get("median_time_seconds"),
                "p99_time_seconds": row.get("p99_time_seconds"),
                "speedup": row.get("speedup"),
                "efficiency": row.get("efficiency"),
                "counted_logs": row.get("counted_logs"),
                "expected_logs": row.get("expected_logs"),
                "completeness_percent": row.get("completeness_percent"),
                "cost_model_ozsu": {
                    "formula": "Cost = IO + CPU + Comm",
                    "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written": row.get(
                        "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written", 0
                    ),
                    "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time": row.get(
                        "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time", 0
                    ),
                    "comm_rows_formula_rows_returned_to_coordinator": row.get(
                        "comm_rows_formula_rows_returned_to_coordinator", 0
                    ),
                    "comm_kb_formula_comm_bytes_div_1024": row.get(
                        "comm_kb_formula_comm_bytes_div_1024", 0
                    ),
                    "total_cost_formula_io_plus_cpu_plus_comm_kb": row.get(
                        "total_cost_formula_io_plus_cpu_plus_comm_kb", 0
                    ),
                },
                "shards": {
                    "s1": row.get("s1_source", ""),
                    "s2": row.get("s2_source", ""),
                    "s3": row.get("s3_source", ""),
                    "s4": row.get("s4_source", ""),
                },
                "notes": [notes] if notes else [],
            }
        )
    return normalized


def _run_times_table(results: list[dict[str, Any]]) -> pd.DataFrame:
    max_runs = max((len(row.get("run_times_seconds", [])) for row in results), default=0)
    rows = []
    for result in results:
        row = {"Số shard": result["nodes"]}
        run_times = result.get("run_times_seconds", [])
        for index in range(max_runs):
            row[f"Lần {index + 1}"] = (
                _format_seconds(run_times[index]) if index < len(run_times) else ""
            )
        rows.append(row)
    return pd.DataFrame(rows)


def _summary_table(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for result in results:
        shards = result.get("shards", {})
        rows.append(
            {
                "Số shard": result.get("nodes"),
                "Trung bình (s)": _format_seconds(result.get("mean_time_seconds")),
                "Trung vị (s)": _format_seconds(result.get("median_time_seconds")),
                "P99 (s)": _format_seconds(result.get("p99_time_seconds")),
                "Mức tăng tốc": _format_ratio(result.get("speedup")),
                "Hiệu suất": _format_ratio(result.get("efficiency")),
                "Số log đếm được": result.get("counted_logs"),
                "Độ đầy đủ": _format_percent(result.get("completeness_percent")),
                "S1": shards.get("s1", ""),
                "S2": shards.get("s2", ""),
                "S3": shards.get("s3", ""),
                "S4": shards.get("s4", ""),
            }
        )
    return pd.DataFrame(rows)


def _cost_model(result: dict[str, Any]) -> dict[str, Any]:
    return result.get("cost_model_ozsu") or {}


def _cost_table(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for result in results:
        cost = _cost_model(result)
        rows.append(
            {
                "Số shard": result.get("nodes"),
                "IO blocks = hit + read + temp_read + temp_written": f"{float(cost.get('io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written') or 0):.2f}",
                "CPU ms = shard_actual_time + coordinator_merge_time": f"{float(cost.get('cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time') or 0):.2f}",
                "Comm rows = số dòng trả về coordinator": f"{float(cost.get('comm_rows_formula_rows_returned_to_coordinator') or 0):.2f}",
                "Comm KB = comm_bytes / 1024": f"{float(cost.get('comm_kb_formula_comm_bytes_div_1024') or 0):.2f}",
                "Total Cost = IO + CPU + Comm KB": f"{float(cost.get('total_cost_formula_io_plus_cpu_plus_comm_kb') or 0):.2f}",
            }
        )
    return pd.DataFrame(rows)


def _chart_df(results: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nodes": result.get("nodes"),
                "median_time_seconds": result.get("median_time_seconds"),
                "actual_speedup": result.get("speedup"),
                "ideal_speedup": result.get("nodes"),
                "efficiency": result.get("efficiency"),
                "completeness_percent": result.get("completeness_percent"),
            }
            for result in results
        ]
    )


def _cost_chart_df(results: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nodes": result.get("nodes"),
                "IO blocks": float(
                    _cost_model(result).get(
                        "io_blocks_formula_hit_plus_read_plus_temp_read_plus_temp_written"
                    )
                    or 0
                ),
                "CPU ms": float(
                    _cost_model(result).get(
                        "cpu_ms_formula_shard_actual_time_plus_coordinator_merge_time"
                    )
                    or 0
                ),
                "Comm KB": float(
                    _cost_model(result).get("comm_kb_formula_comm_bytes_div_1024")
                    or 0
                ),
                "Total Cost": float(
                    _cost_model(result).get("total_cost_formula_io_plus_cpu_plus_comm_kb")
                    or 0
                ),
            }
            for result in results
        ]
    )


def _heatmap_df(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for result in results:
        for index, value in enumerate(result.get("run_times_seconds", []), start=1):
            rows.append(
                {
                    "Nodes": result.get("nodes"),
                    "Run Number": f"Lần {index}",
                    "Run": index,
                    "Query Time (seconds)": value,
                }
            )
    return pd.DataFrame(rows)


def _summary_cards(payload: dict[str, Any], results: list[dict[str, Any]]) -> None:
    dataset = payload.get("dataset", {})
    config = payload.get("benchmark_config", {})
    complete_values = [float(result.get("completeness_percent") or 0) for result in results]
    best_median = min(results, key=lambda item: item.get("median_time_seconds") or float("inf"))
    speedup_results = [result for result in results if result.get("speedup") is not None]
    best_speedup = max(speedup_results, key=lambda item: item.get("speedup") or 0) if speedup_results else None

    cols = st.columns(6)
    cols[0].metric("Tổng số log", f"{dataset.get('expected_logs', payload.get('dataset_rows', 0)):,}")
    cols[1].metric("Số user riêng biệt", f"{dataset.get('distinct_users', 0):,}")
    cols[2].metric("Số lần benchmark", config.get("runs", max(len(r.get("run_times_seconds", [])) for r in results)))
    cols[3].metric("Trung vị tốt nhất", f"{best_median.get('nodes')} shard")
    cols[4].metric("Mức tăng tốc tốt nhất", f"{best_speedup.get('nodes')} shard" if best_speedup else "N/A")
    cols[5].metric("Độ đầy đủ hiện tại", _format_percent(min(complete_values) if complete_values else None))


def main() -> None:
    st.set_page_config(page_title="Bảng điều khiển Sharding Gains", layout="wide")
    st.title("Bảng điều khiển Sharding Gains")
    st.caption("Benchmark phân mảnh ngang trên các shard PostgreSQL, có fallback từ primary sang replica.")

    payload = _load_payload()
    if payload is None:
        st.warning("Chưa tìm thấy tệp kết quả benchmark. Hãy chạy `python -m coordinator.main benchmark` trước.")
        st.code("python -m coordinator.main benchmark", language="bash")
        return

    results = _normalize_results(payload)
    if not results:
        st.warning("Tệp kết quả benchmark không có dòng kết quả nào.")
        return

    _summary_cards(payload, results)

    st.header("Thời gian chạy truy vấn")
    st.caption("Đơn vị: giây")
    st.dataframe(_run_times_table(results), width="stretch", hide_index=True)
    st.write(
        "Mỗi ô là thời gian thực tế của một lần chạy benchmark truy vấn. Thời gian này gồm truy vấn song song trên các shard, nhận kết quả và gộp kết quả ở coordinator. Thời gian này không gồm sinh dữ liệu, nạp dữ liệu hoặc khởi động Docker."
    )

    st.header("Tóm tắt benchmark")
    st.dataframe(_summary_table(results), width="stretch", hide_index=True)
    st.markdown(
        """
**Chú giải bảng tóm tắt benchmark**

Số shard: số logical shard được dùng trong kịch bản benchmark.

Trung bình (s): thời gian chạy trung bình của các lần benchmark.

Trung vị (s): thời gian đại diện, lấy giá trị median của các lần chạy.

P99 (s): thời gian ở phân vị 99%, dùng để quan sát lần chạy chậm nhất gần cực trị.

Mức tăng tốc: so sánh tốc độ với baseline 1 shard. Formula: `Speedup = T1 / Tn`.

Hiệu suất: mức sử dụng hiệu quả của số shard khi chạy song song. Formula: `Efficiency = Speedup / n`.

Số log đếm được: tổng số bản ghi log được truy vấn và gộp từ các shard.

Độ đầy đủ: tỷ lệ dữ liệu đếm được so với tổng số log kỳ vọng.

S1, S2, S3, S4: nguồn dữ liệu được dùng cho từng logical shard.

P = dùng primary.

R = dùng replica.

Ô trống = primary và replica đều không khả dụng.

- = shard không dùng trong kịch bản này.
"""
    )

    all_notes = [note for result in results for note in result.get("notes", [])]
    used_replica = any(
        "R" in str(source)
        for result in results
        for source in result.get("shards", {}).values()
    )
    if used_replica or any("replica" in note.lower() for note in all_notes):
        st.info("Một số primary shard không khả dụng nên hệ thống đã dùng replica.")
    if any((result.get("completeness_percent") or 0) < 100 for result in results):
        st.warning(
            "Một số logical shard không khả dụng vì cả primary và replica đều lỗi. Kết quả là một phần và không nên so sánh như benchmark đầy đủ."
        )

    chart_df = _chart_df(results)
    cost_chart_df = _cost_chart_df(results)

    st.header("Thời gian truy vấn trung vị theo số shard")
    fig = px.bar(
        chart_df,
        x="nodes",
        y="median_time_seconds",
        labels={"nodes": "Số shard", "median_time_seconds": "Thời gian truy vấn trung vị (giây)"},
    )
    st.plotly_chart(fig, width="stretch")
    st.write(
        "Biểu đồ này cho thấy thời gian truy vấn trung vị có giảm khi số shard tăng hay không. Trung vị thấp hơn khi dùng nhiều shard cho thấy phân mảnh ngang và thực thi song song đang giúp mỗi shard xử lý ít dữ liệu hơn."
    )

    st.header("Mức tăng tốc thực tế so với mức tăng tốc lý tưởng")
    st.caption("Formula: Speedup = T1 / Tn")
    speedup_df = chart_df.melt(
        id_vars=["nodes"],
        value_vars=["actual_speedup", "ideal_speedup"],
        var_name="Chuỗi",
        value_name="Mức tăng tốc",
    )
    speedup_df["Chuỗi"] = speedup_df["Chuỗi"].map(
        {"actual_speedup": "Mức tăng tốc thực tế", "ideal_speedup": "Mức tăng tốc lý tưởng"}
    )
    fig = px.line(
        speedup_df,
        x="nodes",
        y="Mức tăng tốc",
        color="Chuỗi",
        markers=True,
        labels={"nodes": "Số shard"},
    )
    st.plotly_chart(fig, width="stretch")
    st.write(
        "Nếu mức tăng tốc thực tế thấp hơn mức tăng tốc lý tưởng, hệ thống vẫn nhanh hơn nhưng chưa mở rộng tuyến tính do giao tiếp với coordinator, chi phí gộp kết quả, overhead mạng Docker và thời gian chờ shard chậm nhất."
    )

    st.header("Hiệu suất song song theo số shard")
    st.caption("Formula: Efficiency = Speedup / n")
    fig = px.bar(
        chart_df,
        x="nodes",
        y="efficiency",
        labels={"nodes": "Số shard", "efficiency": "Hiệu suất"},
    )
    st.plotly_chart(fig, width="stretch")
    st.write(
        "Hiệu suất cho biết mỗi shard được sử dụng hiệu quả đến mức nào. Chỉ số này có thể giảm khi số shard tăng vì chi phí giao tiếp, chi phí gộp và overhead điều phối cũng tăng."
    )

    st.header("Heatmap thời gian truy vấn theo từng lần chạy")
    heatmap_df = _heatmap_df(results)
    run_order = [f"Lần {run}" for run in sorted(heatmap_df["Run"].unique())]
    heatmap_pivot = heatmap_df.pivot(
        index="Nodes",
        columns="Run Number",
        values="Query Time (seconds)",
    ).reindex(columns=run_order)
    fig = go.Figure(
        data=go.Heatmap(
            z=heatmap_pivot.values,
            x=heatmap_pivot.columns,
            y=heatmap_pivot.index,
            colorscale="Viridis",
            colorbar={"title": "giây"},
            hovertemplate="Số shard=%{y}<br>Lần chạy=%{x}<br>Thời gian truy vấn=%{z:.4f}s<extra></extra>",
        )
    )
    fig.update_layout(xaxis_title="Lần chạy", yaxis_title="Số shard")
    st.plotly_chart(fig, width="stretch")
    st.write(
        "Heatmap thể hiện độ dao động thời gian truy vấn giữa các lần chạy. Ô tối hơn là lần chạy chậm hơn, có thể do tail latency, overhead Docker, hiệu ứng cache, dao động I/O hoặc shard chậm nhất kéo dài tổng thời gian."
    )
    st.header("Mô hình chi phí Özsu: Cost = IO + CPU + Comm")
    st.dataframe(_cost_table(results), width="stretch", hide_index=True)
    st.markdown(
        """
Chú giải cost:
- IO: số block PostgreSQL truy cập, lấy từ `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.
- CPU: thời gian thực thi truy vấn tại các shard cộng với thời gian coordinator gộp kết quả.
- Comm: lượng dữ liệu kết quả truyền từ các shard về coordinator, ước lượng bằng số byte JSON.
- Total Cost: chi phí thực nghiệm đã chuẩn hóa để liên hệ với mô hình của Özsu, không phải cost nội bộ tuyệt đối của PostgreSQL.
"""
    )

    st.header("Tổng chi phí theo mô hình Özsu")
    fig = px.bar(
        cost_chart_df,
        x="nodes",
        y="Total Cost",
        labels={"nodes": "Số shard", "Total Cost": "Total Cost = IO + CPU + Comm KB"},
    )
    st.plotly_chart(fig, width="stretch")

    st.header("Thành phần chi phí: IO, CPU, Comm")
    io_col, cpu_col, comm_col = st.columns(3)

    with io_col:
        st.subheader("Chi phí IO")
        fig = px.bar(
            cost_chart_df,
            x="nodes",
            y="IO blocks",
            labels={"nodes": "Số shard", "IO blocks": "IO blocks"},
        )
        st.plotly_chart(fig, width="stretch")

    with cpu_col:
        st.subheader("Chi phí CPU")
        fig = px.bar(
            cost_chart_df,
            x="nodes",
            y="CPU ms",
            labels={"nodes": "Số shard", "CPU ms": "CPU ms"},
        )
        st.plotly_chart(fig, width="stretch")

    with comm_col:
        st.subheader("Chi phí Comm")
        fig = px.bar(
            cost_chart_df,
            x="nodes",
            y="Comm KB",
            labels={"nodes": "Số shard", "Comm KB": "Comm KB"},
        )
        st.plotly_chart(fig, width="stretch")


if __name__ == "__main__":
    main()
