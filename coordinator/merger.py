from __future__ import annotations


def merge_count_rows(shard_rows: list[list[tuple]]) -> dict[int, int]:
    """Gộp kết quả đếm log từ nhiều shard thành tổng số log theo từng user_id."""
    global_counts: dict[int, int] = {}

    for rows in shard_rows:
        for row in rows:
            user_id = int(row[0])
            log_count = int(row[1])
            global_counts[user_id] = global_counts.get(user_id, 0) + log_count

    return global_counts
