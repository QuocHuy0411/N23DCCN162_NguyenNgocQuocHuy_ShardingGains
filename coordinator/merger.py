from __future__ import annotations


def merge_count_rows(shard_rows: list[list[dict]]) -> dict[int, int]:
    global_counts: dict[int, int] = {}

    for rows in shard_rows:
        for row in rows:
            user_id = int(row["user_id"])
            log_count = int(row["log_count"])
            global_counts[user_id] = global_counts.get(user_id, 0) + log_count

    return global_counts
