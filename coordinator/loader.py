from __future__ import annotations

import csv
import shutil
from pathlib import Path

from coordinator.config import DATA_DIR, DATA_FILE, SCENARIOS, TABLE_BY_NODES
from coordinator.db import copy_csv_to_table, iter_all_endpoints, truncate_table
from coordinator.router import active_shards, shard_id_for_user


CHUNK_DIR = DATA_DIR / "load_chunks"


def _create_chunk_files(nodes: int, source_csv: Path = DATA_FILE) -> dict[int, Path]:
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    chunk_paths = {
        shard_id: CHUNK_DIR / f"user_logs_n{nodes}_shard{shard_id}.csv"
        for shard_id in range(1, nodes + 1)
    }

    handles = {
        shard_id: path.open("w", encoding="utf-8", newline="")
        for shard_id, path in chunk_paths.items()
    }
    writers = {shard_id: csv.writer(handle) for shard_id, handle in handles.items()}

    try:
        for writer in writers.values():
            writer.writerow(("id", "user_id", "action", "created_at"))

        with source_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                shard_id = shard_id_for_user(int(row["user_id"]), nodes)
                writers[shard_id].writerow(
                    (row["id"], row["user_id"], row["action"], row["created_at"])
                )
    finally:
        for handle in handles.values():
            handle.close()

    return chunk_paths


def init_database() -> None:
    from coordinator.db import run_init_sql

    for endpoint in iter_all_endpoints():
        print(f"Initializing schema on {endpoint.name}...")
        run_init_sql(endpoint)
    print("All shard schemas are initialized.")


def load_all_scenarios(source_csv: Path = DATA_FILE, keep_chunks: bool = False) -> None:
    if not source_csv.exists():
        raise FileNotFoundError(
            f"Dataset file not found: {source_csv}. Run generate first."
        )

    if CHUNK_DIR.exists():
        shutil.rmtree(CHUNK_DIR)

    try:
        for nodes in SCENARIOS:
            table_name = TABLE_BY_NODES[nodes]
            print(f"Preparing chunks for {nodes} shard scenario...")
            chunk_paths = _create_chunk_files(nodes, source_csv)

            print(f"Truncating {table_name} on active primary/replica nodes...")
            for shard in active_shards(nodes):
                truncate_table(shard.primary, table_name)
                truncate_table(shard.replica, table_name)

            print(f"Loading {table_name} into primary and replica nodes...")
            for shard in active_shards(nodes):
                chunk_path = chunk_paths[shard.shard_id]
                copy_csv_to_table(shard.primary, table_name, chunk_path)
                copy_csv_to_table(shard.replica, table_name, chunk_path)
                print(
                    f"Loaded shard{shard.shard_id} {table_name} "
                    f"into primary and replica."
                )
    finally:
        if CHUNK_DIR.exists() and not keep_chunks:
            shutil.rmtree(CHUNK_DIR)

    print("Data load completed for scenarios: 1, 2, 4.")
