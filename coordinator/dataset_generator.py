from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from coordinator.config import (
    ACTIONS,
    DATA_DIR,
    DATA_FILE,
    DEFAULT_ROWS,
    RANDOM_SEED,
    USER_ID_COUNT,
)


def generate_dataset(#Sinh dataset giả lập user logs với các trường id, user_id, action, created_at
    rows: int = DEFAULT_ROWS,
    output_file: Path = DATA_FILE,
    force: bool = False,
) -> Path:
    if output_file.exists() and not force:
        print(f"Dataset đã tồn tại: {output_file}")
        print("Dùng --force để sinh lại.")
        return output_file

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(RANDOM_SEED)
    base_time = datetime(2025, 1, 1, 0, 0, 0)
    max_offset_seconds = 365 * 24 * 60 * 60 - 1

    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("id", "user_id", "action", "created_at"))

        for row_id in range(1, rows + 1):
            user_id = rng.randint(1, USER_ID_COUNT)
            action = rng.choice(ACTIONS)
            created_at = base_time + timedelta(
                seconds=rng.randint(0, max_offset_seconds)
            )
            writer.writerow(
                (
                    row_id,
                    user_id,
                    action,
                    created_at.isoformat(sep=" ", timespec="seconds"),
                )
            )

            if row_id % 100_000 == 0:
                print(f"Đã sinh {row_id:,}/{rows:,} dòng")

    print(f"Đã ghi dataset vào: {output_file}")
    return output_file
