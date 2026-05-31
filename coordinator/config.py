from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = BASE_DIR / "results"
DATA_FILE = DATA_DIR / "user_logs.csv"
INIT_SQL_FILE = BASE_DIR / "db" / "init.sql"
RESULTS_CSV = RESULTS_DIR / "benchmark_results.csv"
RESULTS_JSON = RESULTS_DIR / "benchmark_results.json"

POSTGRES_DB = "userlogs"
POSTGRES_USER = "benchmark"
POSTGRES_PASSWORD = "benchmark"
POSTGRES_HOST = "localhost"

DEFAULT_ROWS = 1_000_000
USER_ID_COUNT = 100_000
RANDOM_SEED = 20260531
ACTIONS = ("login", "logout", "view_product", "search", "add_to_cart", "checkout")

SCENARIOS = (1, 2, 4)
DEFAULT_RUNS = 3
EXPECTED_LOGS = DEFAULT_ROWS
CONNECT_TIMEOUT_SECONDS = 2
STATEMENT_TIMEOUT_MS = 30_000
POOL_MIN_CONNECTIONS = 1
POOL_MAX_CONNECTIONS = 1

TABLE_BY_NODES = {
    1: "user_logs_n1",
    2: "user_logs_n2",
    4: "user_logs_n4",
}


@dataclass(frozen=True)
class DbEndpoint:
    name: str
    host: str
    port: int
    database: str = POSTGRES_DB
    user: str = POSTGRES_USER
    password: str = POSTGRES_PASSWORD


@dataclass(frozen=True)
class LogicalShard:
    shard_id: int
    primary: DbEndpoint
    replica: DbEndpoint


SHARDS = {
    1: LogicalShard(
        1,
        DbEndpoint("shard1_primary", POSTGRES_HOST, 5433),
        DbEndpoint("shard1_replica", POSTGRES_HOST, 5443),
    ),
    2: LogicalShard(
        2,
        DbEndpoint("shard2_primary", POSTGRES_HOST, 5434),
        DbEndpoint("shard2_replica", POSTGRES_HOST, 5444),
    ),
    3: LogicalShard(
        3,
        DbEndpoint("shard3_primary", POSTGRES_HOST, 5435),
        DbEndpoint("shard3_replica", POSTGRES_HOST, 5445),
    ),
    4: LogicalShard(
        4,
        DbEndpoint("shard4_primary", POSTGRES_HOST, 5436),
        DbEndpoint("shard4_replica", POSTGRES_HOST, 5446),
    ),
}
