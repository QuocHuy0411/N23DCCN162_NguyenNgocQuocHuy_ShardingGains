from __future__ import annotations

from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from coordinator.config import (
    CONNECT_TIMEOUT_SECONDS,
    DbEndpoint,
    INIT_SQL_FILE,
    STATEMENT_TIMEOUT_MS,
)


def connect(endpoint: DbEndpoint):
    return psycopg2.connect(
        host=endpoint.host,
        port=endpoint.port,
        dbname=endpoint.database,
        user=endpoint.user,
        password=endpoint.password,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
    )


def run_sql(endpoint: DbEndpoint, statement: str) -> None:
    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)


def run_init_sql(endpoint: DbEndpoint, sql_file: Path = INIT_SQL_FILE) -> None:
    run_sql(endpoint, sql_file.read_text(encoding="utf-8"))


def truncate_table(endpoint: DbEndpoint, table_name: str) -> None:
    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
            )


def copy_csv_to_table(endpoint: DbEndpoint, table_name: str, csv_file: Path) -> None:
    copy_statement = sql.SQL(
        """
        COPY {} (id, user_id, action, created_at)
        FROM STDIN
        WITH (FORMAT CSV, HEADER TRUE)
        """
    ).format(sql.Identifier(table_name))

    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            with csv_file.open("r", encoding="utf-8", newline="") as handle:
                cursor.copy_expert(copy_statement, handle)


def query_grouped_counts(endpoint: DbEndpoint, table_name: str) -> list[dict]:
    statement = sql.SQL(
        """
        SELECT user_id, COUNT(*) AS log_count
        FROM {}
        GROUP BY user_id
        """
    ).format(sql.Identifier(table_name))

    with connect(endpoint) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(statement)
            return [dict(row) for row in cursor.fetchall()]


def iter_all_endpoints() -> Iterable[DbEndpoint]:
    from coordinator.config import SHARDS

    for shard in SHARDS.values():
        yield shard.primary
        yield shard.replica
