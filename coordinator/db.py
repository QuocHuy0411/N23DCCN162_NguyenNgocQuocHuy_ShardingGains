from __future__ import annotations

from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import sql

from coordinator.config import (
    CONNECT_TIMEOUT_SECONDS,
    DbEndpoint,
    INIT_SQL_FILE,
    POOL_MAX_CONNECTIONS,
    POOL_MIN_CONNECTIONS,
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


class EndpointConnectionPool:
    def __init__(
        self,
        endpoint: DbEndpoint,
        minconn: int = POOL_MIN_CONNECTIONS,
        maxconn: int = POOL_MAX_CONNECTIONS,
    ) -> None:
        self.endpoint = endpoint
        self.error: Exception | None = None
        self._pool: ThreadedConnectionPool | None = None

        try:
            self._pool = ThreadedConnectionPool(
                minconn,
                maxconn,
                host=endpoint.host,
                port=endpoint.port,
                dbname=endpoint.database,
                user=endpoint.user,
                password=endpoint.password,
                connect_timeout=CONNECT_TIMEOUT_SECONDS,
                options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
            )
        except Exception as exc:
            self.error = exc

    @property
    def available(self) -> bool:
        return self._pool is not None

    def getconn(self):
        if self._pool is None:
            raise RuntimeError(f"Pool unavailable for {self.endpoint.name}: {self.error}")
        return self._pool.getconn()

    def putconn(self, conn, close: bool = False) -> None:
        if self._pool is not None and conn is not None:
            self._pool.putconn(conn, close=close)

    def closeall(self) -> None:
        if self._pool is not None:
            self._pool.closeall()


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


def query_grouped_counts(endpoint: DbEndpoint, table_name: str) -> list[tuple]:
    statement = sql.SQL(
        """
        WITH per_user_action AS (
            SELECT user_id, action, COUNT(*) AS action_count
            FROM {}
            GROUP BY user_id, action
        )
        SELECT user_id, SUM(action_count) AS log_count
        FROM per_user_action
        GROUP BY user_id
        """
    ).format(sql.Identifier(table_name))

    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall()


def query_grouped_counts_with_pool(
    endpoint_pool: EndpointConnectionPool,
    table_name: str,
) -> list[tuple]:
    statement = sql.SQL(
        """
        WITH per_user_action AS (
            SELECT user_id, action, COUNT(*) AS action_count
            FROM {}
            GROUP BY user_id, action
        )
        SELECT user_id, SUM(action_count) AS log_count
        FROM per_user_action
        GROUP BY user_id
        """
    ).format(sql.Identifier(table_name))

    conn = None
    close_connection = False
    try:
        conn = endpoint_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
        conn.commit()
        return rows
    except Exception:
        close_connection = True
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn is not None:
            endpoint_pool.putconn(conn, close=close_connection)


def iter_all_endpoints() -> Iterable[DbEndpoint]:
    from coordinator.config import SHARDS

    for shard in SHARDS.values():
        yield shard.primary
        yield shard.replica
