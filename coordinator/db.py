from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import sql

# Các hàm và lớp để tương tác với cơ sở dữ liệu PostgreSQL
from coordinator.config import (
    CONNECT_TIMEOUT_SECONDS,
    DbEndpoint,
    INIT_SQL_FILE,
    POOL_MAX_CONNECTIONS,
    POOL_MIN_CONNECTIONS,
    STATEMENT_TIMEOUT_MS,
)

@dataclass
class QueryCostMetrics:#Lớp để lưu trữ các chỉ số chi phí của truy vấn, bao gồm số block được hit, đọc, ghi tạm thời và thời gian thực tế
    shared_hit_blocks: int
    shared_read_blocks: int
    temp_read_blocks: int
    temp_written_blocks: int
    actual_total_time_ms: float
    actual_rows: int

    @property
    def io_blocks(self) -> int:
        return (
            self.shared_hit_blocks
            + self.shared_read_blocks
            + self.temp_read_blocks
            + self.temp_written_blocks
        )


def _grouped_counts_statement(table_name: str) -> sql.Composed:#Truy vấn SQL để tính tổng số log cho mỗi user_id, nhóm theo action
    return sql.SQL(
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


def connect(endpoint: DbEndpoint):#Tạo kết nối đến cơ sở dữ liệu PostgreSQL dựa trên thông tin trong DbEndpoint
    return psycopg2.connect(
        host=endpoint.host,
        port=endpoint.port,
        dbname=endpoint.database,
        user=endpoint.user,
        password=endpoint.password,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
    )


class EndpointConnectionPool:#Lớp quản lý pool kết nối đến một endpoint cơ sở dữ liệu, sử dụng ThreadedConnectionPool của psycopg2 để tạo và quản lý các kết nối
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
    def available(self) -> bool:#Kiểm tra xem pool kết nối có sẵn hay không, dựa trên việc pool đã được khởi tạo thành công hay chưa
        return self._pool is not None

    def getconn(self):#Lấy một kết nối từ pool. Nếu pool không khả dụng, sẽ ném ra lỗi với thông tin lỗi đã lưu
        if self._pool is None:
            raise RuntimeError(f"Pool không khả dụng cho {self.endpoint.name}: {self.error}")
        return self._pool.getconn()

    def putconn(self, conn, close: bool = False) -> None:#Trả lại một kết nối vào pool. Nếu close là True, kết nối sẽ bị đóng thay vì được trả lại vào pool
        if self._pool is not None and conn is not None:
            self._pool.putconn(conn, close=close)

    def closeall(self) -> None:#Đóng tất cả các kết nối trong pool. Nếu pool không tồn tại, sẽ không làm gì
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
    statement = _grouped_counts_statement(table_name)

    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall()


def query_grouped_counts_with_pool(
    endpoint_pool: EndpointConnectionPool,
    table_name: str,
) -> list[tuple]:
    statement = _grouped_counts_statement(table_name)

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


def explain_grouped_counts_cost_with_pool(
    endpoint_pool: EndpointConnectionPool,
    table_name: str,
) -> QueryCostMetrics:
    statement = sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(
        _grouped_counts_statement(table_name)
    )

    conn = None
    close_connection = False
    try:
        conn = endpoint_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(statement)
            raw_plan = cursor.fetchone()[0]
        conn.commit()

        if isinstance(raw_plan, str):
            raw_plan = json.loads(raw_plan)

        root = raw_plan[0]["Plan"]
        return QueryCostMetrics(
            shared_hit_blocks=int(root.get("Shared Hit Blocks", 0)),
            shared_read_blocks=int(root.get("Shared Read Blocks", 0)),
            temp_read_blocks=int(root.get("Temp Read Blocks", 0)),
            temp_written_blocks=int(root.get("Temp Written Blocks", 0)),
            actual_total_time_ms=float(root.get("Actual Total Time", 0)),
            actual_rows=int(root.get("Actual Rows", 0)),
        )
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
