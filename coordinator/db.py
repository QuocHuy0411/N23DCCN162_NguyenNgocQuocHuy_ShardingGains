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
    def io_blocks(self) -> int:#Tính tổng số block I/O được sử dụng trong truy vấn bằng cách cộng các block được hit, đọc và ghi tạm thời. Điều này giúp đánh giá hiệu suất của truy vấn dựa trên lượng I/O mà nó thực hiện.
        return (
            self.shared_hit_blocks
            + self.shared_read_blocks
            + self.temp_read_blocks
            + self.temp_written_blocks
        )


def _grouped_counts_statement(table_name: str) -> sql.Composed:#Truy vấn SQL để tính tổng số log cho mỗi user_id, nhóm theo action
    return sql.SQL(#Truy vấn SQL để tính tổng số log cho mỗi user_id, nhóm theo action. Truy vấn này sử dụng một Common Table Expression (CTE) để tính số lượng log cho mỗi cặp user_id và action, sau đó tổng hợp lại để tính tổng số log cho mỗi user_id.
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
    #Chạy một câu lệnh SQL trên một endpoint cụ thể. 
    #Kết nối sẽ được mở, câu lệnh sẽ được thực thi, và sau đó kết nối sẽ được đóng lại. 
    #Nếu có lỗi xảy ra trong quá trình thực thi, lỗi sẽ được ném ra.
    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)


def run_init_sql(endpoint: DbEndpoint, sql_file: Path = INIT_SQL_FILE) -> None:
    #Chạy các câu lệnh SQL khởi tạo cơ sở dữ liệu trên một endpoint cụ thể. 
    #Câu lệnh SQL sẽ được đọc từ một file và sau đó được thực thi trên endpoint đó. 
    #Điều này thường được sử dụng để tạo bảng và cấu trúc cơ sở dữ liệu cần thiết cho ứng dụng.
    run_sql(endpoint, sql_file.read_text(encoding="utf-8"))


def truncate_table(endpoint: DbEndpoint, table_name: str) -> None:#Truncate (xóa sạch) một bảng dữ liệu trên một endpoint cụ thể. Câu lệnh SQL sẽ được xây dựng để truncate bảng có tên table_name, và sau đó được thực thi trên endpoint đó. Điều này sẽ xóa tất cả dữ liệu trong bảng mà không xóa cấu trúc của bảng.
    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
            )


def copy_csv_to_table(endpoint: DbEndpoint, table_name: str, csv_file: Path) -> None:#Sao chép dữ liệu từ một file CSV vào một bảng dữ liệu trên một endpoint cụ thể. Câu lệnh SQL sẽ được xây dựng để sử dụng lệnh COPY của PostgreSQL, cho phép sao chép dữ liệu từ file CSV vào bảng một cách hiệu quả. Dữ liệu sẽ được đọc từ file CSV và được truyền vào câu lệnh COPY để thực thi trên endpoint đó.
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


def query_grouped_counts(endpoint: DbEndpoint, table_name: str) -> list[tuple]:#Thực thi truy vấn để tính tổng số log cho mỗi user_id, nhóm theo action trên một endpoint cụ thể. Câu lệnh SQL sẽ được xây dựng bằng cách sử dụng hàm _grouped_counts_statement để tạo câu lệnh SQL phù hợp với tên bảng, sau đó được thực thi trên endpoint đó. Kết quả của truy vấn sẽ được trả về dưới dạng một danh sách các tuple, mỗi tuple chứa user_id và tổng số log tương ứng.
    statement = _grouped_counts_statement(table_name)

    with connect(endpoint) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall()


def query_grouped_counts_with_pool(#Thực thi truy vấn để tính tổng số log cho mỗi user_id, nhóm theo action trên một endpoint cụ thể sử dụng pool kết nối. Câu lệnh SQL sẽ được xây dựng bằng cách sử dụng hàm _grouped_counts_statement để tạo câu lệnh SQL phù hợp với tên bảng, sau đó được thực thi trên endpoint đó thông qua pool kết nối. Kết quả của truy vấn sẽ được trả về dưới dạng một danh sách các tuple, mỗi tuple chứa user_id và tổng số log tương ứng.
    endpoint_pool: EndpointConnectionPool,
    table_name: str,
) -> list[tuple]:#Thực thi truy vấn để tính tổng số log cho mỗi user_id, nhóm theo action trên một endpoint cụ thể sử dụng pool kết nối. Câu lệnh SQL sẽ được xây dựng bằng cách sử dụng hàm _grouped_counts_statement để tạo câu lệnh SQL phù hợp với tên bảng, sau đó được thực thi trên endpoint đó thông qua pool kết nối. Kết quả của truy vấn sẽ được trả về dưới dạng một danh sách các tuple, mỗi tuple chứa user_id và tổng số log tương ứng.
    statement = _grouped_counts_statement(table_name)

    conn = None
    close_connection = False
    try:#Lấy một kết nối từ pool, thực thi câu lệnh SQL, và trả lại kết quả. Nếu có lỗi xảy ra trong quá trình thực thi, kết nối sẽ được đánh dấu để đóng và lỗi sẽ được ném ra. Sau khi thực thi xong, kết nối sẽ được trả lại vào pool, với tùy chọn đóng nếu có lỗi xảy ra.
        conn = endpoint_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
        conn.commit()
        return rows
    except Exception:#Nếu có lỗi xảy ra trong quá trình thực thi, kết nối sẽ được đánh dấu để đóng và lỗi sẽ được ném ra. Điều này đảm bảo rằng các kết nối bị lỗi sẽ không được trả lại vào pool, giúp duy trì tính ổn định của pool kết nối.
        close_connection = True
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:#Sau khi thực thi xong, kết nối sẽ được trả lại vào pool, với tùy chọn đóng nếu có lỗi xảy ra. Điều này đảm bảo rằng các kết nối được sử dụng sẽ được quản lý đúng cách, giúp duy trì hiệu suất và ổn định của ứng dụng khi tương tác với cơ sở dữ liệu.
        if conn is not None:
            endpoint_pool.putconn(conn, close=close_connection)


def explain_grouped_counts_cost_with_pool(#Thực thi truy vấn EXPLAIN để lấy các chỉ số chi phí của truy vấn tính tổng số log cho mỗi user_id, nhóm theo action trên một endpoint cụ thể sử dụng pool kết nối. 
    endpoint_pool: EndpointConnectionPool,
    table_name: str,
) -> QueryCostMetrics:
    statement = sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(
        #Analyze truy vấn để lấy các chỉ số chi phí của truy vấn tính tổng số log cho mỗi user_id, nhóm theo action trên một endpoint cụ thể sử dụng pool kết nối. 
        #Buffers để thu thập thông tin về block được hit, đọc và ghi tạm thời trong quá trình thực thi truy vấn. 
        #Format JSON để trả về kết quả dưới dạng JSON, giúp dễ dàng phân tích và trích xuất các chỉ số chi phí từ kế hoạch thực thi của truy vấn. Câu lệnh SQL sẽ được xây dựng bằng cách sử dụng hàm
        _grouped_counts_statement(table_name)#Truy vấn EXPLAIN để lấy các chỉ số chi phí của truy vấn tính tổng số log cho mỗi user_id, nhóm theo action trên một endpoint cụ thể sử dụng pool kết nối. Câu lệnh SQL sẽ được xây dựng bằng cách sử dụng hàm _grouped_counts_statement để tạo câu lệnh SQL phù hợp với tên bảng, sau đó được bao bọc trong câu lệnh EXPLAIN để thu thập thông tin chi phí và hiệu suất của truy vấn. Kết quả của truy vấn sẽ được phân tích và trả về dưới dạng một đối tượng QueryCostMetrics chứa các chỉ số chi phí của truy vấn.
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
        return QueryCostMetrics(#Tạo một đối tượng QueryCostMetrics chứa các chỉ số chi phí của truy vấn, được trích xuất từ kế hoạch thực thi của truy vấn. Các chỉ số bao gồm số block được hit, đọc và ghi tạm thời, thời gian thực tế và số lượng hàng thực tế được trả về bởi truy vấn.
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


def iter_all_endpoints() -> Iterable[DbEndpoint]:#Hàm generator để lặp qua tất cả các endpoint của các shard, bao gồm cả primary và replica. Điều này cho phép thực hiện các thao tác trên tất cả các endpoint một cách dễ dàng, chẳng hạn như khởi tạo cơ sở dữ liệu hoặc tải dữ liệu vào các shard.
    from coordinator.config import SHARDS

    for shard in SHARDS.values():
        yield shard.primary
        yield shard.replica
