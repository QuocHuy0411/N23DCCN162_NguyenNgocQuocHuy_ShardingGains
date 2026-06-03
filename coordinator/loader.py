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
    chunk_paths = {#Tạo các file chunk riêng biệt cho mỗi shard dựa trên số lượng node được chỉ định
        shard_id: CHUNK_DIR / f"user_logs_n{nodes}_shard{shard_id}.csv"
        for shard_id in range(1, nodes + 1)
    }

    handles = {#Mở các file chunk để ghi dữ liệu. Mỗi file chunk sẽ được mở với mã hóa UTF-8 và chế độ ghi mới. Các handle này sẽ được sử dụng để ghi dữ liệu vào các file chunk tương ứng với mỗi shard.
        shard_id: path.open("w", encoding="utf-8", newline="")
        for shard_id, path in chunk_paths.items()
    }
    #Tạo các đối tượng csv.writer cho mỗi file chunk để ghi dữ liệu dưới dạng CSV. Mỗi writer sẽ được liên kết với một handle tương ứng với file chunk của shard đó.
    writers = {shard_id: csv.writer(handle) for shard_id, handle in handles.items()}

    try:
        for writer in writers.values():#Ghi tiêu đề cột vào mỗi file chunk. Các cột bao gồm id, user_id, action và created_at.
            writer.writerow(("id", "user_id", "action", "created_at"))

        with source_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)#Đọc dữ liệu từ file CSV nguồn bằng cách sử dụng csv.DictReader, cho phép truy cập dữ liệu theo tên cột. Mỗi dòng dữ liệu sẽ được đọc vào dưới dạng một dictionary, với khóa là tên cột và giá trị là dữ liệu tương ứng.
            for row in reader:
                shard_id = shard_id_for_user(int(row["user_id"]), nodes)#Xác định shard_id cho mỗi dòng dữ liệu dựa trên user_id và số lượng node. Hàm shard_id_for_user sẽ tính toán shard_id bằng cách sử dụng thuật toán băm hoặc phân phối dựa trên user_id.
                writers[shard_id].writerow(#Ghi dữ liệu vào file chunk tương ứng với shard_id đã xác định. Dữ liệu sẽ được ghi dưới dạng CSV, với các trường id, user_id, action và created_at được lấy từ dòng dữ liệu gốc.
                    (row["id"], row["user_id"], row["action"], row["created_at"])
                )
    finally:
        for handle in handles.values():#Đảm bảo rằng tất cả các file chunk được đóng sau khi ghi xong, bất kể có lỗi xảy ra hay không. Điều này giúp giải phóng tài nguyên và đảm bảo rằng dữ liệu được ghi vào file một cách an toàn.
            handle.close()

    return chunk_paths#Trả về một dictionary chứa đường dẫn đến các file chunk đã tạo, với khóa là shard_id và giá trị là đường dẫn đến file chunk tương ứng.


def init_database() -> None:
    #Hàm init_database để khởi tạo cơ sở dữ liệu trên mỗi shard. 
    #Hàm sẽ lặp qua tất cả các endpoint của các shard và chạy các câu lệnh SQL khởi tạo schema cần thiết để chuẩn bị cơ sở dữ liệu cho việc lưu trữ dữ liệu người dùng và thực hiện các bài kiểm tra hiệu suất.
    from coordinator.db import run_init_sql
    #Hàm run_init_sql sẽ được sử dụng để chạy các câu lệnh SQL khởi tạo cơ sở dữ liệu trên mỗi endpoint của các shard. 
    #Điều này đảm bảo rằng tất cả các shard đều có cấu trúc cơ sở dữ liệu cần thiết để lưu trữ dữ liệu người dùng và thực hiện các bài kiểm tra hiệu suất.

    for endpoint in iter_all_endpoints():
        print(f"Đang khởi tạo schema trên {endpoint.name}...")
        run_init_sql(endpoint)
    print("Đã khởi tạo xong schema trên toàn bộ shard.")


def load_all_scenarios(source_csv: Path = DATA_FILE, keep_chunks: bool = False) -> None:
    if not source_csv.exists():#Kiểm tra xem file CSV nguồn có tồn tại hay không trước khi bắt đầu quá trình tải dữ liệu. Nếu file không tồn tại, sẽ ném ra lỗi với thông báo yêu cầu người dùng chạy lệnh generate để tạo dữ liệu trước khi tải.
        raise FileNotFoundError(
            f"Không tìm thấy tệp dataset: {source_csv}. Hãy chạy generate trước."
        )

    if CHUNK_DIR.exists():#Nếu thư mục chứa các file chunk đã tồn tại, sẽ xóa toàn bộ thư mục đó để đảm bảo rằng quá trình tải dữ liệu sẽ bắt đầu với một thư mục sạch, tránh việc sử dụng lại các file chunk cũ có thể
        shutil.rmtree(CHUNK_DIR)

    try:
        for nodes in SCENARIOS:#Lặp qua các kịch bản khác nhau dựa trên số lượng node được định nghĩa trong SCENARIOS. Mỗi kịch bản sẽ tương ứng với một cấu hình shard khác nhau, và quá trình tải dữ liệu sẽ được thực hiện riêng biệt cho từng kịch bản.
            table_name = TABLE_BY_NODES[nodes]
            print(f"Đang chuẩn bị chunk cho kịch bản {nodes} shard...")
            chunk_paths = _create_chunk_files(nodes, source_csv)

            print(f"Đang truncate {table_name} trên các node primary/replica đang dùng...")
            for shard in active_shards(nodes):#Truncate (xóa sạch) bảng dữ liệu trên các node primary và replica đang được sử dụng trong kịch bản hiện tại. Điều này đảm bảo rằng dữ liệu cũ sẽ không ảnh hưởng đến kết quả của quá trình tải dữ liệu mới.
                truncate_table(shard.primary, table_name)
                truncate_table(shard.replica, table_name)

            print(f"Đang nạp {table_name} vào các node primary và replica...")
            for shard in active_shards(nodes):
                #Nạp dữ liệu từ các file chunk vào bảng dữ liệu trên các node primary và replica đang được sử dụng trong kịch bản hiện tại. 
                #Dữ liệu sẽ được nạp từ file chunk tương ứng với shard_id của mỗi node, đảm bảo rằng dữ liệu được phân phối đúng theo cấu hình shard đã định nghĩa.
                chunk_path = chunk_paths[shard.shard_id]
                copy_csv_to_table(shard.primary, table_name, chunk_path)
                copy_csv_to_table(shard.replica, table_name, chunk_path)
                print(
                    f"Đã nạp shard{shard.shard_id} {table_name} "
                    f"vào primary và replica."
                )
    finally:
        if CHUNK_DIR.exists() and not keep_chunks:
            shutil.rmtree(CHUNK_DIR)

    print("Đã nạp dữ liệu xong cho các kịch bản: 1, 2, 4.")
