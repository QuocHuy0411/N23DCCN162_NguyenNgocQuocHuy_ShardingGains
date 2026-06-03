from __future__ import annotations

from coordinator.config import SCENARIOS, SHARDS, LogicalShard


def validate_nodes(nodes: int) -> None: #Hàm validate_nodes để kiểm tra xem số lượng nodes được chỉ định có hợp lệ hay không. Nếu số lượng nodes không nằm trong danh sách các kịch bản được định nghĩa trong SCENARIOS, hàm sẽ ném ra lỗi với thông báo yêu cầu người dùng chỉ định một giá trị hợp lệ.
    if nodes not in SCENARIOS:
        allowed = ", ".join(str(item) for item in SCENARIOS)
        raise ValueError(f"nodes phải là một trong các giá trị: {allowed}")

#Xác định shard_id dựa trên user_id và số lượng nodes
def shard_id_for_user(user_id: int, nodes: int) -> int:
    validate_nodes(nodes)
    if nodes == 1:
        return 1
    return (user_id % nodes) + 1 
    #Hàm shard_id_for_user để xác định shard_id cho một user_id cụ thể dựa trên số lượng nodes. 
    #Nếu chỉ có một node, tất cả user_id sẽ được gán vào shard_id 1. 
    #Nếu có nhiều hơn một node, shard_id sẽ được tính bằng cách lấy phần dư của user_id chia cho số lượng nodes và sau đó cộng thêm 1 để đảm bảo rằng shard_id bắt đầu từ 1 thay vì 0.


def active_shards(nodes: int) -> list[LogicalShard]:#Hàm active_shards để lấy danh sách các shard đang hoạt động dựa trên số lượng nodes được chỉ định. Hàm sẽ kiểm tra tính hợp lệ của số lượng nodes và trả về một danh sách các shard tương ứng với số lượng nodes đó, được lấy từ cấu hình SHARDS.
    validate_nodes(nodes)
    return [SHARDS[shard_id] for shard_id in range(1, nodes + 1)]
