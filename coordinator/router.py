from __future__ import annotations

from coordinator.config import SCENARIOS, SHARDS, LogicalShard


def validate_nodes(nodes: int) -> None:
    if nodes not in SCENARIOS:
        allowed = ", ".join(str(item) for item in SCENARIOS)
        raise ValueError(f"nodes phải là một trong các giá trị: {allowed}")

#Xác định shard_id dựa trên user_id và số lượng nodes
def shard_id_for_user(user_id: int, nodes: int) -> int:
    validate_nodes(nodes)
    if nodes == 1:
        return 1
    return (user_id % nodes) + 1


def active_shards(nodes: int) -> list[LogicalShard]:
    validate_nodes(nodes)
    return [SHARDS[shard_id] for shard_id in range(1, nodes + 1)]
