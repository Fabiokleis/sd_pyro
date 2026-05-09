from dataclasses import dataclass


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    host: str
    port: int

    @property
    def uri(self) -> str:
        """URI used by other Raft nodes (request_vote, append_entries)."""
        return f"PYRO:{self.node_id}@{self.host}:{self.port}"

    @property
    def client_object_id(self) -> str:
        return f"{self.node_id}-client"

    @property
    def client_uri(self) -> str:
        """URI exposed to clients (submit_command only)."""
        return f"PYRO:{self.client_object_id}@{self.host}:{self.port}"


NODES: list[NodeConfig] = [
    NodeConfig(node_id="node1", host="localhost", port=9001),
    NodeConfig(node_id="node2", host="localhost", port=9002),
    NodeConfig(node_id="node3", host="localhost", port=9003),
    NodeConfig(node_id="node4", host="localhost", port=9004),
]

NAMESERVER_HOST = "localhost"
NAMESERVER_PORT = 9090

LEADER_NAME = "raft-leader"
