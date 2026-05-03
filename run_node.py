import sys
import time

from raft.config import NODES
from raft.node import RaftNode


def run(node_id: str) -> None:
    cfg = next((n for n in NODES if n.node_id == node_id), None)
    if cfg is None:
        valid = ", ".join(n.node_id for n in NODES)
        print(f"Unknown node '{node_id}'. Valid: {valid}")
        sys.exit(1)

    node = RaftNode(cfg, NODES)
    try:
        node.start()
        print(f"Node {cfg.node_id} listening at {cfg.uri}")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        print(f"Node {cfg.node_id} stopped.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_node.py <node_id>")
        print(f"Nodes: {', '.join(n.node_id for n in NODES)}")
        sys.exit(1)
    run(sys.argv[1])
