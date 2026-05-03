from raft.config import NODES
from raft.node import RaftNode
from raft.types import NodeRole


class TestRaftNodeInit:
    def setup_method(self) -> None:
        self.node = RaftNode(config=NODES[0], peers=NODES)

    def test_initial_role_is_follower(self) -> None:
        assert self.node.role == NodeRole.FOLLOWER

    def test_initial_term_is_zero(self) -> None:
        assert self.node.current_term == 0

    def test_initial_voted_for_is_none(self) -> None:
        assert self.node.voted_for is None

    def test_initial_log_is_empty(self) -> None:
        assert self.node.log == []

    def test_initial_commit_index(self) -> None:
        assert self.node.commit_index == 0

    def test_initial_last_applied(self) -> None:
        assert self.node.last_applied == 0

    def test_node_id_property(self) -> None:
        assert self.node.node_id == "node1"

    def test_peers_exclude_self(self) -> None:
        peer_ids = [p.node_id for p in self.node.peers]
        assert "node1" not in peer_ids
        assert len(self.node.peers) == 3

    def test_no_daemon_before_start(self) -> None:
        assert self.node._daemon is None
        assert self.node._daemon_thread is None

    def test_leader_state_empty_initially(self) -> None:
        assert self.node.next_index == {}
        assert self.node.match_index == {}
