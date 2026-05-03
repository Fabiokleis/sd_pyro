from unittest.mock import MagicMock, patch

from raft.config import NODES
from raft.node import RaftNode
from raft.types import AppendEntries, LogEntry, NodeRole


def make_node(index: int = 0) -> RaftNode:
    return RaftNode(config=NODES[index], peers=NODES)


def heartbeat(
    term: int = 1,
    leader_id: str = "node2",
    leader_commit: int = 0,
) -> AppendEntries:
    return AppendEntries(
        term=term,
        leader_id=leader_id,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=leader_commit,
    )


# --------------------------------------------------------------------------- #
# append_entries — follower heartbeat handling                                 #
# --------------------------------------------------------------------------- #


class TestAppendEntriesHeartbeat:
    def test_rejects_stale_term(self) -> None:
        node = make_node()
        node.current_term = 5
        reply = node.append_entries(heartbeat(term=3))
        assert reply.success is False
        assert reply.term == 5

    def test_accepts_valid_heartbeat(self) -> None:
        node = make_node()
        node.current_term = 1
        reply = node.append_entries(heartbeat(term=1))
        assert reply.success is True

    def test_steps_down_candidate_on_heartbeat(self) -> None:
        node = make_node()
        node.role = NodeRole.CANDIDATE
        node.current_term = 2
        reply = node.append_entries(heartbeat(term=2))
        assert reply.success is True
        assert node.role == NodeRole.FOLLOWER

    def test_updates_term_on_higher_term(self) -> None:
        node = make_node()
        node.current_term = 1
        node.append_entries(heartbeat(term=4))
        assert node.current_term == 4
        assert node.role == NodeRole.FOLLOWER

    def test_resets_election_timer(self) -> None:
        node = make_node()
        with patch.object(node, "_reset_election_timer") as mock_reset:
            node.append_entries(heartbeat(term=1))
        mock_reset.assert_called_once()

    def test_advances_commit_index(self) -> None:
        node = make_node()
        node.log = [LogEntry(term=1, index=1, command="x")]
        node.commit_index = 0
        node.current_term = 1
        node.append_entries(heartbeat(term=1, leader_commit=1))
        assert node.commit_index == 1

    def test_commit_index_capped_at_log_length(self) -> None:
        node = make_node()
        node.log = [LogEntry(term=1, index=1, command="x")]
        node.commit_index = 0
        node.current_term = 1
        node.append_entries(heartbeat(term=1, leader_commit=99))
        assert node.commit_index == 1  # min(99, len(log)=1)

    def test_does_not_decrease_commit_index(self) -> None:
        node = make_node()
        node.commit_index = 3
        node.current_term = 1
        node.append_entries(heartbeat(term=1, leader_commit=1))
        assert node.commit_index == 3

    def test_leader_steps_down_on_higher_term(self) -> None:
        node = make_node()
        node.role = NodeRole.LEADER
        node.current_term = 3
        reply = node.append_entries(heartbeat(term=5))
        assert reply.success is True
        assert node.role == NodeRole.FOLLOWER
        assert node.current_term == 5


# --------------------------------------------------------------------------- #
# _send_heartbeat — leader broadcasting                                        #
# --------------------------------------------------------------------------- #


class TestSendHeartbeat:
    def _mock_proxy(self, term: int = 1, success: bool = True) -> MagicMock:
        proxy = MagicMock()
        proxy.__enter__ = MagicMock(return_value=proxy)
        proxy.__exit__ = MagicMock(return_value=False)
        proxy.append_entries = MagicMock(
            return_value={"term": term, "success": success, "match_index": 0}
        )
        return proxy

    def test_sends_to_all_peers(self) -> None:
        node = make_node()
        node.role = NodeRole.LEADER
        node.current_term = 2
        proxies = [self._mock_proxy() for _ in node.peers]
        with patch("Pyro5.api.Proxy", side_effect=proxies):
            node._send_heartbeat()
        assert all(p.append_entries.called for p in proxies)

    def test_skips_if_not_leader(self) -> None:
        node = make_node()
        node.role = NodeRole.FOLLOWER
        with patch("Pyro5.api.Proxy") as mock_proxy:
            node._send_heartbeat()
        mock_proxy.assert_not_called()

    def test_steps_down_on_higher_term_in_reply(self) -> None:
        node = make_node()
        node.role = NodeRole.LEADER
        node.current_term = 2
        proxy = self._mock_proxy(term=10, success=False)
        with patch("Pyro5.api.Proxy", return_value=proxy):
            node._send_heartbeat()
        assert node.role == NodeRole.FOLLOWER
        assert node.current_term == 10


# --------------------------------------------------------------------------- #
# _heartbeat_loop — control flow                                               #
# --------------------------------------------------------------------------- #


class TestHeartbeatLoop:
    def test_exits_immediately_when_not_leader(self) -> None:
        node = make_node()
        node.role = NodeRole.FOLLOWER
        with patch.object(node, "_send_heartbeat") as mock_send:
            node._heartbeat_loop()
        mock_send.assert_not_called()

    def test_exits_when_role_changes_to_follower(self) -> None:
        node = make_node()
        node.role = NodeRole.LEADER
        send_count = 0

        def fake_send() -> None:
            nonlocal send_count
            send_count += 1
            node.role = NodeRole.FOLLOWER  # step down after first heartbeat

        with (
            patch.object(node, "_send_heartbeat", side_effect=fake_send),
            patch.object(node, "_stop_event") as mock_event,
        ):
            mock_event.is_set.return_value = False
            mock_event.wait.return_value = False
            node._heartbeat_loop()

        assert send_count == 1

    def test_stops_on_stop_event(self) -> None:
        node = make_node()
        node.role = NodeRole.LEADER

        call_count = 0

        def fake_is_set() -> bool:
            nonlocal call_count
            call_count += 1
            return call_count > 1  # first call False, then True

        with (
            patch.object(node, "_send_heartbeat"),
            patch.object(node._stop_event, "is_set", side_effect=fake_is_set),
            patch.object(node._stop_event, "wait"),
        ):
            node._heartbeat_loop()

        assert call_count >= 2
