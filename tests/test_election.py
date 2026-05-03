from unittest.mock import MagicMock, patch

import pytest

from raft.config import NODES
from raft.node import RaftNode
from raft.types import LogEntry, NodeRole, VoteReplyWire, VoteRequest


def make_node(index: int = 0) -> RaftNode:
    return RaftNode(config=NODES[index], peers=NODES)


# --------------------------------------------------------------------------- #
# request_vote — vote granting rules                                           #
# --------------------------------------------------------------------------- #


class TestRequestVote:
    def test_grants_vote_to_first_candidate(self) -> None:
        node = make_node()
        req = VoteRequest(
            term=1, candidate_id="node2", last_log_index=0, last_log_term=0
        )
        reply = node.request_vote(req)
        assert reply.vote_granted is True
        assert reply.term == 1

    def test_rejects_stale_term(self) -> None:
        node = make_node()
        node.current_term = 5
        req = VoteRequest(
            term=3, candidate_id="node2", last_log_index=0, last_log_term=0
        )
        reply = node.request_vote(req)
        assert reply.vote_granted is False
        assert reply.term == 5

    def test_rejects_second_candidate_same_term(self) -> None:
        node = make_node()
        req1 = VoteRequest(
            term=1, candidate_id="node2", last_log_index=0, last_log_term=0
        )
        req2 = VoteRequest(
            term=1, candidate_id="node3", last_log_index=0, last_log_term=0
        )
        node.request_vote(req1)
        reply = node.request_vote(req2)
        assert reply.vote_granted is False

    def test_grants_vote_again_to_same_candidate(self) -> None:
        node = make_node()
        req = VoteRequest(
            term=1, candidate_id="node2", last_log_index=0, last_log_term=0
        )
        node.request_vote(req)
        reply = node.request_vote(req)
        assert reply.vote_granted is True

    def test_steps_down_on_higher_term(self) -> None:
        node = make_node()
        node.current_term = 3
        node.role = NodeRole.CANDIDATE
        req = VoteRequest(
            term=5, candidate_id="node2", last_log_index=0, last_log_term=0
        )
        node.request_vote(req)
        assert node.role == NodeRole.FOLLOWER
        assert node.current_term == 5

    def test_rejects_candidate_with_stale_log_term(self) -> None:
        node = make_node()
        node.log = [LogEntry(term=3, index=1, command="x")]
        req = VoteRequest(
            term=4, candidate_id="node2", last_log_index=1, last_log_term=2
        )
        reply = node.request_vote(req)
        assert reply.vote_granted is False

    def test_rejects_candidate_with_shorter_log(self) -> None:
        node = make_node()
        node.log = [
            LogEntry(term=2, index=1, command="x"),
            LogEntry(term=2, index=2, command="y"),
        ]
        req = VoteRequest(
            term=3, candidate_id="node2", last_log_index=1, last_log_term=2
        )
        reply = node.request_vote(req)
        assert reply.vote_granted is False

    def test_grants_vote_equal_log(self) -> None:
        node = make_node()
        node.log = [LogEntry(term=2, index=1, command="x")]
        node.current_term = 2
        req = VoteRequest(
            term=3, candidate_id="node2", last_log_index=1, last_log_term=2
        )
        reply = node.request_vote(req)
        assert reply.vote_granted is True


# --------------------------------------------------------------------------- #
# _start_election — becoming leader / staying follower                         #
# --------------------------------------------------------------------------- #


class TestStartElection:
    def _mock_proxy_factory(self, granted: bool, term: int = 1) -> MagicMock:
        """Returns a context-manager mock with request_vote."""
        proxy = MagicMock()
        proxy.__enter__ = MagicMock(return_value=proxy)
        proxy.__exit__ = MagicMock(return_value=False)
        result: VoteReplyWire = {"term": term, "vote_granted": granted}
        proxy.request_vote = MagicMock(return_value=result)
        return proxy

    def test_becomes_leader_with_majority_votes(self) -> None:
        node = make_node()
        proxy = self._mock_proxy_factory(granted=True, term=1)
        with (
            patch("Pyro5.api.Proxy", return_value=proxy),
            patch.object(node, "_register_as_leader"),
            patch.object(node, "_heartbeat_loop"),
        ):
            node._start_election()
        assert node.role == NodeRole.LEADER

    def test_stays_follower_without_majority(self) -> None:
        node = make_node()
        proxy = self._mock_proxy_factory(granted=False, term=1)
        with patch("Pyro5.api.Proxy", return_value=proxy):
            node._start_election()
        assert node.role == NodeRole.FOLLOWER

    def test_term_increments_on_election(self) -> None:
        node = make_node()
        proxy = self._mock_proxy_factory(granted=False, term=1)
        with patch("Pyro5.api.Proxy", return_value=proxy):
            node._start_election()
        assert node.current_term == 1

    def test_voted_for_self_during_election(self) -> None:
        node = make_node()
        proxy = self._mock_proxy_factory(granted=False, term=1)
        with patch("Pyro5.api.Proxy", return_value=proxy):
            node._start_election()
        assert node.voted_for == "node1"

    def test_leader_initializes_next_and_match_index(self) -> None:
        node = make_node()
        proxy = self._mock_proxy_factory(granted=True, term=1)
        with (
            patch("Pyro5.api.Proxy", return_value=proxy),
            patch.object(node, "_register_as_leader"),
            patch.object(node, "_heartbeat_loop"),
        ):
            node._start_election()
        assert set(node.next_index.keys()) == {"node2", "node3", "node4"}
        # next_index = len(log)+1 = 1 for empty log; match_index = 0
        assert all(v == 1 for v in node.next_index.values())
        assert all(v == 0 for v in node.match_index.values())

    def test_steps_down_if_higher_term_in_vote_reply(self) -> None:
        node = make_node()
        node.current_term = 1
        proxy = self._mock_proxy_factory(granted=False, term=10)
        with patch("Pyro5.api.Proxy", return_value=proxy):
            node._start_election()
        assert node.role == NodeRole.FOLLOWER
        assert node.current_term == 10

    def test_leader_does_not_start_election(self) -> None:
        node = make_node()
        node.role = NodeRole.LEADER
        node.current_term = 3
        with patch("Pyro5.api.Proxy") as mock_proxy:
            node._start_election()
        mock_proxy.assert_not_called()
        assert node.role == NodeRole.LEADER
        assert node.current_term == 3

    @pytest.mark.parametrize(
        ("n_grants", "expected_role"),
        [
            (1, NodeRole.FOLLOWER),  # self+1=2 < majority(3)
            (2, NodeRole.LEADER),  # self+2=3 >= majority(3)
        ],
    )
    def test_majority_threshold(
        self, n_grants: int, expected_role: NodeRole
    ) -> None:
        """With 4 nodes, majority=3. Self+1=2 < 3, self+2=3 >= 3."""
        node = make_node()
        call_count = 0

        def proxy_factory(uri: str) -> MagicMock:
            nonlocal call_count
            proxy = MagicMock()
            proxy.__enter__ = MagicMock(return_value=proxy)
            proxy.__exit__ = MagicMock(return_value=False)
            call_count += 1
            granted = call_count <= n_grants
            proxy.request_vote = MagicMock(
                return_value={"term": 1, "vote_granted": granted}
            )
            return proxy

        with (
            patch("Pyro5.api.Proxy", side_effect=proxy_factory),
            patch.object(node, "_register_as_leader"),
            patch.object(node, "_heartbeat_loop"),
        ):
            node._start_election()

        assert node.role == expected_role
