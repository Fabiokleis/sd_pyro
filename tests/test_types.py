from raft.types import (
    AppendEntries,
    AppendReply,
    LogEntry,
    NodeRole,
    VoteReply,
    VoteRequest,
)


class TestNodeRole:
    def test_all_roles_exist(self) -> None:
        assert NodeRole.FOLLOWER.value == "follower"
        assert NodeRole.CANDIDATE.value == "candidate"
        assert NodeRole.LEADER.value == "leader"


class TestLogEntry:
    def test_creation(self) -> None:
        entry = LogEntry(term=1, index=0, command="set x 1")
        assert entry.term == 1
        assert entry.index == 0
        assert entry.command == "set x 1"

    def test_equality(self) -> None:
        a = LogEntry(term=1, index=0, command="set x 1")
        b = LogEntry(term=1, index=0, command="set x 1")
        assert a == b

    def test_inequality(self) -> None:
        a = LogEntry(term=1, index=0, command="set x 1")
        b = LogEntry(term=2, index=0, command="set x 1")
        assert a != b


class TestVoteRequest:
    def test_creation(self) -> None:
        req = VoteRequest(
            term=3,
            candidate_id="node2",
            last_log_index=5,
            last_log_term=2,
        )
        assert req.term == 3
        assert req.candidate_id == "node2"
        assert req.last_log_index == 5
        assert req.last_log_term == 2


class TestVoteReply:
    def test_granted(self) -> None:
        reply = VoteReply(term=3, vote_granted=True)
        assert reply.vote_granted is True

    def test_rejected(self) -> None:
        reply = VoteReply(term=5, vote_granted=False)
        assert reply.vote_granted is False
        assert reply.term == 5


class TestAppendEntries:
    def test_heartbeat(self) -> None:
        msg = AppendEntries(
            term=1,
            leader_id="node1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        assert msg.entries == []

    def test_with_entries(self) -> None:
        entries = [
            LogEntry(term=1, index=1, command="set a 1"),
            LogEntry(term=1, index=2, command="set b 2"),
        ]
        msg = AppendEntries(
            term=1,
            leader_id="node1",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=0,
        )
        assert len(msg.entries) == 2


class TestAppendReply:
    def test_success(self) -> None:
        reply = AppendReply(term=1, success=True, match_index=2)
        assert reply.success is True
        assert reply.match_index == 2

    def test_failure(self) -> None:
        reply = AppendReply(term=2, success=False, match_index=0)
        assert reply.success is False
