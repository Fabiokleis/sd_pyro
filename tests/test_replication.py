import logging
import threading
import time

import pytest

from raft.config import NODES
from raft.node import RaftNode
from raft.types import AppendEntries, LogEntry, NodeRole


def make_node(index: int = 0) -> RaftNode:
    return RaftNode(config=NODES[index], peers=NODES)


def ae(
    term: int,
    prev_log_index: int,
    prev_log_term: int,
    entries: list[LogEntry],
    leader_commit: int = 0,
    leader_id: str = "node2",
) -> AppendEntries:
    return AppendEntries(
        term=term,
        leader_id=leader_id,
        prev_log_index=prev_log_index,
        prev_log_term=prev_log_term,
        entries=entries,
        leader_commit=leader_commit,
    )


def entry(term: int, index: int, cmd: str = "set x 1") -> LogEntry:
    return LogEntry(term=term, index=index, command=cmd)


def make_leader(index: int = 0) -> RaftNode:
    node = make_node(index)
    node.role = NodeRole.LEADER
    node.current_term = 1
    node.next_index = {"node2": 1, "node3": 1, "node4": 1}
    node.match_index = {"node2": 0, "node3": 0, "node4": 0}
    return node


class TestAppendEntriesWithEntries:
    def test_appends_single_entry(self) -> None:
        node = make_node()
        node.current_term = 1
        reply = node.append_entries(
            ae(term=1, prev_log_index=0, prev_log_term=0, entries=[entry(1, 1)])
        )
        assert reply.success is True
        assert len(node.log) == 1
        assert node.log[0] == entry(1, 1)

    def test_appends_multiple_entries(self) -> None:
        node = make_node()
        node.current_term = 1
        reply = node.append_entries(
            ae(
                term=1,
                prev_log_index=0,
                prev_log_term=0,
                entries=[entry(1, 1, "set a 1"), entry(1, 2, "set b 2")],
            )
        )
        assert reply.success is True
        assert len(node.log) == 2

    def test_rejects_missing_prev_entry(self) -> None:
        node = make_node()
        node.current_term = 1
        reply = node.append_entries(
            ae(term=1, prev_log_index=2, prev_log_term=1, entries=[entry(1, 3)])
        )
        assert reply.success is False

    def test_rejects_wrong_prev_log_term(self) -> None:
        node = make_node()
        node.current_term = 2
        node.log = [entry(1, 1)]
        reply = node.append_entries(
            ae(term=2, prev_log_index=1, prev_log_term=2, entries=[entry(2, 2)])
        )
        assert reply.success is False
        assert len(node.log) == 1

    def test_truncates_conflicting_entries(self) -> None:
        node = make_node()
        node.current_term = 2
        node.log = [entry(1, 1), entry(1, 2)]
        reply = node.append_entries(
            ae(term=2, prev_log_index=1, prev_log_term=1, entries=[entry(2, 2)])
        )
        assert reply.success is True
        assert len(node.log) == 2
        assert node.log[1] == entry(2, 2)

    def test_does_not_truncate_matching_entries(self) -> None:
        node = make_node()
        node.current_term = 1
        node.log = [entry(1, 1)]
        node.append_entries(
            ae(term=1, prev_log_index=0, prev_log_term=0, entries=[entry(1, 1)])
        )
        assert len(node.log) == 1
        assert node.log[0] == entry(1, 1)

    def test_returns_match_index(self) -> None:
        node = make_node()
        node.current_term = 1
        reply = node.append_entries(
            ae(
                term=1,
                prev_log_index=0,
                prev_log_term=0,
                entries=[entry(1, 1), entry(1, 2)],
            )
        )
        assert reply.match_index == 2

    def test_advances_commit_index_with_leader_commit(self) -> None:
        node = make_node()
        node.current_term = 1
        node.append_entries(
            ae(
                term=1,
                prev_log_index=0,
                prev_log_term=0,
                entries=[entry(1, 1)],
                leader_commit=1,
            )
        )
        assert node.commit_index == 1

    def test_appends_at_correct_offset(self) -> None:
        node = make_node()
        node.current_term = 1
        node.log = [entry(1, 1)]
        reply = node.append_entries(
            ae(term=1, prev_log_index=1, prev_log_term=1, entries=[entry(1, 2)])
        )
        assert reply.success is True
        assert len(node.log) == 2
        assert node.log[1] == entry(1, 2)

    def test_logs_uncommitted_entries_when_received(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        node = make_node()
        node.current_term = 1
        with caplog.at_level(logging.INFO, logger=f"raft.{node.node_id}"):
            node.append_entries(
                ae(
                    term=1,
                    prev_log_index=0,
                    prev_log_term=0,
                    entries=[entry(1, 1)],
                )
            )
        assert any(
            "APPENDED     follower" in record.message
            for record in caplog.records
        )


class TestSubmitCommand:
    def _patch_replicate(self, node: RaftNode) -> None:
        node._replicate_to_all = lambda: None  # type: ignore[method-assign]

    def test_rejects_if_not_leader(self) -> None:
        node = make_node()
        assert node.submit_command("set x 1") is False

    def test_appends_entry_to_log(self) -> None:
        node = make_leader()
        self._patch_replicate(node)

        def commit_later() -> None:
            time.sleep(0.05)
            with node._commit_condition:
                node.match_index["node2"] = 1
                node.match_index["node3"] = 1
                node._commit_condition.notify_all()

        threading.Thread(target=commit_later, daemon=True).start()
        node.submit_command("set x 1")

        assert len(node.log) == 1
        assert node.log[0].command == "set x 1"
        assert node.log[0].term == 1

    def test_returns_true_when_committed(self) -> None:
        node = make_leader()
        self._patch_replicate(node)
        result: list[bool] = []

        def do_submit() -> None:
            result.append(node.submit_command("set x 1"))

        t = threading.Thread(target=do_submit)
        t.start()
        time.sleep(0.05)

        with node._commit_condition:
            node.match_index["node2"] = 1
            node.match_index["node3"] = 1
            node._commit_condition.notify_all()

        t.join(timeout=2.0)
        assert result == [True]

    def test_returns_false_if_leader_steps_down(self) -> None:
        node = make_leader()
        self._patch_replicate(node)
        result: list[bool] = []

        def do_submit() -> None:
            result.append(node.submit_command("set x 1"))

        t = threading.Thread(target=do_submit)
        t.start()
        time.sleep(0.05)

        with node._commit_condition:
            node.role = NodeRole.FOLLOWER
            node._commit_condition.notify_all()

        t.join(timeout=2.0)
        assert result == [False]

    def test_assigns_correct_term_and_index(self) -> None:
        node = make_leader()
        node.current_term = 3
        node.log = [entry(2, 1), entry(3, 2)]
        self._patch_replicate(node)

        def commit_later() -> None:
            time.sleep(0.05)
            with node._commit_condition:
                node.match_index["node2"] = 3
                node.match_index["node3"] = 3
                node._commit_condition.notify_all()

        threading.Thread(target=commit_later, daemon=True).start()
        node.submit_command("set z 9")

        assert node.log[2].term == 3
        assert node.log[2].index == 3

    def test_triggers_immediate_replication(self) -> None:
        node = make_leader()
        triggered: list[bool] = []
        node._replicate_to_all = lambda: triggered.append(True)  # type: ignore[method-assign]

        def commit_later() -> None:
            time.sleep(0.05)
            with node._commit_condition:
                node.match_index["node2"] = 1
                node.match_index["node3"] = 1
                node._commit_condition.notify_all()

        threading.Thread(target=commit_later, daemon=True).start()
        node.submit_command("set x 1")

        assert triggered == [True]
