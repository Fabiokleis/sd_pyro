import concurrent.futures
import random
import threading
from typing import cast

import Pyro5.api
import Pyro5.server

from raft.config import (
    LEADER_NAME,
    NAMESERVER_HOST,
    NAMESERVER_PORT,
    NodeConfig,
)
from raft.types import (
    AppendEntries,
    AppendEntriesWire,
    AppendReply,
    AppendReplyWire,
    LogEntry,
    LogEntryWire,
    NodeRole,
    VoteReply,
    VoteReplyWire,
    VoteRequest,
    VoteRequestWire,
)

_ELECTION_TIMEOUT_MIN = 0.150  # seconds (150 ms)
_ELECTION_TIMEOUT_MAX = 0.300  # seconds (300 ms)
_HEARTBEAT_INTERVAL = 0.050  # seconds (50 ms)


class RaftNode:
    """Core Raft state and logic — no Pyro5 dependency."""

    def __init__(self, config: NodeConfig, peers: list[NodeConfig]) -> None:
        self.config = config
        self.peers = [p for p in peers if p.node_id != config.node_id]

        # Persistent state (would survive restarts)
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.role: NodeRole = NodeRole.FOLLOWER
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Leader volatile state — reinitialized on each election win
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._daemon: Pyro5.server.Daemon | None = None
        self._daemon_thread: threading.Thread | None = None

        self._lock = threading.Lock()
        self._commit_condition = threading.Condition(self._lock)
        self._reset_event = threading.Event()
        self._stop_event = threading.Event()
        self._election_thread: threading.Thread | None = None

    @property
    def node_id(self) -> str:
        return self.config.node_id

    def start(self) -> None:
        self._stop_event.clear()
        rpc = RaftNodeRPC(self)
        self._daemon = Pyro5.api.Daemon(
            host=self.config.host, port=self.config.port
        )
        self._daemon.register(rpc, objectId=self.config.node_id)
        self._daemon_thread = threading.Thread(
            target=self._daemon.requestLoop,
            daemon=True,
            name=f"daemon-{self.node_id}",
        )
        self._daemon_thread.start()

        self._election_thread = threading.Thread(
            target=self._election_timer_loop,
            daemon=True,
            name=f"election-{self.node_id}",
        )
        self._election_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._reset_event.set()  # wake up waiting timer
        if self._daemon is not None:
            self._daemon.shutdown()

    # ------------------------------------------------------------------ #
    # Election timer                                                       #
    # ------------------------------------------------------------------ #

    def _reset_election_timer(self) -> None:
        self._reset_event.set()

    def _election_timer_loop(self) -> None:
        while not self._stop_event.is_set():
            timeout = random.uniform(
                _ELECTION_TIMEOUT_MIN, _ELECTION_TIMEOUT_MAX
            )
            got_reset = self._reset_event.wait(timeout=timeout)
            self._reset_event.clear()
            if not got_reset and not self._stop_event.is_set():
                self._start_election()

    # ------------------------------------------------------------------ #
    # Election                                                             #
    # ------------------------------------------------------------------ #

    def _start_election(self) -> None:
        with self._lock:
            if self.role == NodeRole.LEADER:
                return
            self.role = NodeRole.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            term = self.current_term
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0

        peer_votes = self._gather_votes(term, last_log_index, last_log_term)
        total_votes = 1 + peer_votes  # self-vote + peer votes
        majority = (len(self.peers) + 1) // 2 + 1

        with self._lock:
            if self.role == NodeRole.CANDIDATE and self.current_term == term:
                if total_votes >= majority:
                    self._become_leader()
                else:
                    self.role = NodeRole.FOLLOWER

    def _gather_votes(
        self, term: int, last_log_index: int, last_log_term: int
    ) -> int:
        def request_from(peer: NodeConfig) -> bool:
            try:
                with Pyro5.api.Proxy(peer.uri) as remote:
                    request: VoteRequestWire = {
                        "term": term,
                        "candidate_id": self.node_id,
                        "last_log_index": last_log_index,
                        "last_log_term": last_log_term,
                    }
                    result = cast(VoteReplyWire, remote.request_vote(request))
                    if result["term"] > term:
                        with self._lock:
                            if result["term"] > self.current_term:
                                self.current_term = result["term"]
                                self.role = NodeRole.FOLLOWER
                                self.voted_for = None
                        return False
                    return result["vote_granted"]
            except Exception:
                return False

        with concurrent.futures.ThreadPoolExecutor() as executor:
            return sum(executor.map(request_from, self.peers))

    def _become_leader(self) -> None:
        """Must be called while holding self._lock."""
        self.role = NodeRole.LEADER
        next_idx = len(self.log) + 1
        self.next_index = {p.node_id: next_idx for p in self.peers}
        self.match_index = {p.node_id: 0 for p in self.peers}
        threading.Thread(target=self._register_as_leader, daemon=True).start()
        threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self.node_id}",
        ).start()

    def _register_as_leader(self) -> None:
        try:
            ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            ns.register(
                LEADER_NAME, self.config.uri, safe=False
            )  # overwrite on re-election
        except Exception:
            pass

    def _advance_commit_index(self) -> None:
        """Commit entries replicated by a majority.

        Must be called while holding self._lock.
        """
        majority = (len(self.peers) + 1) // 2 + 1
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            count = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if count >= majority:
                self.commit_index = n
                self._commit_condition.notify_all()
                break

    # ------------------------------------------------------------------ #
    # Heartbeat                                                            #
    # ------------------------------------------------------------------ #

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                if self.role != NodeRole.LEADER:
                    return
            self._send_heartbeat()
            self._stop_event.wait(timeout=_HEARTBEAT_INTERVAL)

    def _send_heartbeat(self) -> None:
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
            term = self.current_term
            leader_commit = self.commit_index
            # Snapshot per-peer send parameters while holding the lock
            peer_params: dict[str, tuple[int, int, list[LogEntry]]] = {}
            for peer in self.peers:
                ni = self.next_index.get(peer.node_id, len(self.log) + 1)
                prev_idx = ni - 1
                prev_term = self.log[prev_idx - 1].term if prev_idx > 0 else 0
                peer_params[peer.node_id] = (
                    prev_idx,
                    prev_term,
                    list(self.log[ni - 1 :]),
                )

        def send_to(peer: NodeConfig) -> None:
            prev_idx, prev_term, entries = peer_params[peer.node_id]
            try:
                with Pyro5.api.Proxy(peer.uri) as remote:
                    payload: AppendEntriesWire = {
                        "term": term,
                        "leader_id": self.node_id,
                        "prev_log_index": prev_idx,
                        "prev_log_term": prev_term,
                        "entries": [
                            LogEntryWire(
                                term=e.term, index=e.index, command=e.command
                            )
                            for e in entries
                        ],
                        "leader_commit": leader_commit,
                    }
                    result = cast(
                        AppendReplyWire, remote.append_entries(payload)
                    )
                    with self._lock:
                        if result["term"] > self.current_term:
                            self.current_term = result["term"]
                            self.role = NodeRole.FOLLOWER
                            self.voted_for = None
                            return
                        if (
                            self.role != NodeRole.LEADER
                            or self.current_term != term
                        ):
                            return  # stale response
                        if result["success"]:
                            new_match = result["match_index"]
                            self.match_index[peer.node_id] = max(
                                self.match_index.get(peer.node_id, 0), new_match
                            )
                            self.next_index[peer.node_id] = new_match + 1
                            self._advance_commit_index()
                        else:
                            self.next_index[peer.node_id] = max(
                                1, self.next_index.get(peer.node_id, 1) - 1
                            )
            except Exception:
                pass

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for peer in self.peers:
                executor.submit(send_to, peer)

    # ------------------------------------------------------------------ #
    # RPC handlers                                                         #
    # ------------------------------------------------------------------ #

    def request_vote(self, req: VoteRequest) -> VoteReply:
        with self._lock:
            if req.term < self.current_term:
                return VoteReply(term=self.current_term, vote_granted=False)

            if req.term > self.current_term:
                self.current_term = req.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None

            my_last_log_index = len(self.log)
            my_last_log_term = self.log[-1].term if self.log else 0
            log_ok = req.last_log_term > my_last_log_term or (
                req.last_log_term == my_last_log_term
                and req.last_log_index >= my_last_log_index
            )
            can_vote = (
                self.voted_for is None or self.voted_for == req.candidate_id
            ) and log_ok

            if can_vote:
                self.voted_for = req.candidate_id
                self._reset_election_timer()
                return VoteReply(term=self.current_term, vote_granted=True)

            return VoteReply(term=self.current_term, vote_granted=False)

    def append_entries(self, req: AppendEntries) -> AppendReply:
        with self._lock:
            if req.term < self.current_term:
                return AppendReply(
                    term=self.current_term, success=False, match_index=0
                )

            if req.term > self.current_term or self.role == NodeRole.CANDIDATE:
                self.current_term = req.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None

            self._reset_election_timer()

            # Log consistency check
            if req.prev_log_index > 0:
                if len(self.log) < req.prev_log_index:
                    return AppendReply(
                        term=self.current_term, success=False, match_index=0
                    )
                if self.log[req.prev_log_index - 1].term != req.prev_log_term:
                    return AppendReply(
                        term=self.current_term, success=False, match_index=0
                    )

            for i, entry in enumerate(req.entries):
                log_idx = req.prev_log_index + i  # 0-based position in self.log
                if log_idx < len(self.log):
                    if self.log[log_idx].term != entry.term:
                        # Conflict: truncate and append remaining
                        self.log = self.log[:log_idx]
                        self.log.extend(req.entries[i:])
                        break
                    # Matching entry already present — skip
                else:
                    self.log.extend(req.entries[i:])
                    break

            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log))

            return AppendReply(
                term=self.current_term, success=True, match_index=len(self.log)
            )

    def submit_command(self, command: str) -> bool:
        with self._commit_condition:
            if self.role != NodeRole.LEADER:
                return False
            entry = LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                command=command,
            )
            self.log.append(entry)
            target = entry.index
            self._commit_condition.wait_for(
                lambda: (
                    self.commit_index >= target or self.role != NodeRole.LEADER
                ),
                timeout=5.0,
            )
            return self.commit_index >= target


@Pyro5.api.expose
class RaftNodeRPC:
    """Thin Pyro5 RPC adapter — only these methods are remotely accessible."""

    def __init__(self, node: RaftNode) -> None:
        self._node = node

    def request_vote(self, data: VoteRequestWire) -> VoteReplyWire:
        req = VoteRequest(
            term=data["term"],
            candidate_id=data["candidate_id"],
            last_log_index=data["last_log_index"],
            last_log_term=data["last_log_term"],
        )
        reply = self._node.request_vote(req)
        return VoteReplyWire(term=reply.term, vote_granted=reply.vote_granted)

    def append_entries(self, data: AppendEntriesWire) -> AppendReplyWire:
        entries = [
            LogEntry(term=e["term"], index=e["index"], command=e["command"])
            for e in data["entries"]
        ]
        req = AppendEntries(
            term=data["term"],
            leader_id=data["leader_id"],
            prev_log_index=data["prev_log_index"],
            prev_log_term=data["prev_log_term"],
            entries=entries,
            leader_commit=data["leader_commit"],
        )
        reply = self._node.append_entries(req)
        return AppendReplyWire(
            term=reply.term,
            success=reply.success,
            match_index=reply.match_index,
        )

    def submit_command(self, command: str) -> bool:
        return self._node.submit_command(command)
