import concurrent.futures
import logging
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

        # state
        self.role: NodeRole = NodeRole.FOLLOWER
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Leader state — reinitialized on each election win
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._daemon: Pyro5.server.Daemon | None = None
        self._daemon_thread: threading.Thread | None = None

        self._lock = threading.Lock()
        self._commit_condition = threading.Condition(self._lock)
        self._reset_event = threading.Event()
        self._stop_event = threading.Event()
        self._election_thread: threading.Thread | None = None

        self._logger = logging.getLogger(f"raft.{config.node_id}")

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
        self._logger.info("started  uri=%s", self.config.uri)

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
                if self._has_quorum():
                    self._start_election()
                else:
                    self._logger.debug(
                        "skipping election: not enough peers reachable"
                    )

    def _is_peer_reachable(self, peer: NodeConfig) -> bool:
        try:
            with Pyro5.api.Proxy(peer.uri) as remote:
                remote._pyroTimeout = 0.1
                remote.ping()
                return True
        except Exception:
            return False

    def _has_quorum(self) -> bool:
        majority = (len(self.peers) + 1) // 2 + 1
        needed = majority - 1  # self counts as one vote
        reachable = sum(1 for p in self.peers if self._is_peer_reachable(p))
        return reachable >= needed

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

        self._logger.info("term=%d  CANDIDATE  starting election", term)

        peer_votes, responded = self._gather_votes(
            term, last_log_index, last_log_term
        )
        total_votes = 1 + peer_votes  # self-vote + peer votes
        majority = (len(self.peers) + 1) // 2 + 1

        with self._lock:
            if self.role == NodeRole.CANDIDATE and self.current_term == term:
                if total_votes >= majority:
                    self._logger.info(
                        "term=%d  LEADER  won election (%d/%d votes)",
                        term,
                        total_votes,
                        len(self.peers) + 1,
                    )
                    self._become_leader()
                elif responded == 0:
                    # No peers reachable — suppress per-election spam
                    self._logger.debug(
                        "term=%d  no peers reachable, retrying", term
                    )
                    self.role = NodeRole.FOLLOWER
                else:
                    self._logger.info(
                        "term=%d  FOLLOWER  lost election"
                        " (%d/%d votes, need %d)",
                        term,
                        total_votes,
                        len(self.peers) + 1,
                        majority,
                    )
                    self.role = NodeRole.FOLLOWER

    def _gather_votes(
        self, term: int, last_log_index: int, last_log_term: int
    ) -> tuple[int, int]:
        """Return ``(granted_votes, responded_peers)``."""
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    self._request_vote_from,
                    peer,
                    term,
                    last_log_index,
                    last_log_term,
                )
                for peer in self.peers
            ]
            results = [f.result() for f in futures]
        granted = sum(1 for v, _ in results if v)
        responded = sum(1 for _, r in results if r)
        return granted, responded

    def _request_vote_from(
        self,
        peer: NodeConfig,
        term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> tuple[bool, bool]:
        """Ask *peer* for a vote. Returns ``(vote_granted, peer_responded)``."""
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
                            self._commit_condition.notify_all()
                    return False, True
                granted = result["vote_granted"]
                self._logger.debug(
                    "term=%d  vote %s  from %s",
                    term,
                    "GRANTED" if granted else "DENIED",
                    peer.node_id,
                )
                return granted, True
        except Exception as exc:
            self._logger.debug(
                "term=%d  %s unreachable: %s", term, peer.node_id, exc
            )
            return False, False

    def _become_leader(self) -> None:
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
            self._logger.info(
                "registered as leader in nameserver: %s", self.config.uri
            )
        except Exception as exc:
            self._logger.warning("failed to register as leader: %s", exc)

    def _advance_commit_index(self) -> None:
        majority = (len(self.peers) + 1) // 2 + 1
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            count = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if count >= majority:
                self.commit_index = n
                self._logger.info(
                    "commit_index -> %d  (quorum %d/%d)",
                    n,
                    count,
                    len(self.peers) + 1,
                )
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
            sends: list[tuple[NodeConfig, int, int, list[LogEntry]]] = []
            for peer in self.peers:
                ni = self.next_index.get(peer.node_id, len(self.log) + 1)
                prev_idx = ni - 1
                prev_term = self.log[prev_idx - 1].term if prev_idx > 0 else 0
                sends.append(
                    (peer, prev_idx, prev_term, list(self.log[ni - 1 :]))
                )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for peer, prev_idx, prev_term, entries in sends:
                executor.submit(
                    self._send_append_entries,
                    peer,
                    term,
                    leader_commit,
                    prev_idx,
                    prev_term,
                    entries,
                )

    def _send_append_entries(
        self,
        peer: NodeConfig,
        term: int,
        leader_commit: int,
        prev_idx: int,
        prev_term: int,
        entries: list[LogEntry],
    ) -> None:
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
                result = cast(AppendReplyWire, remote.append_entries(payload))
                with self._lock:
                    if result["term"] > self.current_term:
                        self._logger.info(
                            "term=%d  FOLLOWER  higher term=%d from %s",
                            self.current_term,
                            result["term"],
                            peer.node_id,
                        )
                        self.current_term = result["term"]
                        self.role = NodeRole.FOLLOWER
                        self.voted_for = None
                        self._commit_condition.notify_all()
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

    # ------------------------------------------------------------------ #
    # RPC handlers                                                         #
    # ------------------------------------------------------------------ #

    def request_vote(self, req: VoteRequest) -> VoteReply:
        with self._lock:
            if req.term < self.current_term:
                self._logger.debug(
                    "vote DENIED  candidate=%s  stale term=%d (current=%d)",
                    req.candidate_id,
                    req.term,
                    self.current_term,
                )
                return VoteReply(term=self.current_term, vote_granted=False)

            if req.term > self.current_term:
                self.current_term = req.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                self._commit_condition.notify_all()

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
                self._logger.info(
                    "term=%d  vote GRANTED  candidate=%s",
                    req.term,
                    req.candidate_id,
                )
                return VoteReply(term=self.current_term, vote_granted=True)

            self._logger.info(
                "term=%d  vote DENIED  candidate=%s  voted_for=%s  log_ok=%s",
                req.term,
                req.candidate_id,
                self.voted_for,
                log_ok,
            )
            return VoteReply(term=self.current_term, vote_granted=False)

    def append_entries(self, req: AppendEntries) -> AppendReply:
        with self._lock:
            if req.term < self.current_term:
                return AppendReply(
                    term=self.current_term, success=False, match_index=0
                )

            if req.term > self.current_term or self.role == NodeRole.CANDIDATE:
                self._logger.info(
                    "term=%d  FOLLOWER  leader=%s", req.term, req.leader_id
                )
                self.current_term = req.term
                self.role = NodeRole.FOLLOWER
                self.voted_for = None
                self._commit_condition.notify_all()

            self._reset_election_timer()

            if req.prev_log_index > 0:
                if len(self.log) < req.prev_log_index:
                    return AppendReply(
                        term=self.current_term, success=False, match_index=0
                    )
                if self.log[req.prev_log_index - 1].term != req.prev_log_term:
                    return AppendReply(
                        term=self.current_term, success=False, match_index=0
                    )

            if req.entries:
                self._logger.info(
                    "AppendEntries  leader=%s  %d entr%s  prev_index=%d",
                    req.leader_id,
                    len(req.entries),
                    "y" if len(req.entries) == 1 else "ies",
                    req.prev_log_index,
                )

            for i, entry in enumerate(req.entries):
                log_idx = req.prev_log_index + i  # 0-based position in self.log
                if log_idx < len(self.log):
                    if self.log[log_idx].term != entry.term:
                        self.log = self.log[:log_idx]
                        self.log.extend(req.entries[i:])
                        break
                else:
                    self.log.extend(req.entries[i:])
                    break

            if req.leader_commit > self.commit_index:
                old = self.commit_index
                self.commit_index = min(req.leader_commit, len(self.log))
                if self.commit_index > old:
                    self._logger.info("commit_index -> %d", self.commit_index)

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
            self._logger.info(
                "submit_command  index=%d  %r", entry.index, command
            )
            target = entry.index
            self._commit_condition.wait_for(
                lambda: (
                    self.commit_index >= target or self.role != NodeRole.LEADER
                ),
                timeout=5.0,
            )
            ok = self.commit_index >= target
            if ok:
                self._logger.info("committed  index=%d  %r", target, command)
            else:
                self._logger.warning(
                    "timeout or lost leadership  index=%d  %r", target, command
                )
            return ok


@Pyro5.api.expose
class RaftNodeRPC:
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

    def ping(self) -> bool:
        return True
