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

_ELECTION_TIMEOUT_MIN = 0.150
_ELECTION_TIMEOUT_MAX = 0.300
_HEARTBEAT_INTERVAL = 0.100


class RaftNode:
    def __init__(self, config: NodeConfig, peers: list[NodeConfig]) -> None:
        self.config = config
        self.peers = [p for p in peers if p.node_id != config.node_id]

        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        self.role: NodeRole = NodeRole.FOLLOWER
        self.commit_index: int = 0

        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._catchup_inflight: set[str] = set()

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
        self._daemon = Pyro5.api.Daemon(
            host=self.config.host, port=self.config.port
        )
        self._daemon.register(RaftNodeRPC(self), objectId=self.config.node_id)
        self._daemon.register(
            RaftLeaderCommandRPC(self),
            objectId=self.config.client_object_id,
        )
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
        self._reset_event.set()
        if self._daemon is not None:
            self._daemon.shutdown()

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
        needed = self._majority_threshold() - 1
        reachable = sum(1 for p in self.peers if self._is_peer_reachable(p))
        return reachable >= needed

    def _cluster_size(self) -> int:
        return len(self.peers) + 1

    def _majority_threshold(self) -> int:
        return self._cluster_size() // 2 + 1

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
        total_votes = 1 + peer_votes
        majority = self._majority_threshold()

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
                            self._become_follower(result["term"])
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
        self._catchup_inflight = set()
        threading.Thread(target=self._register_as_leader, daemon=True).start()
        threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self.node_id}",
        ).start()

    def _become_follower(self, new_term: int) -> None:
        self._logger.info(
            "term=%d  FOLLOWER  higher term=%d seen",
            self.current_term,
            new_term,
        )
        self.current_term = new_term
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self._commit_condition.notify_all()

    def _register_as_leader(self) -> None:
        try:
            ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            ns.register(LEADER_NAME, self.config.client_uri, safe=False)
            self._logger.info(
                "registered as leader in nameserver: %s",
                self.config.client_uri,
            )
        except Exception as exc:
            self._logger.warning("failed to register as leader: %s", exc)

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
            sends: list[tuple[NodeConfig, int, int]] = []
            for peer in self.peers:
                ni = self.next_index[peer.node_id]
                prev_idx = ni - 1
                prev_term = self.log[prev_idx - 1].term if prev_idx > 0 else 0
                sends.append((peer, prev_idx, prev_term))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for peer, prev_idx, prev_term in sends:
                executor.submit(
                    self._send_empty_to,
                    peer,
                    term,
                    leader_commit,
                    prev_idx,
                    prev_term,
                )

    def _send_empty_to(
        self,
        peer: NodeConfig,
        term: int,
        leader_commit: int,
        prev_idx: int,
        prev_term: int,
    ) -> None:
        try:
            with Pyro5.api.Proxy(peer.uri) as remote:
                payload: AppendEntriesWire = {
                    "term": term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": [],
                    "leader_commit": leader_commit,
                }
                result = cast(AppendReplyWire, remote.append_entries(payload))
        except Exception:
            return

        with self._lock:
            if result["term"] > self.current_term:
                self._become_follower(result["term"])
                return
            if self.role != NodeRole.LEADER or self.current_term != term:
                return
            if not result["success"] or result["match_index"] < len(self.log):
                next_idx = max(1, result["match_index"] + 1)
                if next_idx < self.next_index[peer.node_id]:
                    self.next_index[peer.node_id] = next_idx
                    self._logger.info(
                        "CATCH-UP NEXT  peer=%s  next_index=%d  match_index=%d",
                        peer.node_id,
                        next_idx,
                        result["match_index"],
                    )
                if peer.node_id in self._catchup_inflight:
                    return
                self._catchup_inflight.add(peer.node_id)
                self._logger.info(
                    "CATCH-UP    peer=%s  success=%s  "
                    "match_index=%d  log_len=%d",
                    peer.node_id,
                    result["success"],
                    result["match_index"],
                    len(self.log),
                )
                threading.Thread(
                    target=self._send_entries_to,
                    args=(peer, term, True),
                    daemon=True,
                ).start()

    def _send_entries_to(
        self, peer: NodeConfig, term: int, catchup: bool = False
    ) -> None:
        while True:
            with self._lock:
                if self.role != NodeRole.LEADER or self.current_term != term:
                    self._catchup_inflight.discard(peer.node_id)
                    return
                ni = self.next_index[peer.node_id]
                prev_idx = ni - 1
                prev_term = self.log[prev_idx - 1].term if prev_idx > 0 else 0
                entries = list(self.log[ni - 1 :])
                leader_commit = self.commit_index

            if not entries:
                with self._lock:
                    self._catchup_inflight.discard(peer.node_id)
                return

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
            except Exception:
                with self._lock:
                    self._catchup_inflight.discard(peer.node_id)
                return

            with self._lock:
                if result["term"] > self.current_term:
                    self._become_follower(result["term"])
                    self._catchup_inflight.discard(peer.node_id)
                    return
                if self.role != NodeRole.LEADER or self.current_term != term:
                    self._catchup_inflight.discard(peer.node_id)
                    return
                if result["success"]:
                    new_match = result["match_index"]
                    self.match_index[peer.node_id] = max(
                        self.match_index[peer.node_id], new_match
                    )
                    self.next_index[peer.node_id] = new_match + 1
                    if catchup:
                        self._logger.info(
                            "CATCH-UP DONE  peer=%s  next_index=%d  "
                            "match_index=%d",
                            peer.node_id,
                            self.next_index[peer.node_id],
                            new_match,
                        )
                    self._commit_condition.notify_all()
                    self._catchup_inflight.discard(peer.node_id)
                    return
                self.next_index[peer.node_id] = max(
                    1, self.next_index[peer.node_id] - 1
                )
                if catchup:
                    self._logger.info(
                        "CATCH-UP RETRY  peer=%s  next_index=%d",
                        peer.node_id,
                        self.next_index[peer.node_id],
                    )

    def _replicate_to_all(self) -> None:
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
            term = self.current_term
            peers_snapshot = list(self.peers)

        for peer in peers_snapshot:
            threading.Thread(
                target=self._send_entries_to,
                args=(peer, term),
                daemon=True,
            ).start()

    def _reconcile_log(
        self, prev_log_index: int, entries: list[LogEntry]
    ) -> list[LogEntry]:
        received: list[LogEntry] = []
        for i, entry in enumerate(entries):
            pos = prev_log_index + i
            if pos >= len(self.log):
                received = list(entries[i:])
                self.log.extend(received)
                return received
            if self.log[pos].term != entry.term:
                received = list(entries[i:])
                self.log[pos:] = received
                return received
        return received

    def _ack_count(self, target: int) -> int:
        return 1 + sum(1 for m in self.match_index.values() if m >= target)

    def _majority_acked(self, target: int) -> bool:
        if self.role != NodeRole.LEADER:
            return True
        return self._ack_count(target) >= self._majority_threshold()

    def _commit_if_majority(self, target: int, command: str) -> bool:
        acks = self._ack_count(target)
        majority = self._majority_threshold()
        if acks < majority:
            self._logger.warning("timeout  index=%d  %r", target, command)
            return False
        self.commit_index = max(self.commit_index, target)
        self._logger.info("COMMITTED    index=%d  %r", target, command)
        return True

    def submit_command(self, command: str) -> bool:
        with self._lock:
            if self.role != NodeRole.LEADER:
                return False
            entry = LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                command=command,
            )
            self.log.append(entry)
            target = entry.index
        self._logger.info(
            "UNCOMMITTED  index=%d  term=%d  %r",
            entry.index,
            entry.term,
            command,
        )
        self._replicate_to_all()

        with self._commit_condition:
            self._commit_condition.wait_for(
                lambda: self._majority_acked(target), timeout=5.0
            )
            if self.role != NodeRole.LEADER:
                self._logger.warning(
                    "lost leadership  index=%d  %r", target, command
                )
                return False
            return self._commit_if_majority(target, command)

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
                self._become_follower(req.term)

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

            if req.term > self.current_term:
                self._become_follower(req.term)
            elif self.role == NodeRole.CANDIDATE:
                self.role = NodeRole.FOLLOWER

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
                received = self._reconcile_log(req.prev_log_index, req.entries)
                for entry in received:
                    self._logger.info(
                        "APPENDED     follower index=%d  term=%d  %r",
                        entry.index,
                        entry.term,
                        entry.command,
                    )
            else:
                self._logger.debug(
                    "HEARTBEAT    leader=%s  prev_index=%d  leader_commit=%d",
                    req.leader_id,
                    req.prev_log_index,
                    req.leader_commit,
                )

            if req.leader_commit > self.commit_index:
                old = self.commit_index
                self.commit_index = min(req.leader_commit, len(self.log))
                if self.commit_index > old:
                    self._logger.info("commit_index -> %d", self.commit_index)

            return AppendReply(
                term=self.current_term, success=True, match_index=len(self.log)
            )


@Pyro5.api.expose
class RaftLeaderCommandRPC:
    def __init__(self, node: RaftNode) -> None:
        self._node = node

    def submit_command(self, command: str) -> bool:
        return self._node.submit_command(command)


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

    def ping(self) -> bool:
        return True
